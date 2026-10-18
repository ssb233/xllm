# xLLM 端到端请求 trace：插桩落点 / 上下文传播 / ID 清单（task-1）

> **范围与证据等级**：基线 commit `4b05c484`，本地只读源码核验（未构建、未运行、未起服务）。
> `S` = 本笔记直接读源码确认（附 file:line）；`S(base)` = 沿用 `brpc_metrics_baseline.md` 且本轮未逐行复验；`H` = 假设/推断，需后续验证。
> **本文档不做任何 R（runtime）结论**——所有"延迟在哪/多大"的判断均属 H，需 §9 最小实验后再定。
>
> **基线**：`docs/research/observability/brpc_metrics_baseline.md` §5 阶段表、§8 事件模型（t0–t10 / c0–c2）。
> 本文档定位是「§5 的落点细化 + 锚点纠偏 + 跨线程/进程传播边界 + ID 生命周期 + 插桩风险」，不重复 bvar/brpc 语义。
> 全部路径相对仓库根 `<repo-root>`。

---

## 0. 基线锚点核验与漂移修正（先读这节，避免引用旧行号）

| 基线引用 | 实测（当前 checkout） | 结论 |
|---|---|---|
| `api_service.cpp:568` HTTP handler | `APIService::ChatCompletionsHttp` :568 | ✓ 精确 |
| `api_service.cpp:444` chat http impl | `chat_completions_http_impl` :444 | ✓ |
| `chat_service_impl.cpp:731` | `ChatServiceImpl::process_async_impl` :731 | ✓ |
| `llm_master.cpp:484 / :497` | `handle_request(messages)` :484；`AUTO_COUNTER(request_handling_latency_seconds_chat)` :497 | ✓（prompt 版 :438 / Counter :451） |
| `llm_request_factory.cpp:496` Request ctor | `std::make_shared<Request>(...)` :497-503 | 轻微漂移，应引 :497 |
| `llm_request_factory.cpp:519 / :182` | chat template `Timer` :519 / tokenize `Timer` :182 | ✓ |
| `request_base.cpp:28` created_time | `created_time_(absl::Now())` :28 | ✓ |
| `sequence.cpp:117` | Sequence ctor `latest_generate_time_(absl::Now())` :117（另有 :91） | ✓ |
| `continuous_scheduler.cpp:231 / :376 / :377-445` | `add_request` :231；`prepare_batch` :376，`Timer` :377 | ✓（内部：drain :382、schedule :429、Counter :445） |
| `llm_engine.cpp:511-581` | `LLMEngine::step` :511…`engine_latency_seconds` :581 | ✓（collectAll :545、sample update :572） |
| `remote_worker.cpp:169` | `step_remote_async(LlmForwardInput)` :169 | ✓ |
| `worker_service.cpp:609 / :1048` | 实际 `ExecuteModel` :988、`worker_service_latency_seconds` :1042、`GetLastStepResult` :1052、SHM polling thread :564 | **漂移，必须改** |
| `llm_worker_impl.cpp:233-421` | `step_internal` :233、`execution_latency_seconds_model` :421 | ✓（NO_SYNC 早退 :408-418 会绕过该 Counter 与 device sync） |
| `continuous_scheduler.cpp:626` | `process_batch_output` :626；metrics update 实际 :637；`process_stream_requests` :665 | ✓（内部行号微调） |
| `request_output_dispatcher.cpp:143` | `AUTO_COUNTER(responsing_latency_seconds_non_stream)` :144；stream 侧 :245 | 轻微漂移 |
| `sequence.cpp:518 / :677` | detok `AUTO_COUNTER(detokenization_latency_seconds_stream/non_stream)` | ✓ |
| `stream_call.h:142 / :273` | `write()` :142；`start_stream()` :273 | ✓（首个 `pa_->Write` 在 :167/:180；`done_->Run()` :287） |
| 基线 §8.2 事件 t0–t10/c0–c2 | 本笔记直接复用其编号 | ✓ 无需重定义 |

**新增/纠正的事实**（基线未覆盖）：`x_request_id` 在 `RequestOutput` 边界丢失（§C）；跨进程 packed payload 无 trace 字段（§B-4/5）；`step_id` 在 LLM 路径不存在（§C）；worker 可用的请求键是 `sample_sequence_ids`（§B-6）；已存在 `XLLM_VERBOSE_TRACE` 异步事件通道（§B-15），这是最小改动的落点。

---

## A. 端到端阶段表（span 落点）

对象缩写：`Call` = brpc 连接/请求载体（`call.h:39-67`）；`ReqParams` = `RequestParams`；`Req` = `Request`；`Seq` = `Sequence`；`Out` = `RequestOutput`；`BatchGroup`/`Batch`；`LlmFwdIn` = `LlmForwardInput`；`RawOut` = `RawForwardOutput`。

| # | 阶段 | 入口函数签名 | file:line | 上下文对象 | 建议 span begin → end | 可复用的现有时序 | 阶段后线程/队列边界 |
|---|---|---|---|---|---|---|---|
| A1 | HTTP 入口/路由 | `void APIService::ChatCompletionsHttp(::google::protobuf::RpcController*, const proto::HttpRequest*, proto::HttpResponse*, ::google::protobuf::Closure*)` | `api_service.cpp:568` | `brpc::Controller`、`done`、`ClosureGuard` | `:568` → `:595`（handler 调用返回，仅覆盖同步解析段） | `request_in_metric`/`request_out_metric`（`rpc_request_metrics.h:23-42`），但 `release()` 提前触发，见 §E-5 | 同 brpc bthread 内继续；`done` 生命周期移至 Call |
| A2 | JSON 解析 / OpenAI 规范化 / proto 解码 | `template <typename ChatCall, typename Service> void chat_completions_http_impl(std::unique_ptr<Service>&, xllm::ClosureGuard&, brpc::Controller*, const proto::HttpRequest*, proto::HttpResponse*, const ChatJsonParser&, const std::string&, const std::string&)` | `api_service.cpp:444`；`normalize_openai_request` :468；`decode_chat_request` :493 | `proto::ChatRequest`（`req_pb`）、`call` | `:459` → `:500`（Call 构造前） | 无专用 Counter（H：仅字符串处理） | 之后仍是 bthread 同步调用，无队列 |
| A3 | Call/身份初始化 | `Call::Call(brpc::Controller*, std::string body_x_request_id, bool is_http_request)`；`void Call::init(std::string, bool)` | `call.cpp:26`；`init` :33 | `Call`（x_request_id/x_request_time） | `:33` → `:52`（`init_request_payload` 前） | 已有 `event=request_received`（`call.cpp:47`，verbose trace） | 回写 `x-request-id` 响应头 :43-45 |
| A4 | 业务预处理（限流/消息/参数） | `void ChatServiceImpl::process_async_impl(std::shared_ptr<ChatCall>)` | `chat_service_impl.cpp:731` | `ChatCall`→`Call`、`ReqParams` | `:731` → `:828`（`handle_request` 调用前） | `get_rate_limiter()->acquire()` :754（无 timing） | 同步进入 LLMMaster，随后入线程池 |
| A5 | 请求线程池入队 | `void LLMMaster::handle_request(std::vector<Message>, std::optional<std::vector<int>>, RequestParams, std::optional<Call*>, OutputCallback)` | `llm_master.cpp:484`；`incr_pending_requests` :489；`threadpool_->schedule` :491 | `ReqParams`+`call*`+`callback`（lambda 值捕获） | **t2**：`:489`→`:491`；**t3**：lambda 体首行 :493 | 并入 `request_handling_latency_seconds_chat` :497（Counter 含排队时间，S） | ⛔ **std::thread 池** `LLMMaster.request`（:327，`num_request_handling_threads` 默认 4，`service_config.cpp:49`） |
| A6 | chat template | `std::shared_ptr<Request> LLMRequestFactory::create(const std::vector<Message>&, std::optional<std::vector<int>>, const RequestParams&, std::optional<Call*>, OutputCallback, std::optional<ChatTemplateGenerationMode>)` | `llm_request_factory.cpp:506`；`Timer` :519；`apply_with_generation_mode` :521-523 | `ReqParams`、`messages` | `:519` → `:533` | ✅ `chat_template_latency_seconds`（:533，明文只包 render） | 同线程继续 tokenize |
| A7 | tokenize | `std::string LLMRequestFactory::encode_and_validate_prompt(...)`（实际返回 token 容器） | `llm_request_factory.cpp:165`；`Timer` :182；`tokenizer_->encode` :187-188 | `ReqParams`、`Tokenzier` | `:182` → `:197` | ✅ `tokenization_latency_seconds`（:197） | 同线程 |
| A8 | Request/Sequence 构造 | `std::shared_ptr<Request> LLMRequestFactory::create(std::string prompt, ...)`；`std::make_shared<Request>` :497-503；`Request::create_sequences_group()` `request.cpp:51/:54`；`Sequence::Sequence(...)` `sequence.cpp:117` | 见左 | `Req`、`SequencesGroup`、`Seq` | **t4**：`:497` → `:503`（含 `create_sequences_group` 与 `Sequence` ctor） | `RequestBase::created_time_` `request_base.cpp:28`（**内部 E2E 起点**）；`Seq::latest_generate_time_` `sequence.cpp:117`（**内部 TTFT 起点**） | 同线程；`Request` 交给调度器 |
| A9 | 调度准入队列 | `bool ContinuousSchedulerBase::add_request(std::shared_ptr<Request>&)`；`bool enqueue_ready_request(std::shared_ptr<Request>)` | `continuous_scheduler.cpp:231`；`:308`；`request_queue_.write` `:309` | `Req` | `:518`（LLMMaster 调用点）→ `:309` 入队 | 无 per-request 时间戳；仅 `pending_requests` 计数 `:489/:500` | ⛔ **`folly::MPMCQueue` 队列**（`continuous_scheduler.h:180`，lock-free，S） |
| A10 | 调度出队 / KV 分配 / 组 batch | `BatchGroup ContinuousSchedulerBase::prepare_batch()` | `:376`；`Timer` :377；`drain_request_queue` :382；`policy_->schedule` :429；`process_completed_requests` :433；`create_batches` :436 | `SchedulerState`、`BatchGroup`、`Batch` | 单调度步：`:377` → `:445`；出队/准入点 `:382`（batch 内逐 seq） | ✅ `scheduling_latency_seconds`（:445，仅非空 batch；S） | 同调度线程；KV allocate 在 `scheduler_policy.cpp:483/:520`（与 Counter 区间重叠，S） |
| A11 | 调度线程 step / overlap | `void ContinuousSchedulerBase::step(const absl::Duration&)`；`void ...::step_with_schedule_overlap(...)`；`void ...::generate()` | `:525`；`:546`；`:599` | `BatchGroup`、`last_batch_` | `:525`→`:540`（非 overlap）；`:546`→`:591`（overlap：先消费 `result_callback_(last_batch_)` :580/:589） | 无 per-request；用 t6 采样点 | 调度线程 `llm_master.cpp:540-547` 独立 std::thread（S），`scheduler_->step(500ms)` 循环 |
| A12 | engine prepare + 分发 worker | `ForwardOutput LLMEngine::step(BatchGroup&)` | `llm_engine.cpp:511`；`Timer` :516；`create_inputs` :523；`step_remote_async` :540；`collectAll().get()` :545 | `BatchGroup`、`std::vector<LlmForwardInput>` | `:516` → `:581`（engine_latency） | ✅ `engine_latency_seconds`（:581） | ⛔ **collectAll 同步点** :545（非 overlap 时阻塞调度线程）；overlap 时结果在 A15 才 `.get()` |
| A13 | master→worker 通道 | `void RemoteWorker::step_remote_async(const LlmForwardInput&)`；`bool CommChannel::execute_model_with_brpc(const LlmForwardInput&, folly::Promise<std::optional<RawForwardOutput>>&)`；`void ShmChannel::execute_model_async(const LlmForwardInput&, folly::Promise<...>&)` | `remote_worker.cpp:169`；`comm_channel.cpp:614`；`shm_channel.cpp:84`（SHM 实现 :50-63） | `LlmFwdIn`（`sample_sequence_ids`/`meta.batch_id`）、`folly::Promise` | `:169` → `promise` 完成（A14 结束时） | 无 span；仅 brpc 自身 latency（不含 SHM） | ⛔ **RemoteWorker 线程池**（`remote_worker.cpp:169`）+ **brpc async callback / bthread**（`ExecuteModelClosure`）；SHM 模式为忙等版本号（`forward_shared_memory_manager.cpp:3787/:3994`，S） |
| A14 | worker 执行（forward+sampling） | `void WorkerService::ExecuteModel(::google::protobuf::RpcController*, const proto::ForwardInput*, proto::ForwardOutput*, ::google::protobuf::Closure*)`；`void WorkerService::step(...)`；`folly::SemiFuture<std::optional<ForwardOutput>> Worker::step_async(const LlmForwardInput&)`；`std::optional<ForwardOutput> LLMWorkerImpl::step_internal(const LlmForwardInput&, ForwardSyncPolicy, bool)` | `worker_service.cpp:988`（Counter :1042）；`step` :394；`worker.cpp:323`；`llm_worker_impl.cpp:233`（`Timer` :240；executor `:291`；sampler `:364`；Counter `:421`） | `ForwardInput`（packed）、`ForwardOutput`、`TaskSubmission.task_id` | `:988` → `:1042`（worker_service_latency）；执行体 `:240`→`:421` | ✅ `worker_service_latency_seconds`、`execution_latency_seconds_model`（**NO_SYNC 早退 :408-418 不记 S**）；`AUTO_COUNTER` 在 `WorkerImpl` 外层（base_executor）另有采样计数（H） | ⛔ WorkerService 线程池 `threadpool_->schedule`（:988）；SHM 模式下改为 polling 线程 :564；task pipeline 拆 submit/consume（`worker.cpp:327/:333`，S） |
| A15 | 采样结果回写 master | `void LLMEngine::update_last_step_result(BatchGroup&)`；`void Batch::process_sample_output(const RawForwardOutput&, bool)`；`void BatchOutputHandler::process_sample_output(const BatchOutputData&, const RawForwardOutput&, bool)` | `llm_engine.cpp:585`（`get_last_step_result_async` :617、collectAll :625、sample :637）；`batch_output_handler.cpp:168`（`SampleOutput` 重载 :251） | `Batch`、`RawForwardOutput`、`Seq` | 非 overlap：`:545` 后直接 `:572`；overlap：`:585`→`:645` | 无独立 Counter；序列更新走 `sequence.cpp:238/:318` | `get_last_step_result_async` 在 `CommChannel` 实际为**同步** brpc 调用（`comm_channel.cpp:558-570`，S；名字 async 但阻塞） |
| A16 | 响应派发入队 | `void RequestOutputDispatcher::process_completed_requests(std::vector<std::shared_ptr<Request>>&)`；`...::process_stream_requests(...)`；`void ...::schedule_response(...)` | `request_output_dispatcher.cpp:214`（调用点 `continuous_scheduler.cpp:433`）；`:362`（调用点 `:665`）；`schedule_response` :92（sticky `response_thread_id`，S） | `Req`、`Out` | **t7**：`prepare_batch` 内 `:433` → `schedule_response` :92（enqueue） | 无 per-request；现有 Counter 在 runnable 内，不含排队 | ⛔ **response_threadpool_**（`request_output_dispatcher.h:74`，`num_response_handling_threads` 默认 4，S） |
| A17 | 响应 runnable：detokenize + 回调 | `void RequestOutputDispatcher::process_completed_request(std::shared_ptr<Request>)`；`void ...::process_stream_request(std::shared_ptr<Request>)` | `:124`（Counter :144、e2e :147-148、`generate_output` :152、`output_func` :159）；`:229`（Counter :245、`generate_streaming_output` :251、`output_func` :259） | `Req`、`Out`、`Seq` | **t8**：runnable 首行 :126/:231 → `output_func` 返回 | ✅ `responsing_latency_seconds_non_stream/stream`（:144/:245，含 detok+回调）、`end_2_end_latency_milliseconds`（:148）、`detokenization_latency_seconds_*`（`sequence.cpp:518/:677`，S） | ⛔ 回调在 response 线程执行；`output_func` 内同步做序列化+SSE 写 |
| A18 | 序列化 + SSE/HTTP 写出 | `template <typename Request, typename Response> bool StreamCall::write(Response&)`；`bool StreamCall::write_and_finish(Response&)`；`void StreamCall::start_stream()`；`bool StreamCall::finish()` | `stream_call.h:142`（`pa_->Write` :167/:180）；`:83`；`start_stream` :273（`CreateProgressiveAttachment` :278、`done_->Run()` :287）；`finish` :187（`[DONE]` :196、`pa_.reset()` :203） | `Call`/`StreamCall`、`Response` proto | **t9**：首次 `write()` :142 → `pa_->Write` 返回 :180；**t10**：`finish()` :187→`:205` | ✅ `event=response_serialized / stream_closed`（:100/:205，verbose trace，需开 flag）；无 duration | `brpc::ProgressiveAttachment` 写回连接；`Write` 成功 ≠ 客户端收到（S：语义仅表示写入 brpc） |
| A19 | 非流式/其他入口变体 | `void TextGenerationServiceImpl::process_async_impl(std::shared_ptr<TextGenerationCall>)`；`NonStreamCall<...>::write_and_finish(Response&)`；`void APIService::ChatCompletions(...)` | `text_generation_service_impl.cpp:61`；`non_stream_call.h:74`；`api_service.cpp:546`（`process_async_rpc_impl` :565） | `TextGenerationCall`/`NonStreamCall`；RPC 路径无 Call | non-stream：`:74` → dtor `done_->Run()`（`non_stream_call.h:60-67`）；RPC：`:546`→`:565`（直接进 `chat_service_impl.cpp:629`，回调走 `handle_rpc_response`） | `event=response_serialized`（`non_stream_call.h:92`） | RPC 路径**不经** SSE，回包在 response 线程 pool 内经 `xservice_client.cpp:410` 转发 |

> 说明：A1→A18 是 Chat HTTP 主链；Completions/TextGeneration/Anthropic 走同一模板（`api_service.cpp:206 / :834 / :1160`，S），差异点在 `Call` 类型与是否流式。
> **首字节（t9）时间点精确定义**：OpenAI HTTP 流式（`openai_http_==true`）时 `start_stream()` 被延迟到第一次 `write()`（`stream_call.h:146-148`，S），因此 t9 = 第一次 `write()` 内 `pa_->Write` 完成；非 openai 流式在 `StreamCall` 构造即 `start_stream()`（:62-63），`done_->Run()` 发生在其构造线程（brpc bthread）上——两种路径的 t9 语义不同，埋点必须区分。

---

## B. 上下文传播边界表（必须显式携带 trace 上下文的地方）

| # | 边界 | 机制 | file:line | 必须携带 | 风险 / 备注 |
|---|---|---|---|---|---|
| B1 | brpc bthread → `LLMMaster` std::thread 池 | `ThreadPool::schedule(Runnable)` + lambda 值捕获（`call` 为裸指针） | `llm_master.cpp:491-524`；`threadpool.h:140`；池创建 :327 | `x_request_id`(可放 lambda 值捕获)、`request_id`(尚未生成)、`t0/t2` | bthread TLS 不自动继承；`Call*` 裸指针依赖 brpc done 生命周期（`ClosureGuard::release` `closure_guard.h:57`，S）——span 对象必须由 lambda 按值持有，不可挂 TLS |
| B2 | 请求线程池 → 调度器 MPMCQueue | `request_queue_.write(std::shared_ptr<Request>)` | `continuous_scheduler.cpp:309`；队列 `continuous_scheduler.h:180` | `x_request_id`（`RequestBase` 内，`request_base.h:82`）、`t4` | 无 per-request 出队时间戳；只能靠 `Request` 自有字段携带 span 上下文（H：可加 `absl::Time` 字段而不改行为） |
| B3 | 调度线程（独立 std::thread） | `scheduler_->step(timeout)` 循环；`step_callback_` = `engine->step` | `llm_master.cpp:540-547`；`continuous_scheduler.h:304-307` | `batch_id`、`BatchGroup`（含全部 `Seq*`） | 跨线程丢失原 bthread 上下文；请求集合通过 `Batch::sequences` 反查（H） |
| B4 | engine → RemoteWorker 线程池 + brpc 异步 | `threadpool_.schedule` + `folly::Promise`；`input.clone()` 深拷贝 | `remote_worker.cpp:169-177`；`comm_channel.cpp:614-629` | `batch_id`、`sample_sequence_ids`（`LlmForwardInput` `forward_params.h:578`/builder `forward_input_builder.cpp:1060`） | 无 trace id；`clone()` 已是成本点（§E-2）；promise 完成即 A13 结束 |
| B5 | master → worker 进程（RPC / SHM） | `PackedForwardInput.payload`（版本化不透明二进制）；SHM 控制块 version | `worker.proto:354-367`；`comm_channel.cpp:614`；`shm_channel.cpp:50-63`；`forward_shared_memory_manager.cpp:3787/:3994` | `meta.batch_id`（写 `:3173`/读 `:2710`）、`sample_sequence_ids`（写 `:3293`/读 `:2839`） | **proto 层无 request_id/x_request_id/step_id**；跨进程只能靠 batch_id + `request_id#index` 字符串；SHM 模式无 per-request RPC，忙等含 worker 执行时间 |
| B6 | worker 进程内部：brpc bthread → WorkerService 池 | `threadpool_->schedule` + `brpc::ClosureGuard` | `worker_service.cpp:988-1042` | packed payload 内 `batch_id`/`sample_sequence_ids` | worker 侧无 `Call`/`Request`，只有 `ForwardInput`/`sample_sequence_ids`（`sequence.h:296` `request_id#index`） |
| B7 | worker task pipeline：submit / consume 拆分 | `TaskExecutionPipeline::submit` → `take_result_async(task_id)` | `worker.cpp:323-341`（submit :327、take :333）、`:411-433`；`task_execution_pipeline.h:101/:154/:159/:162` | `task_id`（`TaskSubmission.task_id` :103） | `task_id` 仅 worker 进程内单调，**不回传 master 协议**；master 侧无法把 prepare 与 consume 关联到同一 step（H：需 batch_id 或新字段） |
| B8 | schedule overlap（结果延后一步） | `last_batch_` 持有，`update_last_step_result` 下一步消费 | `continuous_scheduler.cpp:546-591`；`llm_engine.cpp:585-645` | `batch_id`（生成于 `batch_state.cpp:80-83`）、`Seq*` | 输出 N 属于 batch N-1；span 父子关系不能按"当前 step"推断，必须用 batch_id join（H） |
| B9 | 调度线程 → response_threadpool | `schedule_response`（sticky `state().response_thread_id`） | `request_output_dispatcher.cpp:92-103`、`:214`、`:362`；`request_state.h:194` | `request_id`、`service_request_id`、`call*`/`shared_ptr<Call>`（chat 侧捕获 `call`） | `RequestOutput` **不含 x_request_id**（`request_output.h:92-133`）；HTTP 侧靠回调捕获的 `call` 取回；RPC 侧只剩 `request_id`+`service_request_id` |
| B10 | response 线程 → 回调 → SSE 写 | `state().output_func(Out)`；`StreamCall::write` → `pa_->Write` | `request_state.h:38/:164`；`chat_service_impl.cpp:845-869`；`stream_call.h:142/:167/:180` | `x_request_id`（通过 `call`）、`request_id`、chunk 序号 | `output_func` 在 response 线程同步执行；span 只能包住 runnable，不可跨 chunk 复用同一 span（跨线程且生命周期长） |
| B11 | P→D 首 token 交接（prefill→decode） | `prefill_threadpool_`(1 线程) 内 `stub->FirstGeneration(...)` **同步** RPC | `disagg_pd_scheduler.cpp:820/:974-975`；`disagg_pd.proto:256-278` | `x_request_id`(:266? 实为 :169 在 chat.proto；disagg_pd.proto `x_request_id=11` 在 :266)、`upstream_elapsed_seconds`(:270)、`reservation_id`(:22) | 1 线程池 + 同步 RPC + `output_dispatcher_->wait_completion()`（:995，强制同步，§E-1）；span 若包住 :975 会阻塞 prefill 线程 |
| B12 | D→P / D→xllm_service 回包 | brpc `DoNothing` + `brpc::Join`（bthread-aware） | `xservice_client.cpp:410`（组包 :452-454；异步 :530-571）；`xservice.proto:114`；`disagg_pd.proto:225-233` | 仅 `request_id`(req_id) + `service_request_id`(service_req_id) | **x_request_id 在此丢失**；跨进程关联必须回退到 `service_request_id`（由 xllm_service 生成，`request_params.h:112`） |
| B13 | P/D 入站 RPC：brpc bthread → 调度器队列 | `DisaggPDService::AddNewRequests/FirstGeneration` → impl → `decode_schedule`/`decode_recv_first_generation` → `request_queue_.write` | `disagg_pd_service.cpp:38/:49`；`disagg_pd_service_impl.cpp:223/:372/:406`；`disagg_pd_scheduler.cpp:1080/:1119`（入队实际 :1304） | `DisaggRequest.x_request_id`（`disagg_pd.proto:56`）、service_req_id、KV 恢复结果 | brpc bthread 内做 KV 恢复/入队（H：耗时可能受 pull 影响，见 :1300 附近 VLOG 计时）；span 需跨 bthread→队列 |
| B14 | worker→master 输出回传 | brpc `ForwardOutput` / SHM `raw_output_write`；按 output 下标映射 `output_targets_` | `worker_service.cpp:1188` 后 `to_proto`；`shm_channel.cpp:50-63`；`batch_output_handler.cpp:168+` | 依赖**顺序**而非 ID；可用的只有 `src_seq_idxes`/`sample_sequence_ids` | `ForwardOutput` proto 无 request_id（`worker.proto:404`，S）；顺序错位即错挂（H：task pipeline/overlap 下需验证） |
| B15 | 事件输出通道（已存在） | `XLLM_VERBOSE_TRACE()`：thread-local 缓冲 → spdlog async 队列 → 后台线程写文件；`enabled()` 为 relaxed load | `verbose_trace_logger.h:42-56`、`:132`；初始化 `xllm.cpp:551`；开关 `service_config.cpp:64`（默认 false） | `x-request-id=` 字符串（现有 3 类事件） | 已具备"低开销异步事件"形态，是 t0–t10 最自然的承载；但只有 api_service 侧 3 个事件（`request_received`/`response_serialized`/`stream_closed`/`request_error`），无 scheduler/worker 事件、无 span/duration |

---

## C. Identity / ID 清单（字段 · 定义 · 生成 · 传播 · 丢失）

| 字段 | 定义处 | 生成处 | 传播路径 | 丢失/风险 |
|---|---|---|---|---|
| `x_request_id` | `Call`（`call.h:39-67`）、`RequestBase`（`request_base.h:82`）、proto：`chat.proto:169`、`completion.proto:138`、`anthropic.proto:203`、`disagg_pd.proto:56`、`:266` | HTTP：`request_id.cpp:71-75`（`"req-"+InstanceName hash+"-"+uuid`），`resolve_x_request_id` :77-91 优先级 header→body→generate；header 别名 `x-request-id`/`x-ms-client-request-id`/`trace_id`（:54-69） | Call→`RequestParams.x_request_id`（:114）→`RequestBase`→`Request`；P→D 首 token `prefill_send_first_generation` `disagg_pd_scheduler.cpp:831` | **RequestOutput 无该字段**（`request_output.h:92-133`）；→xllm_service 的 `DisaggStreamGeneration` 无（`disagg_pd.proto:225-233`）；packed ForwardInput 无；worker 不可见。RPC 直连路径 `RequestParams(rpc_request,"","")`（`chat_service_impl.cpp:675`）会回退读 proto 字段（`request_params.cpp:529-531`，S） |
| `request_id` | `RequestParams.request_id`（`request_params.h:111`）；`SequenceParams.request_id`（`sequence.h:108`）；`RequestOutput.request_id`（`request_output.h:105`） | `request_params.cpp:48-51` `generate_chat_request_id()`（`"chatcmpl-"+hash+"-"+uuid`）；completion :38；客户端显式 `request_id` 覆盖：`init_from_chat_request` `request_params.cpp:370-371` | `RequestBase:75`→`Seq`→`sample_sequence_id`（`sequence.h:296` = `request_id#index`）→worker `sample_sequence_ids`→响应 `RequestOutput:105`→xservice `req_id` | worker 侧只有 `request_id#index` 字符串；`RequestOutput` 之后仍是主键，但无法回溯 `x_request_id` |
| `service_request_id` | `RequestParams.service_request_id`（:112）；`RequestBase`（`request_base.h:77`）；`RequestOutput:108` | **xllm_service 侧生成**（本仓仅透传；HTTP OpenAI 路径为空串） | Call/RPC→`RequestParams`→`RequestBase`→`RequestOutput`→`DisaggStreamGeneration.service_req_id`（`:227` 附近） | 是 D/P→xllm_service 唯一可用关联键；HTTP 路径恒空 |
| `sequence_id` / `sample_sequence_id` | `sequence.h:295`(request_id)、`:296`(sample_sequence_id) | `sample_sequence_id() = request_id + "#" + index_`（S） | `Batch`→`LlmForwardInput.sample_sequence_ids`（`forward_params.h:578`；builder `forward_input_builder.cpp:1060`）→packed payload（`forward_shared_memory_manager.cpp:3293`）→worker | 无独立 sequence_id 字段；worker 侧可用键；master 侧主要用 `Seq*` 指针（无跨 step 稳定性风险 H） |
| `batch_id` | `BatchState.batch_id_`（`batch_state.h:112`）；`batch.h:74` | `batch_state.cpp:80-83` 进程内 `std::atomic<uint64_t> next_batch_id{1}` fetch_add | `Batch`→`ForwardInputBuilder`→`input_params.meta.batch_id`（`forward_input_builder.cpp:1373`）→packed/SHM（`:3173`/`:2710`） | 只到 worker；不进 `ForwardOutput` proto；与请求无直接映射（一批多请求）；可作 step join key（H） |
| `step_id` | — | **LLM 服务路径不存在**（仅 DiT cache `dit_cache_type.h:27`、Rec tensor `max_decode_step_ids`） | — | 无法 join "第 k 次 step"；建议用 `batch_id` 代偿（H） |
| `task_id` | `TaskSubmission.task_id`（`task_execution_pipeline.h:103`）；`TaskResult.task_id`（:109） | `TaskExecutionPipeline::submit`（:154，worker 进程内单调） | worker 本地 `submit`→`take_result_async(task_id)`（`worker.cpp:327/:333/:416`） | 仅 worker 内；master 不可见（§B-7） |
| `x_request_time` | `Call.x_request_time_`（`call.h`）；`RequestBase.x_request_time_`（`request_base.h:85`） | 客户端 header `x-request-time`/`x-request-timems`（`call.cpp:34-39`，S） | Call→`RequestParams`→`RequestBase` | 之后不再传播；不是 absl::Time（字符串），不能与 `absl::Now()` 直接相减 |
| `generation` | — | 本仓 proto 无 `generation` 字段（S：grep `xllm/proto/*.proto` 无匹配；`DisaggStreamGeneration` 只是消息名） | — | xllm_service 侧概念；本仓只能靠 `service_request_id`+`request_id` 组合近似 |
| `trace_id` / W3C `traceparent` | — | 仅 HTTP header `trace_id` 被当作 x_request_id 别名（`request_id.cpp:66`，S） | 同 x_request_id | 无 W3C traceparent 解析/透传；无 `trace_state` |
| `pd_reservation_id` | `RequestState.pd_reservation_id`（`request_state.h:172`） | disagg PD 预约（P 侧分配） | `DisaggGenerationsRequest.reservation_id`（`disagg_pd.proto:22` 附近） | P/D 间有效，HTTP 与 worker 不可见 |
| `response_thread_id` | `RequestState.response_thread_id`（`request_state.h:194`） | `schedule_response` 首次分配（`request_output_dispatcher.cpp:92-103`） | 同 request 的后续 chunk 复用（sticky） | 仅是线程亲和线索，非 trace 字段 |

---

## D. 逐请求 waterfall 可行性：现有 / 缺失

**已存在（可复用）**
- D1 `x_request_id` 在 HTTP 入口即有（`call.cpp:41`），并有 `event=request_received`（`call.cpp:47`）——全链路最好的关联键，且已能写异步事件文件（`verbose_trace_logger.h:42-56`）。
- D2 `RequestBase.created_time_`（`request_base.cpp:28`）+ `Request::end_to_end_latency_seconds()`（`request.cpp:103`）提供"内部 E2E"，且已带 upstream 补偿（`request.cpp:97-105`，P/D 用 `upstream_elapsed_seconds` 还原，`disagg_pd_scheduler.cpp:1318-1336`）。
- D3 `Sequence::latest_generate_time_`（`sequence.cpp:117`）、TTFT/ITL：`scheduler_metrics.cpp:85-124` 在 `process_batch_output` 内记录并回写 `Sequence::time_to_first_token_latency_seconds`（`sequence.h:408-413`）——现有 TTFT/ITL 的权威定义点。
- D4 响应侧已有 per-request-ish 的 Counter/Histogram：`responsing_latency_seconds_*`（:144/:245）、`end_2_end_latency_milliseconds`（:148）、`Request::log_statistic`（:107；调用点 `:156/:191`、disagg `:797`）。
- D5 阶段级 Counter 齐全：`tokenization_latency_seconds`、`chat_template_latency_seconds`、`request_handling_latency_seconds_*`、`scheduling_latency_seconds`、`engine_latency_seconds`、`worker_service_latency_seconds`、`execution_latency_seconds_model`、`detokenization_latency_seconds_*`（各自 file:line 见 §A）。
- D6 worker 侧有可挂请求的键：`sample_sequence_ids`（`request_id#index`）+ `meta.batch_id`（§B-5/B-6）。
- D7 已具备低开销异步事件通道与 4 个既有事件名（`event=request_received/response_serialized/request_error/stream_closed`，§B-15）。

**缺失（必须新增或换设计）**
- D8 **per-request 的 t0–t10 时间戳**：现有 Counter 全是聚合量，且 `AUTO_COUNTER` 只在阶段内部计时（`request_handling_latency_seconds_chat` 甚至含线程池排队时间，`llm_master.cpp:497`），无法拆出 queue wait。
- D9 **线程池 enqueue/start 的成对时间戳**：`ThreadPool::schedule`（`threadpool.h:140`）没有 per-runnable 的入队/开始时间；t2/t3 只能新增字段（H：给 Runnable 加 header 或包一层，需评估开销）。
- D10 **调度出队时间戳**：`request_queue_` 是 `folly::MPMCQueue`（`continuous_scheduler.h:180`），出队在 `policy_->schedule`/`drain_request_queue`（`continuous_scheduler.cpp:382/:429`）内部；只能逐 seq 打点（H）。
- D11 **step_id / 每步批次身份到请求**：`batch_id` 有但只进 worker（§C）；请求→batch 的映射只在 step 生命周期内存在，事后无法 join。
- D12 **worker → master 的 per-request 事件回传**：`ForwardOutput`（`worker.proto:404`）无请求字段；若要 worker 侧 span/事件，需新侧信道（H：可在 packed payload 或 proto 加 `batch_id` 回显，或用现有 counters 拆维）。
- D13 **overlap/task pipeline 的因果 join**：结果延后一步（`continuous_scheduler.cpp:546-591`），task 拆分 submit/consume（`worker.cpp:327/:333`）；naive "一步一 span" 会错挂。
- D14 **SSE 客户端可见时间**：只有 brpc `Write` 返回（`stream_call.h:167/:180`），无 flush/客户端 ack；c0/c1/c2 客户端事件必须由外层（xllm_service/网关）补。
- D15 **P/D 跨实例拼接**：P→D 有 `x_request_id`+`upstream_elapsed_seconds`（`disagg_pd.proto:266/:270`），D→xllm_service 只有 req_id+service_req_id（`xservice_client.cpp:452-454`）；跨 P→D→service 的单条瀑布需要两侧 ID 约定（H）。
- D16 **trace 采样/开关**：目前只有 `enable_verbose_trace_log` 全有/全无（`service_config.cpp:64`），无按请求采样、无 span 上下文存储；新增 trace 必须默认零开销（参考 `enabled()` relaxed load）。

---

## E. 朴素插桩最可能改变行为的 5 个风险（必须规避）

| # | 风险 | 证据锚点 | 规避/替代 |
|---|---|---|---|
| E1 | **引入强制同步**：在 overlap / task pipeline / P-D 路径加 `.get()`/`wait()`/flush | `llm_engine.cpp:545`（collectAll）、`:625`；`continuous_scheduler.cpp:580/:589`；`disagg_pd_scheduler.cpp:995`（`wait_completion`）；`worker.cpp:330-333`（overlap 直接返回空 ack） | span 只记录"已存在时间点"的时间戳，绝不在 span 析构里等待；D2H/结果获取仍在原位置；P-D 的 span 只覆盖 `:975` 前后已有同步区间 |
| E2 | **额外深拷贝/张量拷贝**：为打点复制 `LlmForwardInput`/`ForwardOutput`/`sample_sequence_ids` | `remote_worker.cpp:169-177`（`input.clone()` 已存在）；`worker_service.cpp:435-436`（`safe_to(..., kCPU)`） | trace 只带 `batch_id`/`request_id` 之类小值 + 指针/`Seq*`（生命周期内）；禁止在 step 内复制 forward input/output |
| E3 | **热路径锁/堆分配**：token 级插桩在调度/响应/SSE 线程上分配或加锁 | `continuous_scheduler.cpp:235`（`prefetch_admission_mutex_`）、`stream_call.h:273`（`stream_start_mutex_`）、`scheduler_metrics.h:36`（metrics mutex）、`sequence.cpp:518/:677` 每个输出 token 都会走 | 复用 `VerboseTraceLogger` 模式（thread-local 预分配 buffer + 单次 memcpy 入异步队列，`verbose_trace_logger.h:42-56`）；不要 per-token 构造 `std::string`/`ostringstream`/`shared_ptr`；span 事件按 chunk 粒度而非 token 粒度（H：需测开销） |
| E4 | **改变 device stream / sync 语义**：为计时加 event / `synchronize` | `llm_worker_impl.cpp:408-418`（`ForwardSyncPolicy::NO_SYNC` 提前返回，不做 `synchronize_default_stream`；`COUNTER_ADD(execution_latency_seconds_model)` 在 :421 之后才执行） | 只在既有同步点之后取时间（如 `:421` 处、`worker_service.cpp:1042` 处）；新 span 不记录/不创建 CUDA/NPU event；注意 NO_SYNC 路径的 span 结束点必须与计数器语义一致，否则会误把"未同步段"算进执行时间 |
| E5 | **SSE/连接生命周期越界 + brpc 指标语义误解**：`done_->Run()` 后 controller/pa 生命周期变化；`ClosureGuard::release()` 提前触发 `request_out_metric` | `stream_call.h:72-81`/`:287`/`:203`/`:210-225`（注释明确 controller 可能已被销毁）；`closure_guard.h:57-63`（`release()` 立即调 `_after_done`）；`api_service.cpp:500-505` + `:573-578` | span/事件必须在 `done_->Run()` 与 `pa_.reset()` 之前发出并完成（只写 thread-local buffer，不做 I/O）；不要用 `server_request_total_*` 当"请求完成"指标（它在入队期就 +1，S：`api_service.cpp:500` 处 `guard.release()`）；流式结束的 t10 以 `finish()`（`stream_call.h:187`）为准 |

> 附加约束（非 Top5 但仍要注意）：SHM 忙等循环（`forward_shared_memory_manager.cpp:3787/:3994`）内禁止插桩/日志（会改变轮询节奏）；packed payload 是版本化 schema（`worker.proto:354-362`），加字段需双端版本兼容；Prometheus 侧**禁止**把 `x_request_id`/`request_id` 作为 label（`brpc_metrics_baseline.md` §8.1，S(base)），请求级信息只进 trace/log。

---

## F. 最小落点建议（零行为变更的增量清单）

1. **t0**：`ChatCompletionsHttp` 入口（`api_service.cpp:568`）或 `Call::init`（`call.cpp:33`）记录 host 时间 → 复用 `x-request-id`（`call.cpp:47` 已有事件点，成本 = 1 次 relaxed load，S）。
2. **t1**：`decode_chat_request` 返回处（`api_service.cpp:493`）——JSON→proto 结束。
3. **t2/t3**：`llm_master.cpp:489`（入队前）与 lambda 首行 `:493`——两处各取一次 `absl::Now()`，放在 lambda 捕获的轻量结构里（不挂 TLS）。
4. **t4**：`llm_master.cpp:512`（factory 返回）或 `LLMRequestFactory::create` 内 `request.cpp:51`（Sequence 构造完成）。
5. **t5–t6**：不要新加同步；直接复用 `scheduler_metrics.cpp:105` 的 TTFT 定义点，并把 `batch_id`（`batch_state.h:112`）在 `process_batch_output` 时写入事件（`continuous_scheduler.cpp:637` 附近）。
6. **t7/t8**：`request_output_dispatcher.cpp:92`（enqueue，可拿到 sticky tid）与 runnable 首行 `:126`/`:231`。
7. **t9/t10**：`stream_call.h:142`/`:187`（写出/结束）与 `non_stream_call.h:74`（非流式），确保日志在 `pa_.reset()`（:203）与 `done_->Run()` 之前。
8. **跨进程**：P→D 用现有 `x_request_id`+`upstream_elapsed_seconds`（`disagg_pd_scheduler.cpp:831/:974`、`disagg_pd.proto:266/:270`）；D→service 端只能补 `service_request_id` 关联（`xservice_client.cpp:452-454`）。
9. **worker 侧**：若必须加，只回显 `batch_id`（来自 packed payload，S），不新增每请求字段；worker 事件导出需另行设计（H）。

---

### 附：本轮未复验 / 待后续核实的点（H）
- `rate_limiter.cpp:26`、`block_manager_pool.cpp:198`、`block_manager_impl.cpp:145`（基线 §5 引用，本轮未读）。
- `batch_builder.cpp:70/:125/:127`（基线 F04 引用；本仓 `BatchBuilder::build` 在 `batch_builder.cpp:33`，内部行号未核）。
- `WorkerImpl`/`base_executor_impl` 内是否已有采样/执行子阶段 Counter（H）。
- `xllm_service`（仓外）对 `service_request_id`/`x_request_id` 的映射能力（H）。
