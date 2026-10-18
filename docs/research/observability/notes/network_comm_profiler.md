# 网络 / 通信 / 队列 / Profiler 观测能力盘点（scout-infra · task-3）

- 仓库：`<repo-root>`，xLLM commit `4b05c484a34b12619c5ec536aceb62ab0c53e4db`；brpc 为 pinned fork `67d6372a340cb0663efcd3fda6f4c5e188bf5c24`（声明版本 1.12.1）。
- 本文是在 `docs/research/observability/brpc_metrics_baseline.md`（§2-§5）与 `metric_inventory.json`（129 族）之上的**网络/通信/Profiler 专题**，不重复基线文档已有的 bvar 导出机制细节。
- 证据等级：**S** = 源码确认（附 `file:line`）；**R** = 本轮实际运行验证（仅限 `tools/npu_timeline.py`，未运行任何 xLLM 二进制）；**H** = 假设/推断，未验证。
- 边界：本地只读 checkout，不构建、不启动服务；因此所有"运行时表现"结论都只能到 S 级，**没有任何 xLLM 运行期实测数据**。

## 0. 速览（12 条）

1. **一条链路三段里，只有两端有指标**：HTTP 入口（brpc server 内建）与 worker 执行（`worker_service_latency_seconds`、`engine_latency_seconds`）有数；**中间 master→worker 的 RPC/SHM/PD 传输几乎是黑盒**。
2. **CommChannel 零指标**：`comm_channel.cpp` 全文没有 `COUNTER/HISTOGRAM/GAUGE`（S：`xllm/core/distributed_runtime/comm_channel.cpp:1-738` 无命中），且 `timeout_ms=-1 / connect_timeout_ms=-1 / max_retry=3`（`comm_channel.cpp:32-35`）——网络 hang 只表现为"请求永不返回"。
3. **SHM 路径零指标且是 1µs 忙等**：master 侧 `input_write`+阻塞 `raw_output_read`、worker 侧 1 线程 1µs 轮询（`worker_service.cpp:564-648`、`forward_shared_memory_manager.cpp:3787-3797`）；**fallback 回 brpc 只打一行 LOG(ERROR)**（`shm_channel.cpp:55-58/:70-74`）→ 传输模式静默切换、延迟台阶无痕。
4. **线程池 enqueue→start 等待完全不可见**：所有 `AUTO_COUNTER` 都在 runnable **内部**才开始计时（`llm_master.cpp:497`、`request_output_dispatcher.cpp:144/:245`、`worker_service.cpp:997`），队列长度也没有 gauge，`ThreadPool` 只有 `empty()`（`threadpool.h:144`）。
5. **PD 解耦传输只有 VLOG(1)**：`decode_allocation_seconds / allocation_rpc_seconds / FirstGeneration / pull_kv / enqueue` 全是 `Timer`+`VLOG(1)`（`disagg_pd_scheduler.cpp:647-654/:965-972/:1239-1314`），无法聚合、无法告警、默认看不到。
6. **唯一真正进 bvar 的"传输类"指标是 mooncake**：`mooncake_transfer_{completed,bytes,failed,latency}`（`metrics.cpp:112-124`，写点 `mooncake_transfer_engine.cpp:675-709`），粒度是 **batch**，不归属请求，submit 与 wait 混在一个 Timer 里。
7. **HCCL（TP/DP/EP、MoE dispatch/combine）零 bvar**：`ProcessGroup` 全部 async 接口无计时（`process_group.h:46-130`、`process_group.cpp:224-330`）；只能靠 profiler（torch_npu trace / CANN `ACL_PROF_HCCL_TRACE`，`npu_profiler.cpp:30-31`）。
8. **在线 profiler 是"全局同步广播"**：`/start_profile` 对所有 worker 生效、与请求无关、`start` 只返回 bool 不返回路径；部分失败会立即回滚（`llm_engine.cpp:652-666`）；`stop_profile` 在 worker 线程里**同步导出 trace**（`npu_torch_profiler.cpp:141-186`）→ 停采瞬间会卡 step。
9. **trace 与请求无法关联**：profiler 产出里没有 request/step id；仓库里现成的 `MstxRange/LLM_MSTX_RANGE`（`mspti_helper.h:27-42`）**零调用点**，且其析构里 `aclrtSynchronizeStream`（`mspti_helper.cpp:40-43`）会把异步流串行化，**不能直接用于异步 op 打点**。
10. **`tools/npu_timeline.py` 能跑但有三个硬坑**（R 实测）：需 Python≥3.10；HCCL 事件被静默丢弃；MARKER 缺 start/end 配对直接 KeyError。
11. **worker 的 brpc 诊断端口是临时端口**：`addr + ":0"`（`worker_server.cpp:144`），只能从启动日志 `Worker N: server address: ...`（`worker_server.cpp:151-152`）拿到，因此 worker 侧 `/vars`、`/rpcz`、`/brpc_metrics` 默认"不可达"。
12. **推荐落点**：默认开启的轻量仪表（~14 条 histogram/counter，全部 CPU 侧 `clock_gettime`，<1µs/step）+ 故障窗口才开的采样（`/rpcz`、profiler、`msprof`）；两者都要按 §F 的"不打扰 overlap/pipeline"约束实现。

---

## A. 现状可直接用的观测入口

> 关键前提：brpc 的 `/v*` 内置服务是**每个 brpc::Server 进程内**的页面，只暴露**本进程**的 bvar。xLLM 里至少有 4 类 brpc Server，各自独立：
> - API/HTTP 服务：仅 master 节点（`xllm_server.cpp:128-166`），端口 `--port` 默认 **8010**（`service_config.cpp:23`）。
> - WorkerService：**每个 worker 进程**一个（`worker_server.cpp:134-152`），监听 **`0` 临时端口**（`worker_server.cpp:144`）。
> - DisaggPD 服务：`disagg_pd_port`（`xllm_server.cpp:194`，`disagg_pd_config.h`）。
> - CollectiveService（rendezvous）：`xllm_server.cpp:210` / `collective_service.cpp:28-56`，同样临时端口。
>
> 所以"网络慢"排查时**必须分别抓 master 与每个 worker 进程的页面**；只抓 8010 会漏掉 worker 侧全部指标。

| 入口 | 能看到什么 | 作用域 / 口径坑 | file:line | 成本 / 风险 |
|---|---|---|---|---|
| `GET /vars` | `xllm_*` 129 个自研族 + brpc 内置 bvar（服务方法级 latency/concurrency/error/eps、`bthread_*`、`rpc_num_sockets`、`iobuf_*` 等） | **进程级**；窗口统计（P80/90/99/999/9999，非累计）；多维度默认最多 dump 1024 个（`bvar_max_dump_multi_dimension_metric_number`）；`/vars` 是 HTML/文本，不适合机器采集 | 注册：`third_party/brpc/src/brpc/server.cpp:485` `AddBuiltinServices()`；方法级：`brpc/details/method_status.cpp:41-60`；页面清单：`brpc/builtin/index_service.cpp:106-113` | 低；一次全量 dump 在 brpc bthread 执行，大进程可达 ms 级（H）；不要高频轮询 |
| `GET /vars/<glob>`（如 `/vars/rpc_server*_count`、`/vars/rpc_num_sockets`） | 按通配过滤 bvar，**适合只抓网络相关几个** | glob 用 `$` 代替 `?`（`index_service.cpp:110-111`）；仍受多维度 1024 上限 | `brpc/builtin/index_service.cpp:106-111` | 低；**推荐替代全量 `/vars`** |
| `GET /brpc_metrics` | Prometheus 文本；含 xllm 自研指标 + brpc 指标 | **无 flag 开关，默认注册**（`server.cpp:537`）；导出语义坑（基线文档已详述）：普通数值一律 `TYPE gauge`，只有 `rpc_server*` 前缀组成 summary，`_sum = avg*count` 非真累计；没有 histogram 分桶；`latency_scale_factor=1` 即**单位秒** | `brpc/builtin/prometheus_metrics_service.cpp`（`server.cpp:537` 注册）；fork commit 即为修 Prometheus 格式 | 中；全量序列化，Prometheus 15s 抓一次可接受，1s 级抓取会明显占 CPU（H） |
| `GET /status` | 每个 service/method 的 qps、latency、error、concurrency 汇总（人看友好） | 服务端视角；只含"已进入 brpc 方法"的 RPC，不含前排队 | `brpc/builtin/status_service.cpp`（由 `AddBuiltinServices` 注册） | 低 |
| `GET /rpcz`、`GET /rpcz/stats` | 最近 RPC span：服务端接收/发送时间、方法、错误码、请求/响应字节数、trace_id/span_id；支持过滤器 `?min_latency=`、`?min_request_size=`、`?min_response_size=`、`?error_code=`、`?log_id=`、`?trace_id=N&span_id=M`、`?max_scan=N`、`?time=` | **默认关闭**：`DEFINE_bool(enable_rpcz, false)`（`rpcz_service.cpp:40`）；运行期可 `/rpcz/enable`、`/rpcz/disable`（`rpcz_service.cpp:150/:513`）；span 保留 3600s（`span.cpp:63`）；span DB 默认写 `./rpc_data/rpcz`（`span.cpp:44-45`），`--rpcz_save_span_min_latency_us=0`（`span.cpp:66`）；采样比例看 bvar `rpcz_sampling_ratio`（`span.cpp:402-404`）；**只有服务端 span**，master→worker 的客户端排队/连接/重试看不到 | `brpc/builtin/rpcz_service.cpp`；`brpc/span.cpp:44-66,402-422`；页面说明 `brpc/builtin/index_service.cpp:113-137` | 中；开后会写盘并占 span DB 内存，**只在故障窗口开**，事后记得 `/rpcz/disable` |
| `GET /connections` | fd 级连接列表（对端地址、收发字节、RTT 等，取决于版本） | 只看**当前存活**连接；pooled 长连接下能判断"连接在不在、有没有在传数据" | `brpc/builtin/connections_service.cpp`（`server.cpp:58` include） | 低 |
| `GET /bthreads` | bthread worker 状态与堆积 | 是 brpc bthread 池，不是 xLLM 的 `ThreadPool`；能判断"HTTP 入口是否被 brpc 工作线程堵住" | `brpc/builtin/bthreads_service.cpp`（`server.cpp:64`）；bvar `bthread_worker_usage` | 低 |
| `GET /hotspots/cpu`（另有 `/hotspots/heap|growth|contention`） | 在线 CPU/堆/锁竞争采样 | CPU profiler 默认关闭（`index_service.cpp:135-136` 会标 disabled）；打开有采样开销 | `brpc/builtin/hotspots_service.cpp`（`server.cpp:67`） | 中；短窗口 |
| `/sockets`、`/ids`、`/protobufs`、`/list`、`/flags`、`/memory` | 单 socket 状态、bthread_id、proto 服务清单、json 方法签名、gflags、malloc 信息 | `/threads`、`/dir` 默认关闭（`server.cpp:119-120`）；`/flags/NAME?setvalue=VALUE` 可**运行期改 flag**（`index_service.cpp:101-104`） | `brpc/builtin/index_service.cpp:99-175` | 低 |
| `GET /health` | xLLM 自定义健康：`{"status":"healthy"/"unhealthy","reason":...}`，不健康返回 503 | **只有 master 节点注册**（`xllm_server.cpp:148-149`），反映 `HealthCheckManager` 对 worker 连接的聚合健康，不反映单次请求慢 | `xllm/server/health_reporter.h:31-70`；`xllm_server.cpp:148-149` | 低 |
| `POST /start_profile`、`POST /stop_profile` | 打开/关闭全局 profiler（详见 §D） | 需 `--enable_online_profile=true` 否则直接拒绝（`control_service_impl.cpp:126-160`）；全局广播、与请求无关 | `xllm_server.cpp:56-57`（路由）；`control_service_impl.cpp:126/:144` | **高**；见 §D/§F |
| 进程日志（VLOG(1)） | PD 的 `allocation_prepare_ms / allocation_rpc_ms / pull_ms / enqueue_ms / total_ms`、FIRST_GENERATION ms 等 | 需 `--v=1` 或 `--vmodule=disagg_pd_scheduler=1`；仅事后 grep，无法聚合/告警 | `disagg_pd_scheduler.cpp:647-654/:965-972/:1239-1314` | 低，但默认不产出 |
| 进程日志（启动行） | worker brpc 地址 `Worker N: server address: <ip>:<port>`；shared memory 名字与大小；profile 输出目录 | 排查 worker 侧 `/vars` 的**唯一**入口 | `worker_server.cpp:151-152`；`shm_channel.cpp:41-46`；`npu_profiler.cpp:122` | 无 |
| bvar 自研族（在 `/vars` 内） | 基线文档 §4 已列；与网络/队列最相关的有 `worker_service_latency_seconds`(:337)、`engine_latency_seconds`(:339)、`request_handling_latency_seconds_*`(:40/:42)、`scheduling_latency_seconds`(:193)、`num_*_requests`(:170/:172/:247-249)、`num_concurrent_requests`(:235)、`server_request_total_limit`、TTFT/ITL/TBT、`mooncake_*`(:112-124)、prefix cache(:61-65) | 见基线文档 §5 的 F01-F09 口径风险；**所有值都是进程本地，dp/tp 多进程需要分别抓** | `xllm/core/common/metrics.cpp` 对应行 | 低（默认常开） |

### A.0 建议的最小采集 runbook（S + H）

```bash
# master
curl -s localhost:8010/vars/rpc_server*_latency     # 服务端方法 p50..p9999
curl -s localhost:8010/brpc_metrics > master.prom     # Prometheus
curl -s 'localhost:8010/rpcz?min_latency=1000000&max_scan=50'   # 需先 /rpcz/enable

# 每个 worker（端口来自启动日志 "Worker N: server address: ip:PORT"）
curl -s <worker_ip>:<port>/vars/worker_service_latency_seconds
curl -s <worker_ip>:<port>/vars/mooncake_transfer*          # PD 场景
curl -s 'http://<master>/rpcz/enable'                       # 打开 span 记录（事后必须 disable）
```

风险：`/rpcz` 打开后写 `./rpc_data/rpcz`，长期开启会带来磁盘 I/O 与内存增长（H，量级取决于 RPC 速率）；`/hotspots/cpu` 属于采样 profiler，短窗口使用。

---

## B. 队列 / 线程池可见性

xLLM 的 `ThreadPool`（`xllm/core/util/threadpool.h:106`）与 `MPMCThreadPool`（`threadpool.h:173`）都**没有**排队时间、队列深度指标：`schedule()` 只是入队 + 唤醒（`threadpool.cpp:113-139`/`:261-286`），`internal_loop` 只取任务执行（`threadpool.cpp:141-…`/`:287-…`），`empty()`（`threadpool.h:144`/`:270`）是唯一的内省接口，`pool_name_` 仅用于日志。

| 池 / 队列 | 配置 flag 与默认 | 今天对 enqueue→start 的可见性 | file:line | 要补什么才能看"排队等待" |
|---|---|---|---|---|
| `LLMMaster.request`（ThreadPool） | `--num_request_handling_threads`，默认 **4**（`service_config.cpp:46`） | **不可见**。`handle_request` 先 `incr_pending_requests(1)`，lambda 内才 `AUTO_COUNTER(request_handling_latency_seconds_*)` → 指标不含池内排队 | 建池 `llm_master.cpp:327-333`；入口 `llm_master.cpp:438-497`（`:451` completion / `:497` chat） | 入队前取时间戳 + 池内 histogram；或线程池统一加 `enqueue_us` 字段 |
| `RequestOutputDispatcher.response`（ThreadPool） | `--num_response_handling_threads`，默认 **4**（`service_config.cpp:50`） | **不可见**。`schedule_response` 按 `response_thread_id` 做 tid 亲和（`request_output_dispatcher.cpp:92-101`），`responsing_latency_seconds_*` 在 runnable 内起算 | 建池 `request_output_dispatcher.cpp:66-75`；入口 `:92-101`；计时 `:144/:179/:245/:299` | 在 `schedule_response` 记录 enqueue ts，runnable 首行算 delta |
| `RequestOutputDispatcher.rpc`（ThreadPool，1 线程） | 硬编码 **1**（`request_output_dispatcher.cpp:76`） | 不可见（批量响应汇总用） | `request_output_dispatcher.cpp:76-78,:203` | 同上（低优先级） |
| `RequestOutputDispatcher.generate_output`（ThreadPool） | 硬编码 **16**（`request_output_dispatcher.cpp:79-86`） | 不可见；detokenize/生成输出线程池，是**响应侧 CPU 热点** | `request_output_dispatcher.cpp:79-86,:151` | 队列深度 gauge + 等待 histogram |
| `RemoteWorker.request`（ThreadPool） | 硬编码 **1**（`remote_worker.h:172`） | **不可见，且这是每个 remote worker 的串行入口**：`step_remote_async` 先入池再进 channel，SHM 模式下整段 worker step 都在这一个线程里（阻塞 `raw_output_read`） | `remote_worker.h:172`；`remote_worker.cpp:190-203`；SHM 写读 `shm_channel.cpp:50-94` | 该池排队 histogram + `remote_worker` 维度（rank） |
| `RemoteWorker.copy`（ThreadPool） | 硬编码 **4**（`remote_worker.h:176`） | 不可见（proto/clone 拷贝） | `remote_worker.h:176` | 同上（低） |
| `WorkerService.request`（ThreadPool） | 硬编码 **4**（`worker_service.cpp:305/:322`） | **部分可见**：brpc 方法级 latency 从收到请求算到发响应，**包含**本池排队；但 `worker_service_latency_seconds` 的 `Timer` 在 lambda 内部（`worker_service.cpp:997`）→ 不含排队，含反序列化+step+序列化 | `worker_service.cpp:305-330`；`:962/:976`（profile）；`:989-1048`（ExecuteModel） | 在 20 处 `threadpool_->schedule` 前取 ts，加 `worker_service_queue_seconds` |
| `WorkerImpl.worker`（ThreadPool） | 硬编码 **1**，且注释明确 INVARIANT 不能 >1（`worker_impl.h:543-549`） | 不可见；所有 step 的串行执行点 | `worker_impl.h:543-549`；`worker.cpp:411-490` | 与 `engine_latency_seconds` 对照即可，优先级低 |
| RateLimiter 准入（非队列） | `--max_concurrent_requests`，默认 **200**（`service_config.cpp:41`） | **拒绝式**：超限返回 `RATE_LIMITED`，**不排队**；`num_concurrent_requests` gauge（`:49/:57`）；`server_request_total_limit` 计数（`:38`） | `rate_limiter.cpp:26-60`；`rate_limiter.h:41-59` | 无需排队时间；建议加"准入等待/拒绝"按模型维度，区分 200 上限造成的尾延迟（E 级） |
| PD prefill dispatch 队列（moodycamel BlockingConcurrentQueue + `pending_dispatch_requests_`） | 无独立 flag（受 `--max_concurrent_requests` 与 dispatch 宽度影响） | `num_queued_requests()` 已把 dispatch backlog 算进去（`disagg_pd_scheduler.cpp:401-403`），但**没有任何 `GAUGE_SET` 导出** → `/vars` 看不到 PD 侧积压 | `disagg_pd_scheduler.cpp:401-424,:448-470` | 周期 `GAUGE_SET(num_pd_dispatch_requests, pending_dispatch_requests_)`（E9） |
| Scheduler 通用队列 gauge | — | 有：`num_running_requests` / `num_waiting_requests`（`scheduler_policy.cpp:980-981`）、fixed/dit 变体（`fixed_steps_scheduler.cpp:456-459`、`dit_scheduler.cpp:234-237`） | 同左；声明 `metrics.h:247-249` | PD 场景需与 decode/prefill 角色区分（H：现无 role 维度） |
| Mooncake transfer batch | `kMaxRegionsPerBatch=4096`（`mooncake_transfer_engine.cpp`） | 无 in-flight 数、无排队；`latency` 覆盖 submit+`wait_batch` 整段 | `mooncake_transfer_engine.cpp:60,675-709` | 拆 submit/wait 两段 + `inflight_batches` gauge（E10） |
| brpc 服务端 bthread 排队 | `--num_threads`（默认 8，`service_config.cpp:39`） | 间接可见：`rpc_server_*_{concurrency,max_concurrency,error,eps,latency}`（`method_status.cpp:41-60`）、`bthread_worker_usage` | `method_status.cpp:41-60`；`server.cpp:292-294`（`rpc_server_<port>_*` 前缀） | 无需新增，文档化即可 |
| SHM 轮询线程（worker 侧） | `--enable_shm` 默认 **false**（`execution_config.h:90`、`runtime/options.h:240`） | 1 个 `std::thread` 每 1µs 轮询 `wait_for_input`（`kNumWaitNanoseconds=1000`，`forward_shared_memory_manager.h:31`） | `worker_service.cpp:564-648`；`forward_shared_memory_manager.cpp:3787-3797` | 轮询等待时长 histogram（E6） |

> 结论 B：**"请求在队列里等了多久"这一个最常规的问题，今天无法从任何指标回答**；能回答的只有"任务开始执行后花了多久"和"处于 pending 的请求数"。

---

## C. 网络 / 通信分层可见性

| 层 | 今天可观测信号 | file:line | 定位"网络导致的慢"的缺口 | 候选 hook |
|---|---|---|---|---|
| L1 HTTP 入口（client→master，SSE） | brpc 方法级 `latency/concurrency/error/eps`、`/status`、`/rpcz` span | `method_status.cpp:41-60`；`brpc/builtin/status_service.cpp`；`rpcz_service.cpp` | 流式响应中 brpc span 的结束点与最后一个 chunk 的关系需要小心（基线文档 §3 已列 `stream_call.h:142/:187/:273`）——**不能用 brpc latency 当 E2E**；入口排队（brpc bthread + `LLMMaster.request` 池）不可分 | 复用 `request_handling_latency_seconds_*` 再加"入池前/后"两段（E1） |
| L2 master→worker RPC（CommChannel，`RemoteWorker.request` 1 线程） | **客户端侧零信号**；只有 as-is 的 brpc `Channel`（pooled，`timeout=-1`、`connect_timeout=-1`、`max_retry=3`，`comm_channel.cpp:32-35`）与 worker 服务端方法指标 | `comm_channel.cpp:32-35`；`remote_worker.cpp:190-203`；`comm_channel.cpp:558-738`（四个 `execute_model_with_brpc`） | 连不上/重试/超时/序列化/反序列化耗时、单次 RPC 端到端、按 worker 拆分——全都没有；且 `-1` 超时意味着网络挂起时**永久等待**（靠上层无兜底） | 在 closure 外包 `Timer`；失败/重试 counter；见 E4/E5 |
| L3 SHM handoff（`--enable_shm=true`） | **完全无指标**。master：`input_write` → 阻塞 `raw_output_read`（`pb_read` 1µs 睡眠轮询）；worker：1 线程 1µs 轮询 `wait_for_input` + `input_read` + step + `raw_output_write` | master `shm_channel.cpp:50-94`、`forward_shared_memory_manager.h:66-101`；worker `forward_shared_memory_manager.cpp:3787-3797`、`worker_service.cpp:564-648` | 单 step 的 SHM 往返、输入等待、输出等待、poll 次数、**fallback 事件**（`shm_channel.cpp:55-58/:70-74` 只 LOG(ERROR)）全部不可见 → 一旦 fallback 发生，延迟台阶无法归因 | E6（a/b/c/d） |
| L4 PD 控制面（Prefill↔Decode 的 `AddNewRequests` / `FirstGeneration` / `release_reservation`） | 仅 `VLOG(1)` ms 日志：`allocation_prepare_ms/rpc_ms`、`prepare_ms/rpc_ms/total_ms`（PULL）、`prepare_ms/enqueue_ms/total_ms`；失败仅 `LOG(ERROR)`/`SetFailed` | `disagg_pd_scheduler.cpp:550,647-654,651-655,781,842,952,965-972`；decode 侧 `:1137,1239-1301,1305-1314,1321`；服务端 `disagg_pd_service_impl.cpp:213,223,372,433,462` | 无 bvar → 无法聚合/告警/P99；无 role/instance 维度；无失败原因分类 | E8/E9 |
| L5 PD 数据面 PULL（mooncake `pull_kv_blocks`） | `mooncake_transfer_completed_total_{read,write}`、`..._bytes_total_*`、`..._failed_total`、`latency_microseconds_{read,write}`；提交点 `mooncake_transfer_engine.cpp:675-709`（`wait_batch` 在 `:60`） | `metrics.cpp:112-124`；`mooncake_transfer_engine.cpp:60,675-709`；协调器 `kv_cache_transfer_coordinator.h:36`（`prefetch_timeout_ms`，0→-1 无超时） | 粒度=batch；无 request/seq 归属、无排队、无失败原因、无 session/QPS；engine submit 与 wait 不可分 | E10 |
| L5' PD 数据面 PUSH（默认模式） | **无指标**（`kv_cache_transfer_mode` 默认 `"PUSH"`，`disagg_pd_config.h:63`） | `disagg_pd_config.h:52,63`；`worker_service.cpp:808`（TransferBlocks 直接同步执行，不走 threadpool） | 默认模式反而最黑；只能靠 decode 侧 VLOG 的 `prepare_ms` 间接近似 | E15（需先确认 push 实现点，H） |
| L6 KV 预取 / 存储流（mooncake stream） | `ClientStreamReceiver` 记录 `stop_sent_/failed_/worker_quiescent`，仅日志；`prefetch_from_storage` 无指标 | `comm_channel.cpp:431-540`；`kv_cache_transfer_coordinator.cpp`（pull/transfer/prefetch 编排） | 预取命中率、流控等待、idle timeout 全部不可见 | E10 扩展 |
| L7 HCCL 集合通信（TP/DP/EP、MoE dispatch/combine、权重 allgather） | **零 bvar**。`ProcessGroup` 全部 async 接口无计时；MoE 走 `mega_moe_comm`/hccl 句柄 | `process_group.h:46-130`；`process_group.cpp:224(allreduce_async),231/236(allgather),245(allgather_base_async),275(reduce_scatter),290(broadcast),324(alltoall),352(hccl_comm_name),580(hccl_comm),586(mega_moe_comm)`；服务 `worker_service.cpp` ProcessGroupTest | CPU 侧计时对"异步 launch"不准（H）；跨 rank 快慢不均（straggler）无法定位；只有 profiler 能看 | E11（CPU 侧 counter 先落到"调用次数/字节"，耗时以 profiler 为准） |
| L8 权重/P2P link（`link_cluster`/`link_p2p`） | 只有 brpc RPC 本身；启动期为主 | `comm_channel.h:56-66`；`comm_channel.cpp` | 启动慢归因；运行期无流量 | 低优先级（不做） |
| L9 主机网络/网卡 | **xLLM 不采集**（无 ethtool/IB counters） | — | 需外部 node exporter / `ip -s link` / `ibstat` 补 | 运维侧（H） |

> 结论 C：**只有 L1（入口 brpc）与 L5（mooncake batch 级）是"有数"的**；L2/L3/L4/L6/L7 只能靠日志或 profiler。对"网络诱导的慢"，现有手段的极限是：**从 master 的 brpc 方法延迟 + worker 的 `worker_service_latency_seconds` 两端夹逼**，中间段（尤其 SHM）无法定量。

---

## D. Profiler 能力（/start_profile、/stop_profile、msprof、npu_timeline.py）

### D.1 控制链（S）

1. 路由：`start_profile => StartProfileHttp, stop_profile => StopProfileHttp`，只注册在 master 节点（`xllm_server.cpp:56-57`、路由选择 `:78-90`）。
2. `ControlServiceImpl::start_profile/stop_profile` 校验 `ProfileConfig::enable_online_profile`，未开启直接返回失败；然后 `master_manager_->start_profile(&error_message)`（`control_service_impl.cpp:126/:144`）。
3. `LLMMaster::start_profile/stop_profile` → `dispatch_engine(...)`（`llm_master.cpp:612/:619`）。
4. `Engine::start_profile` 默认 `LOG(ERROR) << "not implemented"`（`engine.h:58-66`）；`LLMEngine` 覆写：加 `profile_mutex_`、`profile_workers(true)`、**部分失败立即回滚**（`llm_engine.cpp:652-666`、`llm_engine.h:83-89`）。
5. 每个 `WorkerClient::start_profile_async` → `CommChannel::start_profile`（brpc 同步 RPC，`comm_channel.cpp:405/:418`）→ `WorkerService::StartProfile` 在 4 线程池内 `worker_->start_profile()`（`worker_service.cpp:962/:976`）→ `Worker/WorkerImpl::start_profile`（`worker.cpp:470-490`；`worker_impl.cpp:2007/2063`）。
6. 后端实现在 `TorchProfiler`（`torch_profiler.cpp:76/106`）、`CudaProfiler`（`cuda_profiler.cpp:35/55`）、`NpuProfiler`（CANN，`npu_profiler.cpp:33-172`）、`NpuTorchProfiler`（`npu_torch_profiler.cpp:54/141`）。

### D.2 语义与限制（S）

- **全局、与请求无关**：一次 start 影响所有 worker；没有 per-request / per-batch 作用域，也没有采样率参数。
- **与请求 trace 无关联**：trace 里只有算子/kernel/stream，没有 request_id；`/stop_profile` 只返回 bool，不返回任何 trace 路径（路径只出现在 worker 日志）。
- **同步广播**：`start_profile` 在所有 worker 上串行完成才返回；`stop_profile` 让每个 worker **同步导出** trace（`npu_torch_profiler.cpp:141-186`），导出耗时全部算在停采窗口里 → **停采瞬间会卡住 step**。
- **幂等**：重复 start/stop 不报错，但每次都新建输出目录/文件（`make_trace_path` 带毫秒时间戳，`torch_profiler.cpp:44-55`）。
- **NPU torch 后端前置条件**：一进程一 worker（`npu_torch_profiler.cpp:58-66`）；`--enable_task_pipeline=false`（`worker_impl.cpp:2007` 分支的 hard requirement）；必须与 start 同线程 stop（owner 校验）。
- **cuda 后端**：只 `cudaProfilerStart/Stop`（`cuda_profiler.cpp:42/62`），必须外部 `nsys --capture-range=cudaProfilerApi`，否则没有任何数据。
- **ascend（CANN）后端**：`aclprofInit/Start/Stop/Finalize`（`npu_profiler.cpp:94/113/135/161`），activities = `ACL_PROF_ACL_API | ACL_PROF_TASK_TIME | ACL_PROF_HCCL_TRACE`（`:30-31`）→ **覆盖 NPU 任务与 HCCL**，但采不到 PyTorch CPU 栈；会话目录 `xllm_npu_<pid>_XXXXXX`（`mkdtemp`，`:46-50`）；导出靠 `msprof --export=on --output=<dir>`（`:167-168` 的日志提示）；与外部 `PROFILING_MODE=dynamic` 冲突（在线采集文档 S）。
- **torch(非 NPU) 后端**：Kineto，activities=CPU+GPU，`report_input_shapes/profile_memory/with_stack/with_flops/with_modules` 全 false（`torch_profiler.cpp:86-95`）；输出 `<dir>/xllm_rank<rank>_<pid>_<ms>.pt.trace.json`（`:44-55`）。

### D.3 后端矩阵

| 后端 | 平台 | 产出 | 输出位置 | 关键前置 | 能看通信吗 |
|---|---|---|---|---|---|
| `torch`（默认） | CUDA/DCU/MUSA | Kineto Chrome trace | `<profile_dir 或 cwd>/xllm_rank<rank>_<pid>_<ms>.pt.trace.json`（`torch_profiler.cpp:44-55`） | 无外部工具 | CUDA NCCL kernel 可见（在算子级），无请求关联 |
| `torch` | NPU | torch_npu trace（`trace_view.json`） | `<dir>/xllm_rank<rank>_<pid>_<ts>_ascend_pt/ASCEND_PROFILER_OUTPUT/trace_view.json`（`npu_torch_profiler.cpp:165`） | 一进程一 worker；`--enable_task_pipeline=false` | **能**：HCCL 事件在 trace 中（含 commName/bandwidth） |
| `ascend` | NPU | CANN profiling 原始数据 + `msprof` 导出 | `<profile_dir>/xllm_npu_<pid>_XXXXXX`（`npu_profiler.cpp:46-50`） | 需 `msprof --export=on --output=`；不能与其他 dynamic profiler 同时用 | **能**：`ACL_PROF_HCCL_TRACE`（`:30-31`） |
| `cuda` | CUDA | 无内置产物 | 由 nsys 生成 `.nsys-rep` | 必须 `nsys --capture-range=cudaProfilerApi --capture-range-end=repeat` | 能（nsys NCCL/NVTX），但需要外部工具链 |

### D.4 采集窗口与请求关联（当前能力边界）

- 只能按"时间窗口"归因：`start_profile` 时间点 t0、`stop_profile` 时间点 t1，在 master/worker 日志里都有（`npu_profiler.cpp:107-122` 等），但**不同进程的时钟需自行对齐**（H：依赖 NTP/同机）。
- **没有任何 request_id / batch_id 维度**：一个 step 里混了多个请求，trace 无法拆分。`MstxRange`/`LLM_MSTX_RANGE`（`mspti_helper.h:27-42`）是唯一现成的关联脚手架，但全仓 **0 调用点**；且析构函数 `aclrtSynchronizeStream`（`mspti_helper.cpp:40-43`）会**串行化异步流**，不能拿去包 async op；`MsptiMetrics`（事件收集）只在 `USE_MSPTI=ON` 时编译（`mspti_helper.cpp:45-207`、option 定义 `xllm/CMakeLists.txt:133`、默认 OFF）。
- 因此现状下"某请求 TTFT 慢，是因为哪次 HCCL / 哪次 kernel"**无法直接回答**，只能靠"时间重叠 + 算子名 + 步进节奏"人工推断（H）。

### D.5 `tools/npu_timeline.py` 实测分析（R + S）

脚本定位（S）：把 **mspti 事件日志**（`mspti_helper.cpp` 输出的 `{"AscendKind":...}` JSON 行）转成 Chrome Trace 的离线工具，与 `/start_profile` 产出**无关**（两套路径：一个吃 torch_npu trace，一个吃 mspti 日志）。

- CLI：`-i/--input`（默认 `./node_0.log`）、`-o/--output`（默认 `mspti_chrome_trace.json`）（`tools/npu_timeline.py:495-508`）。
- 解析：逐行找 `"AscendKind"`，从第一个 `{` 起 `json.loads`（`:309-323`）——因此**能容忍 glog 行前缀**（S）。
- 输出：Chrome trace JSON；MARKER→`Marker` 区间（需 start/end 配对，`flag` 32/4 视为 end，`:370,433`）、MEMORY→`memory_alloc/memory_free`、KERNEL→算子区间；`ts/dur` 除以 1000（按 ns→µs，`:343-352,466-470`）；pid 由 deviceId 分配，deviceId<50 归为 "CPU Process"（`:380-400` 一带）。

本轮实测（R，用 bundled Python 3.12.14 + 合成 mspti 日志，**不是** xLLM 真实日志）：

1. **Python 版本硬门槛**：脚本类注解用了 PEP 604（`dict[str, str | int]`，`:44`），系统 `python3` 是 **3.9.6** → 直接 `TypeError: unsupported operand type(s) for '|'`。必须 3.10+。
2. KERNEL+MEMORY 合成日志 → 成功生成 4 个事件（2 个 `process_name` 元数据 + kernel + memory），说明主路径可用。
3. **HCCL 事件被丢弃**：即使日志里有 `{"AscendKind":"HCCL",...}`（`mspti_helper.cpp:161-180` 确实会产出），脚本的过滤条件只保留 `MARKER/MEMORY/KERNEL`（`:320`）→ **这个 timeline 看不到集合通信**，与"网络/通信排查"目标直接冲突。
4. MARKER 缺 end 时 `KeyError`（`:442`）；start+end 配对时可正常输出（duration 由 end-start 合成，`:449-452`）。
5. `mspti` 事件本身只有 `deviceId/streamId/start/end/bandWidth`，**没有 request 关联字段**（`mspti_helper.cpp:124-180`）。

### D.6 `msprof` 外部采集

- ascend 后端把 CANN 原始 profiling 落到会话目录，日志提示 `Export with: msprof --export=on --output=<dir>`（`npu_profiler.cpp:167-168`）；导出后得到算子/HCCL 时间线（S：文档与源码提示；具体导出命令参数组合未在本机验证 H）。
- 冲突：不能与外部已开的 `PROFILING_MODE=dynamic` 同时启用（在线采集文档 S）。

---

## E. 缺口与建议新增（按"能否定位网络/通信根因"排序）

> 形态建议：优先**加在现有 metrics.cpp 体系里**（COUNTER/GAUGE/HISTOGRAM），成本低、默认常开；采样类（span/trace）只在故障窗口开。括号内为估算成本（H）。

| # | 缺什么 | 现状证据 | 实现位置（file:line） | 建议指标/形态 | 成本 |
|---|---|---|---|---|---|
| E1 | 请求处理池 enqueue→start 等待 | `AUTO_COUNTER` 在 runnable 内起算（`llm_master.cpp:497/:451`） | `llm_master.cpp:484`（`handle_request`）取 `now`；在 `:497/:451` 计算 delta | `HISTOGRAM request_handling_queue_latency_seconds_{chat,completion}`（定义加在 `metrics.cpp:40/42` 附近 + `metrics.h` DECLARE） | 2 次 `clock_gettime`/请求（<0.2µs） |
| E2 | 响应池 enqueue→start 等待 | `request_output_dispatcher.cpp:92-101` 无时间戳 | `request_output_dispatcher.cpp:92-101`；消费点 `:144/:245/:299` | `HISTOGRAM response_queue_latency_seconds_{stream,non_stream}` | 同上 |
| E3 | 线程池通用可见性（所有池） | `ThreadPool` 无 enqueue ts / 队列深度（`threadpool.h:106-171`、`threadpool.cpp:113-160`） | `threadpool.h:140/144/213`、`threadpool.cpp:113/261`；`pool_name_` 已存在可做维度 | `GAUGE threadpool_queue_length{pool}`（MULTI_GAUGE：pool_name）+ `HISTOGRAM threadpool_task_wait_seconds{pool}` | 每任务 ~50ns + 每池 1 atomic（H） |
| E4 | RemoteWorker（master→worker）排队与失败 | `remote_worker.cpp:190-203` 无时间戳；失败只 `promise.setValue(nullopt)` | `remote_worker.cpp:193`（入池前 ts）、`comm_channel.cpp:558-738` closures | `HISTOGRAM remote_worker_request_queue_seconds` + `MULTI_COUNTER remote_worker_rpc_failures_total{method}`（rank 维度） | 低 |
| E5 | CommChannel 客户端 RPC 延迟/重试/超时 | `comm_channel.cpp` 全文零指标；`timeout=-1`（`:32-35`） | 每个 stub 调用点（`comm_channel.cpp:405-738`；`init_model_async`/`activate`/`sleep`/`wakeup` 等） | `MULTI_HISTOGRAM comm_rpc_latency_seconds{method}` + `comm_rpc_error_total{method}` + `comm_rpc_retry_total` | 低；注意 bvar 多维度上限 1024 |
| E6 | SHM 三段耗时 + fallback 事件 | `shm_channel.cpp:50-94` 无计时；fallback 仅 `LOG(ERROR)`（`:55-58/:70-74`） | (a) master `shm_channel.cpp:50-94`；(b) worker `forward_shared_memory_manager.cpp:3787-3797`；(c) `worker_service.cpp:564-648` | `HISTOGRAM shm_input_wait_seconds`（worker）、`HISTOGRAM shm_master_roundtrip_seconds`（master）、`COUNTER shm_fallback_brpc_total` | 低（仅 CPU 侧）；**不要**在轮询里加 sleep 变化 |
| E7 | WorkerService RPC 池排队 | `Timer` 在 lambda 内（`worker_service.cpp:997`），`worker_service_latency_seconds` 不含排队 | `worker_service.cpp:992` 前（以及另外 ~19 处 `threadpool_->schedule`，`:668-1213`） | `HISTOGRAM worker_service_queue_seconds`（可按 method 维度或单一值） | 低；建议封装 helper 统一埋 |
| E8 | PD 控制面 RPC 延迟从 VLOG 升级为指标 | `disagg_pd_scheduler.cpp:647-654/:965-972` 仅 `VLOG(1)` | 同上（保留 VLOG，另加 bvar） | `HISTOGRAM pd_allocation_rpc_seconds`、`pd_first_generation_rpc_seconds`、`pd_pull_kv_seconds`、`pd_dispatch_enqueue_seconds`；`COUNTER pd_rpc_errors_total{phase}` | 低 |
| E9 | PD dispatch 队列深度与 decode 侧积压 | `num_queued_requests()` 已算 backlog（`disagg_pd_scheduler.cpp:401-403`）但未导出 | `disagg_pd_scheduler.cpp:401-403`；周期更新可挂 `continuous_scheduler.cpp:238` | `GAUGE num_pd_dispatch_requests`、`GAUGE num_pd_pending_dispatch` | 低 |
| E10 | Mooncake submit 与 wait 拆分、in-flight、失败原因 | 一个 Timer 包住 submit+wait（`mooncake_transfer_engine.cpp:675-709`） | `mooncake_transfer_engine.cpp:675-709` | `HISTOGRAM mooncake_transfer_wait_seconds`、`GAUGE mooncake_inflight_batches`、`MULTI_COUNTER mooncake_failed_reason_total{reason}` | 低 |
| E11 | HCCL 调用计数/字节（耗时交给 profiler） | `ProcessGroup` 全 async 无埋点（`process_group.h:46-130`） | `process_group.cpp:224-330`（allreduce/allgather/reduce_scatter/broadcast/alltoall） | `MULTI_COUNTER hccl_calls_total{op}` + `MULTI_COUNTER hccl_bytes_total{op}`（size bucket）；耗时用 E12/profiler | 低（CPU 侧） |
| E12 | 请求/step ↔ trace 关联（现有脚手架不可用） | `MstxRange/LLM_MSTX_RANGE` 0 调用点（`mspti_helper.h:27-42`），`~MstxRange` 有 `aclrtSynchronizeStream`（`mspti_helper.cpp:40-43`） | 新增不落同步的 `mstxRangeStartA/End` 包装，用于 `worker_impl.cpp` step 边界/scheduler batch 边界；采样类，需 `USE_MSPTI=ON`（`xllm/CMakeLists.txt:133`） | trace marker（`req:<id>`/`step:<n>`）或退化为 step 边界日志（t0/t1） | **中高**；同步风险见 F |
| E13 | step 内部分段（prepare/forward/sample/serialize） | `engine_latency_seconds` 单值（`llm_engine.cpp:516/581`） | `llm_engine.cpp:516-581`；worker 侧 `worker_service.cpp:997-1048` | `HISTOGRAM worker_step_{prepare,forward,sample,serialize}_seconds` | 低 |
| E14 | 网络诊断 runbook 与采集脚本（把 A 表落成工具） | `/rpcz` 默认关、worker 端口临时、指标口径有多处坑（A 表） | 新增 `tools/observability/collect_network_snapshot.sh`（仓库外亦可） | 一次性抓取 master+workers 的 `/vars/<glob>`、`/brpc_metrics`、`/rpcz?min_latency=`，附 t0 | 低 |
| E15 | PUSH 模式 KV 传输可见性 | `kv_cache_transfer_mode` 默认 `"PUSH"`（`disagg_pd_config.h:63`），无指标 | 需先定位 PUSH 的完成点（H：`worker_service.cpp:808` TransferBlocks 或异构 push 实现） | `HISTOGRAM pd_push_kv_seconds` + bytes counter | 中（需确认实现，H） |

---

## F. 安全与不打扰约束

1. **打点只用 CPU 侧时钟**：`clock_gettime` 量级 ~25ns（H）；禁止在热路径加 `cudaStreamSynchronize`/`aclrtSynchronizeStream`/D2H 拷贝——`MstxRange::~MstxRange` 就是反例（`mspti_helper.cpp:41`），用它包异步 step 会把 overlap 变成串行。
2. **profiler 采集窗口**：torch_npu/CANN 采集期间开销显著（H：量级取决于模型与活动数），**不可在性能基线时开启**；`start/stop` 是全局广播，`stop` 的 trace 导出同步发生在 worker 线程（`npu_torch_profiler.cpp:141-186`）→ 停采瞬间的 step 时长会被污染，报告里必须把该区间剔除。建议窗口 = 数十个 step（H），并在 master/worker 日志记录 t0/t1 以便剔除。
3. **采集会改变被测行为**：`--enable_task_pipeline=false` 是 NPU torch profiler 的硬前置（`worker_impl.cpp:2007` 分支），即"能采集的配置"与"生产最优配置"不同；`ascend` 后端与 `PROFILING_MODE=dynamic` 冲突。任何 profiler 结论都要标注"该窗口下的配置"。
4. **`/rpcz` 只在故障窗口开**：开启后 span DB 写 `./rpc_data/rpcz`（`span.cpp:44-45`），保留 3600s（`:63`）；高 QPS 下会持续产生磁盘 I/O 与内存（H）。用完 `/rpcz/disable`（`rpcz_service.cpp:97`）。
5. **指标维度别放请求 id**：bvar 多维度默认上限 1024（基线文档），把 request_id 当 label 会撑爆并污染 `/vars` 输出；请求级信息走采样/trace。
6. **`/vars` 全量 dump 非免费**：改用 `/vars/<glob>`（`index_service.cpp:106-111`）；Prometheus 抓取间隔 ≥15s（H）。
7. **SHM 忙等线程本就占 1 核**（`worker_service.cpp:564-648`）；增加打点时不要在该循环里加日志或 syscall（会放大 1µs 轮询的抖动）。
8. **线程池打点的锁问题**：若用 bvar `LatencyRecorder` 在每任务上更新，注意其内部开销与竞争（H）；更轻的做法是线程局部累加 + 周期 flush，或直接 `HISTOGRAM`（xLLM 现有宏）并核实其内部实现（`metrics.h`）。
9. **不要为定位网络问题改 brpc Channel 选项**（如把 `timeout_ms` 从 -1 改成正数）——会改变故障语义；只在观测层加计时。
10. **版本/口径标注**：本文所有 S 结论绑定上述 commit；升级后 `metrics.cpp` 行号会漂移，引用时请用符号名一起 grep。

---

## 附：本次盘点的证据边界

- 只读仓库 + 运行 `tools/npu_timeline.py`（合成日志）两个动作；**未**运行 xLLM、未打开任何 `/vars`、`/rpcz`、未采集真实 trace。
- 因此本文中"某入口能看到什么"是对源码路径的静态推断（S），**没有实测样本**（例如 `/rpcz` 打开后 grep 到 `xllm.proto.WorkerService_ExecuteModel` 的 span 属于 R 级空位）。
- 真实运行期的待验证清单（建议由 runtime scout 补 R）：
  1. master `8010/brpc_metrics` 是否含 xllm 全族（64 位 double gauge 精度/命名是否如基线文档所述）；
  2. worker 临时端口是否可达、`/vars` 是否有 `worker_service_latency_seconds` 的 p99；
  3. `--enable_shm=true` 与 `false` 两种模式下 `engine_latency_seconds` 的差值（这是 SHM 是否生效/回退的唯一现成间接信号）；
  4. `/rpcz/enable` 后是否能看到 `ExecuteModel` 服务端 span、`min_latency` 过滤是否可用；
  5. PD 场景 `mooncake_transfer_latency_microseconds_*` 的单位与量级（微秒）；
  6. `enable_online_profile=true` 下 `/stop_profile` 的尖峰长度（验证 F2）。
