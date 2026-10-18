# xLLM 内存/资源侧观测面盘点（task-2：KV cache / device / host memory）

> **范围与证据等级**：基线 commit `4b05c484`，本地只读源码核验（未构建、未运行、未起服务）。
> `S` = 本笔记直接读源码确认（附 `file:line`）；`S(base)` = 沿用 `brpc_metrics_baseline.md` / `metric_inventory.json` 且本轮未逐行复验；`H` = 假设/推断，需 runtime（R）验证。
> **本文档不做 R 结论**：所有"实际占用多大/是否真的 OOM"的判断均属 H，需最小实验（`/metrics` 对比 + allocator 打点）后再定。
>
> **基线**：`docs/research/observability/brpc_metrics_baseline.md` §3（bvar 宏语义）、§4（导出契约）、§6（F05/F06/F07）、§12（129 个公共指标族）；`metric_inventory.json`。跨文档：`docs/research/observability/notes/instrumentation_points.md`（task-1，请求级 span 落点；本文索引内存侧）。
> 全部路径相对仓库根 `<repo-root>`。

---

## 0. 结论摘要（先读这 12 条）

1. **内存类公共指标实际可用面很窄**：129 个族里与内存直接相关的约 26 个，其中 9 个是"声明未接线"（`xllm_cpu_num`/`xllm_cpu_utilization`/`xllm_gpu_num`/`xllm_gpu_utilization`/`total_activation_size_in_kilobytes` + 4 个 `num_*_preempt_*`）(`metrics.cpp:238-241,345-346,175-182`, S(base))。
2. **KV 逐 rank 视野缺失**：`BlockManagerPool` 的 `num_free_blocks()`/`num_used_blocks()`/`num_blocks_in_prefix_cache()` 本来返回 **per-DP vector**（`block_manager_pool.cpp:357-384`，S），但公共 Gauge 只导出 `min`/`max` 单值（`scheduler_policy.cpp:987-992`，S）；`kv_cache_utilization_perc` 也是各 DP 的 `min`（`block_manager_pool.cpp:386-396`，S）。rank skew 在导出面被主动抹平。
3. **fixed_steps 调度器写的是向量长度而不是块数**：`GAUGE_SET(num_blocks_in_prefix_cache, ...num_blocks_in_prefix_cache().size())`、`num_free_blocks`、`num_used_blocks` 同样取 `.size()` → 值 = dp_size（`fixed_steps_scheduler.cpp:455-472`，S）。这是**语义性 bug**，Rec/DiT 路径把"块数"变成了"DP 数"。
4. **`kv_cache_utilization_perc` 实际是 0–1 比例**，没乘 100（`composite_block_manager.cpp:978-984`，S），与命名/基线 F06 一致但极易被当百分比读。
5. **`total_kv_cache_size_in_kilobytes` 是估算容量，不是实测占用**：启动期由 `KVCacheEstimator::estimate()` 的 `cache_size_in_bytes` 写入（`kv_cache_manager_factory.cpp:78-80`，S）；`active_kv_cache_size_in_kilobytes` = 全局容量 × occupied_slots 比例（`scheduler_metrics.cpp:182-195`，S），不是 per-rank 实际分配字节。
6. **权重口径有两套且不一致**：`weight_size_in_kilobytes = total - available`（含 torch cache，`kv_cache_estimation.cpp:763`，S）；`DeviceStats.weights_memory = total - available - torch_cache`（`worker_impl.cpp:738-742`，S）。且估算在 per-worker 循环内反复 `GAUGE_SET`，**单维度 Gauge 会被最后一个 worker 覆盖**（`kv_cache_estimation.cpp:751-777`，S）。
7. **`active_activation_memory` 不是活跃 tensor 求和**：它是自上次 update 以来 `DeviceMonitor::monitor_buffer()` 观察到的**最大值**，来源是 ATB workspace 的 `bufferSize` 请求（`atb_workspace.cpp:35-49` → `device_monitor.cpp:86-89,92-97`，S）；且只在 stream 同步/默认流同步后更新（`llm_worker_impl.cpp:415-423`、`worker_service.cpp:1165,1185`，S）。
8. **KV evict 有耗时无次数**：`prefix_cache_latency_seconds_evict` 是 AUTO_COUNTER 的累计时间（`block_manager_impl.cpp:145-152`，S）；`evict()` 返回的块数没有进入任何指标（`prefix_cache.cpp:237-269`，S）。抢占同理：`num_preempted_requests` 在 SchedulerPolicy 是**每步 set**、在 FixedSteps 是 **GAUGE_ADD 累计**（`scheduler_policy.cpp:983` vs `fixed_steps_scheduler.cpp:461`，S），两种语义混在同一名字上。
9. **运行期拿不到 device free memory 的指标**：公共指标里没有任何 `device_memory_free` 家族；可用的只有启动期 `DeviceMonitor` 快照与两个 worker RPC：`ProfileDeviceMemory`（`worker_service.cpp:701-715`，S）**有副作用**——它调用 `estimate_kv_cache_capacity_async()`，内部先 `Device::empty_cache()`（`worker_impl.cpp:727-745`，S），不能当轻量 scrape 用；`GetActiveActivationMemory`（`worker_service.cpp:1208-1220`，S）依赖上面第 7 条的采样值。
10. **精确的分配器账本存在但没有指标出口**：`PhysicalPagePool`（free/total pages，`physical_page_pool.cpp:331-334`）、`VirtualKVCachePageAllocator`（per model+dp 的 inuse/reserved/free/total virt pages，`virtual_kv_cache_page_allocator_impl.cpp:305-338`）、`WeightVirtualMemoryManager`（`WeightAllocationInfo{base_ptr,num_pages}`，`weight_allocation.h:32-35`）都可精确读，但无 bvar/Prometheus 暴露。
11. **传输侧**：mooncake 有 bytes/completed/failed/latency 计数（`metrics.cpp:112-125`，S），覆盖全部 `move_memory_regions` 路径；但**共享内存尺寸配置疑似单位错位**：flag 注释写 "default is 1GB/128MB"，值却是 `1024`/`128`（`execution_config.cpp:93-101`，S），而 `SharedMemoryManager` 直接按 **字节** 使用（`shared_memory_manager.cpp:41-70`，S），全仓未见 ×1024×1024 的换算 → 默认 shm payload 只有 1024 B，`pb_write` 必然溢出回退 brpc（`forward_shared_memory_manager.h:65-88`，H 待验证运行时是否另有换算路径）。
12. **Host 侧（host prefix cache / offload / 进程 RSS）完全没有账**：host blocks 由 `host_blocks_factor × device_blocks` 估算（`llm_engine.cpp:361`、`hierarchy_kv_cache_transfer.cpp:233-261`，S），host KV/shared memory/mooncake 注册缓冲都不产生内存指标；全仓无 `getrusage`/`/proc/self/status` 读取（S）。

---

## 1. 观测面分层（谁在什么进程里、什么线程读）

```
device 层      Device::get_device_mem()（cudaMemGetInfo/aclrtGetMemInfo/...）   ← 同步驱动调用，任意线程
allocator 层   PhysicalPagePool / GlobalVirtualMemoryRegion / VirtualKVCachePageAllocator / WeightVirtualMemoryManager / torch caching allocator / SharedVMMAllocator
KV 管理 层     BlockManagerPool(dp) → CompositeBlockManager → leaves(KV/SWA/C4/C128/EMBEDDING/LINEAR) + PrefixCache + Host 层(HierarchyBlockManagerPool)
聚合/导出 层   DeviceMonitor(进程内单例, 无锁) → SchedulerMetrics(调度线程) → bvar(metrics.cpp) → brpc /metrics
传输 层        ForwardSharedMemoryManager / mooncake transfer engine / host KV transfer
```

进程/线程边界（S）：
- 调度与 `SchedulerPolicy::report_metrics` 在调度线程（`continuous_scheduler.cpp:376-445` 每步）；`SchedulerMetrics::update` 也在同一线程（`continuous_scheduler.cpp:636-637`），其中 `get_active_activation_in_bytes()` 会 **阻塞式 fan-out RPC 到所有 worker**（`distributed_worker_manager.cpp:133-149`，S）。
- `DeviceMonitor` 是进程内单例、`std::map` + 裸 `int64_t`、**无锁/非线程安全**（`device_monitor.h:32-51`、`device_monitor.cpp:46-97`，S）；写入来自 worker 执行线程/RPC 线程池，读取来自调度线程或另一 worker 线程 → 并发读写（H：当前未观察到崩溃，但存在 torn read/数据竞争，C++ 层面 UB）。
- 每个 rank 独立进程 → `DeviceMonitor`/bvar 都是**进程级**；Prometheus 侧同名指标若不加 `instance`/`rank` 标签会混叠（S(base) + H，取决于 scrape 配置）。

---

## A. 信号表

### A1. 已导出（bvar → `/metrics`，类型全部是 gauge，见基线 §4.1 S(base)）

| 信号 | 来源 API / 字段 + file:line | 单位/缩放 | 粒度 | 精度类 | 读取成本 | 建议用途 |
|---|---|---|---|---|---|---|
| `total_memory_size_in_kilobytes` | `GAUGE_SET(total_memory_size_in_kilobytes, total/1024)` `kv_cache_estimation.cpp:765`；int64 bytes 来自 `Device::total_memory()` `device.cpp:148` | KB（`/1024`） | 进程（每 rank 一个值），**只在启动期按 worker 循环写、最后一个 worker 生效** | 估算（依赖启动快照） | 启动期一次 | scrape（容量基线）/ 不可用于变化检测 |
| `weight_size_in_kilobytes` | `kv_cache_estimation.cpp:763`：`(total-available)/1024` | KB | 同上（单值，丢 per-rank） | 估算（含 torch cache 的残差） | 启动期一次 | scrape（仅作量级参考，勿做趋势） |
| `total_kv_cache_size_in_kilobytes` | `kv_cache_manager_factory.cpp:78-80`：`capacity.cache_size_in_bytes()/1024` | KB | 进程（全局容量） | 估算（布局公式，假设整块可用） | 启动期一次 | scrape（分母/容量基线），与 `active_kv` 配对 |
| `total_activation_size_in_kilobytes` | 声明 `metrics.cpp:345-346`，无写入点 | KB | — | **占位**（0/无值） | — | 不可用（F05 继承） |
| `active_kv_cache_size_in_kilobytes` | `MULTI_HISTOGRAM_OBSERVE(..., dp_rank)` `scheduler_metrics.cpp:188-195` | KB；= `occupied_slots_ratio × GAUGE_VALUE(total_kv_cache_size_in_kilobytes)` | per `dp_rank`（每步一个样本） | 估算（比例×全局容量；非实测字节） | 调度步内；bvar 窗口聚合 | scrape（趋势）；**不能当 per-rank 实际占用** |
| `prefill_active_activation_size_in_kilobytes` | `scheduler_metrics.cpp:200-208` | KB | per `dp_rank`/步 | 估算（见第 7 条：ATB workspace 峰值的下界） | 同上 | scrape（趋势） |
| `decode_active_activation_size_in_kilobytes` | `scheduler_metrics.cpp:196-205` | KB | per `dp_rank`/步 | 同上 | 同上 | scrape（趋势） |
| `num_free_blocks` | `scheduler_policy.cpp:989-990`：`util::max(manager->num_free_blocks())`；数据来自 `block_manager_pool.cpp:370-376` per-DP vector | 块数 | 单值 = **跨 DP 最大**（丢 skew） | 精确（atomic 读，`block_manager_impl.h:76`） | 每调度步，O(dp) | scrape；**勿与 `num_used_blocks` 相加**（见 §B2） |
| `num_used_blocks` | `scheduler_policy.cpp:991-992`：`util::min(...)`；`block_manager_pool.cpp:378-384` | 块数 | 单值 = 跨 DP 最小 | 精确 | 每步 | scrape |
| `num_blocks_in_prefix_cache` | `scheduler_policy.cpp:987-988`：`util::min(...)`；`block_manager_impl.h:67-73` → `PrefixCache::num_blocks()` | 块数 | 单值 = 跨 DP 最小 | 精确（`prefix_cache.h:105-109`，CHECK 一致性） | 每步 | scrape |
| `kv_cache_utilization_perc` | `scheduler_policy.cpp:985-986`；实现 `block_manager_pool.cpp:386-396`（min over DP）→ `composite_block_manager.cpp:978-984`（`used/total`，0–1） | **比例 0–1（未 ×100）** | 单值 = 跨 DP 最小 | 精确（含 §B2 口径问题） | 每步 | scrape；告警阈值需按 0–1 设 |
| `num_preempted_requests` | `scheduler_policy.cpp:983`（每步 `GAUGE_SET`）；`fixed_steps_scheduler.cpp:461`（`GAUGE_ADD`） | 个/步 vs 累计 | 单值 | 精确但**语义不一致** | 每步 | scrape；需按 scheduler 类型解释 |
| `prefix_cache_token_hit_rate_perc` | `metrics.cpp:97`（`record_prefix_cache_hit_metrics` :86-99） | 百分比（`hit/prompt*100`） | 进程级、按 prefill 调用更新 | 精确（token 维度） | 每 prefill（含 mutex） | scrape（趋势） |
| `prefix_cache_prompt_tokens_total` / `prefix_cache_hit_tokens_total` | `metrics.cpp:68-70`；写 `metrics.cpp:93-94` | tokens 累计 | 进程级 | 精确 | 每次 prefill | scrape；注意导出类型为 gauge（基线 §4.1） |
| `prefix_cache_latency_seconds_{match,insert,evict}` | `metrics.cpp:61-65`；写 `block_manager_impl.cpp:145,168,195,204` | 秒（AUTO_COUNTER 累计） | 进程级、每次调用 | 精确计时样本 | 每 block 操作 | trace/趋势；**不含次数**，evict 次数不可得 |
| `allocate_blocks_latency_seconds` | `metrics.cpp:75-76`；`block_manager_pool.cpp:198` | 秒（累计） | 每次 `allocate(Sequence*)` | 精确计时 | 每 step×seq | 趋势（KV alloc 等待代理，但**无成败 label**） |
| `mooncake_transfer_{completed_total_bytes,failed_total,latency_microseconds}_{read,write}` | `metrics.cpp:112-125`；写 `mooncake_transfer_engine.cpp:685,693,701-708` | bytes/count/µs | 进程级、每次批传输 | 精确（覆盖 `move_memory_regions` 全路径） | 每传输批 | scrape/趋势；**region 校验失败的路径不计 failed**（:630-660，S） |
| `xllm_cpu_num` / `xllm_cpu_utilization` / `xllm_gpu_num` / `xllm_gpu_utilization` | `metrics.cpp:238-241`，无写入 | — | — | **占位** | — | 不可用（F05） |
| 4× `num_*preempt*` 细分 Gauge | `metrics.cpp:175-182`，无写入 | — | — | **占位** | — | 不可用 |

### A2. 未导出但可直接读的进程内接口（可做"零新分配器成本"的指标源）

| 信号 | 来源 + file:line | 单位 | 粒度 | 精度类 | 读取成本 | 建议用途 |
|---|---|---|---|---|---|---|
| DeviceStats{total,weights,total_kv,total_activation,active_activation} | `types.h:340-347`；`DeviceMonitor::get_device_stats()` `device_monitor.h:39-44` | bytes | per device（进程内多个 device） | 估算（weights/activation 为残差） | 极低，但**非线程安全** | on-demand / 内部 snapshot |
| `PhysicalPagePool` free/total pages | `physical_page_pool.cpp:331-334`（`num_available()` 加锁）；`physical_page_pool.h:90-94` | pages（×`page_size()` bytes） | 进程级（VMM 模式全局池） | 精确 | `/proc` 级别（mutex + deque size） | 定期采样 |
| Virtual page inuse/reserved/free/total | `virtual_kv_cache_page_allocator_impl.cpp:305-338`；接口 `virtual_kv_cache_page_allocator.h:31-55` | virt pages | per `model_id` × per `dp_rank` | 精确 | mutex（低） | 定期采样（VMM 模式核心账本） |
| Weight 分配 pages/segments | `WeightVirtualMemoryManager::get_weight_allocation_info()` → `WeightAllocationInfo{base_ptr,num_pages}` `weight_allocation.h:32-35`；`get_model_weight_segments()` → `WeightSegment{offset,size}` `types.h:222-225` | pages / bytes | per model | 精确 | mutex + vector 拷贝 | 定期采样（VMM 权重账本） |
| `SharedVMMAllocator` high-water/current/mapped/reserved | `shared_vmm_allocator.h:92,97,102,107,120-121` | bytes | per graph VMM pool（每 executor 按 physical_pool_id 多池） | 精确（虚拟地址空间视角） | 极低（成员读） | on-demand（graph capture 峰值归因） |
| PrefixCache 块命中率 | `prefix_cache.h:110-119` `block_match_rate()`（`matched_blocks_/total_blocks_`，atomics `:200`）；**全仓无调用点**（S） | 比例 | 进程级 | 精确 | 极低（atomic） | 可直接接成 gauge（E5） |
| torch caching allocator（NPU） | `c10_npu::NPUCachingAllocator::cacheInfo(device_id,&reserved,&largest_block)` `worker_impl.cpp:734-736`（仅启动一次，`largest_block` 未使用） | bytes | per device | 精确（启动快照） | 启动期；VMM 模式下 `VMMTorchAllocator` 覆盖为 `LOG(FATAL)`（`vmm_torch_allocator.h:147-160`） | 定期采样（非 VMM 模式） |
| torch stats（CUDA） | 无调用点；`getDeviceStats/resetPeakStats` 在 VMM 模式直接 FATAL（`vmm_torch_allocator.h:86-115`） | — | — | 占位 | — | 需新增（E8） |

### A3. 跨进程只读（worker RPC / 原生 API）

| 信号 | 来源 + file:line | 单位 | 粒度 | 精度类 | 读取成本/副作用 | 建议用途 |
|---|---|---|---|---|---|---|
| `ProfileDeviceMemory` → `{available_memory,total_memory}` | RPC `worker.proto:426`；服务实现 `worker_service.cpp:701-715`；底层 `estimate_kv_cache_capacity()` `worker_impl.cpp:727-745` | bytes | per worker rank | 精确（但 **`empty_cache()` 有副作用**） | **危险**：阻塞 + empty_cache + 改 DeviceMonitor；勿做 scrape | 仅初始化/排障 on-demand |
| `GetActiveActivationMemory` → `active_activation_memory` | RPC `worker.proto:439`；`worker_service.cpp:1208-1220`；源 `device_monitor`（见第 7 条） | bytes | per worker rank | 估算（下界） | 低（读 DeviceMonitor），但值本身稀疏 | on-demand 调试；生产建议改为指标 |
| `ModelMemoryDist::GetMemoryInfo` | `model_memory_dist.proto:80-81`；`worker_memory_rpc_service.cpp:54-81` | bytes | per worker rank | 精确（free/total） | 低（但也会写 DeviceMonitor set_total_memory） | 初始化路径 |
| `Device::get_device_mem()` | `device.cpp:128-146`（NPU `aclrtGetMemInfo(ACL_HBM_MEM)`/CUDA `cudaMemGetInfo`/…） | bytes | per device | 精确 | 同步驱动调用（H：µs~ms 级），**不适合每 step** | 后台 1–5s 采样 |
| `Device::empty_cache()` | `device.cpp:150-164` | — | per device | 精确 | **昂贵/破坏性**（NPU `emptyCache`+`FreeDeviceCachedMemory`） | 禁止周期性调用 |

### A4. 一句话数据流

`Device/allocator 快照` → `DeviceMonitor`（无锁写）→ `SchedulerMetrics`（步内 + 阻塞 RPC fan-out）→ `bvar` → `/metrics`；**KV 精细账本（page/KV block/prefix/evict）停在进程内，未进入导出面**。

---

## B. KV cache 记账深挖

### B1. 数据结构与容量定义（以普通 LLM 为例；S）

- 一个 `BlockManagerPool` 持有 **dp_size 个 `CompositeBlockManager`**（`block_manager_pool.cpp:30-60`，S），每个 manager 内按 `manager_types` 再分成多个 **leaf**：`BlockManagerImpl`（KV；DSV4 为 C4/C128 压缩块）、`SlidingWindowBlockManager`（SWA）、`LinearStateBlockManager`（LINEAR）、`EmbeddingBlockManager`（EMBEDDING）等（`composite_block_manager.cpp:395-450`，S）。
- 多 leaf 的容量是**从同一份预算切出来的**：C4/C128 leaf 的 `num_blocks = options.num_blocks/compress_ratio` 且 `block_size = block_size*compress_ratio`（`composite_block_manager.cpp:404-410`，S）——所以不可能用"总块数 × 单一 block_size × slot_size"还原总字节，必须 **per-leaf 分别算**。
- 每块字节（标准 layout）：
  `block_bytes = block_size × (num_full_attention_layers × (kv_slot_size + scale_slot_size) + num_indexer_layers × index_slot_size)`
  （`kv_cache_estimation.cpp:149-166`；`kv_slot_size` :66-92、`index_slot_size` :94-131、`scale_slot_size` :133-147；S）
- 容量推导：`cache_size`（预算内可用字节）先扣 `linear_cache_size_in_bytes`（GDN/KPool state 预留，`kv_cache_estimation.cpp:606-635`）→ `n_blocks = available_full / full_cache_block_size`（含 embedding context bytes，:635-661，S）；`cache_size_in_bytes` 进入 `KVCacheCapacity`（`kv_cache_capacity.h:25-63`）。
- `n_blocks` 随后进入 `BlockManagerPool::Options.num_blocks`（`llm_engine.cpp:355`），`host_num_blocks = n_blocks × host_blocks_factor`（`llm_engine.cpp:361`；host offload 时按 BlockType 分摊 `:445-471`，S）。

### B2. 六类量的口径与守恒关系

| 量 | 精确定义 | 来源 | 精度 | 关键坑 |
|---|---|---|---|---|
| **total** | `BlockManagerImpl::num_total_blocks() = free_blocks_.size()-1`（扣 padding block 0）`block_manager_impl.h:96`；`BlockManagerPool::num_blocks() = options_.num_blocks()`，是**配置值而非实测**（`block_manager_pool.cpp:353`） | 构造时分配 | 精确（配置） | padding block 0 永久占用（`block_manager_impl.cpp:51-77`，S）；多 leaf 时 capacity leaf ≠ 全部 |
| **free** | `num_free_blocks_`（atomic，`block_manager_impl.h:76`）；`BlockManagerImpl` 构造 `num_free=total`，padding allocate 后 = total-1 | 原子读 | 精确 | 只是 **free-list 长度**；prefix-cache 常驻块不在 free-list 但可被 evict 复用（`block_manager_pool.cpp:99-134` 注释，S）→ `free` 低估可分配空间 |
| **used** | `num_used_blocks_`（atomic），allocate/allocate_shared 加，deallocate/free 满足 `ref_count<=2` 或 prefix cache 关闭时减（`block_manager_impl.cpp:100-138`、`:164-186`，S） | 原子读 | 精确 | "Effective number of blocks in use"（`metrics.cpp:191`），**不等于** 分配/预分配的物理 KV 块（虚拟内存模式另有 page 层） |
| **prefix** | `PrefixCache::num_blocks()` = `cached_blocks_.size()`（`prefix_cache.h:105-109`） | 单锁对象内计数 | 精确 | prefix 块**同时可能被 used 计数**（共享块 `num_used_blocks_++`，`block_manager_impl.cpp:174-182`，S）→ `free + used + prefix ≠ total`（可 > total）；`record_num_prefix_cache_tokens` 走 request usage（`request.cpp:208-247`，S） |
| **evicted** | `PrefixCache::evict(n)`：LRU 跳过 `is_shared()`，返回实际 evict 数，但**返回值只用于判断是否够用**（`block_manager_impl.cpp:145-152`） | 无指标 | **不可观测**（只有 evict 累计耗时 Counter） | 逐 block 无次数、无来源标记 |
| **preempted** | 调度层：chunk prefill 抢占 `++budget.num_preempted_requests`（`scheduler_policy.cpp:811-819`）；decode 抢占后 `enqueue_decode_restore`（`:826-829,872-884`）。不是 KV 层事件 | Gauge | 每步 count | `decode_restore_waiting` 队列深度只被并入 `num_waiting_requests`（`scheduler_policy.cpp:982`，S），超时被 deallocate + RESOURCE_EXHAUSTED（`continuous_scheduler.cpp:340-369`，S） |

**守恒式（S）**：
- 单 leaf：`num_free + num_used + (prefix 中未被 used 持有的块) = num_total`；但 `num_free + num_used + num_blocks_in_prefix_cache` **通常 > num_total**（共享块双计）。
- 多 leaf：`composite.num_free/num_used/num_total` 只取 **capacity leaf**（`composite_block_manager.cpp:949-999`，S），SWA/C4/C128/EMBEDDING/LINEAR 的占用**完全不在** `num_free_blocks`/`num_used_blocks`/`kv_cache_utilization_perc` 里；`num_blocks_in_prefix_cache` 是各 leaf 求和（`:941-947`）——**同一族指标内部口径都不一致**（分子 vs 分母）。

### B3. 聚合方式（现有导出）与丢掉的维度

| 指标 | 聚合 | file:line | 丢掉的维度 |
|---|---|---|---|
| `num_free_blocks` | 跨 DP **max** | `scheduler_policy.cpp:989-990` | DP 间方差、TP（每 DP 首 worker 之外的进程）、per-leaf |
| `num_used_blocks` | 跨 DP **min** | `scheduler_policy.cpp:991-992` | 同上 |
| `num_blocks_in_prefix_cache` | 跨 DP **min** | `scheduler_policy.cpp:987-988` | 同上 |
| `kv_cache_utilization_perc` | 先 per-DP `used/total`，再 **min** | `block_manager_pool.cpp:386-396` | 同上；且 `num_used` 与 `num_total` 都可能来自 capacity leaf |
| `get_gpu_cache_usage_perc()` | 跨 DP **算术平均**（存在但调度器未使用） | `block_manager_pool.cpp:345-351` | 看不到（仅 bench/profiling 可能用） |
| fixed_steps（Rec/DiT） | 三个块 Gauge 写 **vector.size()**（=dp_size） | `fixed_steps_scheduler.cpp:455-472` | 全部；**值本身错误** |
| `active_kv_cache_size_in_kilobytes` | per `dp_rank`（唯一逐 rank 的 KV 指标） | `scheduler_metrics.cpp:188-195` | 非实测、TP 不分、容量全局 |

### B4. rank skew 检测器需要什么（当前缺什么）

一个可落地的 skew 检测需要（每 DP rank，最好每 TP/worker 也能归并）：

1. **per-rank 三元组**：`free_blocks[dp]`、`used_blocks[dp]`、`prefix_blocks[dp]`（现在被 min/max 吞掉，`block_manager_pool.cpp:357-384` 的 vector 已有数据，只是没导出）。
2. **per-rank 分母**：`num_total_blocks[dp]`（多 leaf 时至少 capacity leaf + 每 leaf 各一份字节数）。只给 min/max 时，无法区分"全局满"与"某 rank 满"。
3. **per-rank 分配压力**：`allocate` 失败次数 / evict 块数 / preempt 次数 / `decode_restore_waiting[dp]`（如果 decode restore 可归属 DP）。
4. **per-rank 物理侧**：device free bytes、active activation（现只有 per-dp_rank 的稀疏直方图）、physical page free / virtual page inuse（VMM 模式）。
5. **时间对齐**：同一 step 的 snapshot（现有指标由不同更新点写入：块 Gauge 每 `prepare_batch`、active_kv 每 `SchedulerMetrics::update`、容量启动期一次，**同一 scrape 里不是同一时刻**）。
6. **拓扑标签**：`instance`、`dp_rank`、`tp_rank`/worker index、`model_id`；否则多进程同名指标在 Prometheus 里不可区分（S(base) + H）。

**检测规则建议**（H，实现后验证）：`spread = max(util[dp]) - min(util[dp])`，连续 N step > 阈值（如 0.2）且 `max(free) < ε` 同时 `min(free) > 0` → rank skew；配合 `evict_total`/`alloc_fail` 增量判断"是否已影响准入"；再叠加 device free 差分排除"物理侧真的不一致"。

---

## C. Device/Host 内存记账

### C1. 物理总量与底层查询（S）

- `Device::get_device_mem()` → `{total_memory, free_memory}`：NPU `aclrtGetMemInfo(ACL_HBM_MEM,&free,&total)`（`device.cpp:128-146`）；CUDA/ILU `cudaMemGetInfo`、MLU `cnrtMemGetInfo`、MUSA `musaMemGetInfo`、DCU `hipMemGetInfo`（同文件 :133-142）。`total_memory()` :148、`free_memory()` :166、`empty_cache()` :150-164。
- **用哪一份 total** 决定了后续所有分解的口径：估算路径用 `worker` 的 total（`worker_impl.cpp:738-739` → `DeviceMonitor::set_total_memory` :740）。

### C2. 权重（weights）公式与误差

- **口径 A（DeviceMonitor，供 activation 残差用）**：`weights = total - available - torch_cache`
  `worker_impl.cpp:738-742`（先 `Device::empty_cache`、再 NPU `cacheInfo` 取 `torch_cache`/`largest_block`，然后 `available=free_memory()`）。
  误差源：`torch_cache` 只是 torch 缓存池；非 torch 的常驻分配（graph pool、通信缓冲、其它库）也被算进 weights；`empty_cache` 改变基线。
- **口径 B（导出 Gauge）**：`weight_size_in_kilobytes = (total - available)/1024`
  `kv_cache_estimation.cpp:763`（`available` 来自启动期 worker 快照，`KVCacheMemorySnapshot`）。与口径 A 差一个 `torch_cache`（S）。
- **per-worker 覆盖**：估算对 `context.worker_memory` 每个 worker 循环 `GAUGE_SET`（`kv_cache_estimation.cpp:751-777`）→ 单维度 Gauge 只剩最后一个 worker 的值；而 KV 容量是取所有 worker 的 `min`（:775）→ **导出的权重与容量可能来自不同 rank**。
- **VMM 权重账本**（VMM 模式更精确，但无指标）：`WeightAllocationInfo.num_pages`（`weight_allocation.h:32-35`）；`WeightVirtualMemoryManager` 先连续预留、失败走 `MappedVirtualMemoryRegion` 碎片映射（`weight_virtual_memory_manager.cpp:41-57,99-132`，S）。权重字节 = `Σ num_pages × PhysicalPagePool::page_size()`；与 `DeviceMonitor.weights_memory` 的差 = torch cache + 未计入的非权重常驻（H）。
- **配置**：`kv_cache_config.max_memory_utilization` 默认 0.8（`kv_cache_config.h:64`），`runtime::Options.max_memory_utilization` 默认 0.9（`runtime/options.h:51`）——两处默认值不一致，需确认实际生效者（H）。

### C3. KV cache 公式与误差

`total_kv = capacity.cache_size_in_bytes()`（`kv_cache_manager_factory.cpp:78-80`），其推导：
1. 预算：`available = worker_free + torch_cache`（`worker_impl.cpp:744`；估算时加回 torch cache，S）；
2. `max_memory_utilization < 1` 时 `available -= ceil(total × (1-util))`（`kv_cache_estimation.cpp:766-770`）；
3. `max_cache_size > 0` 时取 `min(available, max_cache_size)`（:771-773）；
4. 多模态再扣 `encoder_cache_reserved = max_encoder_cache_size × 1024 × 1024`（:747-749,774）；
5. 取所有 worker 的 **min**（:775）→ 所有 rank 用同一 n_blocks（S）；
6. 扣 linear state cache 后 `n_blocks = available_full / full_cache_block_size`（:580-663），`CHECK(n_blocks>0)`。
误差源（H/S）：`cache_size_in_bytes` 是"估计可用容量"，不是实际分配；多 leaf（SWA/C4/C128/LINEAR/EMBEDDING）是否都在这套公式里取决于 layout（`init_standard_counts` 只覆盖 full-attention + linear state + embedding context，S），`capacity.swa_count()/c4_count()/c128_count()` 另行传出（`kv_cache_capacity.h:25-63`）并作为独立 leaf 的块数（`llm_engine.cpp:445-471`、`composite_block_manager.cpp:433-450`，S）→ **用 `cache_size_in_bytes` 当"KV 总字节"在多 leaf 模型上可能低估/口径错**（H 待 runtime 对账）。
- 虚拟内存模式：KV 容量 = `PhysicalPagePool::num_total() × granularity`（`kv_cache_estimation.cpp:737-742`，S）——**这是整池大小**；权重页与 KV 页共享同一个 pool（`WorkerMemoryResources` 同时持有 `weight_memory_` 与 `kv_cache_memory_`，`worker_memory_resources.h:76-101`，S）→ 若权重也从此池分配，KV "可用容量"被高估（H，重要）。
- 关联指标风险：`active_kv_cache_size_in_kilobytes = occupied_slots_ratio × 全局 total_kv`（`scheduler_metrics.cpp:182-195`），其中 `occupied_slots` 只由 capacity leaf 的 `num_used_blocks` 与每 seq 的 `kv_cache_tokens_num % block_size` 组成（`scheduler_metrics.cpp:129-151`，S）→ 多 leaf/linear state 占用不进入该值。

### C4. 激活（activation）公式与误差

- `total_activation = total - weights - total_kv`（`device_monitor.cpp:77-84`，S）→ **纯残差桶**：包含 fragmentation、graph capture pool、通信/传输缓冲、ATB 之外的库分配等；不是"activation tensor 求和"（F07 继承）。
- `active_activation`：`DeviceMonitor::monitor_buffer(device, buffer_size)` 取 `max`（`device_monitor.cpp:86-89`）；`update_active_activation_memory` 把该 max 写入 `active_activation_memory` 后清零（:92-97）。写入点：
  - NPU ATB：`AtbWorkspace::get_workspace_buffer()` 每次请求 workspace 时 `monitor_buffer`（`atb_workspace.cpp:35-49`，S）；`AtbBuffer::get_buffer()` 在尺寸变大时 `aclrtSynchronizeStream` + 重建 tensor（`atb_buffer.cpp:53-68`，S）→ 峰值大致= 当前图算子 workspace 需求，而不是所有 tensor 之和。
  - 采样时点：`llm_worker_impl.cpp:415-423`（默认流同步后）、`worker_service.cpp:1165,1185`（NPU stream sync 分支）。**CUDA 分支不更新**（S，`#if defined(USE_NPU)` 包裹），因此 CUDA 上该值长期为 0/陈旧（H）。
- `prefill_/decode_active_activation_size_in_kilobytes` 的 phase 判定：chunked prefill 时全记为 decode（`scheduler_metrics.cpp:196-199`）；否则用 `sequences.front()->is_first_token()` 代表整个 dp_rank（:200-208，S）→ 混合 batch 会归错桶。

### C5. allocator reserved/allocated/peak 与碎片证据

| 观察对象 | 可用证据 | file:line | 精度 | 备注 |
|---|---|---|---|---|
| torch caching allocator | 启动期 NPU `cacheInfo(reserved, largest_block)`；`largest_block` 拿到但未使用 | `worker_impl.cpp:734-736` | 精确（启动） | 非 VMM 模式；CUDA 无调用点 |
| torch 统计接口 | `getDeviceStats/allocated/reserved/peak` **无调用点**；VMM 模式 `VMMTorchAllocator` 全部 `LOG(FATAL)` | `vmm_torch_allocator.h:86-160`；私有 `total_allocated_/alloc_count_` 无 getter :301-302 | 不可观测 | 见 E8 |
| PhysicalPagePool | `num_available()`（加锁）；连续预留失败会走 `allocate_pages_from_right` 碎片回退；`LOG(WARNING) "no free pages available"` / "not enough free pages" | `physical_page_pool.cpp:86-130,331-334`；头文件 :66-105 | 精确 | 碎片直接表现为 **连续预留失败→分散页映射**（`weight_virtual_memory_manager.cpp:99-132`，S） |
| VirtualKV page allocator | per model+dp 的 free/inuse/reserved/total virt pages；预留 8 页小池（`kMinReservedPages=8`） | `virtual_kv_cache_page_allocator_impl.cpp:305-338`；`impl.h:102`；prealloc :466-520 | 精确 | reserved = 为映射吞吐预取的页，不是泄漏 |
| SharedVMMAllocator（CUDA graph VMM pool） | `high_water_mark()`/`current_offset()`/`mapped_size()`/`reserved_size()` | `shared_vmm_allocator.h:92-121`；创建 :1213-1300（`cuda_graph_executor_impl.cpp`） | 精确 | 每 executor 每个 `physical_pool_id` 一个 pool；**当前完全无指标** |
| 进程 host RSS | 无 `getrusage`/`/proc/self/status` 读取（全仓 grep，S） | — | 不可观测 | host cache/shm 只能靠配置推算 |

碎片对 **KV 的直接影响有限**（虚拟地址是整段映射，物理页可分散），主要影响：权重连续预留、graph VMM pool、以及 `allocate_contiguous_from_right` 失败后的 fallback 路径（S）。

### C6. Host 侧（host cache / offload / shm / mooncake）

- Host KV cache：`host_blocks_factor > 1` 时创建 CPU KVCache，容量 = `host_num_blocks = n_blocks × factor`（`llm_engine.cpp:361`；`hierarchy_kv_cache_transfer.cpp:233-261,361-380`，S）；host 字节 ≈ `host_num_blocks × block_bytes(host dtype)`，但**没有任何 counter**（S）。
- `HierarchyBlockManagerPool` 有 `host_block_managers_` 及各 BlockType 的 host 容量（`hierarchy_block_manager_pool.h:36-40,42`；`llm_engine.cpp:445-471`，S），但 `num_free_blocks()` 等接口只覆盖 device leaf。
- Shared memory：payload 尺寸 = `input_shm_size/output_shm_size`（`execution_config.cpp:93-101`、`worker_server.cpp:405-433`、`shm_channel.cpp:30-48`，S），内存映射 = 64B header + payload（`shared_memory_manager.cpp:41-70`，S）。全部无指标；溢出时 `pb_write` 直接 `LOG(ERROR) "pb size overflow"` 并回退 brpc（`forward_shared_memory_manager.h:65-88`，S）。
- Mooncake：注册缓冲 `memory_registrations_`（`mooncake_transfer_engine.h:63-65,105-109`）只存 `{bytes,lifetime}`，无 getter（S）；传输 bytes/latency 已导出（A1）。KV cache 注册的 buffer 总量在 `mooncake_kv_cache_transfer.cpp:590-625,850-871` 只打日志（S）。
- 权重 transfer：`mooncake_weight_transfer.cpp:51` 注册（S），无字节指标。

---

## D. 现有内存指标契约（更新频率/维度/精度）与误读风险

### D1. 契约表

| 家族 | 宏类型 | 写入点 | 更新频率 | 维度 | 导出形状风险 | 精度类 |
|---|---|---|---|---|---|---|
| `total_memory_size_in_kilobytes` | Gauge | `kv_cache_estimation.cpp:765` | 启动一次（每 worker 覆盖） | 无 | 单值 | 估算 |
| `weight_size_in_kilobytes` | Gauge | `kv_cache_estimation.cpp:763` | 启动一次（每 worker 覆盖） | 无 | 单值 | 估算 |
| `total_kv_cache_size_in_kilobytes` | Gauge | `kv_cache_manager_factory.cpp:79-80` | 启动一次 | 无 | 单值 | 估算 |
| `total_activation_size_in_kilobytes` | Gauge | — | 无 | — | 常量/缺省 | 占位 |
| `num_free_blocks` / `num_used_blocks` / `num_blocks_in_prefix_cache` | Gauge | `scheduler_policy.cpp:987-992`（min/max）；`fixed_steps_scheduler.cpp:467-471`（.size()，错误） | 每 `prepare_batch`（~每调度步） | 无（聚合后） | 单值；不同指标来自不同 rank | 精确但聚合丢维 |
| `kv_cache_utilization_perc` | Gauge | `scheduler_policy.cpp:985-986` | 每步 | 无 | 0–1 比例 | 精确+口径问题 |
| `num_preempted_requests` | Gauge | `scheduler_policy.cpp:983`（set）/`fixed_steps_scheduler.cpp:461`（add） | 每步 | 无 | 语义随 scheduler 变 | 精确但语义不一致 |
| `active_kv_cache_size_in_kilobytes` | MultiHistogram(dp_rank) | `scheduler_metrics.cpp:192-195` | 每个**有 running sequences 的** step（`scheduler_metrics.cpp:61-67`；调用 `continuous_scheduler.cpp:636-637`） | dp_rank | 导出为多个 gauge 子变量（`_latency/_count/_qps/...`，基线 §4.2/4.4 S(base)） | 估算 |
| `prefill_/decode_active_activation_size_in_kilobytes` | MultiHistogram(dp_rank) | `scheduler_metrics.cpp:196-208` | 同上 | dp_rank | 同上 | 估算（下界） |
| `prefix_cache_*` | Counter/Gauge | `block_manager_impl.cpp:145,168,195,204`；`metrics.cpp:93-97` | 每次 match/insert/evict/prefill | 无 | Counter 导出成 gauge（基线 §4.1） | 精确（token 级） |
| `allocate_blocks_latency_seconds` | Counter | `block_manager_pool.cpp:198` | 每次 allocate | 无 | 累计 time、无结果 label | 精确计时 |
| 4× preempt 细分 Gauge | Gauge | — | 无 | — | 恒定 | 占位 |
| `xllm_cpu_*/gpu_*` | Gauge | — | 无 | — | 恒定 | 占位 |
| mooncake 传输 6 族 | Counter/Histogram | `mooncake_transfer_engine.cpp:685-708` | 每次传输批 | 无 | histogram 无 bucket | 精确 |

**维度现状（S）**：全部内存指标里只有 `dp_rank` 一个业务维度（且仅 3 个 MultiHistogram 使用）；没有 `rank/tp_rank/device_id/model_id/instance` 维度，没有 per-leaf、per-phase（除 activation）拆分。

### D2. 声明未接线（S，与基线 F05 一致）

- `xllm_cpu_num`、`xllm_cpu_utilization`、`xllm_gpu_num`、`xllm_gpu_utilization`：`metrics.cpp:238-241` 定义，无写点/无读点。
- `total_activation_size_in_kilobytes`：`metrics.cpp:345-346` 定义，无写点。**注意**：`DeviceMonitor` 内部有 `total_activation_memory` 并更新，但它不是这个 Gauge（S）。
- `num_offline_decode_preempt_offline_requests`、`num_online_decode_preempt_online_requests`、`num_online_prefill_preempt_offline_requests`、`num_online_decode_preempt_offline_requests`：`metrics.cpp:175-182`，无写点。
- 存量 `num_preempted_requests` 只在 SchedulerPolicy/FixedSteps 写；其它调度器（DiT/PD）视图不同（H）。

### D3. 误读风险清单（含 F06/F07 延伸）

1. **F06 继承**：`kv_cache_utilization_perc` 实际是 `used/total`（0–1）且先按 DP 取 min（`composite_block_manager.cpp:978-984`、`block_manager_pool.cpp:386-396`）→ 单值会掩盖 rank skew；不能当"集群利用率"读。
2. **F06+（新）**：`num_free_blocks` 取 max、`num_used_blocks` 与 `num_blocks_in_prefix_cache` 取 min（`scheduler_policy.cpp:987-992`），三者的"聚合方向"不同 → 用它们做"剩余可用 = free - prefix"之类的推算会跨 rank 混合。更隐蔽的是：**free 只代表 free-list，可分配上限是 `total - used`**（prefix 块可 evict，`block_manager_pool.cpp:99-134` 注释，S）。
3. **F06++（新，最危险）**：`fixed_steps_scheduler.cpp:467-471` 把 per-DP vector 的 `.size()` 当块数写 Gauge → Rec/DiT 实例上 `num_free_blocks` 数值 = dp_size（通常 1/2/4），看起来永远"没内存"或"不变"。若仪表盘混用 LLM/Rec 实例会误判。
4. **F07 继承**：`weight_size_in_kilobytes` 是 `total-available` 的残差（含 torch cache），不是独立测量的权重；`total_activation` 是 `total-weights-KV` 的残差（`device_monitor.cpp:77-84`），会吸收碎片/graph pool/通信缓冲 → 不能当"激活峰值"或"泄漏量"。
5. **F07+（新）**：`active_activation` 是 **ATB workspace 请求量的 max**（`atb_workspace.cpp:43` → `device_monitor.cpp:86-89`），不是 tensor 求和；CUDA 上不更新（`#if defined(USE_NPU)`）；只在同步点采样 → 中间态峰值会漏采（H）。
6. **F07++（新）**：`active_kv_cache_size_in_kilobytes` 用全局 `total_kv`（可能来自不同 rank；见 C3）乘 occupied 比例；多 leaf 模型上分子只含 capacity leaf（`scheduler_metrics.cpp:129-151`）→ 值和真实 KV 占用可能"线性但缩放错误"（H，需 runtime 对账）。
7. **F08 延伸**：块 Gauge 每 `prepare_batch` 更新、active_kv/activation 每 `SchedulerMetrics::update` 更新、容量类启动一次 → 同一 scrape 里**不同指标不是同一时刻**；且 `SchedulerMetrics::update` 只在 sequences 非空时执行（`scheduler_metrics.cpp:61-67`）→ 空闲后 memory histograms 停止更新（bvar 窗口统计变陈旧，非零残留）。
8. **导出类型**：所有 Counter 被 Prometheus 服务导出为 `TYPE gauge`（基线 §4.1，S(base)）→ `rate()`/`increase()` 语义错误，必须自行做 delta；histogram 无 `_bucket`，不能 `histogram_quantile`（基线 §4.2/4.4）。
9. **单位**：`*_in_kilobytes` 用 `/1024`（KB），`prefix_cache_token_hit_rate_perc` 是 0–100 百分比而 `kv_cache_utilization_perc` 是 0–1 比例——同后缀不同量纲。
10. **跨进程同名**：bvar 是进程级；多 rank/多实例 scrape 若不区分 `instance`，同名 `num_free_blocks` 会互相覆盖/混叠（H，取决于部署）。
11. **VMM 模式特有**：`enable_virtual_memory=true` 时 `enable_prefix_cache` 被强制 false（`llm_engine.cpp:363-365`，S）→ `num_blocks_in_prefix_cache` 恒 0，不代表"没有前缀命中"；`num_used_blocks` 是逻辑块，物理页账本在 page allocator 里（C5）。
12. **Host 侧不可见**：host cache/offload 的块与字节、shm payload、mooncake 注册缓冲都没有指标 → host OOM/交换抖动只能靠日志与外部 node exporter（H）。
13. **并发一致性**：`DeviceMonitor` 无锁（`device_monitor.cpp`，S），多线程写读；快照可能撕裂（H）。
14. **`num_preempted_requests` 语义**：SchedulerPolicy 是"本步新增抢占数"（set），FixedSteps 是 `GAUGE_ADD`（累积）→ 同一仪表盘两者不可比。

---

## E. 推荐新增观测点（8–12 个，按收益/成本排序）

> 原则：优先"已存在精确账本、只差导出"（低风险），其次"在已有日志/计时代码处加 counter"，最后才是新采样线程。所有新指标名带单位后缀，维度优先 `dp_rank`/`leaf`，禁止 request 级 label。

| # | 建议名 | 类型 | 语义 | 更新点（候选 file:line） | 基数 | 成本 | 用途 |
|---|---|---|---|---|---|---|---|
| E1 | `kv_cache_free_blocks{dp_rank,leaf}` / `kv_cache_used_blocks{dp_rank,leaf}` / `kv_cache_prefix_blocks{dp_rank,leaf}` | Gauge（per-DP） | 直接导出 `BlockManagerPool::num_free_blocks()` 等 vector + 各 leaf 对应值；保留 min/max 旧名以兼容 | `block_manager_pool.cpp:357-384`；`report_metrics` `scheduler_policy.cpp:987-992` | dp_size×leaf（≤~8） | 极低（atomic） | rank skew 检测、容量规划（E 的核心） |
| E2 | `kv_cache_total_blocks{dp_rank}` + `kv_cache_block_bytes{leaf}` | Gauge | 分母与单块字节（`block_size×slot_size` 由布局估算得到），让利用率/占用可解释 | `block_manager_pool.cpp:353`；`kv_cache_estimation.cpp:149-166` | dp_size×leaf | 启动一次 | 换算 bytes、校验 E1 |
| E3 | `kv_cache_evicted_blocks_total{dp_rank}` | Counter | `prefix_cache_->evict(n)` 的实际返回块数累加（当前丢弃，`block_manager_impl.cpp:145-152`） | 同上（含 `sliding_window_block_manager.cpp:242` 路径） | dp_size | 极低 | 判断"是否靠 evict 续命" |
| E4 | `kv_cache_alloc_total{result=ok\|evict_ok\|fail}` + `kv_cache_alloc_wait_seconds{result}` | Counter + Histogram | 分配成功/靠 evict 成功/失败（`allocate` 返回 false 与 `allocate_sequence` 失败）；等待/失败原因是 OOM 前兆 | `block_manager_pool.cpp:197-235`（AUTO_COUNTER 处）、`composite_block_manager.cpp` 分配失败分支 | 无/低 | 低（每 alloc） | KV alloc wait/OOM 根因 |
| E5 | `prefix_cache_block_match_rate` | Gauge | `PrefixCache::block_match_rate()`（`matched/total`，块维度，现有 token 维度指标的补充；当前无调用点） | `prefix_cache.h:110-119`；在 `block_manager_impl.cpp:168`（match）后 set，或调度步读 | 进程级 | 极低（atomic） | 前缀命中/内存复用 |
| E6 | `preempt_requests_total{reason=chunk\|decode_restore}` + `decode_restore_waiting_requests` | Counter + Gauge | 抢占累计与 restore 队列深度（现在只并入 `num_waiting_requests`） | `scheduler_policy.cpp:815,828-829,872-884`；`continuous_scheduler.cpp:340-369` | 2×dp | 极低 | 内存压力/抖动 |
| E7 | `near_oom_events_total{kind=...}` | Counter | 在既有告警点 +1：`Potential block leak`（`block_manager_impl.cpp:157-160`）、`PhysicalPagePool: no free pages`（`physical_page_pool.cpp:92,115-118`）、`not enough space in weight region`（`weight_virtual_memory_manager.cpp:143-148`）、`pb size overflow`（`forward_shared_memory_manager.h:65-88`）、`No enough memory to schedule single sequence`（`scheduler_policy.cpp:955-965`） | 上述各点 | kind 枚举 | 极低 | OOM 前兆告警 |
| E8 | `device_memory_free_bytes{rank}` / `device_memory_total_bytes{rank}` | Gauge | `Device::get_device_mem()`（`device.cpp:128-146`）后台采样；**不要**经 `ProfileDeviceMemory`（有 empty_cache 副作用） | 新增 monitor 线程（1–5s），或 worker 侧采样后 RPC/指标导出 | per rank | 中（驱动调用） | 物理侧对账、显存碎片/泄漏 |
| E9 | `torch_allocator_reserved_bytes{rank}` / `_allocated_bytes` / `_peak_bytes` | Gauge | NPU `cacheInfo`（`worker_impl.cpp:734-736` 已有调用方式）；CUDA 非 VMM 走 `getDeviceStats`（当前无调用点）；VMM 模式明确置 NaN/不导出 | 新增采样线程；注意 `VMMTorchAllocator::getDeviceStats` 是 FATAL（`vmm_torch_allocator.h:147-160`） | per rank | 中 | torch 缓存/碎片 |
| E10 | `physical_page_pool_free_pages` / `_total_pages` + `virt_page_{inuse,reserved,free}_total` | Gauge | VMM 模式核心账本（`physical_page_pool.cpp:331-334`；`virtual_kv_cache_page_allocator_impl.cpp:305-338`） | 新增采样线程读接口（均加锁，只读） | 进程级 + per model/dp | 低（mutex） | VMM 容量/页碎片 |
| E11 | `weight_bytes_allocated{model_id}` + `weight_fragmented_allocations_total` | Gauge + Counter | Σ`WeightAllocationInfo.num_pages × page_size`（`weight_allocation.h:32-35`）；fallback 映射次数（`weight_virtual_memory_manager.cpp:99-132` 日志处 +1） | 同上（`get_all_model_weight_segments()` 已返回 segments） | per model | 低（mutex+拷贝） | 权重账本、VMM 碎片 |
| E12 | `activation_peak_workspace_bytes{dp_rank}` + `batch_shape_{seqs,tokens}` 归因直方图 + `kv_alloc_fail` span | Gauge/Histogram/Span | 在 `monitor_buffer` 峰值上升时记录当步 batch shape（num_seqs、sum tokens、max seq len）与请求数；分配失败时打 span（attrs：dp_rank,wanted_blocks,free,prefix,total,seq_len,prompt_len） | `device_monitor.cpp:86-89` + `scheduler_metrics.cpp:176-210` 的 batch 信息；失败点 `block_manager_pool.cpp:197-235`/`scheduler_policy.cpp:483,520` | 低 | 低 | OOM/峰值归因、backtrace 到具体 batch |

补充（若允许第 13 项）：`forward_shm_{input,output}_bytes` Gauge + `forward_shm_overflow_total`（`forward_shared_memory_manager.h:65-88` 的 `LOG(ERROR)` 处 +1）、`mooncake_registered_bytes`（`mooncake_transfer_engine.cpp:390-412` 注册成功处累加，`unregister` 处减）。这两项直接对应"transport buffer accounting"。

**OOM/near-OOM 专项（E 的重点）**：
- 现在"OOM"只表现为：`RESOURCE_EXHAUSTED` 错误文案（`scheduler_policy.cpp:937-981`、`continuous_scheduler.cpp:340-369`）+ `LOG(WARNING)` 若干 + `LOG(FATAL)`（block 重复释放/未释放，**并非 OOM**）。建议统一为 `near_oom_events_total{kind}` + 失败 span，并把 `wanted/free/prefix/total/seq_len` 写进 span，才能在 trace 里定位到"哪个 batch/请求打爆了哪类块"。
- `AtbWorkspace` 的 ATB buffer 增长会 `aclrtSynchronizeStream`（`atb_buffer.cpp:53-68`）——它既是"activation 峰值归因点"，也是**隐藏的同步点**（见 F）。

---

## F. 开销与安全（哪些读取强制 sync/加锁、安全 cadence）

### F1. 读取代价分级

| 级别 | 读取 | 成本来源 | 结论 |
|---|---|---|---|
| A（可每步） | KV 块计数（atomic，`block_manager_impl.h:76-84`）、`kv_cache_utilization`（O(dp) min）、prefix `num_blocks()`（CHECK）、bvar observe | 原子读/小 vector/一次 CHECK | 调度步内安全；`report_metrics` 已在每步做 |
| B（10–100ms 级，可秒级采样） | `DeviceMonitor` 字段读（无锁，但有 race）、`PhysicalPagePool::num_available()`（mutex）、page allocator getters（mutex）、`WeightVirtualMemoryManager` 查询（mutex + vector 拷贝）、`PrefixCache::block_match_rate`（atomic） | mutex / 数据竞争 | 后台 1–5s 采样；不要在 forward 前关键路径反复调用 |
| C（昂贵/危险，仅 on-demand） | `Device::get_device_mem()`（同步驱动查询）、`NPUCachingAllocator::cacheInfo`（H：可能涉及锁/设备同步）、`Device::empty_cache`（释放+重分配，破坏性能基线）、`ProfileDeviceMemory` RPC（触发 empty_cache + 改 DeviceMonitor）、`std::vector` 拷贝大段 segments | 驱动调用/全局锁/副作用 | 禁止周期调用；排障时人工触发 |
| D（禁止进 hot path） | 每 token 打点、request 级 label、在 `pb_write/pb_read` 忙等循环里加日志/同步（`forward_shared_memory_manager.h:90-111`；`forward_shared_memory_manager.cpp` 的 SHM polling 线程） | 改变轮询节奏 | 只允许 atomic/counter |

### F2. 已在 hot path 的观测成本（S）

- `SchedulerMetrics::update_memory_metrics` **每步**调用 `distributed_worker_manager_->get_active_activation_memory()`（`scheduler_metrics.cpp:153-174`）→ `folly::collectAll(...).get()`（`distributed_worker_manager.cpp:133-149`，S）→ **同步等待所有 worker 的 RPC**。这是当前内存观测里最大的性能风险：任一 worker 卡住会把调度步拖住。建议改为 worker 侧把值 push/缓存在 master（或后台每 100–500ms 拉一次），调度步只读本地快照。
- `update_memory_metrics` 里 `sequences.front()->is_first_token()` 代表整个 dp_rank（`scheduler_metrics.cpp:200-208`）→ 不增加成本但语义粗糙。
- `prefix_cache_token_hit_rate_perc` 的更新带 `prefix_cache_hit_metrics_mutex`（`metrics.cpp:84,93-97`，S）——每 prefill 一次，可接受但别扩展到每 token。
- `report_metrics` 每步写 8 个 Gauge + 3 个 per-DP 条件判断（`scheduler_policy.cpp:975-993`）——可忽略。
- `multi_dimension` 观测走 bvar map（基线 §4.4，S(base)）：维度基数受 `bvar_max_dump_multi_dimension_metric_number`（默认 1024）限制；`dp_rank` 基数小，安全。
- `num_blocks_in_prefix_cache()` 等每次调用 `CHECK(num_blocks_==cached_blocks_.size())`（`prefix_cache.h:105-109`）——纯 debug 价值，不贵。

### F3. 建议 cadence（H，实施后验证）

| 指标族 | cadence | 说明 |
|---|---|---|
| KV 块计数/利用率/prefix/evict/alloc 结果、preempt/restore 队列 | 每调度步（现有 report_metrics 处） | atomic 读，O(dp×leaf) |
| active_kv / active_activation 直方图 | 每调度步（现有），但**激活值改为 worker 本地快照直读** | 避免 per-step RPC fan-out |
| device free/total、torch allocated/reserved/peak、page pool、page allocator、weight pages | 1–5s 后台线程 | mutex/驱动调用；与 scrape 周期对齐 |
| host cache 字节、shm 尺寸、mooncake 注册字节 | 启动 + 配置变更时 | 变化少，事件式更新即可 |
| OOM/near-OOM 事件、alloc 失败 span | 事件驱动 | 只在错误路径 |
| `empty_cache`/`ProfileDeviceMemory` | 禁止周期 | 仅人工排障 |

### F4. 并发/一致性安全

1. `DeviceMonitor` 无锁：建议把 `DeviceStats` 改为 `std::atomic<int64_t>` 或加 `mutex`，或改为"worker 线程写本地、后台线程合快照"（S 问题 + H 方案）。
2. 内存指标由多个更新点写入（D1）→ 任何"free+used=total"或"active_kv/总容量=利用率"的跨指标算式都应在**同一采集快照**上做；否则优先只看单指标趋势。
3. 不要用 `ProfileDeviceMemory` 做健康检查：它会 `empty_cache` 并改写 `DeviceMonitor` 的 total/weights（`worker_impl.cpp:727-745`），把观测行为变成扰动。
4. 多进程同名 bvar：若 Prometheus 每 rank 独立 scrape，需在 scrape 配置加 `instance`/`rank` relabel，否则数值会因抓取到不同目标而跳变（H）。
5. 建议把 `S` 类改动集中在：`scheduler_policy.cpp:report_metrics`（扩展 per-DP 导出）、`block_manager_pool.cpp`/`block_manager_impl.cpp`（evict/alloc 计数）、`device_monitor.cpp`（线程安全 + peak 归因）、`worker_service.cpp`（去掉/降频 fan-out）。

### F5. 待 runtime（R）验证的清单（本文不做 R 结论）

1. `active_kv_cache_size_in_kilobytes` 与真实 KV 字节（E1/E2 计算）的对账误差，多 leaf 模型（DSV4/Qwen3.5 SWA）上尤其重要。
2. `total_kv_cache_size_in_kilobytes`（容量）与 `num_blocks × block_bytes` 是否一致（含 linear state 预留）。
3. shm 尺寸单位：`input_shm_size=1024` 是字节还是"按 MB 解释但漏乘"——用 `enable_shm=true` 小规模跑一次看是否回退 brpc/`pb size overflow`。
4. `ProfileDeviceMemory` 的 `empty_cache` 对 TTFT/吞吐的实际扰动幅度。
5. CUDA 路径上 `active_activation_memory` 是否恒 0（代码上 NPU 才 update）。
6. VMM 模式下权重页与 KV 页共享 pool 时，KV 容量估计是否高估（C3 的 H）。

---

## 附录：本文核心 file:line 索引（快捷）

- 指标定义：`xllm/core/common/metrics.cpp:61-76`（prefix/alloc）、`:112-125`（mooncake）、`:173-191`（调度/KV）、`:238-241`（cpu/gpu 占位）、`:341-358`（内存 gauge+hist）；声明 `metrics.h:344-353`。
- DeviceMonitor：`xllm/core/common/device_monitor.h:32-51`、`.cpp:46-97`；`types.h:340-347`。
- SchedulerMetrics：`xllm/core/scheduler/scheduler_metrics.cpp:59-210`；`scheduler_metrics.h`。
- report_metrics/抢占：`xllm/core/scheduler/scheduler_policy.cpp:811-884,937-993`。
- fixed_steps 指标：`xllm/core/scheduler/fixed_steps_scheduler.cpp:455-472`。
- 调度步：`xllm/core/scheduler/continuous_scheduler.cpp:340-369,376-445,636-637`。
- KV 接口：`xllm/core/framework/block/kv_cache_manager.h:94-109`；`block_manager_pool.cpp:99-134,197-235,345-432`；`composite_block_manager.cpp:395-450,941-1005`；`block_manager_impl.{h,cpp}:67-133 / 51-265`；`prefix/prefix_cache.{h,cpp}:105-119 / 237-269`。
- 容量估算：`xllm/core/kv_cache/layout/kv_cache_estimation.cpp:48-166,580-663,734-778,869`；`kv_cache_capacity.h:25-63`；`kv_cache_manager_factory.cpp:40-170`；`llm_engine.cpp:355-380,445-471`。
- 设备/allocator：`xllm/core/platform/device.cpp:128-166`；`vmm_torch_allocator.h:86-160`；`shared_vmm_allocator.h:92-121`；`cuda_graph_executor_impl.cpp:1213-1300`；`physical_page_pool.{h,cpp}:66-105 / 28-130,331-334`；`global_virtual_memory_region.h:31-63`；`virtual_kv_cache_page_allocator{,_impl}.h/.cpp:31-55 / 305-338`。
- 权重 VMM：`weight_virtual_memory_manager.{h,cpp}`；`weight_allocation.h:32-35`；`types.h:222-225`。
- Host/传输：`hierarchy_block_manager_pool.h:36-42`；`hierarchy_kv_cache_transfer.{h,cpp}:59,233-261,361-380`；`shared_memory_manager.{h,cpp}:26-48 / 41-70`；`forward_shared_memory_manager.h:31-111`；`mooncake_transfer_engine.{h,cpp}:63-65,105-109 / 390-412,590-713`。
- Worker/RPC：`worker_impl.cpp:727-745,852-857,2780-2830,2889-2897`；`worker_service.cpp:701-715,1165-1185,1208-1220`；`worker.proto:38-45,426,439`；`model_memory_dist.proto:80-81`；`distributed_worker_manager.cpp:133-149`。
- 配置：`runtime/options.h:45-57,193-194,243-246`；`kv_cache_config.h:60-80`；`execution_config.cpp:73-101`；`profile_config.h:73`；`kv_cache_store_config_validation.cpp:38-50`。
