# kiro.rs 无状态化剩余项

更新时间：2026-04-16

## 本次提交聚焦

当前提交不再把“迁移冻结 / 回滚 runbook”作为主线阻塞项，主线只看下面 3 个目标是否成立：

1. `kiro.rs` 能在 k3s 中以多副本方式正常运行
2. 滚动发布过程中服务不中断
3. 开启 external state 后，转发和业务处理语义与原有状态版本保持一致

## 当前判断

当前代码已经具备以下基础能力：

- 持久状态可切到 PostgreSQL
- 短生命周期协调状态可切到 Redis
- 调度关键热态已通过 Redis 共享运行态外置
- 已有 `/healthz`、`/readyz`、drain 标记、shutdown release
- 已有 Redis leader/follower 运行时协调
- 已有 Admin 写请求 leader 路由闭环

2026-04-16 本地分析时，以下与原有调度语义直接相关的能力已具备测试覆盖：

- `test_priority_mode_respects_per_account_concurrency_limit`
- `test_priority_mode_returns_to_highest_priority_after_fallback_recovers`
- `test_balanced_mode_spreads_concurrent_reservations`
- `test_acquire_context_waits_for_capacity_when_queue_enabled`
- `test_rate_limited_credential_enters_cooldown_and_falls_back`
- `test_priority_mode_uses_default_max_concurrency_when_credential_has_no_override`
- `test_shared_dispatch_runtime_enforces_global_max_concurrency_when_test_redis_is_set`
- `test_shared_dispatch_runtime_shares_rate_limit_bucket_when_test_redis_is_set`
- `test_shared_dispatch_runtime_shares_rate_limit_cooldown_when_test_redis_is_set`

这意味着当前主问题已经不再是“热态仍在本地内存”。当前更需要补的是：把现有实现变成可提交、可验收、可在 k3s 中证明语义不变的交付件。

## 已完成的关键改造

以下原本停留在单进程内存、直接参与选号和限流判断的运行态，现已通过 Redis 共享运行态统一管理：

- `active_requests`
- `rate_limit_cooldown_until`
- `rate_limit_bucket`
- `rate_limit_hit_streak`

实现原则保持不变：

- `priority` / `balanced` 的选号策略不变
- 等待队列、429 固定冷却、token bucket、自适应回填逻辑不变
- token refresh / fallback / disable / recover 业务分支不变
- 改动的核心只是“这些热态从本地内存改为可共享后端承载”

仍需说明的一点是：`success_count` 这类统计/公平性字段仍主要依赖现有周期同步。它会影响长时间运行下的均衡细节，但不再阻塞“全局并发 / 429 / bucket 语义一致”这一层结论。

## 当前主阻塞项

### 1. k3s 多副本正常运行的实测证据仍不足

当前代码和本地单测已经表明多副本所需的共享状态基础具备，但仓库内仍缺少针对本次提交目标的明确验收记录：

- `kiro.rs replicas=2` 长时间运行验证
- 两个 Pod 同连同一套 PostgreSQL + Redis 的稳定性验证
- leader 切换后 follower / leader 路由行为仍正常的记录
- 在多副本下凭据并发、429 冷却、bucket 共享语义的集群侧实测记录

这部分现在是“能否提交为多副本可运行版本”的直接证据缺口。

### 2. 服务不中断滚动发布的实测闭环仍不足

虽然当前已经有 readiness/drain、shutdown release、leader 路由等基础能力，但仓库内仍缺少围绕连续流量的验收留档：

- 持续流量下滚动升级 `kiro.rs`
- 升级过程中是否出现 5xx、长阻塞、SSE 中断
- upgrade 期间请求是否仍能落到非 draining Pod
- leader 切换时 Admin 写路由是否持续可用

这部分现在是“能否提交为可滚动发布版本”的直接证据缺口。

### 3. 与原有状态版本“业务语义完全一致”的提交口径还缺留档

当前代码已经尽量保持原有处理逻辑不变，但仓库里还缺一份面向提交的兼容性说明或验收记录，至少需要覆盖：

- `priority` 模式下高优先级账号打满后的 fallback 与恢复
- `balanced` 模式下的并发分散
- `queueMaxSize` / `queueMaxWaitMs` 的等待、满队列和超时语义
- 固定 429 冷却不被连续 429 放大
- `defaultMaxConcurrency` 与凭据级覆盖的优先级关系
- 热态共享后，跨 Pod / 跨 manager 观察到的行为仍与单副本规则一致

当前已经有单测，但还没有把这些单测归档成“本次提交的语义一致性证据”。

## 非主线但可保留的能力

以下内容可以保留在仓库里，但不是本次提交的主阻塞项：

- 首次从 file/PVC 切到 external backend 的迁移 runbook
- `export-file-state` 导出工具及其回滚说明
- 更完整的冻结步骤、回滚阈值和切换窗口说明

这些内容属于后续上线治理项，不应继续压过当前这次提交真正需要证明的 3 个目标。

## 建议收敛后的提交口径

如果按你当前要求收敛，本次提交更适合定义为：

- `kiro.rs` 已完成调度关键热态共享化
- 已具备在 k3s 中多副本运行和滚动发布的实现基础
- 当前待补的是 k3s 多副本 / 滚动发布 / 语义一致性的实测证据与文档留档

而不是：

- “迁移回滚体系已经全部完成”
- “首次替换旧版所需的所有运维动作已经固化”

## 下一步实施计划

### 阶段 1. 固化 k3s 多副本运行样例

- 给 `kiro.rs` 补一份明确面向 state-ext 的正式配置样例
- 明确 `replicas=2` 时 PostgreSQL / Redis / readiness / leader 路由的部署前提
- 让提交后的部署资产直接服务于多副本验收

### 阶段 2. 固化滚动发布验收记录

- 在持续流量下实际跑一次滚动升级
- 记录 5xx、SSE 中断、耗时抖动、leader 切换表现
- 把结果写成提交后的验收记录，而不是只保留脚本

### 阶段 3. 固化语义一致性回归记录

- 把当前已覆盖的关键 token manager / shared runtime 测试整理成兼容性证据
- 明确“共享热态只改变状态承载位置，不改变业务分支语义”
- 如有必要，再补 1 份面向 `agentgear -> kiro.rs` 链路的端到端记录

## 建议验收口径

在以下条件全部满足前，不建议把 `kiro.rs` 定义为“本次提交目标已完成”：

1. `kiro.rs` 在 k3s 中已完成 `replicas=2` 实测并留档
2. 连续流量下滚动发布已实际跑通并留档
3. 关键转发 / 选号 / 冷却 / fallback 语义与原有状态版本一致的证据已留档
