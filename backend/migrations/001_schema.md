# Schema v1
SQLAlchemy metadata creates users, audits, datasets, plans, traces. Natural names are unique; foreign keys reference parent entities. Each trace stores immutable JSON input snapshots. PostgreSQL persists metadata and teaching datasets; the bounded Python operators execute the plan.

## 按份额调度增量（v1.1）
- `traces` 增加 `share INTEGER`（提交时申报的资源份额，确定公式，不可变）与 `cancelled_at DATETIME`（取消确认时间）。
- 新增 `capacity_configs`：`dataset_id` 为空表示全局容量，其余为单数据集容量；运行时调整同时写入 PostgreSQL 与 Redis 容量键。
- 调度队列、占用计数、取消标记与放行上界计数全部存于 Redis（`sched:*` 键族），由 Lua 脚本保证领取/取消/释放的原子性与幂等。
- 建表与补列由 `src/config/migrations.py` 幂等执行，已存在的表只补缺失列。
