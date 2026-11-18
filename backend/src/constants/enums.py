ROLES=('admin','operator','viewer')
STATUSES=('queued','running','completed','failed','cancelled')
OPERATORS=('scan','filter','hash_join','group')
# 调度放行上界：任意排队任务被更晚任务插队（占用空闲份额先行）的次数上限。
# 达到上界后该任务成为屏障，后续任务不得再越过它，保证可容纳的大任务不会无限等待。
MAX_SKIP=2
# 等待原因代码（与前端 constants/enums.js 的 WAIT_REASONS 保持一致）
WAIT_REASONS=('awaiting','global_capacity','dataset_capacity','bounded_skip_barrier','oversize','shrink_paused')
