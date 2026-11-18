export const ROLES={admin:'管理员',operator:'操作员',viewer:'观察员'};
export const STATUSES={queued:'等待执行',running:'执行中',completed:'已完成',failed:'执行失败',cancelled:'已取消'};
export const OPERATORS=['scan','filter','hash_join','group'];
// 与后端 WAIT_REASONS 对齐
export const WAIT_REASONS={
 awaiting:'等待调度轮询',
 global_capacity:'全局份额不足，等待占用释放',
 dataset_capacity:'该数据集份额不足，等待同数据集占用释放',
 bounded_skip_barrier:`已达放行上界（插队上限），后续任务不得越过`,
 oversize:'申报份额超过当前容量，等待管理员扩容',
 shrink_paused:'在线缩容超占中，暂停领取新任务直到占用回落'
};
