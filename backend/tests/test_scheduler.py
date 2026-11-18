"""调度器核心不变量：FIFO + 容量 + 放行上界 + 原子性。"""
from src.constants.enums import WAIT_REASONS
from src.services import scheduler


def enqueue_many(items):
 for tid,share,ds in items:scheduler.enqueue(tid,share,ds)


def test_declare_share_bounded_and_deterministic():
 empty={'readings':[],'sensors':[]}
 assert scheduler.declare_share(empty)==1
 big={'readings':[{}]*2000,'sensors':[{}]*100}
 assert scheduler.declare_share(big)==8
 mid={'readings':[{}]*500,'sensors':[{}]*30}
 # 500//250=2, 30//25=1, +1 = 4
 assert scheduler.declare_share(mid)==4
 # 确定性：重复计算一致
 assert scheduler.declare_share(mid)==scheduler.declare_share(mid)


def test_capacity_fit_concurrent_admission():
 # 全局容量 8：4+4 可并发，第三个 4 必须等待
 scheduler.set_global_capacity(8)
 scheduler.set_dataset_capacity(1,8)
 enqueue_many([(1,4,1),(2,4,1),(3,4,1)])
 a=scheduler.claim('w1');b=scheduler.claim('w2')
 assert {a[0],b[0]}=={1,2}
 assert scheduler.claim('w3') is None
 snap=scheduler.snapshot()
 assert snap['used']['global']==8
 assert snap['used']['datasets'][1]==8
 q=snap['queued'][0]
 assert q['trace_id']==3 and q['wait_reason'] in ('global_capacity','dataset_capacity')


def test_idle_shares_run_small_tasks_behind_blocking_head():
 # 队首要 8 份，先预置 6 份占用使其阻塞；后面的 1 份小任务应占用空闲份额先行
 scheduler.set_global_capacity(8)
 scheduler.set_dataset_capacity(1,8);scheduler.set_dataset_capacity(2,8)
 scheduler.enqueue(99,6,1);scheduler.claim('w0')  # 先占 6，全局仅剩 2
 enqueue_many([(10,8,1),(11,1,2),(12,1,2)])
 first=scheduler.claim('w1')
 assert first[0]==11  # 小任务越过队首
 second=scheduler.claim('w1')
 assert second[0]==12
 # 第三次：队首仍阻塞，且已被插队 2 次（=MAX_SKIP），成为屏障
 assert scheduler.claim('w1') is None
 snap=scheduler.snapshot()
 head=next(q for q in snap['queued'] if q['trace_id']==10)
 assert head['overtaken']==2
 # 队首达到放行上界后呈现明确的屏障等待原因（即使其仍因容量不足放不下）
 assert head['wait_reason']=='bounded_skip_barrier'


def test_explicit_admission_bound_two_overtakes_only():
 # 直接构造持续小任务流，验证大任务被插队次数永远不超过 MAX_SKIP
 scheduler.set_global_capacity(10)
 for ds in (1,2):scheduler.set_dataset_capacity(ds,10)
 scheduler.enqueue(90,2,1);scheduler.claim('w0')  # 先占 2，全局剩 8，大任务 9 份放不下
 scheduler.enqueue(1,9,1)          # 大任务随后入队，必然阻塞
 for tid in range(2,12):          # 10 个连续 1 份小任务（数据集 2，容量充足）
  scheduler.enqueue(tid,1,2)
 admitted=[]
 for _ in range(20):
  got=scheduler.claim('w1')
  if not got:break
  admitted.append(got[0])
 # 空闲份额 8 份，但放行上界只允许两个小任务越过大任务，之后屏障生效
 assert admitted==[2,3]
 head=scheduler.snapshot()['queued'][0]
 assert head['trace_id']==1 and head['overtaken']==2
 # 释放预占后：占用仅剩两个小任务（2 份），空闲 8 份仍 < 大任务 9 份，继续严格等待
 scheduler.finish(90,'w0')
 assert scheduler.snapshot()['queued'][0]['trace_id']==1
 # 再等一个小任务结束，空闲 9 份，大任务严格下一个放行，不再被越过
 scheduler.finish(admitted[0],'w1')
 got=scheduler.claim('w1')
 assert got[0]==1
 # 大任务已占 9 份，仅剩 1 份空闲，队列严格按 FIFO 推进，不会再发生越过
 snap=scheduler.snapshot()
 assert snap['queued'][0]['trace_id']==4


def test_dataset_capacity_is_independent_constraint():
 scheduler.set_global_capacity(8)
 scheduler.set_dataset_capacity(1,2);scheduler.set_dataset_capacity(2,8)
 enqueue_many([(1,2,1),(2,2,1),(3,2,2)])
 a=scheduler.claim('w1');b=scheduler.claim('w2')
 assert {a[0],b[0]}=={1,3}  # 同数据集第二个被挡，别的数据集放行
 assert scheduler.claim('w1') is None
 blocked=scheduler.snapshot()['queued'][0]
 assert blocked['trace_id']==2 and blocked['wait_reason']=='dataset_capacity'


def test_missing_dataset_capacity_key_uses_default_not_zero():
 # 新数据集尚未在 Redis 配置容量：缺键应回落默认容量，而不是当作 0 永久阻塞
 from src.config.settings import DEFAULT_DATASET_CAPACITY
 scheduler.set_global_capacity(8)
 scheduler.enqueue(77,2,999)  # 999 从未 set_dataset_capacity
 got=scheduler.claim('w1')
 assert got==(77,2,999)
 snap=scheduler.snapshot()
 assert snap['capacities']['datasets'].get(999) is None
 assert scheduler.finish(77,'w1')=='done'


def test_finish_is_idempotent_and_releases_exactly_once():
 scheduler.set_global_capacity(6)
 scheduler.set_dataset_capacity(1,6)
 scheduler.enqueue(1,4,1);got=scheduler.claim('w1')
 assert got[0]==1
 assert scheduler.snapshot()['used']['global']==4
 assert scheduler.finish(1,'w1')=='done'
 assert scheduler.snapshot()['used']['global']==0
 # 重复归还：token 已不在 running 表，absent 且占用保持 0
 assert scheduler.finish(1,'w1')=='absent'
 assert scheduler.snapshot()['used']['global']==0
 # 错误 token 不能释放别人的占用
 scheduler.enqueue(2,3,1);scheduler.claim('w2')
 assert scheduler.finish(2,'wrong-token')=='absent'
 assert scheduler.snapshot()['used']['global']==3


def test_concurrent_claims_never_double_allocate():
 import threading
 scheduler.set_global_capacity(10)
 for ds in range(1,6):scheduler.set_dataset_capacity(ds,10)
 for tid in range(1,21):scheduler.enqueue(tid,1,tid%5+1)
 got_ids=[];lock=threading.Lock()
 def worker(tag):
  local=[]
  for _ in range(20):
   r=scheduler.claim(tag)
   if not r:break
   local.append(r[0])
  with lock:got_ids.extend(local)
 threads=[threading.Thread(target=worker,args=(f'w{i}',)) for i in range(8)]
 for t in threads:t.start()
 for t in threads:t.join()
 assert len(got_ids)==10                    # 容量 10，份额各 1
 assert sorted(got_ids)==list(range(1,11)) # 无重复领取
 assert scheduler.snapshot()['used']['global']==10


def test_cancel_queued_removes_and_cancel_is_idempotent():
 scheduler.set_global_capacity(4)
 scheduler.set_dataset_capacity(1,4)
 scheduler.enqueue(1,3,1);scheduler.enqueue(2,3,1)
 scheduler.claim('w1')                      # id1 占用 3，id2 排队
 assert scheduler.request_cancel(2)=='queued'
 assert scheduler.request_cancel(2)=='absent'  # 重复取消：已不在队列
 snap=scheduler.snapshot()
 assert all(q['trace_id']!=2 for q in snap['queued'])
 assert scheduler.claim('w2') is None      # 容量仍只有 1，不会凭空放行


def test_cancel_running_sets_flag_then_finish_returns_cancelled():
 scheduler.set_global_capacity(4)
 scheduler.set_dataset_capacity(1,4)
 scheduler.enqueue(1,2,1);scheduler.claim('w1')
 assert scheduler.request_cancel(1)=='running'
 assert scheduler.is_cancel_requested(1)
 assert scheduler.request_cancel(1)=='running'  # 重复取消不重复归还
 assert scheduler.snapshot()['used']['global']==2  # 计算停止前份额保留
 assert scheduler.finish(1,'w1')=='cancelled'
 assert scheduler.snapshot()['used']['global']==0
 assert scheduler.finish(1,'w1')=='absent'        # 重复归还安全
 assert not scheduler.is_cancel_requested(1)


def test_snapshot_wait_reasons_are_known_enum_values():
 scheduler.set_global_capacity(2)
 scheduler.set_dataset_capacity(1,2);scheduler.set_dataset_capacity(2,8)
 # 超出容量
 scheduler.enqueue(1,5,1)
 # 容量不足 + 数据集约束
 scheduler.enqueue(2,2,1)
 # 正常等待轮询
 scheduler.set_global_capacity(8)
 scheduler.enqueue(3,1,2)
 reasons={q['wait_reason'] for q in scheduler.snapshot()['queued']}
 assert reasons <= set(WAIT_REASONS)
 assert 'oversize' in reasons and 'awaiting' in reasons
