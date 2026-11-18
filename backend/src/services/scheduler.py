"""按资源份额调度的执行队列。

份额模型
========
每个执行任务在提交时申报整数 ``share``（见 :func:`declare_share`）。管理员维护两类容量：
全局容量 ``sched:cap:global`` 与每个数据集的容量 ``sched:cap:ds:{dataset_id}``。
任务被放行当且仅当放行后两类占用都不超过对应容量；不足时在 FIFO 队列中等待。

放行上界（防饿死）
==================
队首因容量不足阻塞时，允许后面的小任务占用空闲份额先行，但每个排队任务被更晚任务
插队的次数有明确上界 :data:`MAX_SKIP`（=2）。插队发生时所有被越过任务的跳过计数 +1；
计数达到上界的任务成为硬屏障，本次调度不再放行任何更晚任务。因此任何可容纳的大任务
至多被两个小任务越过，之后严格按序等待，不会无限等待。

原子性
======
领取 / 取消 / 释放均为单个 Redis Lua 脚本，Redis 单线程串行执行保证：
并发领取不重复占用，重复取消不重复归还，成功或失败都恰好释放一次份额。
"""
import json
import redis
from src.config.settings import REDIS_URL, DEFAULT_GLOBAL_CAPACITY, DEFAULT_DATASET_CAPACITY
from src.constants.enums import MAX_SKIP

Q='sched:queue'              # FIFO，元素为 trace id
META='sched:meta'            # hash: trace id -> 申报份额
DS='sched:ds'                # hash: trace id -> dataset id
SKIPS='sched:skips'          # hash: trace id -> 被更晚任务插队次数
RUNNING='sched:running'      # hash: trace id -> worker token（占用中）
G_USED='sched:used:global'   # 全局已占用份额（整数计数器）
DS_USED='sched:used:ds'      # hash: dataset id -> 已占用份额
G_CAP='sched:cap:global'
DS_CAP_PREFIX='sched:cap:ds:'
CANCEL_PREFIX='sched:cancel:'
CHANNEL='sched:wake'

# 领取：扫描队列，受容量与放行进上界约束，原子地占用份额并移除队首元素。
CLAIM_SCRIPT="""
local queue=KEYS[1] local meta=KEYS[2] local dsh=KEYS[3] local skips=KEYS[4]
local running=KEYS[5] local gusedk=KEYS[6] local dsused=KEYS[7] local channel=KEYS[8]
local token=ARGV[1] local maxskip=tonumber(ARGV[2])
local gcap=tonumber(redis.call('GET',ARGV[3]) or '0')
local default_dcap=tonumber(ARGV[5])
local ids=redis.call('LRANGE',queue,0,-1)
local blocked={}
for i=1,#ids do
 local id=ids[i]
 local sh=tonumber(redis.call('HGET',meta,id))
 if not sh then
  redis.call('LREM',queue,1,id)
 else
  local ds=redis.call('HGET',dsh,id)
  -- 数据集未显式配置容量（键缺失）时回落到默认容量，避免被当作 0 而永久阻塞
  local raw_dcap=redis.call('GET',ARGV[4]..ds)
  local dcap=raw_dcap and tonumber(raw_dcap) or default_dcap
  local gu=tonumber(redis.call('GET',gusedk) or '0')
  local du=tonumber(redis.call('HGET',dsused,ds) or '0')
  local fit=((gu+sh<=gcap) and (du+sh<=dcap))
  if fit and #blocked==0 then
   redis.call('LREM',queue,1,id)
   redis.call('HSET',running,id,token)
   redis.call('INCRBY',gusedk,sh)
   redis.call('HINCRBY',dsused,ds,sh)
   redis.call('PUBLISH',channel,id)
   return {id,sh,ds}
  elseif fit then
   -- 越过队首阻塞任务：仅被越过者的插队计数 +1，放行任务自身不计
   for j=1,#blocked do
    local bid=blocked[j]
    if tonumber(redis.call('HGET',skips,bid) or '0')>=maxskip then return false end
   end
   for j=1,#blocked do redis.call('HINCRBY',skips,blocked[j],1) end
   redis.call('LREM',queue,1,id)
   redis.call('HSET',running,id,token)
   redis.call('INCRBY',gusedk,sh)
   redis.call('HINCRBY',dsused,ds,sh)
   redis.call('PUBLISH',channel,id)
   return {id,sh,ds}
  else
   blocked[#blocked+1]=id
  end
 end
end
return false
"""

# 取消：排队任务立即出队；运行任务只置取消标记（份额待计算停止后由 worker 归还）。
CANCEL_SCRIPT="""
local queue=KEYS[1] local meta=KEYS[2] local dsh=KEYS[3] local skips=KEYS[4]
local running=KEYS[5] local id=ARGV[1] local channel=ARGV[2]
if redis.call('HEXISTS',running,id)==1 then
 redis.call('SET','sched:cancel:'..id,'1')
 redis.call('PUBLISH',channel,id)
 return 'running'
end
local removed=redis.call('LREM',queue,1,id)
if removed>0 then
 redis.call('HDEL',meta,id) redis.call('HDEL',dsh,id) redis.call('HDEL',skips,id)
 redis.call('PUBLISH',channel,id)
 return 'queued'
end
return 'absent'
"""

# 终态释放：token 校验，幂等。若已置取消标记则返回 cancelled，否则 done。
FINISH_SCRIPT="""
local running=KEYS[1] local gusedk=KEYS[2] local dsused=KEYS[3]
local meta=KEYS[4] local dsh=KEYS[5] local skips=KEYS[6] local channel=KEYS[7]
local id=ARGV[1] local token=ARGV[2]
local owner=redis.call('HGET',running,id)
if not owner or owner~=token then return 'absent' end
local sh=tonumber(redis.call('HGET',meta,id) or '0')
local ds=redis.call('HGET',dsh,id)
local cancelled=redis.call('EXISTS','sched:cancel:'..id)
redis.call('HDEL',running,id)
redis.call('DECRBY',gusedk,sh)
redis.call('HINCRBY',dsused,ds,-sh)
redis.call('DEL','sched:cancel:'..id)
redis.call('HDEL',meta,id) redis.call('HDEL',dsh,id) redis.call('HDEL',skips,id)
redis.call('PUBLISH',channel,id)
if cancelled==1 then return 'cancelled' end
return 'done'
"""

client=redis.Redis.from_url(REDIS_URL)
_claim=client.register_script(CLAIM_SCRIPT)
_cancel=client.register_script(CANCEL_SCRIPT)
_finish=client.register_script(FINISH_SCRIPT)


def declare_share(dataset):
    """根据数据集规模申报份额，1..8 的确定整数。

    公式固定可复现：每 250 条采样或每 25 个传感器各计 1 份，取两者之和并夹到 [1,8]。
    页面与 API 回显同一口径，提交后份额不可变。
    """
    readings=len(dataset.get('readings') or [])
    sensors=len(dataset.get('sensors') or [])
    return max(1,min(8,readings//250+sensors//25+1))


def enqueue(trace_id, share, dataset_id):
    client.hset(META,str(trace_id),str(share))
    client.hset(DS,str(trace_id),str(dataset_id))
    client.rpush(Q,str(trace_id))
    client.publish(CHANNEL,str(trace_id))


def claim(token):
    """原子领取一个可放行任务，返回 (trace_id, share, dataset_id) 或 None。"""
    res=_claim(keys=[Q,META,DS,SKIPS,RUNNING,G_USED,DS_USED,CHANNEL],
               args=[token,MAX_SKIP,G_CAP,DS_CAP_PREFIX,DEFAULT_DATASET_CAPACITY])
    if not res:return None
    return int(res[0]),int(res[1]),int(res[2])


def request_cancel(trace_id):
    """返回 'queued'（已立即出队）/ 'running'（已置取消标记）/ 'absent'（不在调度器）。"""
    return _cancel(keys=[Q,META,DS,SKIPS,RUNNING],args=[str(trace_id),CHANNEL]).decode()


def finish(trace_id, token):
    """计算停止后归还份额。返回 'done' / 'cancelled' / 'absent'（重复归还安全）。"""
    return _finish(keys=[RUNNING,G_USED,DS_USED,META,DS,SKIPS,CHANNEL],
                   args=[str(trace_id),token]).decode()


def is_cancel_requested(trace_id):
    return bool(client.exists(CANCEL_PREFIX+str(trace_id)))


def set_global_capacity(capacity):
    client.set(G_CAP,str(capacity));client.publish(CHANNEL,'capacity')


def set_dataset_capacity(dataset_id, capacity):
    client.set(DS_CAP_PREFIX+str(dataset_id),str(capacity));client.publish(CHANNEL,'capacity')


def get_capacities():
    g=client.get(G_CAP)
    prefix_len=len(DS_CAP_PREFIX)
    datasets={}
    for k in client.scan_iter(DS_CAP_PREFIX+'*',count=500):
        suffix=k[prefix_len:]
        if not suffix or not suffix.isdigit():continue  # 跳过异常键（如 ds:None）
        v=client.get(k)
        if v is not None:datasets[int(suffix)]=int(v)
    return {'global':int(g) if g else DEFAULT_GLOBAL_CAPACITY,'datasets':datasets}


def bootstrap(capacity_rows, dataset_ids=()):
    """容量键缺失时用数据库配置（或默认值）初始化，已存在的键不覆盖，保护在线缩容结果。"""
    if not client.exists(G_CAP):
        client.set(G_CAP,str(DEFAULT_GLOBAL_CAPACITY))
    existing=set()
    for k in client.scan_iter(DS_CAP_PREFIX+'*',count=500):
        suffix=k[len(DS_CAP_PREFIX):]
        if suffix and suffix.isdigit():existing.add(int(suffix))
    configured={row.dataset_id:row.capacity for row in capacity_rows if row.dataset_id is not None}
    # 管理员未显式配置的数据集使用默认容量，避免缺键被读取为 0 而阻塞全部任务
    for dataset_id in set(dataset_ids)|set(configured):
        if dataset_id is None or dataset_id in existing:continue
        client.set(DS_CAP_PREFIX+str(dataset_id),str(configured.get(dataset_id,DEFAULT_DATASET_CAPACITY)))


def snapshot():
    """供页面呈现：队列（含等待原因/插队计数）、占用、容量与终态所需的全部调度态。"""
    gcap=int(client.get(G_CAP) or DEFAULT_GLOBAL_CAPACITY)
    gused=int(client.get(G_USED) or 0)
    ids=client.lrange(Q,0,-1)
    ds_used={int(k):int(v) for k,v in client.hgetall(DS_USED).items()}
    prefix_len=len(DS_CAP_PREFIX)
    caps={}
    for k in client.scan_iter(DS_CAP_PREFIX+'*',count=500):
        suffix=k[prefix_len:]
        if not suffix or not suffix.isdigit():continue
        v=client.get(k)
        if v is not None:caps[int(suffix)]=int(v)
    queued=[]
    for position,raw in enumerate(ids):
        tid=int(raw)
        share=int(client.hget(META,tid) or 0)
        ds=int(client.hget(DS,tid) or 0)
        dcap=caps.get(ds,DEFAULT_DATASET_CAPACITY)
        dused=ds_used.get(ds,0)
        oversize=share>gcap or share>dcap
        shrink=gused>gcap or dused>dcap
        fit=(gused+share<=gcap) and (dused+share<=dcap)
        sk=int(client.hget(SKIPS,tid) or 0)
        # 队首任务已被插队到上界：它是硬屏障，即使暂时仍放不下，也不再允许后续任务越过
        is_head_barrier=position==0 and sk>=MAX_SKIP
        if oversize:reason='oversize'
        elif shrink:reason='shrink_paused'
        elif is_head_barrier:reason='bounded_skip_barrier'
        elif not fit and gused+share>gcap:reason='global_capacity'
        elif not fit:reason='dataset_capacity'
        else:reason='awaiting'
        queued.append({'trace_id':tid,'share':share,'dataset_id':ds,
                       'wait_reason':reason,'overtaken':sk})
    running=[{'trace_id':int(k),'share':int(client.hget(META,k) or 0),
              'dataset_id':int(client.hget(DS,k) or 0),'worker':v.decode()}
             for k,v in client.hgetall(RUNNING).items()]
    return {'capacities':{'global':gcap,'datasets':caps,
                          'defaults':{'global':DEFAULT_GLOBAL_CAPACITY,
                                      'dataset':DEFAULT_DATASET_CAPACITY}},
            'used':{'global':gused,'datasets':ds_used},
            'max_skip':MAX_SKIP,
            'queued':queued,'running':running}


def wake_subscriber():
    pub=client.pubsub();pub.subscribe(CHANNEL);return pub


def reset_for_tests():
    for k in client.scan_iter('sched:*',count=1000):client.delete(k)
