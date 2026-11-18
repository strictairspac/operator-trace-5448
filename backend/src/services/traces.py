from datetime import datetime
import redis
from fastapi import HTTPException
from sqlalchemy import update
from src.config.database import Session
from src.models.dataset import Dataset
from src.models.plan import Plan
from src.models.trace import Trace
from src.utils.serialization import record
from src.services import scheduler


def list_items():
 with Session() as s:
  rows=[record(r) for r in s.query(Trace).order_by(Trace.id.desc()).limit(100).all()]
 state=scheduler.snapshot()
 queue_map={q['trace_id']:q for q in state['queued']}
 running_map={r['trace_id']:r for r in state['running']}
 for r in rows:
  if r['id'] in queue_map:r['scheduler']=queue_map[r['id']]
  elif r['id'] in running_map:r['scheduler']=running_map[r['id']]
 rows_append={'_capacities':state['capacities'],'_used':state['used'],'_max_skip':state['max_skip']}
 return {'items':rows,**rows_append}


def detail(item_id):
 with Session() as s:
  row=s.get(Trace,item_id)
  if not row:raise HTTPException(404,'未找到执行轨迹')
  out=record(row)
  if out['status']=='cancelled':out['result']=None
  return out


def create(data):
 with Session() as s:
  p=s.get(Plan,data.plan_id)
  if not p:raise HTTPException(404,'未找到算子计划')
  d=s.get(Dataset,p.dataset_id)
  if not d:raise HTTPException(404,'未找到数据集')
  dataset_id=d.id
  share=scheduler.declare_share(record(d))
  caps=scheduler.get_capacities()
  ds_cap=caps['datasets'].get(dataset_id)
  from src.config.settings import DEFAULT_DATASET_CAPACITY
  if ds_cap is None:ds_cap=DEFAULT_DATASET_CAPACITY
  if share>caps['global']:
   raise HTTPException(422,f'任务申报份额 {share} 超过全局容量 {caps["global"]}，请联系管理员扩容后重新提交')
  row=Trace(name=data.name,plan_id=p.id,dataset_id=dataset_id,status='queued',share=share,
            snapshot={'dataset':record(d),'plan':record(p)})
  s.add(row);s.commit();s.refresh(row);result=record(row)
 try:
  scheduler.enqueue(result['id'],share,dataset_id)
 except redis.RedisError:
  with Session() as s:r=s.get(Trace,result['id']);r.status='failed';r.error='队列不可用，请创建新任务';s.commit()
  raise HTTPException(503,'队列不可用，请稍后重试')
 return result


def cancel(item_id):
 with Session() as s:
  row=s.get(Trace,item_id)
  if not row:raise HTTPException(404,'未找到执行轨迹')
  # 终态（completed/failed/cancelled）幂等返回，重复取消不重复归还
  if row.status in ('completed','failed','cancelled'):return record(row)
  verdict=scheduler.request_cancel(item_id)
  now=datetime.utcnow()
  if verdict=='queued':
   # 排队任务立即退出：原子出队后立刻落终态，不发布结果
   row.status='cancelled';row.result=None;row.cancelled_at=now
  elif verdict=='running':
   # 运行任务保留 running 与份额，等 worker 在算子步边界停止后再归还。
   # 极端情况下 worker 已领取但库内状态尚未翻转，这里统一推进为 running。
   row.status='running'
   row.cancelled_at=now
  elif verdict=='absent':
   if row.status=='queued':
    # 调度器中已不存在而库内仍排队：该任务已无法被任何 worker 领取，立即终态避免悬挂
    row.status='cancelled';row.result=None
   # running 但调度器无记录属于不一致态，保留 running 供人工复查（与崩溃任务策略一致）
   row.cancelled_at=now
  s.commit();s.refresh(row);return record(row)


def finalize_cancelled(trace_id):
 """worker 计算停止后调用：仅当仍是 running 时落 cancelled，防止覆盖其它终态。"""
 with Session() as s:
  result=s.execute(update(Trace).where(Trace.id==trace_id,Trace.status=='running')
                   .values(status='cancelled',result=None,cancelled_at=datetime.utcnow()))
  s.commit();return result.rowcount==1
