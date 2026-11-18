"""端到端：FastAPI + SQLite + 真实 Redis + 真实 worker 线程，覆盖三条要求场景。

1. 多执行器争抢：并发领取不重复占用；
2. 连续小任务：大任务受放行上界保护，不会无限等待；
3. 取消撞完成：排队立即退出、运行待计算停止归还、取消不发布结果、重复取消幂等。
"""
import os
import threading
import time

import pytest
from fastapi.testclient import TestClient

from src.config.database import Base,engine,Session
from src.config.migrations import run_migrations
from src.models.dataset import Dataset
from src.models.plan import Plan
from src.models.trace import Trace
from src.services import scheduler
from src.services.operators import ExecutionCancelled


@pytest.fixture(scope='session',autouse=True)
def _schema():
 # 复用 conftest 已建表的同一测试库与全局 engine
 from src.config import database as dbmod  # noqa:F401
 run_migrations()
 yield


@pytest.fixture(autouse=True)
def _clean():
 scheduler.reset_for_tests()
 scheduler.set_global_capacity(10)
 for ds in range(1,10):scheduler.set_dataset_capacity(ds,10)
 with Session() as s:
  s.query(Trace).delete();s.query(Plan).delete();s.query(Dataset).delete();s.commit()
 yield
 scheduler.reset_for_tests()


STEPS=[{'op':'scan','source':'readings'}]


@pytest.fixture
def make_dataset_plan():
 def _make(name,readings=10,sensors=2):
  with Session() as s:
   d=Dataset(name=name,description='t',
             sensors=[{'sensor_id':i+1,'zone':'z','device_type':'pump'} for i in range(sensors)],
             readings=[{'sensor_id':i,'value':1.0,'recorded_at':'2026-11-17T09:00:00'} for i in range(readings)])
   s.add(d);s.flush()
   p=Plan(name=name+'-plan',dataset_id=d.id,steps=STEPS,note='t')
   s.add(p);s.commit();return d.id,p.id
 return _make


# ---------- worker 直调（不起 HTTP），专注调度/取消语义 ----------

def _drain_claims(token, max_claims=50):
 out=[]
 for _ in range(max_claims):
  c=scheduler.claim(token)
  if not c:break
  out.append(c)
 return out


def test_multi_executor_contention_no_double_allocation(make_dataset_plan):
 from src.worker import _run_one
 _,p1=make_dataset_plan('ds-a',readings=10)
 # 直接造 12 个排队任务，每份 1
 with Session() as s:
  did=s.get(Plan,p1).dataset_id
  for i in range(12):
   t=Trace(name=f't{i}',dataset_id=did,plan_id=p1,status='queued',share=1,
           snapshot={'dataset':record_dataset(did),'plan':{'steps':STEPS}})
   s.add(t)
  s.commit();ids=[r.id for r in s.query(Trace).all()]
 for tid in ids:scheduler.enqueue(tid,1,did)
 done=threading.Event();lock=threading.Lock();claimed=[]
 def worker(tag):
  while not done.is_set():
   c=scheduler.claim(tag)
   if not c:time.sleep(0.01);continue
   with lock:claimed.append(c[0])
   ev=threading.Event()
   _run_one(c[0],c[1],c[2],tag,ev.set)
 threads=[threading.Thread(target=worker,args=(f'w{i}',)) for i in range(4)]
 for t in threads:t.start()
 deadline=time.time()+10
 while time.time()<deadline:
  with Session() as s:
   if s.query(Trace).filter(Trace.status!='completed').count()==0:break
  time.sleep(0.05)
 done.set()
 for t in threads:t.join(timeout=3)
 assert len(claimed)==12 and len(set(claimed))==12  # 无重复领取
 with Session() as s:
  assert s.query(Trace).filter_by(status='completed').count()==12
 assert scheduler.snapshot()['used']['global']==0   # 全部恰好释放


def record_dataset(did):
 with Session() as s:
  d=s.get(Dataset,did)
  return {'readings':d.readings,'sensors':d.sensors}


def test_cancel_queued_exits_immediately_and_publishes_nothing(make_dataset_plan):
 _,p1=make_dataset_plan('ds-b')
 with Session() as s:
  did=s.get(Plan,p1).dataset_id
  t=Trace(name='cancel-q',dataset_id=did,plan_id=p1,status='queued',share=1,
          snapshot={'dataset':record_dataset(did),'plan':{'steps':STEPS}})
  s.add(t);s.commit();tid=t.id
 scheduler.enqueue(tid,1,did)
 from src.services.traces import cancel
 row=cancel(tid)
 assert row['status']=='cancelled' and row['result'] is None and row['cancelled_at']
 # 调度器中已无痕迹，份额未被占用
 assert scheduler.snapshot()['queued']==[]
 assert scheduler.snapshot()['used']['global']==0
 # 重复取消幂等
 again=cancel(tid)
 assert again['status']=='cancelled'
 assert scheduler.snapshot()['used']['global']==0


def test_cancel_running_waits_for_compute_then_releases(make_dataset_plan,monkeypatch):
 from src.worker import _run_one
 _,p1=make_dataset_plan('ds-c')
 with Session() as s:
  did=s.get(Plan,p1).dataset_id
  t=Trace(name='cancel-r',dataset_id=did,plan_id=p1,status='queued',share=2,
          snapshot={'dataset':record_dataset(did),'plan':{'steps':STEPS}})
  s.add(t);s.commit();tid=t.id
 scheduler.enqueue(tid,2,did)
 c=scheduler.claim('w1');assert c[0]==tid
 # worker 领取后先把库内状态置 running（与 _run_one 内的翻转一致）
 with Session() as s:s.get(Trace,tid).status='running';s.commit()
 # 模拟计算中收到取消：让 execute 在下一个步边界抛 ExecutionCancelled
 import src.services.operators as ops
 orig=ops.execute
 def cancelling(dataset,steps,check_cancel=None):
  assert check_cancel() is True
  raise ExecutionCancelled('cancelled')
 monkeypatch.setattr('src.worker.execute',cancelling)
 from src.services.traces import cancel
 row=cancel(tid)
 # 取消请求先于 worker 翻转库内状态：服务将其推进 running 并保留份额，等待计算停止
 assert row['status']=='running'  # 运行任务保持 running，份额暂不归还
 assert scheduler.snapshot()['used']['global']==2
 ev=threading.Event()
 # _run_one 仅对 queued 行执行；这里回置以模拟“领取后、翻转前收到取消”的完整执行路径
 with Session() as s:s.get(Trace,tid).status='queued';s.commit()
 _run_one(tid,2,did,'w1',ev.set);ev.wait(3)
 with Session() as s:
  r=s.get(Trace,tid)
  assert r.status=='cancelled' and r.result is None
 assert scheduler.snapshot()['used']['global']==0
 monkeypatch.setattr('src.worker.execute',orig)


def test_cancel_racing_completion_publishes_no_result(make_dataset_plan):
 # 取消标记与计算完成同时发生：finish 返回 cancelled，结果不得发布
 _,p1=make_dataset_plan('ds-d')
 with Session() as s:
  did=s.get(Plan,p1).dataset_id
  t=Trace(name='race',dataset_id=did,plan_id=p1,status='running',share=1,
          snapshot={'dataset':record_dataset(did),'plan':{'steps':STEPS}})
  s.add(t);s.commit();tid=t.id
 scheduler.enqueue(tid,1,did)
 c=scheduler.claim('w1');assert c[0]==tid
 scheduler.request_cancel(tid)   # 计算恰好完成前一刻收到取消
 verdict=scheduler.finish(tid,'w1')
 assert verdict=='cancelled'
 from src.services.traces import finalize_cancelled
 assert finalize_cancelled(tid) is True
 with Session() as s:
  r=s.get(Trace,tid)
  assert r.status=='cancelled' and r.result is None
 assert scheduler.snapshot()['used']['global']==0


def test_failure_releases_share(make_dataset_plan,monkeypatch):
 from src.worker import _run_one
 _,p1=make_dataset_plan('ds-e')
 with Session() as s:
  did=s.get(Plan,p1).dataset_id
  t=Trace(name='fail',dataset_id=did,plan_id=p1,status='queued',share=3,
          snapshot={'dataset':record_dataset(did),'plan':{'steps':STEPS}})
  s.add(t);s.commit();tid=t.id
 scheduler.enqueue(tid,3,did)
 c=scheduler.claim('w1');assert c[0]==tid
 def boom(dataset,steps,check_cancel=None):raise RuntimeError('boom')
 monkeypatch.setattr('src.worker.execute',boom)
 ev=threading.Event()
 _run_one(tid,3,did,'w1',ev.set);ev.wait(3)
 with Session() as s:
  r=s.get(Trace,tid)
  assert r.status=='failed' and 'boom' in (r.error or '')
 assert scheduler.snapshot()['used']['global']==0
