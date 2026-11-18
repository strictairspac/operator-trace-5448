"""按份额调度的多执行器 worker。

一个进程可通过 WORKER_CONCURRENCY 同时领取多条执行；部署多份容器即多执行器争抢。
领取由 :mod:`src.services.scheduler` 的 Lua 脚本原子完成，因此并发领取不会重复占用份额；
终态（成功 / 失败 / 取消）都走同一个幂等释放脚本，份额恰好归还一次。
"""
import logging
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from src.config.database import Session
from src.config.settings import WORKER_CONCURRENCY, WORKER_IDLE_POLL
from src.models.trace import Trace
from src.services import scheduler
from src.services.audit import log
from src.services.operators import execute, ExecutionCancelled

_stop=threading.Event()


def _heartbeat():
 while not _stop.wait(2):
  Path('/tmp/worker-heartbeat').touch()


def _run_one(trace_id, share, dataset_id, token, on_done):
 try:
  with Session() as s:
   row=s.get(Trace,trace_id)
   # 陈旧队列元素或已被取消：仅释放份额，不执行
   if not row or row.status!='queued':
    scheduler.finish(trace_id,token);return
   row.status='running';snapshot=row.snapshot;s.commit()
  try:
   result=execute(snapshot['dataset'],snapshot['plan']['steps'],
                  check_cancel=lambda:scheduler.is_cancel_requested(trace_id))
   verdict=scheduler.finish(trace_id,token)
   if verdict=='cancelled':
    # 取消与完成撞在同一时刻：以取消为准，不发布结果
    _finalize_cancelled(trace_id);log('worker','trace.cancelled',str(trace_id));return
   with Session() as s:
    r=s.get(Trace,trace_id)
    if r and r.status=='running':r.result=result;r.status='completed'
    s.commit()
   log('worker','trace.completed',str(trace_id))
  except ExecutionCancelled:
   scheduler.finish(trace_id,token)
   _finalize_cancelled(trace_id);log('worker','trace.cancelled',str(trace_id))
  except Exception as exc:
   logging.exception('trace failed')
   verdict=scheduler.finish(trace_id,token)  # 失败同样释放份额
   if verdict=='cancelled':_finalize_cancelled(trace_id)
   else:
    with Session() as s:
     r=s.get(Trace,trace_id)
     if r and r.status=='running':r.status='failed';r.error=str(exc)[:1000]
     s.commit()
    log('worker','trace.failed',str(trace_id))
 finally:
  on_done()


def _finalize_cancelled(trace_id):
 from src.services.traces import finalize_cancelled
 finalize_cancelled(trace_id)


class _Supervisor:
 """持有线程池与在途计数，循环按剩余槽位原子领取并提交。"""

 def __init__(self):
  self.pool=ThreadPoolExecutor(max_workers=WORKER_CONCURRENCY)
  self.in_flight=0
  self.lock=threading.Lock()
  self.token=f'{uuid.uuid4().hex}-{threading.get_ident()}'
  self.sub=scheduler.wake_subscriber()

 def _dec(self):
  with self.lock:self.in_flight-=1

 def pump_once(self):
  if _stop.is_set():return
  with self.lock:
   slots=WORKER_CONCURRENCY-self.in_flight
   for _ in range(max(0,slots)):
    claimed=scheduler.claim(self.token)
    if not claimed:break
    self.in_flight+=1
    self.pool.submit(_run_one,*claimed,self.token,self._dec)
  # pub/sub 即时唤醒；超时轮询作为错过消息时的安全网
  self.sub.get_message(timeout=WORKER_IDLE_POLL, ignore_subscribe_messages=True)

 def run(self):
  while not _stop.is_set():self.pump_once()

 def shutdown(self):
  _stop.set();self.pool.shutdown(wait=True)  # 在途执行保留，直到自然停止或取消停止


def main():
 Path('/tmp/worker-heartbeat').touch()
 threading.Thread(target=_heartbeat,daemon=True).start()
 sup=_Supervisor()
 try:sup.run()
 finally:sup.shutdown()


if __name__=='__main__':main()
