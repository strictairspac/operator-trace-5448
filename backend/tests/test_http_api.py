"""HTTP 层：认证、容量管理（仅管理员）、提交申报份额、取消与列表调度态回显。"""
import os
import pytest
from fastapi.testclient import TestClient

from src.config.database import Session
from src.main import app
from src.models.dataset import Dataset
from src.models.plan import Plan
from src.models.trace import Trace
from src.services import scheduler

PASSWORD='Trace2026!'


@pytest.fixture()
def client():
 with TestClient(app) as c:  # 进入 lifespan：建表/容量引导
  yield c


@pytest.fixture(autouse=True)
def _clean():
 scheduler.reset_for_tests()
 scheduler.set_global_capacity(10)
 for ds in range(1,10):scheduler.set_dataset_capacity(ds,10)
 with Session() as s:
  s.query(Trace).delete();s.query(Plan).delete();s.query(Dataset).delete();s.commit()
 yield
 scheduler.reset_for_tests()


def _token(client,role):
 r=client.post('/auth/login',json={'email':f'{role}@operator.local','password':PASSWORD})
 assert r.status_code==200,r.text
 return r.json()['token']


def _seed(client,role='operator',readings=10):
 tok=_token(client,role)
 h={'Authorization':f'Bearer {tok}'}
 body={'name':f'ds-{readings}-{os.getpid()}-{len(os.listdir("/tmp"))}',
       'description':'t','sensors':[{'sensor_id':1,'zone':'z','device_type':'pump'}],
       'readings':[{'sensor_id':1,'value':1.0,'recorded_at':'2026-11-17T09:00:00'} for _ in range(readings)]}
 r=client.post('/datasets',json=body,headers=h);assert r.status_code==200,r.text
 did=r.json()['id']
 r=client.post('/plans',json={'name':f'p-{did}-{os.urandom(2).hex()}','dataset_id':did,
   'steps':[{'op':'scan','source':'readings'}]},headers=h)
 assert r.status_code==200,r.text
 return h,did,r.json()['id']


def test_login_and_me(client):
 tok=_token(client,'viewer')
 r=client.get('/auth/me',headers={'Authorization':f'Bearer {tok}'})
 assert r.status_code==200 and r.json()['role']=='viewer'


def test_submit_declares_share_and_listing_shows_scheduler_state(client):
 h,did,pid=_seed(client)
 r=client.post('/traces',json={'name':'run-1','plan_id':pid},headers=h)
 assert r.status_code==200,r.text
 tid=r.json()['id'];assert r.json()['share']==1
 r=client.get('/traces',headers=h)
 payload=r.json()
 assert payload['_capacities']['global']==10
 assert payload['_max_skip']==2
 item=next(x for x in payload['items'] if x['id']==tid)
 assert item['scheduler']['wait_reason'] in ('awaiting','global_capacity','dataset_capacity')
 assert item['scheduler']['share']==1
 scheduler.request_cancel(tid)


def test_oversize_submit_rejected_before_enqueue(client):
 ah={'Authorization':f'Bearer {_token(client,"admin")}'}
 client.put('/capacity/global',json={'capacity':1},headers=ah)
 h,did,pid=_seed(client,readings=2000)  # 最大数据集 -> 申报 8 份
 r=client.post('/traces',json={'name':'too-big','plan_id':pid},headers=h)
 assert r.status_code==422 and '容量' in r.json()['detail']
 client.put('/capacity/global',json={'capacity':10},headers=ah)


def test_only_admin_can_change_capacity(client):
 oh={'Authorization':f'Bearer {_token(client,"operator")}'}
 r=client.put('/capacity/global',json={'capacity':3},headers=oh)
 assert r.status_code==403
 ah={'Authorization':f'Bearer {_token(client,"admin")}'}
 r=client.put('/capacity/global',json={'capacity':3},headers=ah)
 assert r.status_code==200 and r.json()['global']['effective']==3


def test_dataset_capacity_crud_and_reflected_in_listing(client):
 ah={'Authorization':f'Bearer {_token(client,"admin")}'}
 h,did,pid=_seed(client)
 r=client.put('/capacity/dataset',json={'dataset_id':did,'capacity':2},headers=ah)
 assert r.status_code==200
 r=client.get('/capacity',headers=h)
 assert r.json()['datasets'][str(did)]['effective']==2


def test_cancel_queued_via_http_is_idempotent_and_blocks_export(client):
 h,did,pid=_seed(client)
 r=client.post('/traces',json={'name':'cancel-http','plan_id':pid},headers=h)
 tid=r.json()['id']
 r1=client.post(f'/traces/{tid}/cancel',json={},headers=h)
 assert r1.status_code==200 and r1.json()['status']=='cancelled'
 r2=client.post(f'/traces/{tid}/cancel',json={},headers=h)  # 重复取消
 assert r2.status_code==200 and r2.json()['status']=='cancelled'
 r3=client.get(f'/traces/{tid}/export',headers=h)
 assert r3.status_code==409  # 取消后不发布结果
 detail=client.get(f'/traces/{tid}',headers=h).json()
 assert detail['result'] is None


def test_viewer_cannot_submit_or_cancel(client):
 vh={'Authorization':f'Bearer {_token(client,"viewer")}'}
 r=client.post('/traces',json={'name':'x','plan_id':1},headers=vh)
 assert r.status_code==403
 r=client.post('/traces/1/cancel',json={},headers=vh)
 assert r.status_code==403
