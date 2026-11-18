from fastapi import HTTPException
from src.config.database import Session
from src.config.settings import DEFAULT_GLOBAL_CAPACITY, DEFAULT_DATASET_CAPACITY
from src.models.capacity import CapacityConfig
from src.models.dataset import Dataset
from src.services import scheduler


def list_capacities():
 with Session() as s:
  rows=s.query(CapacityConfig).all()
  dataset_ids={d for d in (d.id for d in s.query(Dataset.id).all())}
 configured={r.dataset_id:r.capacity for r in rows}
 state=scheduler.snapshot()
 global_cap=configured.get(None,DEFAULT_GLOBAL_CAPACITY)
 per_dataset={}
 for did in sorted(dataset_ids):
  per_dataset[did]={'configured':configured.get(did),
                    'effective':configured.get(did,DEFAULT_DATASET_CAPACITY)}
 return {'global':{'configured':None if None not in configured else global_cap,
                   'effective':global_cap},
         'datasets':per_dataset,
         'scheduler':state}


def _get_global_row(s):
 return s.query(CapacityConfig).filter(CapacityConfig.dataset_id.is_(None)).one_or_none()


def set_global(capacity):
 if capacity<1 or capacity>1000:raise HTTPException(422,'全局容量须为 1 至 1000 的整数份额')
 with Session() as s:
  row=_get_global_row(s)
  if row:row.capacity=capacity
  else:s.add(CapacityConfig(dataset_id=None,capacity=capacity))
  s.commit()
 # 在线缩容不打断在途执行，只通过容量键收紧后续领取
 scheduler.set_global_capacity(capacity)
 return list_capacities()


def set_dataset(dataset_id, capacity):
 if capacity<1 or capacity>1000:raise HTTPException(422,'数据集容量须为 1 至 1000 的整数份额')
 with Session() as s:
  if not s.get(Dataset,dataset_id):raise HTTPException(404,'未找到数据集')
  row=s.query(CapacityConfig).filter_by(dataset_id=dataset_id).one_or_none()
  if row:row.capacity=capacity
  else:s.add(CapacityConfig(dataset_id=dataset_id,capacity=capacity))
  s.commit()
 scheduler.set_dataset_capacity(dataset_id,capacity)
 return list_capacities()
