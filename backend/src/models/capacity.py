from sqlalchemy import Column,Integer,DateTime,func
from src.config.database import Base
class CapacityConfig(Base):
 """管理员设置的容量（份额单位）。dataset_id 为空表示全局容量，其余为数据集容量。"""
 __tablename__='capacity_configs'
 id=Column(Integer,primary_key=True)
 dataset_id=Column(Integer,unique=True,nullable=True)
 capacity=Column(Integer,nullable=False)
 updated_at=Column(DateTime,server_default=func.now(),onupdate=func.now())
