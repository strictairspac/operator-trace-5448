"""轻量可移植建表/补列迁移，PostgreSQL 与 SQLite 均可用。

生产 PostgreSQL 由 SQLAlchemy metadata 建表；已存在的表只补缺失列，不做破坏性变更。
"""
from sqlalchemy import inspect,text
from src.config.database import engine,Base
# 确保所有模型都被导入并注册到 metadata
from src.models.identity import User,Audit  # noqa:F401
from src.models.dataset import Dataset  # noqa:F401
from src.models.plan import Plan  # noqa:F401
from src.models.trace import Trace  # noqa:F401
from src.models.capacity import CapacityConfig  # noqa:F401

_EXTRA_COLUMNS={
 'traces':{
  'share':'INTEGER',
  'cancelled_at':'DATETIME',
 },
}

def run_migrations():
 Base.metadata.create_all(engine)
 insp=inspect(engine)
 with engine.begin() as conn:
  for table,columns in _EXTRA_COLUMNS.items():
   if not insp.has_table(table):continue
   existing={c['name'] for c in insp.get_columns(table)}
   for name,ddl in columns.items():
    if name not in existing:
     conn.execute(text(f'ALTER TABLE {table} ADD COLUMN {name} {ddl}'))
