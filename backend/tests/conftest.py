import os
os.environ.setdefault('JWT_SECRET','test-secret')
os.environ.setdefault('REDIS_URL','redis://127.0.0.1:16399/0')
os.environ.setdefault('DATABASE_URL','sqlite+pysqlite:////tmp/operator_trace_test.db')
os.environ.setdefault('SEED_DEMO','false')
os.environ.setdefault('DEFAULT_GLOBAL_CAPACITY','8')
os.environ.setdefault('DEFAULT_DATASET_CAPACITY','6')
import pytest
from src.config.database import Base,engine
from src.config.migrations import run_migrations
from src.services import scheduler


@pytest.fixture(scope='session',autouse=True)
def _schema():
 if os.path.exists('/tmp/operator_trace_test.db'):os.remove('/tmp/operator_trace_test.db')
 run_migrations()
 # 测试环境关闭自动 seed，这里显式创建三个角色账号
 from src.models.identity import User
 from src.utils.security import hash_password
 from src.config.database import Session
 with Session() as s:
  for role in ('admin','operator','viewer'):
   email=f'{role}@operator.local'
   if not s.query(User).filter_by(email=email).first():
    s.add(User(email=email,role=role,password=hash_password('Trace2026!')))
  s.commit()
 yield


@pytest.fixture(autouse=True)
def _clean_redis():
 scheduler.reset_for_tests()
 # 恢复默认容量键（Redis 键缺失时读取为 0，测试统一显式初始化）
 scheduler.set_global_capacity(int(os.environ['DEFAULT_GLOBAL_CAPACITY']))
 for ds in range(1,6):
  scheduler.set_dataset_capacity(ds,int(os.environ['DEFAULT_DATASET_CAPACITY']))
 yield
 scheduler.reset_for_tests()


@pytest.fixture
def db_session():
 from src.config.database import Session
 with Session() as s:
  yield s
  s.rollback()
