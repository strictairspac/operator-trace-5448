import os
DATABASE_URL=os.getenv('DATABASE_URL','postgresql+psycopg://trace:tracepass@db:5432/operatortrace')
REDIS_URL=os.getenv('REDIS_URL','redis://redis:6379/0')
JWT_SECRET=os.environ['JWT_SECRET']
# 全局默认容量（份额单位）。管理员可在运行时通过 /capacity 调整，无需重启。
DEFAULT_GLOBAL_CAPACITY=int(os.getenv('DEFAULT_GLOBAL_CAPACITY','8'))
# 单个数据集默认容量；管理员可按数据集单独覆盖。
DEFAULT_DATASET_CAPACITY=int(os.getenv('DEFAULT_DATASET_CAPACITY','6'))
# 每个 worker 进程内并发领取的执行数；多进程部署时横向扩展。
WORKER_CONCURRENCY=int(os.getenv('WORKER_CONCURRENCY','2'))
# worker 无任务可领时的空转轮询间隔（秒），正常唤醒走 Redis pub/sub。
WORKER_IDLE_POLL=float(os.getenv('WORKER_IDLE_POLL','1.0'))
