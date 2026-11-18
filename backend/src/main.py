import os,logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from sqlalchemy import text
from src.config.database import engine,Session
from src.config.migrations import run_migrations
from src.models.capacity import CapacityConfig
from src.services import scheduler
from src.routes import auth,datasets,plans,traces,audit,capacity
from src.middlewares.errors import install_errors
from src.middlewares.requests import RequestTrace
@asynccontextmanager
async def lifespan(app):
 run_migrations()
 from src.models.dataset import Dataset
 with Session() as s:
  dataset_ids=[row[0] for row in s.query(Dataset.id).all()]
  scheduler.bootstrap(s.query(CapacityConfig).all(),dataset_ids)
 if os.getenv('SEED_DEMO','true').lower()=='true':
  try:
   from scripts.seed_demo import seed
   seed()
  except Exception:logging.exception('seed failed; API remains available')
 yield
app=FastAPI(title='松原 · 分析算子执行剖面仪',lifespan=lifespan)
app.add_middleware(RequestTrace);install_errors(app)
for r in (auth,datasets,plans,traces,audit,capacity):app.include_router(r.router)
@app.get('/health')
def health():
 with engine.connect() as c:c.execute(text('SELECT 1'))
 scheduler.client.ping();return {'status':'ok'}
