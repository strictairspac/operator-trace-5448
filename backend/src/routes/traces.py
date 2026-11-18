import json
from fastapi import APIRouter,Depends,HTTPException
from fastapi.responses import Response
from src.middlewares.auth import current,writer
from src.schemas.business import TraceInput
from src.services import traces
from src.controllers.business import start_trace,cancel_trace
router=APIRouter()
@router.get('/traces')
def listing(u=Depends(current)):return traces.list_items()
@router.post('/traces')
def create(data:TraceInput,u=Depends(writer)):return start_trace(data,u)
@router.get('/traces/{item_id}')
def detail(item_id:int,u=Depends(current)):return traces.detail(item_id)
@router.post('/traces/{item_id}/cancel')
def cancel(item_id:int,u=Depends(writer)):return cancel_trace(item_id,u)
@router.get('/traces/{item_id}/export')
def export(item_id:int,u=Depends(current)):
 row=traces.detail(item_id)
 if row['status']=='cancelled':raise HTTPException(409,'已取消的执行不发布结果，无法导出')
 return Response(json.dumps(row,ensure_ascii=False,indent=2),media_type='application/json',headers={'Content-Disposition':f'attachment; filename="trace-{item_id}.json"'})
