from pydantic import BaseModel,Field
from fastapi import APIRouter,Depends
from src.middlewares.auth import current,admin
from src.services import capacity
from src.controllers.business import set_global_capacity,set_dataset_capacity
router=APIRouter()
class GlobalCapacity(BaseModel):
 capacity:int=Field(ge=1,le=1000)
class DatasetCapacity(BaseModel):
 dataset_id:int=Field(ge=1)
 capacity:int=Field(ge=1,le=1000)
@router.get('/capacity')
def listing(u=Depends(current)):return capacity.list_capacities()
@router.put('/capacity/global')
def set_global(data:GlobalCapacity,u=Depends(admin)):return set_global_capacity(data.capacity,u)
@router.put('/capacity/dataset')
def set_dataset(data:DatasetCapacity,u=Depends(admin)):return set_dataset_capacity(data.dataset_id,data.capacity,u)
