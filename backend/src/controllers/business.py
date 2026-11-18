from src.services import datasets,plans,traces,capacity
from src.services.audit import log
def save_dataset(data,user,item_id=None):
 row=datasets.save(data,item_id);log(user.email,'dataset.save',row['name']);return row
def save_plan(data,user,item_id=None):
 row=plans.save(data,item_id);log(user.email,'plan.save',row['name']);return row
def start_trace(data,user):
 row=traces.create(data);log(user.email,'trace.queue',f"{row['name']} share={row['share']}");return row
def cancel_trace(item_id,user):
 before=traces.detail(item_id);row=traces.cancel(item_id)
 if before['status']!=row['status']:
  log(user.email,'trace.cancel',f"{row['name']} verdict={row['status']}")
 return row
def set_global_capacity(value,user):
 row=capacity.set_global(value);log(user.email,'capacity.global',str(value));return row
def set_dataset_capacity(dataset_id,value,user):
 row=capacity.set_dataset(dataset_id,value);log(user.email,'capacity.dataset',f'{dataset_id}={value}');return row
