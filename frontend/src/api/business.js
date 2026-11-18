import {api,post} from './client.js';
export const datasets={list:()=>api('/datasets'),save:(d,id)=>post('/datasets'+(id?'/'+id:''),d,id?'PUT':'POST')};
export const plans={list:()=>api('/plans'),save:(d,id)=>post('/plans'+(id?'/'+id:''),d,id?'PUT':'POST')};
export const traces={list:()=>api('/traces'),run:d=>post('/traces',d),detail:id=>api('/traces/'+id),cancel:id=>post('/traces/'+id+'/cancel',{})};
export const capacity={list:()=>api('/capacity'),setGlobal:c=>post('/capacity/global',{capacity:c},'PUT'),setDataset:(id,c)=>post('/capacity/dataset',{dataset_id:id,capacity:c},'PUT')};
