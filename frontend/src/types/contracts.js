/** @typedef {{id:number,name:string,sensors:object[],readings:object[]}} Dataset */
/** @typedef {{id:number,name:string,dataset_id:number,steps:object[]}} Plan */
/** @typedef {{id:number,name:string,status:'queued'|'running'|'completed'|'failed'|'cancelled',result:object|null,share:number|null,cancelled_at:string|null,scheduler?:object}} Trace */
export {};
