import {store} from '../stores/business.js';
import {esc} from '../utils/html.js';
import {table} from '../components/common/table.js';
import {status,waitReason} from '../components/common/status.js';
import {capacityPanel} from '../components/common/capacity.js';
const dsName=id=>store.datasets.find(d=>d.id===id)?.name||`数据集 #${id}`;
function shareCell(t){
 const sch=t.scheduler;
 const occ=sch?`<small class="muted">${sch.worker?('占用中 · '+esc(sch.worker.slice(0,8))):''}</small>`:'';
 return `<b>${esc(t.share??'—')} 份</b>${occ}`;
}
function actionCell(t){
 const canWrite=store.user?.role==='admin'||store.user?.role==='operator';
 if((t.status==='queued'||t.status==='running')&&canWrite)return `<button class="quiet danger" data-cancel="${t.id}">取消</button>`;
 if(t.status==='queued'||t.status==='running')return '<small class="muted">只读</small>';
 return `<button class="quiet" data-result="${t.id}">查看剖面 →</button>`;
}
export function tracesPage(){
 const rows=store.traces.map(t=>[
  `<b>${esc(t.name)}</b><small>${esc(t.created_at)}</small><small>数据集：${esc(dsName(t.dataset_id))}</small>${waitReason(t)}${t.cancelled_at?`<small class="muted">取消于 ${esc(t.cancelled_at)}</small>`:''}`,
  status(t.status),
  shareCell(t),
  actionCell(t)
 ]);
 return `<div class="split traces-layout"><section>
 <h2>把计划交给执行器</h2>
 <p>提交即按数据集规模申报资源份额，容量足够立即并发领取，不足则 FIFO 排队；小任务可先用空闲份额，但不能让可容纳的大任务无限等待（放行上界 ${esc(store.capacity?.max_skip??2)} 次插队）。</p>
 <form id="trace-form"><label>执行名称<input name="name" placeholder="shift-20261117" required></label><label>算子计划<select name="plan_id">${store.plans.map(p=>`<option value="${p.id}">${esc(p.name)}</option>`)}</select></label><button type="submit">运行一条管线 →</button></form>
 <ol class="steps"><li>复制数据集与计划快照，按固定公式申报 1–8 份额</li><li>调度器原子领取：全局 + 数据集容量同时满足才放行</li><li>取消排队立即退出；取消运行待算子步边界停止，不发布结果</li><li>成功、失败、取消都恰好归还一次份额</li></ol>
 </section><section>
 ${capacityPanel()}
 <h2>执行记录 <em>${store.traces.length}</em></h2>
 ${table(['执行 / 时间 / 等待原因','终态','申报份额 / 占用','操作'],rows)}
 </section></div>`;
}
