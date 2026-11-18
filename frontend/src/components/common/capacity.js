import {store} from '../../stores/business.js';
import {esc} from '../../utils/html.js';
// 容量面板：管理员可改全局/各数据集容量（在线缩容保留在途执行，超占期间暂停领取）。
export function capacityPanel(){
 const cap=store.capacity;if(!cap)return '';
 const {capacities,used,max_skip}=cap;
 const isAdmin=store.user?.role==='admin';
 const dsName=id=>store.datasets.find(d=>d.id===id)?.name||`数据集 #${id}`;
 const shrink=used.global>capacities.global;
 const rows=Object.entries(capacities.datasets).map(([id,c])=>{
  const u=used.datasets[id]||0;
  return `<tr><td>${esc(dsName(Number(id)))}</td><td><code>${u} / ${c}</code>${u>c?'<small class="wait shrink_paused">超占中：暂停该数据集新领取</small>':''}</td>${isAdmin?`<td><input type="number" min="1" max="1000" data-ds-cap="${esc(id)}" value="${esc(c)}"></td><td><button class="quiet" data-apply-ds="${esc(id)}">应用</button></td>`:''}</tr>`;
 }).join('');
 return `<section class="capacity-box"><h2>份额容量与占用 <em>${shrink?'超占缩容中':'正常放行'}</em></h2>
 <p>每次执行按规模申报 1–8 份；领取需同时满足全局与数据集容量。放行上界：任意任务至多被更晚的小任务插队 <b>${esc(max_skip)}</b> 次，之后成为屏障，大任务不会无限等待。</p>
 <div class="cap-grid"><div><small>全局占用 / 容量</small><strong><code>${used.global} / ${capacities.global}</code></strong>${shrink?'<small class="wait shrink_paused">在线缩容超占：在途执行保留，暂停新领取直到占用回落</small>':''}${isAdmin?`<label class="inline">调整全局容量<input type="number" min="1" max="1000" id="global-cap" value="${esc(capacities.global)}"></label><button class="quiet" data-apply-global>应用全局容量</button>`:''}</div></div>
 <div class="table-wrap"><table><thead><tr><th>数据集</th><th>占用 / 容量（份额）</th>${isAdmin?'<th>新容量</th><th></th>':''}</tr></thead><tbody>${rows}</tbody></table></div></section>`;
}
