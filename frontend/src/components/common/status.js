import {STATUSES,WAIT_REASONS} from '../../constants/enums.js';
import {esc} from '../../utils/html.js';
export const status=s=>`<span class="status ${s}">${STATUSES[s]||s}</span>`;
export const waitReason=t=>{
 const sch=t.scheduler;
 if(!sch||!sch.wait_reason)return '';
 const text=WAIT_REASONS[sch.wait_reason]||sch.wait_reason;
 return `<small class="wait ${sch.wait_reason}">⏳ ${esc(text)}${sch.overtaken?` · 已被插队 ${esc(sch.overtaken)} 次`:''}</small>`;
};
