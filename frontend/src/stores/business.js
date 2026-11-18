import {datasets,plans,traces} from '../api/business.js';
export const store={datasets:[],plans:[],traces:[],capacity:null,user:null};
export async function refresh(){
 const [ds,pl,tr]=await Promise.all([datasets.list(),plans.list(),traces.list()]);
 store.datasets=ds;store.plans=pl;
 store.traces=tr.items;
 store.capacity={capacities:tr._capacities,used:tr._used,max_skip:tr._max_skip};
}
