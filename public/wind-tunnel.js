import {request,evidenceUrl} from './runtime.js';
const $ = id => document.getElementById(id);
const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const numeric = value => Number.isFinite(Number(value)) && value !== null;
const number = value => numeric(value) ? Number(value).toLocaleString('en-US', {maximumFractionDigits:1}) : '—';
const percent = value => numeric(value) ? `${Number(value)>0?'+':''}${Number(value).toFixed(2)}%` : '—';
const scenarios = {
  duplicate: ['Duplicate burst', 'Replay trades to test whether inflated activity causes unnecessary escalation.', 'Duplicates → inflated activity → unnecessary escalation'],
  drop: ['Missing events', 'Remove known events from this frozen sample to test how a completeness guard responds.', 'Missing events → incomplete picture → misplaced confidence'],
  delay: ['Late arrivals', 'Delay events to test whether stale observations are mistaken for a current signal.', 'Late arrivals → outdated signal → mistimed escalation'],
  shock: ['Price shock', 'Inject a synthetic price move to test whether a real change remains visible through the guards.', 'Price shock → valid movement → justified attention']
};
let snapshot = null, scenario = 'duplicate', busy = false, experiment = null;
function status(message, error = false) {
  $('lab-status').textContent = message;
  $('lab-status').classList.toggle('is-error', error);
}
function controls() {
  $('lab-freeze').disabled = busy;
  $('lab-run').disabled = busy || !snapshot;
  $('lab-product').disabled = busy;
  $('lab-severity').disabled = busy;
  $('lab-threshold').disabled = busy;
  document.querySelectorAll('[data-scenario]').forEach(button => button.disabled = busy);
}
function invalidate() {
  experiment = null;
  $('lab-evidence').hidden = true;
  if (!$('lab-results').hidden) $('lab-result-context').textContent = 'Previous result · run again to apply the current settings';
}
function setScenario(value) {
  scenario = value;
  document.querySelectorAll('[data-scenario]').forEach(button => button.setAttribute('aria-pressed', String(button.dataset.scenario === value)));
  $('lab-scenario-detail').textContent = scenarios[value][1];
  $('lab-impact').textContent = scenarios[value][2];
  invalidate();
}
function timeline(points) {
  const rows = (Array.isArray(points) ? points : []).filter(row => numeric(row.second));
  if (!rows.length) return '<p class="lab-empty-chart">No timeline available for this experiment.</p>';
  const w=850,h=182,left=39,right=15,top=16,bottom=32;
  const max=Math.max(1,...rows.flatMap(row => ['baseline','received','accepted'].map(key => numeric(row[key])?Number(row[key]):0)));
  const minSecond=Math.min(...rows.map(row=>Number(row.second))), maxSecond=Math.max(...rows.map(row=>Number(row.second)));
  const x=second=>left+(Number(second)-minSecond)/Math.max(1,maxSecond-minSecond)*(w-left-right);
  const y=count=>top+(1-Number(count)/max)*(h-top-bottom);
  let svg='';
  for(let i=0;i<3;i++) {const yy=top+i*(h-top-bottom)/2;svg+=`<line x1="${left}" x2="${w-right}" y1="${yy}" y2="${yy}" stroke="#2b3c56" stroke-dasharray="3 5"/><text x="${left-9}" y="${yy+4}" text-anchor="end">${Math.round(max*(1-i/2))}</text>`;}
  for(const [key,color,dash] of [['baseline','#8ca8d0','4 5'],['received','#efa582',''],['accepted','#85e0d0','']]) {
    const line=rows.filter(row=>numeric(row[key])).map(row=>`${x(row.second).toFixed(2)},${y(row[key]).toFixed(2)}`).join(' ');
    svg+=`<polyline points="${line}" fill="none" stroke="${color}" stroke-width="2.4" stroke-dasharray="${dash}" stroke-linejoin="round" vector-effect="non-scaling-stroke"/>`;
  }
  svg+=`<text x="${left}" y="${h-8}">${number(minSecond)}s</text><text x="${w-right}" y="${h-8}" text-anchor="end">${number(maxSecond)}s · experiment time</text>`;
  return `<svg viewBox="0 0 ${w} ${h}" role="img" aria-label="Trades per time bucket: original frozen sample, received events after injection, and accepted events after quality guards. ${rows.length} time buckets.">${svg}</svg>`;
}
function verdict(title, data, type) {
  const state=['ALERT','CLEAR','HOLD','INSUFFICIENT'].includes(data?.state)?data.state:'INSUFFICIENT';
  return `<article class="lab-verdict ${type}"><span class="lab-card-label">${title}</span><strong class="lab-state state-${state.toLowerCase()}">${state}</strong><p>${esc(data?.reason)}</p><div class="lab-card-stats"><span>${number(data?.count)} events</span><span>${percent(data?.movePct)} move</span></div></article>`;
}
async function render(body) {
  const r=body.report;
  experiment=body.experimentId;
  $('lab-results').hidden=false;
  $('lab-result-context').textContent=`${scenarios[scenario][0]} · severity ${$('lab-severity').value} · ${r.product}`;
  $('lab-outcome').textContent=r.outcome.headline;
  $('lab-outcome-detail').textContent=r.outcome.detail;
  $('lab-verdicts').innerHTML=`<article class="lab-verdict baseline"><span class="lab-card-label">01 / Original sample</span><strong class="lab-baseline-count">${number(r.baseline.count)} <small>events</small></strong><p>Frozen before fault injection. The same sample anchors both decisions.</p><div class="lab-card-stats"><span>${number(r.baseline.spanSeconds)}s observed span</span><span>${percent(r.baseline.movePct)} move</span></div></article>${verdict('02 / Without quality guards',r.naive,'naive')}${verdict('03 / With quality guards',r.guarded,'guarded')}`;
  $('lab-timeline').innerHTML=timeline(r.timeline);
  $('lab-quality').innerHTML=[['Duplicate events',r.quality.duplicateCount],['Known removed',r.quality.missingCount],['Late events',r.quality.lateCount],['Sample retained',`${number(r.quality.coveragePct)}%`]].map(([label,value])=>`<div><span>${label}</span><strong>${esc(typeof value==='number'?number(value):value)}</strong></div>`).join('');
  $('lab-checks').innerHTML=(r.checks||[]).map(check=>`<li><span class="lab-check-status ${['pass','fail','info'].includes(check.status)?check.status:'info'}">${esc(check.status)}</span><div><strong>${esc(check.name)}</strong><p>${esc(check.detail)}</p></div></li>`).join('');
  $('lab-injected').textContent=r.injected.label;
  $('lab-digest').textContent=`SHA-256 ${String(body.hash).slice(0,16)}…`;
  $('lab-evidence').href=await evidenceUrl(experiment);
  $('lab-evidence').download=`marketpulse-evidence-${String(body.hash).slice(0,12)}.json`;
  $('lab-evidence').hidden=false;
  status('Experiment complete. Compare the decisions, then download the reproducible evidence.');
}
$('lab-freeze').addEventListener('click',async()=>{
  busy=true;controls();status('Freezing the latest observed market sample…');
  try {
    const body=await request(`/api/lab/snapshot?product=${encodeURIComponent($('lab-product').value)}`);
    snapshot=body.snapshot;invalidate();$('lab-results').hidden=true;
    const captured=new Date(snapshot.capturedAt);
    $('lab-snapshot-meta').textContent=`${snapshot.product} · ${number(snapshot.count)} events · ${number(snapshot.spanSeconds)}s span · ${snapshot.source} · ${Number.isNaN(captured.valueOf())?snapshot.capturedAt:captured.toLocaleString('en-GB',{timeZone:'UTC'})+' UTC'}${snapshot.truncated?' · capped sample':''}`;
    $('lab-snapshot-state').textContent='Sample frozen';
    $('lab-freeze').textContent='Freeze a new window';
    status('Sample frozen. Choose a fault and run your stress test. The live feed continues unchanged.');
  } catch(error) {status(error.message,true);}
  finally {busy=false;controls();}
});
$('lab-product').addEventListener('change',()=>{
  snapshot=null;invalidate();$('lab-results').hidden=true;$('lab-snapshot-meta').textContent='Freeze a window to capture source, timestamp and observed trades.';$('lab-snapshot-state').textContent='No sample frozen';$('lab-freeze').textContent='Freeze live window';controls();status('Market changed. Freeze a new window before running an experiment.');
});
document.querySelectorAll('[data-scenario]').forEach(button=>button.addEventListener('click',()=>setScenario(button.dataset.scenario)));
for(const id of ['lab-severity','lab-threshold']) $(id).addEventListener('change',invalidate);
$('lab-run').addEventListener('click',async()=>{
  if(!snapshot||busy)return;
  busy=true;controls();invalidate();status('Replaying the frozen sample through both decision paths…');
  try {await render(await request(`/api/lab/run?${new URLSearchParams({snapshotId:snapshot.id,scenario,severity:$('lab-severity').value,thresholdPct:$('lab-threshold').value})}`));}
  catch(error){status(error.message,true);}
  finally {busy=false;controls();}
});
setScenario('duplicate');controls();
