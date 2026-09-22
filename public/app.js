import {request,publicMode} from './runtime.js';
const $=id=>document.getElementById(id);
const names={'BTC-USD':'Bitcoin','ETH-USD':'Ethereum','SOL-USD':'Solana'};
const symbols={'BTC-USD':'₿','ETH-USD':'Ξ','SOL-USD':'◎'};
let state=null,selected='BTC-USD',minutes=60,paused=false,loading=false,threshold=.5,fetchError=null;
const money=(value,digits=2)=>Number.isFinite(Number(value))&&value!==null?new Intl.NumberFormat('en-US',{style:'currency',currency:'USD',minimumFractionDigits:digits,maximumFractionDigits:digits}).format(Number(value)):'—';
const time=value=>new Date(value).toLocaleTimeString('en-GB',{timeZone:'UTC',hour:'2-digit',minute:'2-digit',second:'2-digit'});
const escape=value=>String(value).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const age=value=>value?Math.max(0,Math.round((Date.now()-Date.parse(value))/1000)):null;
function chart(candles) {
  const available=candles.filter(c=>c.time>=Date.now()-minutes*60000 && c.time<=Date.now());
  if(available.length<2) {$('chart').innerHTML='<p class="empty">Not enough candle history to draw a chart.</p>';return null;}
  const width=850,height=235,left=1,right=78,top=15,bottom=29;
  const plotWidth=width-left-right,plotHeight=height-top-bottom;
  const values=available.map(c=>c.close),low=Math.min(...values),high=Math.max(...values),pad=Math.max((high-low)*.16,high*.00005);
  const floor=low-pad,ceiling=high+pad,first=available[0].time,last=available.at(-1).time;
  const x=t=>left+(t-first)/(last-first)*plotWidth,y=v=>top+(ceiling-v)/(ceiling-floor)*plotHeight;
  const points=available.map(c=>`${x(c.time).toFixed(1)},${y(c.close).toFixed(1)}`).join(' ');
  let grid='';
  for(let i=0;i<4;i++){const yy=top+i*plotHeight/3;grid+=`<line x1="${left}" x2="${plotWidth}" y1="${yy}" y2="${yy}" stroke="#2a3850" stroke-dasharray="3 5"/><text x="${plotWidth+13}" y="${yy+4}">${escape(money(ceiling-(ceiling-floor)*i/3))}</text>`;}
  for(let i=0;i<5;i++){const ts=first+(last-first)*i/4;grid+=`<text x="${x(ts)}" y="${height-4}" text-anchor="${i===0?'start':i===4?'end':'middle'}">${time(ts).slice(0,5)}</text>`;}
  const lastY=y(available.at(-1).close);
  $('chart').innerHTML=`<svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" role="img" aria-label="${escape(names[selected])} one-minute closing prices for the selected period"><defs><linearGradient id="chart-fill" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="#4c87eb" stop-opacity=".23"/><stop offset="100%" stop-color="#4c87eb" stop-opacity="0"/></linearGradient></defs>${grid}<polygon points="${left},${top+plotHeight} ${points} ${plotWidth},${top+plotHeight}" fill="url(#chart-fill)"/><polyline points="${points}" fill="none" stroke="#83adff" stroke-width="2.4" vector-effect="non-scaling-stroke" stroke-linejoin="round"/><circle cx="${plotWidth}" cy="${lastY}" r="3.4" fill="#bfd6ff"/></svg>`;
  return(available.at(-1).close/available[0].close-1)*100;
}
function signal(icon,title,description,warn=false){return `<div class="signal ${warn?'warn':''}"><span class="signal-icon" aria-hidden="true">${icon}</span><div><strong>${escape(title)}</strong><p>${escape(description)}</p></div></div>`;}
function renderFlink(item) {
  const status=state.flink||{configured:false},value=item.flinkSignal;
  if(!value){
    $('flink-status').textContent=status.error?'Signal feed interrupted':status.configured?'Awaiting Flink output':'Not connected';
    $('flink-content').innerHTML=`<p class="flink-empty">${status.error?escape(status.error)+'. ':''}No Flink signal received for ${selected}. Forecast values appear only after the pipeline publishes them.</p>`;
    return false;
  }
  const windowAge=age(value.window_end),receivedAge=age(value.receivedAt);
  const fresh=Boolean(status.connected&&!status.error&&windowAge<=180&&receivedAge<=180);
  const labels={WARMING_UP:value.model_status==='NOT_CONFIGURED'?'Forecast model not configured':'Warming up',ACTIVITY_SPIKE:'Activity spike',NORMAL:'Within expected activity'};
  const operationalLabels={LOW_SAMPLE:'Low sample',WIDE_PRICE_RANGE:'Wide price range',BUY_PRESSURE:'Buy pressure',SELL_PRESSURE:'Sell pressure',OBSERVING:'Observing'};
  const operationalDetail={LOW_SAMPLE:'Fewer than 10 distinct trades or less than 10 seconds of sample span.',WIDE_PRICE_RANGE:'Sample high-to-low price range reaches 0.5% after the sample check.',BUY_PRESSURE:'Taker buys account for at least 80% of sampled quantity, after prior checks.',SELL_PRESSURE:'Taker buys account for at most 20% of sampled quantity, after prior checks.',OBSERVING:'The sample meets the minimum size; no configured operational threshold is met.'};
  $('flink-status').textContent=fresh?'Flink output live':'Flink output stale';
  const hasForecast=value.model_status!=='NOT_CONFIGURED'&&value.forecast_trades!==null&&Number.isFinite(value.forecast_trades);
  const max=Math.max(value.observed_trades,value.forecast_trades??0,value.upper_bound??0,1)*1.08;
  const width=390,x=v=>30+v/max*330;
  const forecastMark=hasForecast?`<line x1="${x(value.forecast_trades)}" x2="${x(value.forecast_trades)}" y1="18" y2="49" stroke="#f0c88e" stroke-width="2"/>`:'';
  const band=hasForecast&&value.lower_bound!==null&&value.upper_bound!==null?`<rect x="${x(value.lower_bound)}" y="15" width="${Math.max(1,x(value.upper_bound)-x(value.lower_bound))}" height="38" fill="#b7c6e8" fill-opacity=".12" stroke="#73829d" stroke-dasharray="3 3" rx="3"/>`:'';
  const observedWidth=Math.max(0,x(value.observed_trades)-30);
  const graphic=`<svg viewBox="0 0 ${width} 73" role="img" aria-label="${value.observed_trades} observed trades${hasForecast?`, ${value.forecast_trades.toFixed(1)} forecast trades`:'. Forecast pending'}"><line x1="30" x2="360" y1="54" y2="54" stroke="#34445f"/>${band}<rect x="30" y="26" width="${observedWidth}" height="16" rx="3" fill="#83adff"/>${forecastMark}<text x="30" y="69">0</text><text x="360" y="69" text-anchor="end">${Math.ceil(max).toLocaleString()}</text></svg>`;
  $('flink-content').innerHTML=`<div class="flink-activity"><div class="flink-values"><div><span>Observed</span><strong>${value.observed_trades.toLocaleString()}</strong></div><div><span>Expected</span><strong>${hasForecast?value.forecast_trades.toLocaleString('en-US',{maximumFractionDigits:1}):'—'}</strong></div></div>${graphic}<div class="flink-legend"><span><i class="legend-dot"></i>Observed</span><span><i class="expected-dot"></i>${hasForecast?'Forecast':'Forecast pending'}</span><span>Dashed area: forecast bounds</span></div></div><div class="flink-summary ${value.signal==='ACTIVITY_SPIKE'?'spike':''}"><strong>${escape(labels[value.signal])}${fresh?'':' · historical'}</strong><p>Window ended ${time(value.window_end)} UTC · ${windowAge}s ago</p><p>Sample notional ${money(value.observed_notional,0)} · VWAP ${money(value.sample_vwap)}</p><small>${hasForecast?(value.lower_bound!==null&&value.upper_bound!==null?`Forecast bounds ${value.lower_bound.toFixed(1)}–${value.upper_bound.toFixed(1)} trades.`:'Forecast bounds unavailable.'):'The pipeline has not published a forecast for this window.'} ${status.error?escape(status.error):''}</small></div>`;
  if(value.model_status==='NOT_CONFIGURED') {
    const summary=$('flink-content').querySelector('.flink-summary');
    summary.querySelector('small').textContent=`Forecast model not configured. These are transparent operational rules on sampled trades.${status.error?' '+status.error:''}`;
    $('flink-content').querySelector('.flink-legend').innerHTML='<span><i class="legend-dot"></i>Observed</span><span>Forecast model not configured</span>';
    $('flink-content').querySelector('svg').setAttribute('aria-label',`${value.observed_trades} observed trades. Forecast model not configured.`);
  }
  if(operationalLabels[value.operational_signal]) {
    const summary=$('flink-content').querySelector('.flink-summary');
    summary.querySelector('strong').textContent=`${operationalLabels[value.operational_signal]}${fresh?'':' · historical'}`;
    summary.classList.toggle('spike',value.operational_signal!=='OBSERVING');
    const detail=document.createElement('p');detail.className='operational-detail';detail.textContent=operationalDetail[value.operational_signal];summary.append(detail);
  }
  const operationalMetrics=[['Price range',value.price_range_pct,'%'],['Taker buy quantity',value.taker_buy_pct,'%'],['Observed quantity',value.observed_quantity,selected.split('-')[0]],['Sample span',value.sample_span_seconds,'s']].filter(([,value])=>typeof value==='number'&&Number.isFinite(value));
  if(operationalMetrics.length) {
    const metrics=document.createElement('div');metrics.className='operational-metrics';
    metrics.innerHTML=operationalMetrics.map(([label,value,unit])=>`<div><span>${label}</span><strong>${value.toLocaleString('en-US',{maximumFractionDigits:4})}<small> ${unit}</small></strong></div>`).join('');
    $('flink-content').append(metrics);
  }
  return fresh;
}
function render(){
  if(!state)return;
  const item=state.products.find(p=>p.product===selected),stats=item.stats,stale=age(item.updatedAt),tickerAge=age(item.ticker?.time);
  const failed=state.products.filter(p=>p.error||!p.updatedAt||age(p.updatedAt)>30);
  $('connection').classList.toggle('error',Boolean(fetchError||failed.length));
  const integration=state.integration;
  const flinkFresh=renderFlink(item);
  const cloudFresh=integration.configured && integration.connected && integration.consumed>0 && age(integration.lastMessageAt)<=30 && tickerAge!==null && tickerAge<=30;
  const sourceLabel=integration.configured?(cloudFresh?'Confluent live':'Confluent waiting / stale'):publicMode?'Public browser feed':'Public API preview';
  document.querySelector('.preview').innerHTML=`<i></i> ${sourceLabel}`;
  $('connection').textContent=fetchError?`Dashboard connection interrupted: ${fetchError}. Displaying last received data.`:paused?(publicMode?'View paused. Browser polling resumes when you resume the view.':'View paused. The server continues collecting market samples.'):integration.error?`Confluent consumer: ${integration.error}. No direct API fallback is used for trade data.`:failed.length?`${failed.map(p=>p.product).join(', ')}: ${failed[0].error||'Waiting for fresh data'}. Last successful observations are retained.`:`${integration.configured?'Confluent trade stream':'Public market data'} connected. Updated ${time(state.serverTime)} UTC. Sampled trade coverage. ${flinkFresh?'Flink output is current.':'Flink output is not currently verified.'}`;
  $('asset-name').innerHTML=`${names[selected]} <span>${selected.replace('-',' / ')}</span>`;
  $('price').textContent=money(item.ticker?.price??null);
  $('price-time').textContent=item.ticker?`Last exchange trade ${time(item.ticker.time)} UTC${tickerAge>30?' · stale':''}`:'Waiting for a public ticker';
  const change=chart(item.candles);
  $('change').textContent=change===null?'Waiting for candles':`${change>=0?'+':''}${change.toFixed(2)}% over chart range`;
  $('change').className=change===null?'':change>=0?'positive':'negative';
  $('candle-time').textContent=item.candleError?'History fetch interrupted':item.candlesAt?`History fetched ${time(item.candlesAt)} UTC`:'No history loaded';
  $('notional').textContent=item.updatedAt?money(stats.notional,0):'—';$('count').textContent=item.updatedAt?stats.count.toLocaleString():'—';$('vwap').textContent=money(stats.vwap);
  $('coverage').textContent=stats.count?`${stats.coverageSeconds.toFixed(0)}s between first / last sample`:'No trades in the past 60s';
  $('watchlist').innerHTML=state.products.map(p=>`<button class="watch-item ${p.product===selected?'selected':''}" data-product="${p.product}" aria-pressed="${p.product===selected}"><span class="coin ${p.product.slice(0,3)}">${symbols[p.product]}</span><span class="asset-ticker">${p.product.slice(0,3)}<small>${names[p.product]}</small></span><span class="watch-price">${money(p.ticker?.price??null)}<small>${p.error?'Feed error':p.updatedAt?`${age(p.updatedAt)}s ago`:'Connecting'}</small></span></button>`).join('');
  const feedWarn=stale===null||stale>30||tickerAge===null||tickerAge>30;
  let signals=signal(feedWarn?'!':'✓',stale===null?'Feed waiting':feedWarn?'Feed needs attention':'Feed responding',stale===null?'No successful sample has arrived yet.':`Last successful ${integration.configured?'Kafka delivery':'poll'} ${stale}s ago. Latest trade ${tickerAge??'unknown'}s ago. ${item.rejected} invalid rows rejected this session.`,feedWarn);
  signals+=stats.movePercent===null?signal('◷','Building the movement window','At least 30 seconds between observed trades is required.'):signal(Math.abs(stats.movePercent)>=threshold?'!':'✓',Math.abs(stats.movePercent)>=threshold?'Observed movement flagged':'Movement within your threshold',`${stats.movePercent>=0?'+':''}${stats.movePercent.toFixed(3)}% between first and last observed trades in the past 60s. Threshold ${threshold}%.`,Math.abs(stats.movePercent)>=threshold);
  $('signals').innerHTML=signals;
  $('trades').innerHTML=item.trades.length?item.trades.slice(0,6).map(t=>`<tr><td>${time(t.time)}</td><td><span class="side ${t.taker_side}">${t.taker_side==='buy'?'Buy':'Sell'}</span></td><td>${money(t.price)}</td><td>${Number(t.size).toLocaleString('en-US',{maximumFractionDigits:8})}</td></tr>`).join(''):'<tr><td colspan="4" class="empty">No observed trades yet. Check feed status above.</td></tr>';
  $('kafka-status').textContent=integration.configured?integration.consumed?`${integration.consumed.toLocaleString()} records consumed`:'Configured; awaiting records':'Not connected';
  $('pipeline-status').textContent=cloudFresh?'Kafka consumption verified':sourceLabel;
  $('pipeline-detail').textContent=integration.configured?`Trade data: Confluent Kafka via HTTP Source connector. ${integration.rejected} raw records rejected. History: direct Coinbase candles. Flink signals: ${state.flink?.consumed||0} consumed, ${state.flink?.rejected||0} rejected. Source and signal consumers are independent.`:publicMode?'This Pages app fetches public Coinbase data in your browser while open. Experiments stay in this browser session. Confluent is not connected in this mode.':'This dashboard reads the public API directly. Cloud streaming and forecasting are not claimed as active.';
  const nodes=document.querySelectorAll('.pipeline-node');
  nodes[1].querySelector('strong').textContent=integration.configured?'HTTP Source connector':publicMode?'Browser collector':'MarketPulse collector';
  nodes[1].querySelector('small').textContent=integration.configured?'Public trade polling':'Validate · deduplicate · sample';
  nodes[3].querySelector('small').textContent=flinkFresh?'Fresh output consumed':item.flinkSignal?'Output received; stale':state.flink?.configured?'Waiting for output':'Not connected';
}
async function refresh(force=false){
  if(loading||(paused&&!force))return;loading=true;$('refresh').disabled=true;
  try{state=await request('/api/state');fetchError=null;render();}
  catch(error){fetchError=error.message;$('connection').classList.add('error');$('connection').textContent=`Cannot reach the selected data source: ${error.message}. Retry with Refresh view.`;if(state)render();}
  finally{loading=false;$('refresh').disabled=false;}
}
$('watchlist').addEventListener('click',event=>{const button=event.target.closest('[data-product]');if(button){selected=button.dataset.product;render();}});
document.querySelectorAll('[data-minutes]').forEach(button=>button.addEventListener('click',()=>{minutes=Number(button.dataset.minutes);document.querySelectorAll('[data-minutes]').forEach(b=>{b.classList.toggle('selected',b===button);b.setAttribute('aria-pressed',String(b===button));});render();}));
$('threshold').addEventListener('change',event=>{threshold=Number(event.target.value);render();});
$('pause').addEventListener('click',()=>{paused=!paused;$('pause').textContent=paused?'Resume view':'Pause view';$('pause').setAttribute('aria-pressed',String(paused));render();if(!paused)void refresh(true);});
$('refresh').addEventListener('click',()=>refresh(true));
function mountLineage() {
  const stages={
    btc:{title:'BTC-USD raw trades',topic:'marketpulse_trades_BTC-USD',schema:'trade_id · price · size · side · time',transform:'HTTP Source polls public Coinbase Bitcoin trades. Each Kafka value must be one trade object, with the five source fields registered in Schema Registry.',quality:'Source side describes the maker, not the taker. Repeated polling can redeliver the same trade. Source-schema compatibility must be inspected before running downstream SQL.',next:'Flows through marketpulse_parsed into valid events. Typed field-validation failures can branch into the optional quarantine.',link:'https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-trades'},
    eth:{title:'ETH-USD raw trades',topic:'marketpulse_trades_ETH-USD',schema:'trade_id · price · size · side · time',transform:'The Ethereum source is kept in its own raw topic. The parsing view adds the ETH-USD symbol before unioning the three sources.',quality:'A trade ID is only unique within its product. Downstream deduplication uses both symbol and trade ID inside the event-time window.',next:'Joins the same typed event contract as Bitcoin and Solana.',link:'https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-trades'},
    sol:{title:'SOL-USD raw trades',topic:'marketpulse_trades_SOL-USD',schema:'trade_id · price · size · side · time',transform:'The Solana source follows the same registered raw schema. Preserving individual source topics makes each product inspectable before the union.',quality:'Public REST polls are samples. Three sources broaden product coverage; they do not establish complete exchange coverage.',next:'Joins the same typed event contract as Bitcoin and Ethereum.',link:'https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-trades'},
    events:{title:'Validated market events',topic:'marketpulse_events',schema:'symbol STRING · trade_id BIGINT · price / size DECIMAL(24,8) · maker_side STRING · event_time TIMESTAMP(3)',transform:'The shared marketpulse_parsed view uses TRY_CAST and labels invalid business fields. Valid rows enter a materialized event topic with a 30-second event-time watermark.',quality:'Requires positive trade ID, price and size, a buy/sell maker side and a parseable UTC timestamp. Duplicate polls intentionally remain here; the window stage removes them.',next:'Event-time windows consume the valid stream. Optional quarantine reads invalid rows from the parsing view.',link:'https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html'},
    windows:{title:'Deduplicated 30-second windows',topic:'marketpulse_windows',schema:'symbol · window_start / window_end · observed_trades · observed_notional · observed_quantity · sample_vwap · price_range_pct · taker_buy_pct · sample_span_seconds',transform:'TUMBLE groups events into 30-second windows. ROW_NUMBER partitions by window, symbol and trade ID; row 1 is retained before aggregating count, notional, quantity and price statistics.',quality:'Deduplication is bounded to each window. Trades behind the watermark may be dropped; changed timestamps or conflicting payloads are not resolved by this demo policy. Sample span is not exchange completeness.',next:'Completed window aggregates feed transparent operational signal rules.',link:'https://docs.confluent.io/cloud/current/flink/reference/queries/window-tvf.html'},
    signals:{title:'Explainable operational signals',topic:'marketpulse_signals',schema:'symbol · window_end · observed_trades · observed_notional · sample_vwap · operational_signal · model_status · price_range_pct · taker_buy_pct · observed_quantity · sample_span_seconds',transform:'Rules run in order: LOW_SAMPLE if count <10 or span <10s; WIDE_PRICE_RANGE if range ≥0.5%; BUY_PRESSURE if taker-buy quantity ≥80%; SELL_PRESSURE if ≤20%; otherwise OBSERVING.',quality:'These are rules on observed samples. No forecasting model is configured: model_status is NOT_CONFIGURED and forecast fields are null. The dashboard only presents cloud values after actual consumption.',next:'A server-side Kafka consumer can deliver the real output to the dashboard. Public browser mode does not consume private Kafka topics.',link:'https://docs.confluent.io/cloud/current/flink/reference/functions/conditional-functions.html'},
    quarantine:{title:'Optional validation quarantine',topic:'marketpulse_quarantine',schema:'symbol · raw_trade_id · raw_price · raw_size · raw_side · raw_time · invalid_reason',transform:'An optional additional materialized topic retains typed business-field failures from marketpulse_parsed, with the original field values and rejection reason.',quality:'This is not a deserialization dead-letter queue. It cannot catch connector failures or unparseable Kafka bytes. Valid source data may leave the topic empty; no fabricated failures are required.',next:'Inspect rejected values to repair upstream data contracts. This optional branch requires an additional running job and capacity.',link:'https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html'}
  };
  const explorer=document.createElement('div');explorer.className='lineage-explorer';
  explorer.innerHTML=`<div class="lineage-heading"><div><h3>Explore the stream lineage</h3><p>Follow a trade from source contract to operational decision.</p></div><span class="lineage-blueprint">Pipeline blueprint &mdash; cloud execution not yet verified</span></div>
    <div class="lineage-map" role="group" aria-label="Select a pipeline stage to inspect">
      <div class="lineage-sources"><button data-lineage="btc" aria-pressed="true"><small>Raw source</small><strong>BTC-USD</strong><span>Trade records</span></button><button data-lineage="eth" aria-pressed="false"><small>Raw source</small><strong>ETH-USD</strong><span>Trade records</span></button><button data-lineage="sol" aria-pressed="false"><small>Raw source</small><strong>SOL-USD</strong><span>Trade records</span></button></div>
      <div class="lineage-edge" aria-hidden="true"><span>Parse + validate</span>&rarr;</div><button data-lineage="events" aria-pressed="false"><small>Typed stream</small><strong>Events</strong><span>Valid business fields</span></button>
      <div class="lineage-edge" aria-hidden="true"><span>Deduplicate</span>&rarr;</div><button data-lineage="windows" aria-pressed="false"><small>Event time</small><strong>30s windows</strong><span>Sample statistics</span></button>
      <div class="lineage-edge" aria-hidden="true"><span>Apply rules</span>&rarr;</div><button data-lineage="signals" aria-pressed="false"><small>Operations</small><strong>Signals</strong><span>Explainable decisions</span></button>
    </div><div class="lineage-branch"><span>From the parsing view: typed validation failures &rarr;</span><button data-lineage="quarantine" aria-pressed="false">Optional quarantine</button><small>Additional job &middot; no fabricated counts</small></div>
    <div id="lineage-inspector" class="lineage-inspector" aria-live="polite"></div>
    <p class="lineage-scope">This diagram describes the supplied SQL and topic contracts. It is not a live Confluent lineage graph. The source flow above reports the dashboard's actual data path.</p><div class="lineage-cloud-link"><a href="https://confluent.cloud/environments/env-w0kdo9/clusters/lkc-7ykmgx1/stream-lineage" target="_blank" rel="noreferrer">Open Confluent Stream Lineage &#x2197;</a><span>Account access required</span></div>`;
  $('pipeline').append(explorer);
  function inspect(key) {
    const stage=stages[key];
    explorer.querySelectorAll('[data-lineage]').forEach(button=>button.setAttribute('aria-pressed',String(button.dataset.lineage===key)));
    $('lineage-inspector').innerHTML=`<div class="lineage-stage-title"><div><h4>${escape(stage.title)}</h4><code>${escape(stage.topic)}</code></div><a href="${stage.link}" target="_blank" rel="noreferrer">${['btc','eth','sol'].includes(key)?'Source documentation':'Flink documentation'} &#x2197;</a></div><dl><div class="lineage-schema"><dt>Schema contract</dt><dd><code>${escape(stage.schema)}</code></dd></div><div><dt>Transformation</dt><dd>${escape(stage.transform)}</dd></div><div><dt>Quality role</dt><dd>${escape(stage.quality)}</dd></div><div><dt>Downstream use</dt><dd>${escape(stage.next)}</dd></div></dl>`;
  }
  explorer.addEventListener('click',event=>{const button=event.target.closest('[data-lineage]');if(button)inspect(button.dataset.lineage);});
  inspect('btc');
}
mountLineage();
function clock(){$('clock').textContent=`${time(Date.now())} UTC`;}
clock();setInterval(clock,1000);setInterval(()=>refresh(),5000);void refresh();
