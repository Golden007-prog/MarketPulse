// Public REST only. This module never accepts exchange or Confluent credentials.
// Analytics and replay functions are the same modules used by the Node service.
export function createBrowserProvider({analytics,runExperiment,fetchImpl=globalThis.fetch.bind(globalThis),now=Date.now,cryptoImpl=globalThis.crypto}={}) {
  const {PRODUCTS,normalizeTrade,mergeTrades,summarize,normalizeCandles}=analytics;
  const feeds=Object.fromEntries(PRODUCTS.map(product=>[product,{product,trades:[],candles:[],ticker:null,updatedAt:null,candlesAt:null,error:null,candleError:null,rejected:0,flinkSignal:null}]));
  const snapshots=new Map(),experiments=new Map();
  const ttl=30*60*1000;
  let pollPromise=null,lastPoll=-Infinity;
  const source='Coinbase public REST in browser (sampled)';
  const fail=(message,status=409)=>Object.assign(new Error(message),{status});
  function prune(map) {for(const [key,value] of map) if(now()-value.createdAt>ttl) map.delete(key);while(map.size>12)map.delete(map.keys().next().value);}
  function remember(map,key,value) {map.set(key,{value,createdAt:now()});prune(map);}
  function lookup(map,key,label) {prune(map);const entry=map.get(key);if(!entry)throw fail(`${label} expired or is unavailable. Freeze a new live window.`,404);return entry.value;}
  async function json(url) {
    const response=await fetchImpl(url,{cache:'no-store',credentials:'omit',signal:AbortSignal.timeout(9000)});
    if(!response.ok)throw new Error(`Coinbase HTTP ${response.status}${response.status===429?' (rate limited; retry shortly)':''}`);
    return response.json();
  }
  async function pollProduct(product) {
    const feed=feeds[product],base=`https://api.exchange.coinbase.com/products/${product}`;
    try {
      const raw=await json(`${base}/trades?limit=100`);
      if(!Array.isArray(raw))throw new Error('Invalid public trade response');
      const incoming=[];
      for(const row of raw) {try{incoming.push(normalizeTrade(row,product));}catch{feed.rejected++;}}
      const timely=incoming.filter(trade=>trade.timestamp<=now()+10000&&trade.timestamp>=now()-900000);
      if(!timely.length)throw new Error('No valid recent public trades received');
      feed.trades=mergeTrades(feed.trades,timely,now());
      const newest=feed.trades.at(-1);
      feed.ticker={price:newest.price,time:newest.time};feed.updatedAt=new Date(now()).toISOString();feed.error=null;
    } catch(error) {feed.error=`Public browser feed: ${error.message}`;}
    if(!feed.candlesAt||now()-Date.parse(feed.candlesAt)>=60000) {
      try {const candles=normalizeCandles(await json(`${base}/candles?granularity=60`));if(!candles.length)throw new Error('No valid candles received');feed.candles=candles;feed.candlesAt=new Date(now()).toISOString();feed.candleError=null;}
      catch(error){feed.candleError=error.message;}
    }
  }
  async function poll() {
    if(pollPromise)return pollPromise;
    if(now()-lastPoll<10000)return;
    lastPoll=now();
    pollPromise=Promise.all(PRODUCTS.map(pollProduct)).finally(()=>{pollPromise=null;});
    return pollPromise;
  }
  async function state() {
    await poll();
    return {serverTime:new Date(now()).toISOString(),pollMs:10000,runtime:'browser-public',integration:{mode:'Public browser feed',configured:false,connected:false,consumed:0,rejected:0,lastMessageAt:null,error:null},flink:{configured:false,connected:false,consumed:0,rejected:0,lastMessageAt:null,error:null},products:PRODUCTS.map(product=>{const feed=feeds[product];return {...structuredClone(feed),trades:structuredClone(feed.trades.slice(-80).reverse()),stats:summarize(feed.trades,now()),observedCount:feed.trades.length};})};
  }
  async function freeze(product) {
    if(!PRODUCTS.includes(product))throw fail('Choose BTC-USD, ETH-USD, or SOL-USD.',400);
    await poll();
    const feed=feeds[product],captured=now();
    if(!feed.updatedAt||captured-Date.parse(feed.updatedAt)>30000||feed.error)throw fail('The public source is waiting or stale. Wait for fresh data before freezing a window.');
    const candidates=feed.trades.filter(trade=>trade.timestamp>=captured-120000&&trade.timestamp<=captured);
    const trades=structuredClone(candidates.slice(-1000));
    if(trades.length<3||trades.at(-1).timestamp-trades[0].timestamp<30000)throw fail('The sample needs at least 30 seconds of observed history. Keep this page open, then freeze again.');
    const snapshot={id:cryptoImpl.randomUUID(),product,source,capturedAt:new Date(captured).toISOString(),trades,truncated:candidates.length>1000};
    remember(snapshots,snapshot.id,snapshot);
    return {snapshot:{id:snapshot.id,product,source,capturedAt:snapshot.capturedAt,count:trades.length,spanSeconds:(trades.at(-1).timestamp-trades[0].timestamp)/1000,truncated:snapshot.truncated}};
  }
  async function run(params) {
    const snapshot=lookup(snapshots,params.get('snapshotId'),'Snapshot');
    const result=runExperiment({snapshot,scenario:params.get('scenario'),severity:Number(params.get('severity')),thresholdPct:Number(params.get('thresholdPct'))});
    const digest=await cryptoImpl.subtle.digest('SHA-256',new TextEncoder().encode(JSON.stringify(result.evidence)));
    const hash=Array.from(new Uint8Array(digest),byte=>byte.toString(16).padStart(2,'0')).join('');
    const experimentId=hash.slice(0,24);
    remember(experiments,experimentId,{format:'marketpulse-evidence-v1',algorithm:'SHA-256',serialization:'JSON.stringify(evidence)',sha256:hash,evidence:result.evidence});
    return {experimentId,hash,report:result.report};
  }
  return {async request(path) {
    const url=new URL(path,'https://marketpulse.invalid'),q=url.searchParams;
    if(url.pathname==='/api/state')return state();
    if(url.pathname==='/api/lab/snapshot')return freeze(q.get('product'));
    if(url.pathname==='/api/lab/run')return run(q);
    if(url.pathname==='/api/lab/evidence')return structuredClone(lookup(experiments,q.get('experimentId'),'Experiment'));
    throw fail('Unknown browser provider endpoint',404);
  }};
}
