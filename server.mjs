import http from 'node:http';
import {readFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import {PRODUCTS, normalizeTrade, mergeTrades, summarize, normalizeCandles} from './analytics.mjs';
import {connectKafka,connectSignals} from './kafka.mjs';
import {createLabStore} from './lab-service.mjs';

const port = Number(process.env.PORT || 4317);
const host = process.env.HOST || '127.0.0.1';
const publicAppOrigin=process.env.PUBLIC_APP_ORIGIN || '';
if(publicAppOrigin && (new URL(publicAppOrigin).origin!==publicAppOrigin || !publicAppOrigin.startsWith('https://'))) throw new Error('PUBLIC_APP_ORIGIN must be an HTTPS origin without a path');
const pollMs = 10000;
const state = Object.fromEntries(PRODUCTS.map(product => [product, {product, trades:[], candles:[], ticker:null, updatedAt:null, candlesAt:null, error:null, candleError:null, rejected:0,flinkSignal:null}]));
const integration = {mode:'Public API preview', configured:false, connected:false, consumed:0, rejected:0, lastMessageAt:null, error:null};
const lab=createLabStore({getFeed:product=>state[product],getSource:()=>integration.configured?'Confluent Kafka / Coinbase HTTP Source (sampled)':'Coinbase public REST (sampled)'});
const envKeys = ['CONFLUENT_BOOTSTRAP_SERVER','CONFLUENT_API_KEY','CONFLUENT_API_SECRET'];
integration.configured = envKeys.every(key => Boolean(process.env[key]));
if (envKeys.some(key=>process.env[key]) && !integration.configured) integration.error = 'Incomplete Confluent environment configuration';
if(integration.configured) integration.mode='Confluent configured; waiting for data';
let consumer;
let signalConsumer;
const flink={configured:integration.configured&&Boolean(process.env.CONFLUENT_SIGNALS_TOPIC),connected:false,consumed:0,rejected:0,lastMessageAt:null,error:null};
function acceptFlinkSignal(signal) {
  const previous=state[signal.symbol].flinkSignal;
  // Do not let out-of-order replay replace a newer completed window.
  if(!previous || signal.timestamp>=previous.timestamp) state[signal.symbol].flinkSignal={...signal,receivedAt:new Date().toISOString()};
}
const pendingTrades=new Map();
let flushTimer=null;

export async function fetchJson(url, options = {}, attempts = 3) {
  let failure;
  for (let attempt=0; attempt<attempts; attempt++) {
    try {
      const response = await fetch(url, {...options, signal:AbortSignal.timeout(8000), headers:{'User-Agent':'MarketPulse/1.0', ...options.headers}});
      if (!response.ok) {
        const error = new Error(`Upstream HTTP ${response.status}`);
        error.retryable = response.status===429 || response.status>=500;
        throw error;
      }
      return await response.json();
    } catch(error) {
      failure=error;
      if (error.retryable===false || attempt===attempts-1) break;
      await new Promise(resolve=>setTimeout(resolve, 350*2**attempt));
    }
  }
  throw failure;
}

function acceptKafkaTrades(product,incoming) {
  let batch=pendingTrades.get(product);
  if(!batch){batch=new Map();pendingTrades.set(product,batch);}
  for(const trade of incoming) batch.set(trade.id,trade);
  // Merge a burst once, instead of sorting a 10,000-record window for each message.
  if(!flushTimer) flushTimer=setTimeout(()=>{
    for(const [id,rows] of pendingTrades){
      const item=state[id];item.trades=mergeTrades(item.trades,[...rows.values()]);
      const newest=item.trades.at(-1);
      if(newest) item.ticker={price:newest.price,time:newest.time};
      item.updatedAt=new Date().toISOString();item.error=null;
    }
    pendingTrades.clear();flushTimer=null;integration.mode='Confluent live';
  },200);
}

async function pollProduct(product) {
  const item=state[product];
  if(!integration.configured) {
  try {
    const base=`https://api.exchange.coinbase.com/products/${product}`;
    const [raw,ticker] = await Promise.all([fetchJson(`${base}/trades?limit=100`),fetchJson(`${base}/ticker`)]);
    if (!Array.isArray(raw)) throw new Error('Invalid trade response');
    if (!Number.isFinite(Number(ticker.price)) || Number(ticker.price)<=0 || !Number.isFinite(Date.parse(ticker.time))) throw new Error('Invalid ticker response');
    const incoming=[];
    for (const row of raw) { try {incoming.push(normalizeTrade(row,product));} catch {item.rejected++;} }
    if (!incoming.length) throw new Error('No valid trades received');
    item.trades=mergeTrades(item.trades,incoming); item.ticker={price:String(ticker.price),time:ticker.time,bid:String(ticker.bid),ask:String(ticker.ask)}; item.updatedAt=new Date().toISOString(); item.error=null;
  } catch(error) {item.error=error.message;}
  }
  if(!item.candlesAt || Date.now()-Date.parse(item.candlesAt)>60000) {
    try {item.candles=normalizeCandles(await fetchJson(`https://api.exchange.coinbase.com/products/${product}/candles?granularity=60`)); item.candlesAt=new Date().toISOString(); item.candleError=null;}
    catch(error) {item.candleError=error.message;}
  }
}
let polling=false;
async function poll() {
  if(polling) return;
  polling=true;
  try {for(const product of PRODUCTS) await pollProduct(product);} finally {polling=false;}
}
const publicFiles = {'/':['index.html','text/html'], '/app.js':['app.js','text/javascript'], '/style.css':['style.css','text/css'], '/favicon.svg':['favicon.svg','image/svg+xml'], '/wind-tunnel.js':['wind-tunnel.js','text/javascript'], '/wind-tunnel.css':['wind-tunnel.css','text/css']};
publicFiles['/runtime.js']=['runtime.js','text/javascript'];
publicFiles['/runtime-config.js']=['runtime-config.js','text/javascript'];
const server=http.createServer(async(req,res)=>{
  res.setHeader('X-Content-Type-Options','nosniff'); res.setHeader('Referrer-Policy','no-referrer');
  if(publicAppOrigin && req.headers.origin===publicAppOrigin && req.url.startsWith('/api/')) {
    res.setHeader('Access-Control-Allow-Origin',publicAppOrigin);
    res.setHeader('Vary','Origin');
  }
  res.setHeader('Content-Security-Policy',"default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'");
  if(req.method!=='GET') {res.writeHead(405);return res.end('Method not allowed');}
  const requestUrl=new URL(req.url,'http://localhost'), path=requestUrl.pathname;
  if(path.startsWith('/api/lab/')) {
    res.setHeader('Cache-Control','no-store');
    res.setHeader('Content-Type','application/json');
    try {
      const q=requestUrl.searchParams;
      if(path==='/api/lab/snapshot') return res.end(JSON.stringify(lab.freeze(q.get('product'))));
      if(path==='/api/lab/run') return res.end(JSON.stringify(lab.run({snapshotId:q.get('snapshotId'),scenario:q.get('scenario'),severity:Number(q.get('severity')??1),thresholdPct:Number(q.get('thresholdPct')??0.5)})));
      if(path==='/api/lab/evidence') {
        const artifact=lab.evidence(q.get('experimentId'));
        res.setHeader('Content-Disposition',`attachment; filename="marketpulse-evidence-${artifact.sha256.slice(0,12)}.json"`);
        return res.end(JSON.stringify(artifact,null,2));
      }
      res.statusCode=404;return res.end(JSON.stringify({error:'Unknown lab endpoint'}));
    } catch(error) {res.statusCode=error.status||500;return res.end(JSON.stringify({error:error.status?error.message:'Unable to process the experiment.'}));}
  }
  if(path==='/api/state') {
    res.writeHead(200,{'Content-Type':'application/json','Cache-Control':'no-store'});
    return res.end(JSON.stringify({serverTime:new Date().toISOString(),pollMs,integration,flink,products:PRODUCTS.map(product=>{const item=state[product];return {...item,trades:item.trades.slice(-80).reverse(),stats:summarize(item.trades),observedCount:item.trades.length};})}));
  }
  if(path==='/api/health') {res.writeHead(200,{'Content-Type':'application/json'}); return res.end(JSON.stringify({ok:true,productsReady:PRODUCTS.filter(p=>state[p].updatedAt).length}));}
  const file=publicFiles[path];
  if(!file) {res.writeHead(404); return res.end('Not found');}
  try {const body=await readFile(fileURLToPath(new URL(`./public/${file[0]}`,import.meta.url))); res.writeHead(200,{'Content-Type':`${file[1]}; charset=utf-8`});res.end(body);} catch {res.writeHead(500);res.end('Unable to read app file');}
});
if(process.env.NODE_ENV!=='test') {
  server.listen(port,host,()=>console.log(`MarketPulse running at http://${host}:${port}`));
  void poll();
  if(integration.configured) connectKafka(integration,acceptKafkaTrades).then(result=>{consumer=result;}).catch(error=>{integration.error=error.message;integration.connected=false;});
  if(flink.configured) connectSignals(flink,acceptFlinkSignal).then(result=>{signalConsumer=result;}).catch(error=>{flink.error=error.message;flink.connected=false;});
  const timer=setInterval(poll,pollMs);
  for(const signal of ['SIGINT','SIGTERM']) process.on(signal,async()=>{clearInterval(timer);await Promise.allSettled([consumer?.disconnect(),signalConsumer?.disconnect()]);server.close(()=>process.exit(0));});
}
