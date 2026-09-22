import test from 'node:test';
import assert from 'node:assert/strict';
import {decimal,normalizeTrade,mergeTrades,summarize,normalizeCandles} from '../analytics.mjs';
import {decodeTradeMessage,decodeSignalMessage,normalizeSignal} from '../kafka.mjs';

const now=Date.parse('2026-09-22T10:00:00Z');
const row=(id,seconds,price='100.00000000',size='0.10000000',side='sell')=>({trade_id:id,time:new Date(now+seconds*1000).toISOString(),price,size,side});
const trade=(...args)=>normalizeTrade(row(...args),'BTC-USD');

test('fixed decimals preserve all eight places and reject malformed or excessive precision',()=>{
  assert.equal(decimal('123.00000001'),12300000001n);
  for(const value of ['NaN','Infinity','-2','0','1e4','1.123456789'])assert.throws(()=>decimal(value));
});
test('normalization preserves product-scoped IDs and uses opposite taker side',()=>{
  const a=trade(123,-5);assert.equal(a.id,'BTC-USD:123');assert.equal(a.maker_side,'sell');assert.equal(a.taker_side,'buy');
  assert.equal(trade(124,-4,'10','1','buy').taker_side,'sell');
  assert.notEqual(normalizeTrade(row(123,-5),'ETH-USD').id,a.id);
  assert.throws(()=>normalizeTrade({...row(123,-5),trade_id:Number.MAX_SAFE_INTEGER+1},'BTC-USD'));
  assert.throws(()=>normalizeTrade({...row(123,-5),time:'invalid'},'BTC-USD'));
});
test('overlapping polls deduplicate, sort by event time and discard old/future records',()=>{
  const a=trade(1,-20),b=trade(2,-5),c=trade(3,-10),old=trade(4,-901),future=trade(5,11);
  const merged=mergeTrades([a,b],[a,c,old,future],now);
  assert.deepEqual(merged.map(t=>t.trade_id),['1','3','2']);
});
test('60s summary excludes expired observations and computes exact VWAP quantities',()=>{
  const stats=summarize([trade(1,-70,'10000','1'),trade(2,-50,'100','0.1'),trade(3,-10,'200','0.2','buy')],now);
  assert.equal(stats.count,2);assert.equal(stats.quantity,.3);assert.equal(stats.notional,50);assert.equal(stats.vwap,166.66666666);assert.equal(stats.takerBuyPercent,33.33);assert.equal(stats.movePercent,100);
});
test('short windows and empty feeds never produce a misleading movement or VWAP',()=>{
  assert.equal(summarize([],now).vwap,null);assert.equal(summarize([],now).movePercent,null);
  assert.equal(summarize([trade(1,-10),trade(2,-5)],now).movePercent,null);
});
test('candles sort chronologically and malformed values are excluded',()=>{
  const candles=normalizeCandles([[2,99,105,100,104,1],[1,99,101,100,100,2],[3,99,101,100,NaN,1]]);
  assert.deepEqual(candles.map(c=>c.time),[1000,2000]);assert.equal(candles[1].close,104);
  assert.throws(()=>normalizeCandles({message:'invalid'}));
});
test('Kafka decoding accepts real JSON_SR framing and array batches',()=>{
  const header=Buffer.from([0,0,0,1,44]);
  const wire=Buffer.concat([header,Buffer.from(JSON.stringify(row(12,-1)))]);
  assert.equal(decodeTradeMessage(wire,'BTC-USD')[0].trade_id,'12');
  assert.equal(decodeTradeMessage(Buffer.from(JSON.stringify([row(1,-2),row(2,-1)])),'SOL-USD').length,2);
  assert.throws(()=>decodeTradeMessage(Buffer.from([0,1]),'BTC-USD'));
  assert.throws(()=>decodeTradeMessage(Buffer.from('not JSON'),'BTC-USD'));
});

const sampleSignal={symbol:'BTC-USD',window_end:'2026-09-22 10:00:00.000',observed_trades:'120',observed_notional:32100.5,sample_vwap:85000,forecast_trades:100,lower_bound:75,upper_bound:125,signal:'NORMAL'};
test('HTTP connector epoch-millisecond timestamps retain the exact UTC event time',()=>{
  const payload={...row(12,-1),time:now-1000};
  const wire=Buffer.concat([Buffer.from([0,0,1,134,167]),Buffer.from(JSON.stringify(payload))]);
  assert.deepEqual(decodeTradeMessage(wire,'BTC-USD')[0],trade(12,-1));
  for(const time of [1.5,Number.MAX_SAFE_INTEGER,Number.MAX_SAFE_INTEGER+1]) {
    assert.throws(()=>decodeTradeMessage(Buffer.from(JSON.stringify({...payload,time})),'BTC-USD'));
  }
});
test('Flink signals decode JSON_SR and treat timezone-free SQL timestamp as UTC',()=>{
  const wire=Buffer.concat([Buffer.from([0,0,0,1,2]),Buffer.from(JSON.stringify(sampleSignal))]);
  const signal=decodeSignalMessage(wire)[0];
  assert.equal(signal.window_end,'2026-09-22T10:00:00.000Z');assert.equal(signal.observed_trades,120);assert.equal(signal.forecast_trades,100);assert.equal(signal.signal,'NORMAL');
});
test('Flink warmup preserves null forecasts and real zero forecasts remain zero',()=>{
  const warming=normalizeSignal({...sampleSignal,forecast_trades:null,lower_bound:null,upper_bound:null,signal:'WARMING_UP'});
  assert.equal(warming.forecast_trades,null);assert.equal(warming.lower_bound,null);
  assert.equal(normalizeSignal({...sampleSignal,forecast_trades:0,lower_bound:0,upper_bound:0}).forecast_trades,0);
});
test('invalid Flink count, interval, state and source values cannot masquerade as valid signals',()=>{
  for(const change of [{observed_trades:'9007199254740993'},{observed_trades:1.5},{forecast_trades:-1},{lower_bound:200,upper_bound:100},{signal:'SPIKE_GUESSED'},{symbol:'FAKE-USD'},{window_end:'invalid'},{observed_notional:null},{sample_vwap:''}]){
    assert.throws(()=>normalizeSignal({...sampleSignal,...change}));
  }
});
