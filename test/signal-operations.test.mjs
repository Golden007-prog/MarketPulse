import test from 'node:test';
import assert from 'node:assert/strict';
import {normalizeSignal,decodeSignalMessage} from '../kafka.mjs';

const legacy={symbol:'BTC-USD',window_end:'2026-09-22 10:00:00.000',observed_trades:47,observed_notional:90000,sample_vwap:85000,forecast_trades:null,lower_bound:null,upper_bound:null,signal:'WARMING_UP'};
const operations={...legacy,model_status:'NOT_CONFIGURED',operational_signal:'BUY_PRESSURE',price_range_pct:0.27,taker_buy_pct:83.5,observed_quantity:1.05882353,sample_span_seconds:58.2};

test('operational fields survive JSON_SR decoding without claiming a configured model',()=>{
  const wire=Buffer.concat([Buffer.from([0,0,0,1,42]),Buffer.from(JSON.stringify(operations))]);
  const result=decodeSignalMessage(wire)[0];
  for(const key of ['model_status','operational_signal','price_range_pct','taker_buy_pct','observed_quantity','sample_span_seconds']) assert.equal(result[key],operations[key]);
  assert.equal(result.signal,'WARMING_UP');
  assert.equal(result.model_status,'NOT_CONFIGURED');
  assert.equal(result.forecast_trades,null);
});

test('legacy messages do not gain invented model or operations states',()=>{
  const result=normalizeSignal(legacy);
  for(const key of ['model_status','operational_signal','price_range_pct','taker_buy_pct','observed_quantity','sample_span_seconds']) assert.equal(Object.hasOwn(result,key),false);
  const warming=normalizeSignal({...legacy,model_status:'WARMING_UP'});
  assert.equal(warming.model_status,'WARMING_UP');
});

test('all pipeline operational labels are preserved and unknown labels are rejected',()=>{
  for(const operational_signal of ['LOW_SAMPLE','WIDE_PRICE_RANGE','BUY_PRESSURE','SELL_PRESSURE','OBSERVING']) assert.equal(normalizeSignal({...operations,operational_signal}).operational_signal,operational_signal);
  for(const change of [{operational_signal:'ACTIVITY_SPIKE'},{operational_signal:null},{model_status:'RUNNING'},{model_status:null}]) assert.throws(()=>normalizeSignal({...operations,...change}));
});

test('optional measurements accept zero and numeric strings but reject invalid supplied data',()=>{
  for(const key of ['price_range_pct','taker_buy_pct','observed_quantity','sample_span_seconds']) {
    assert.equal(normalizeSignal({...operations,[key]:0})[key],0);
    assert.equal(normalizeSignal({...operations,[key]:'12.5'})[key],12.5);
    for(const invalid of [-1,Infinity,NaN,null,undefined,'',true,{},'unknown']) assert.throws(()=>normalizeSignal({...operations,[key]:invalid}),`${key} must reject ${String(invalid)}`);
  }
  assert.equal(normalizeSignal({...operations,taker_buy_pct:100}).taker_buy_pct,100);
  assert.throws(()=>normalizeSignal({...operations,taker_buy_pct:100.01}));
  // A price range relative to the minimum price can legitimately exceed 100%.
  assert.equal(normalizeSignal({...operations,price_range_pct:150}).price_range_pct,150);
});

test('an unconfigured model cannot carry forecast values or bounds',()=>{
  for(const key of ['forecast_trades','lower_bound','upper_bound']) assert.throws(()=>normalizeSignal({...operations,[key]:0}));
  const configured=normalizeSignal({...legacy,model_status:'READY',signal:'NORMAL',forecast_trades:50,lower_bound:30,upper_bound:70});
  assert.equal(configured.model_status,'READY');assert.equal(configured.forecast_trades,50);
});
