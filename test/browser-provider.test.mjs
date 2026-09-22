import test from 'node:test';
import assert from 'node:assert/strict';
import {webcrypto} from 'node:crypto';
import * as analytics from '../analytics.mjs';
import {runExperiment} from '../wind-tunnel.mjs';
import {verifyEvidence} from '../verify-evidence.mjs';
import {createBrowserProvider} from '../public/browser-provider.js';

function fixture() {
  let clock=Date.parse('2026-09-22T12:00:00Z'),broken=false,calls=0;
  const fetchImpl=async(url,options)=>{
    calls++;assert.equal(options.credentials,'omit');assert.ok(url.startsWith('https://api.exchange.coinbase.com/products/'));
    if(broken)return {ok:false,status:429};
    const rows=[40000,20000,1000].map((age,index)=>({trade_id:index+1,price:'100.00',size:'0.1',side:'buy',time:new Date(clock-age).toISOString()}));
    return {ok:true,json:async()=>url.includes('/candles')?[[clock/1000-60,99,101,100,100,2],[clock/1000,99,101,100,100,2]]:rows};
  };
  const provider=createBrowserProvider({analytics,runExperiment,fetchImpl,now:()=>clock,cryptoImpl:webcrypto});
  return {provider,advance:ms=>{clock+=ms;},breakFeed:()=>{broken=true;},calls:()=>calls};
}
test('browser public feed is sampled, throttled and never reports Confluent connectivity',async()=>{
  const f=fixture();const state=await f.provider.request('/api/state');
  assert.equal(state.runtime,'browser-public');assert.equal(state.integration.configured,false);assert.equal(state.flink.configured,false);
  assert.equal(state.products[0].stats.count,3);assert.equal(state.products[0].ticker.price,'100.00');
  assert.equal(f.calls(),6);await f.provider.request('/api/state');assert.equal(f.calls(),6);
});
test('browser evidence verifies with the unchanged offline Node verifier for every scenario',async()=>{
  const {provider}=fixture();const {snapshot}=await provider.request('/api/lab/snapshot?product=BTC-USD');
  assert.match(snapshot.source,/browser.*sampled/);
  for(const scenario of ['duplicate','drop','delay','shock']) {
    const result=await provider.request(`/api/lab/run?snapshotId=${snapshot.id}&scenario=${scenario}&severity=2&thresholdPct=0.5`);
    const artifact=await provider.request(`/api/lab/evidence?experimentId=${result.experimentId}`);
    assert.equal(artifact.sha256,result.hash);assert.equal(verifyEvidence(artifact).verified,true);
    assert.equal(result.report.guarded.state,scenario==='duplicate'?'CLEAR':['drop','delay'].includes(scenario)?'HOLD':'ALERT');
  }
});
test('public feed failures preserve prior observations and block new snapshots',async()=>{
  const f=fixture();const before=await f.provider.request('/api/state');f.advance(31000);f.breakFeed();
  const after=await f.provider.request('/api/state');assert.equal(after.products[0].updatedAt,before.products[0].updatedAt);assert.match(after.products[0].error,/429/);
  await assert.rejects(f.provider.request('/api/lab/snapshot?product=BTC-USD'),/stale/);
});
test('browser snapshots expire and unknown products are rejected',async()=>{
  const f=fixture();await assert.rejects(f.provider.request('/api/lab/snapshot?product=FAKE'),/Choose/);
  const {snapshot}=await f.provider.request('/api/lab/snapshot?product=BTC-USD');f.advance(1800001);
  await assert.rejects(f.provider.request(`/api/lab/run?snapshotId=${snapshot.id}&scenario=duplicate&severity=2&thresholdPct=0.5`),/expired/);
});
