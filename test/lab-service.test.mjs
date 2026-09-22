import test from 'node:test';
import assert from 'node:assert/strict';
import {normalizeTrade} from '../analytics.mjs';
import {createLabStore} from '../lab-service.mjs';
import {verifyEvidence} from '../verify-evidence.mjs';

const captured=Date.parse('2026-09-22T12:00:00.000Z');
function fixture() {
  let clock=captured;
  const trades=Array.from({length:61},(_,i)=>normalizeTrade({trade_id:i+1,price:'100.00',size:'0.10',side:'buy',time:new Date(captured-60000+i*1000).toISOString()},'BTC-USD'));
  const feed={updatedAt:new Date(captured).toISOString(),error:null,trades};
  const store=createLabStore({getFeed:()=>feed,getSource:()=>'Unit test fixture',now:()=>clock,makeId:()=>`test-${clock}`});
  return {store,feed,advance:ms=>clock+=ms};
}
test('freezing isolates the baseline and replay verifies the downloaded artifact',()=>{
  const {store,feed}=fixture();
  const {snapshot}=store.freeze('BTC-USD');
  feed.trades[0].price='999';
  const run=store.run({snapshotId:snapshot.id,scenario:'duplicate',severity:2,thresholdPct:.5});
  assert.equal(run.report.baseline.count,61);
  assert.equal(run.report.naive.state,'ALERT');
  assert.equal(run.report.guarded.state,'CLEAR');
  assert.equal(verifyEvidence(store.evidence(run.experimentId)).verified,true);
});
test('evidence tampering is rejected',()=>{
  const {store}=fixture();const {snapshot}=store.freeze('BTC-USD');
  const run=store.run({snapshotId:snapshot.id,scenario:'shock',severity:2,thresholdPct:.5});
  const artifact=structuredClone(store.evidence(run.experimentId));
  artifact.evidence.report.product='ETH-USD';
  assert.throws(()=>verifyEvidence(artifact),/checksum mismatch/);
});
test('expired snapshots cannot be rerun and invalid parameters do not become internal errors',()=>{
  const {store,advance}=fixture();const {snapshot}=store.freeze('BTC-USD');
  assert.throws(()=>store.run({snapshotId:snapshot.id,scenario:'invalid',severity:1,thresholdPct:.5}),e=>e.status===400);
  advance(1800001);
  assert.throws(()=>store.run({snapshotId:snapshot.id,scenario:'duplicate',severity:1,thresholdPct:.5}),e=>e.status===404);
});
test('stale source, unknown product and short histories cannot masquerade as a live baseline',()=>{
  const {store,feed,advance}=fixture();
  assert.throws(()=>store.freeze('INVALID'),e=>e.status===400);
  feed.trades=feed.trades.slice(-2);
  assert.throws(()=>store.freeze('BTC-USD'),e=>e.status===409);
  advance(40000);
  assert.throws(()=>store.freeze('BTC-USD'),e=>e.status===409);
});
