import test from 'node:test';
import assert from 'node:assert/strict';
import { normalizeTrade } from '../analytics.mjs';
import { runExperiment } from '../wind-tunnel.mjs';

function snapshot({ count = 12, spacing = 8000, price = '100.00' } = {}) {
  const capturedAt = Date.parse('2026-09-22T10:00:00Z');
  return { id: 'snapshot-1', source: 'coinbase-public', product: 'BTC-USD', capturedAt: new Date(capturedAt).toISOString(), truncated: false, trades: Array.from({ length: count }, (_, i) => normalizeTrade({ trade_id: `${i + 1}`, price, size: '0.10', side: 'buy', time: new Date(capturedAt - (count - 1 - i) * spacing).toISOString() }, 'BTC-USD')) };
}
const run = (scenario, severity = 1, extra = {}) => runExperiment({ snapshot: snapshot(), scenario, severity, thresholdPct: 0.5, ...extra });

test('duplicate replay triggers naive volume alert and guard restores original sample', () => {
  for (const [index, copies] of [1, 2, 4].entries()) {
    const { report, evidence } = run('duplicate', index + 1);
    assert.equal(report.naive.state, 'ALERT');
    assert.equal(report.guarded.state, 'CLEAR');
    assert.equal(report.naive.count, 12 * (copies + 1));
    assert.equal(report.guarded.count, 12);
    assert.equal(report.quality.duplicateCount, copies * 12);
    assert.equal(evidence.transformedEvents.length, 12 * (copies + 1));
    assert.equal(report.quality.coveragePct, 100);
  }
});
test('known loss holds decision and measured coverage is limited to baseline', () => {
  for (const severity of [1, 2, 3]) {
    const { report } = run('drop', severity);
    assert.equal(report.guarded.state, 'HOLD');
    assert.equal(report.naive.state, 'CLEAR');
    assert.equal(report.quality.missingCount, 3 * severity);
    assert.equal(report.injected.removedCount, 3 * severity);
    assert.equal(report.quality.coveragePct, 100 - 25 * severity);
    assert.match(report.guarded.reason, /controlled experiment/);
  }
});
test('delayed half misses per-event deadline at every severity', () => {
  for (const severity of [1, 2, 3]) {
    const { report, evidence } = run('delay', severity);
    assert.equal(report.guarded.state, 'HOLD');
    assert.equal(report.quality.lateCount, 6);
    assert.equal(report.quality.missingCount, 6);
    assert.equal(report.guarded.count, 6);
    assert.equal(report.naive.count, 6);
    const delayed = evidence.transformedEvents.filter(e => e.deliveryTimestamp > e.deadlineTimestamp);
    assert.equal(delayed.length, 6);
    assert.equal(delayed[0].deliveryTimestamp - delayed[0].trade.timestamp, [30000, 60000, 120000][severity - 1]);
  }
});
test('synthetic trustworthy price movement survives the guard', () => {
  for (const severity of [1, 2, 3]) {
    const { report } = run('shock', severity);
    assert.equal(report.naive.state, 'ALERT');
    assert.equal(report.guarded.state, 'ALERT');
    assert.equal(report.guarded.movePct, [1, 3, 5][severity - 1]);
    assert.equal(report.quality.coveragePct, 100);
    assert.match(report.outcome.detail, /not an observed market event/);
  }
  assert.equal(run('shock', 1, { thresholdPct: 2 }).report.guarded.state, 'CLEAR');
});
test('existing movement is preserved after deduplication', () => {
  const baseline = snapshot();
  baseline.trades.at(-1).price = '102';
  const { report } = run('duplicate', 1, { snapshot: baseline });
  assert.equal(report.guarded.state, 'ALERT');
  assert.equal(report.guarded.movePct, 2);
});
test('evidence is deterministic, JSON safe, and replayable without mutating input', () => {
  for (const scenario of ['duplicate', 'drop', 'delay', 'shock']) {
    const baseline = snapshot();
    const before = structuredClone(baseline);
    baseline.trades.forEach(Object.freeze);
    Object.freeze(baseline.trades);
    Object.freeze(baseline);
    const first = run(scenario, 2, { snapshot: baseline });
    assert.deepEqual(baseline, before);
    assert.deepEqual(first, run(scenario, 2, { snapshot: baseline }));
    const portable = JSON.parse(JSON.stringify(first.evidence));
    const replay = runExperiment({ snapshot: portable.snapshot, ...portable.inputs });
    assert.deepEqual(replay.report, first.report);
    assert.equal(JSON.stringify(replay.evidence), JSON.stringify(portable));
    assert.ok(first.report.timeline.length <= 24);
    assert.equal(first.report.timeline.reduce((sum, b) => sum + b.baseline, 0), 12);
    assert.equal(first.report.timeline.reduce((sum, b) => sum + b.received, 0), first.report.naive.count);
    assert.equal(first.report.timeline.reduce((sum, b) => sum + b.accepted, 0), first.report.guarded.count);
  }
});
test('synthetic prices remain JSON safe at normalized decimal limits', () => {
  for (const price of ['999999999999999.99999999', '0.00000001']) {
    const { report, evidence } = run('shock', 3, { snapshot: snapshot({ price }) });
    assert.ok(Number.isFinite(report.guarded.movePct));
    assert.doesNotThrow(() => JSON.stringify(evidence));
  }
});
test('empty, tiny, and short-span samples are insufficient', () => {
  for (const baseline of [snapshot({ count: 0 }), snapshot({ count: 1 }), snapshot({ count: 2 }), snapshot({ count: 12, spacing: 1000 })]) {
    const { report } = run('shock', 3, { snapshot: baseline });
    assert.equal(report.naive.state, 'INSUFFICIENT');
    assert.equal(report.guarded.state, 'INSUFFICIENT');
    assert.doesNotThrow(() => JSON.stringify(report));
  }
});
test('invalid inputs fail closed', () => {
  for (const scenario of ['', 'unknown', undefined]) assert.throws(() => run(scenario));
  for (const severity of [0, 4, 1.5, '1', NaN]) assert.throws(() => run('shock', severity));
  for (const thresholdPct of [0.1, 5.1, Infinity, NaN, '0.5', null]) assert.throws(() => run('shock', 1, { thresholdPct }));
  for (const mutate of [s => s.trades.push(s.trades[0]), s => s.trades[0].price = '-1', s => s.trades[0].timestamp++, s => s.trades[0].product_id = 'ETH-USD', s => s.trades[0].id = 'forged', s => s.capturedAt = 'invalid', s => s.product = 'FAKE', s => s.source = '', s => delete s.truncated, s => s.trades = Array(1001).fill(s.trades[0]), s => s.capturedAt = new Date(Date.parse(s.capturedAt) + 120001).toISOString()]) {
    const baseline = snapshot(); mutate(baseline);
    assert.throws(() => run('duplicate', 1, { snapshot: baseline }));
  }
});
