import { decimal, normalizeTrade, PRODUCTS } from './analytics.mjs';

const VERSION = '1.0.0';
const DEADLINE_MS = 20000;
const WINDOW_MS = 120000;
const SCENARIOS = ['duplicate', 'drop', 'delay', 'shock'];
const round = value => Math.round(value * 10000) / 10000;
const order = (a, b) => a.timestamp - b.timestamp || a.id.localeCompare(b.id);

function validateSnapshot(snapshot) {
  if (!snapshot || typeof snapshot !== 'object') throw new Error('A frozen snapshot is required');
  for (const key of ['id', 'source']) {
    if (typeof snapshot[key] !== 'string' || !snapshot[key].trim() || snapshot[key].length > 500) throw new Error(`Invalid snapshot ${key}`);
  }
  if (!PRODUCTS.includes(snapshot.product)) throw new Error('Invalid snapshot product');
  const captured = typeof snapshot.capturedAt === 'number' ? snapshot.capturedAt : Date.parse(snapshot.capturedAt);
  if (!Number.isSafeInteger(captured) || !Number.isFinite(new Date(captured).getTime())) throw new Error('Invalid snapshot capturedAt');
  if (!Array.isArray(snapshot.trades) || snapshot.trades.length > 1000) throw new Error('Snapshot must contain at most 1000 trades');
  if (typeof snapshot.truncated !== 'boolean') throw new Error('Snapshot truncated flag is required');
  const seen = new Set();
  const trades = snapshot.trades.map(t => {
    if (!t || t.product_id !== snapshot.product) throw new Error('Trade product must match snapshot');
    const normalized = normalizeTrade({ trade_id: t.trade_id, price: t.price, size: t.size, side: t.maker_side, time: t.time }, snapshot.product);
    if (t.id !== normalized.id || t.timestamp !== normalized.timestamp || t.taker_side !== normalized.taker_side) throw new Error('Invalid normalized trade');
    if (normalized.timestamp < captured - WINDOW_MS || normalized.timestamp > captured) throw new Error('Trade is outside the frozen 120-second window');
    if (seen.has(normalized.id)) throw new Error('Baseline trade IDs must be unique');
    seen.add(normalized.id);
    return normalized;
  }).sort(order);
  return { id: snapshot.id, source: snapshot.source, product: snapshot.product, capturedAt: new Date(captured).toISOString(), truncated: snapshot.truncated, trades };
}

function movement(trades) {
  if (trades.length < 2) return null;
  const ordered = [...trades].sort(order);
  // Inputs were validated above; synthetic increases may add an integer digit.
  const units = price => {
    const [whole, fraction = ''] = price.split('.');
    return BigInt(whole) * 100000000n + BigInt(fraction.padEnd(8, '0'));
  };
  const first = units(ordered[0].price), last = units(ordered.at(-1).price);
  return round(Number((last - first) * 1000000n / first) / 10000);
}

function shiftedPrice(price, percent) {
  const value = decimal(price) * BigInt(100 + percent) / 100n;
  return `${value / 100000000n}.${String(value % 100000000n).padStart(8, '0')}`;
}

function decide(trades, baselineCount, thresholdPct) {
  const movePct = movement(trades);
  if (trades.length < 2) return { state: 'INSUFFICIENT', reason: 'Fewer than two delivered trades; movement cannot be evaluated.', count: trades.length, movePct };
  if (trades.length >= baselineCount * 1.5) return { state: 'ALERT', reason: 'Delivered record count is at least 1.5× the frozen baseline count.', count: trades.length, movePct };
  if (Math.abs(movePct) >= thresholdPct) return { state: 'ALERT', reason: `Absolute first-to-last price movement meets the ${thresholdPct}% threshold.`, count: trades.length, movePct };
  return { state: 'CLEAR', reason: 'Neither the record-count nor price-movement threshold is met.', count: trades.length, movePct };
}

/** Deterministic controlled replay. No network, live mutation, clock, or randomness. */
export function runExperiment({ snapshot, scenario, severity, thresholdPct } = {}) {
  if (!SCENARIOS.includes(scenario)) throw new Error('Unknown scenario');
  if (!Number.isInteger(severity) || severity < 1 || severity > 3) throw new Error('Severity must be an integer from 1 to 3');
  if (typeof thresholdPct !== 'number' || !Number.isFinite(thresholdPct) || thresholdPct < 0.2 || thresholdPct > 5) throw new Error('Threshold must be a finite number from 0.2 to 5 percent');
  const baseline = validateSnapshot(snapshot);
  const trades = baseline.trades;
  const count = trades.length;
  const spanSeconds = count > 1 ? (trades.at(-1).timestamp - trades[0].timestamp) / 1000 : 0;
  const duplicateFactor = [1, 2, 4][severity - 1];
  const dropFraction = [0.25, 0.5, 0.75][severity - 1];
  const delayMs = [30000, 60000, 120000][severity - 1];
  const priceShiftPct = scenario === 'shock' ? [1, 3, 5][severity - 1] : 0;
  const removedTarget = scenario === 'drop' ? Math.ceil(count * dropFraction) : 0;
  const delayedTarget = scenario === 'delay' ? Math.ceil(count / 2) : 0;
  // Spread omissions evenly through the ordered sample, deterministically.
  const removedIndices = new Set(Array.from({ length: removedTarget }, (_, i) => Math.floor(i * count / removedTarget)));
  const events = [];
  const removedIds = [];
  let duplicateCount = 0, delayedCount = 0;
  for (const [index, trade] of trades.entries()) {
    if (removedIndices.has(index)) { removedIds.push(trade.id); continue; }
    const delayed = scenario === 'delay' && index >= count - delayedTarget;
    const shifted = scenario === 'shock' && index >= Math.floor(count * 2 / 3);
    const transformed = { ...trade, price: shifted ? shiftedPrice(trade.price, priceShiftPct) : trade.price };
    const copies = scenario === 'duplicate' ? duplicateFactor + 1 : 1;
    if (delayed) delayedCount++;
    for (let copy = 0; copy < copies; copy++) {
      if (copy > 0) duplicateCount++;
      events.push({ sequence: events.length, baselineIndex: index, copy, syntheticPrice: shifted, deliveryTimestamp: trade.timestamp + (delayed ? delayMs : 0), deadlineTimestamp: trade.timestamp + DEADLINE_MS, trade: { ...transformed } });
    }
  }
  const timely = events.filter(event => event.deliveryTimestamp <= event.deadlineTimestamp);
  const acceptedMap = new Map();
  for (const event of timely) if (!acceptedMap.has(event.trade.id)) acceptedMap.set(event.trade.id, event.trade);
  const accepted = [...acceptedMap.values()].sort(order);
  const raw = timely.map(event => event.trade);
  const lateCount = events.length - timely.length;
  const missingCount = count - accepted.length;
  const quality = { duplicateCount: timely.length - accepted.length, missingCount, lateCount, coveragePct: count ? round(accepted.length / count * 100) : 0 };
  let naive = decide(raw, count, thresholdPct);
  let guarded = decide(accepted, count, thresholdPct);
  const enoughBaseline = count >= 3 && spanSeconds >= 30;
  if (!enoughBaseline) {
    const reason = 'Frozen sample needs at least 3 distinct trades spanning 30 seconds; collect more data before interpreting the experiment.';
    naive = { ...naive, state: 'INSUFFICIENT', reason };
    guarded = { ...guarded, state: 'INSUFFICIENT', reason };
  } else if (missingCount) {
    guarded = { ...guarded, state: 'HOLD', reason: `${missingCount} of ${count} expected sample IDs are missing at their event-time + 20 s replay deadlines. Completeness is known only because this controlled experiment has a frozen baseline.` };
  }
  let headline, detail;
  if (!enoughBaseline) {
    headline = 'Collect a longer sample before drawing conclusions';
    detail = 'The experiment is reproducible, but this baseline is too small or too short for a signal comparison.';
  } else if (scenario === 'duplicate') {
    headline = guarded.state === 'CLEAR' ? 'Duplicate delivery creates an alert; deduplication removes it' : 'Deduplication removes the volume artifact while retaining the price alert';
    detail = `${duplicateCount} extra copies inflate the naive record count. The guard evaluates ${accepted.length} distinct trade IDs, preserving the sampled price movement.`;
  } else if (scenario === 'drop' || scenario === 'delay') {
    headline = scenario === 'drop' ? 'Known record loss stops an untrustworthy decision' : 'Late delivery holds the decision at the replay deadline';
    detail = `The naive rule returns ${naive.state}; the guard returns ${guarded.state} because ${missingCount} expected sample IDs are absent at their deadlines. This does not establish completeness of the live venue feed.`;
  } else {
    headline = guarded.state === 'ALERT' ? 'A quality guard preserves a valid simulated price alert' : 'Synthetic movement stays below the selected alert threshold';
    detail = `The final third of copied prices is increased by ${priceShiftPct}%. This is a synthetic perturbation, not an observed market event. Distinct, on-time records pass the quality guard; net movement is compared with the selected threshold.`;
  }
  const start = count ? trades[0].timestamp : Date.parse(baseline.capturedAt);
  const binWidth = Math.max(1, Math.ceil((spanSeconds + 0.001) / 24));
  const binCount = Math.min(24, Math.max(1, Math.floor(spanSeconds / binWidth) + 1));
  const timeline = Array.from({ length: binCount }, (_, i) => ({ second: i * binWidth, baseline: 0, received: 0, accepted: 0 }));
  const binFor = t => timeline[Math.min(binCount - 1, Math.floor((t.timestamp - start) / 1000 / binWidth))];
  trades.forEach(t => binFor(t).baseline++);
  raw.forEach(t => binFor(t).received++);
  accepted.forEach(t => binFor(t).accepted++);
  const report = {
    scenario, severity, product: baseline.product, source: baseline.source, capturedAt: baseline.capturedAt, snapshotId: baseline.id,
    baseline: { count, spanSeconds: round(spanSeconds), movePct: movement(trades) },
    injected: { label: { duplicate: `${duplicateFactor} extra copies per baseline trade`, drop: `${dropFraction * 100}% deterministic record removal (rounded up)`, delay: `Last half of records delayed ${delayMs / 1000} s`, shock: `Synthetic +${priceShiftPct}% price shift on the final third` }[scenario], duplicateCount, removedCount: removedIds.length, delayedCount, priceShiftPct },
    naive, guarded, quality, outcome: { headline, detail }, timeline,
    checks: [
      { name: 'Sample sufficiency', status: enoughBaseline ? 'pass' : 'info', detail: `${count} distinct baseline trades span ${round(spanSeconds)} seconds; interpretation requires ≥3 trades and ≥30 seconds.` },
      { name: 'Unique records after guard', status: accepted.length === new Set(accepted.map(t => t.id)).size ? 'pass' : 'fail', detail: `${quality.duplicateCount} timely duplicate copies removed by product + trade ID.` },
      { name: 'Known sample coverage', status: missingCount ? 'fail' : count ? 'pass' : 'info', detail: `${accepted.length}/${count} frozen sample IDs available at their individual deadlines. This is controlled sample coverage, not live-feed completeness.` },
      { name: 'Delivery deadline', status: lateCount ? 'fail' : 'pass', detail: `${lateCount} deliveries exceed the 20-second per-event tolerance.` },
      { name: 'Source scope', status: 'info', detail: `Public sampled trades; not the entire venue.${baseline.truncated ? ' The frozen sample was capped at 1000 records.' : ''} No live records were changed.` },
      { name: 'Synthetic injection', status: 'info', detail: 'Only deterministic copies of the frozen sample are changed. Alerts are experimental data signals, not trading advice or measured financial impact.' }
    ],
    rules: { version: VERSION, thresholdPct, volumeCountMultiplier: 1.5, deadlineSeconds: 20, deadlineBasis: 'Individual event timestamp + 20 seconds in a controlled replay; late deliveries are excluded from both signal inputs.', deduplicationKey: 'product_id + trade_id', completeness: 'Known frozen sample IDs only; any missing ID causes HOLD after the baseline sufficiency check.', movement: 'Absolute first-to-last event-time price movement, truncated to four decimal percentage places; both rules use the selected percent threshold.', priceShiftRounding: 'Synthetic shifted prices are rounded down to eight decimal places.', minimumBaselineCount: 3, minimumBaselineSpanSeconds: 30, windowSeconds: 120, maxBaselineRecords: 1000, timelineBasis: 'Event-time bins of baseline, timely received copies, and timely unique accepted trades; not wall-clock delivery bins.' }
  };
  return { report, evidence: { version: VERSION, snapshot: baseline, inputs: { scenario, severity, thresholdPct }, transformedEvents: events, removedIds, report: structuredClone(report) } };
}
