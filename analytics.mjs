export const PRODUCTS = ['BTC-USD', 'ETH-USD', 'SOL-USD'];
const SCALE = 100000000n;
export function decimal(value) {
  const text = String(value);
  if (!/^\d{1,15}(\.\d{1,8})?$/.test(text)) throw new Error('Invalid positive decimal');
  const [whole, fraction = ''] = text.split('.');
  const result = BigInt(whole) * SCALE + BigInt(fraction.padEnd(8, '0'));
  if (result <= 0n) throw new Error('Value must be positive');
  return result;
}
export function normalizeTrade(raw, product) {
  if (!PRODUCTS.includes(product)) throw new Error('Unknown product');
  if (!/^\d+$/.test(String(raw.trade_id)) || (typeof raw.trade_id === 'number' && !Number.isSafeInteger(raw.trade_id))) throw new Error('Invalid trade ID');
  decimal(raw.price); decimal(raw.size);
  if (!['buy', 'sell'].includes(raw.side)) throw new Error('Invalid maker side');
  const timestamp = Date.parse(raw.time);
  if (!Number.isFinite(timestamp)) throw new Error('Invalid timestamp');
  return {id: `${product}:${raw.trade_id}`, trade_id: String(raw.trade_id), product_id: product, price: String(raw.price), size: String(raw.size), maker_side: raw.side, taker_side: raw.side === 'buy' ? 'sell' : 'buy', time: new Date(timestamp).toISOString(), timestamp};
}
export function mergeTrades(previous, incoming, now = Date.now()) {
  const trades = new Map(previous.map(t => [t.id, t]));
  for (const trade of incoming) trades.set(trade.id, trade);
  return [...trades.values()].filter(t => t.timestamp >= now - 900000 && t.timestamp <= now + 10000).sort((a,b) => a.timestamp-b.timestamp || a.id.localeCompare(b.id)).slice(-10000);
}
export function summarize(trades, now = Date.now()) {
  const recent = trades.filter(t => t.timestamp >= now - 60000 && t.timestamp <= now);
  let notional = 0n, quantity = 0n, takerBuy = 0n;
  for (const trade of recent) {
    const size = decimal(trade.size), price = decimal(trade.price);
    quantity += size; notional += size * price;
    if (trade.taker_side === 'buy') takerBuy += size;
  }
  const coverageSeconds = recent.length > 1 ? (recent.at(-1).timestamp - recent[0].timestamp)/1000 : 0;
  return {count: recent.length, notional: Number(notional / SCALE) / Number(SCALE), quantity: Number(quantity)/Number(SCALE), vwap: quantity ? Number(notional/quantity)/Number(SCALE) : null, takerBuyPercent: quantity ? Number(takerBuy*10000n/quantity)/100 : null, coverageSeconds, movePercent: coverageSeconds >= 30 ? (Number(recent.at(-1).price)/Number(recent[0].price)-1)*100 : null};
}
export function normalizeCandles(raw) {
  if (!Array.isArray(raw)) throw new Error('Invalid candle response');
  return raw.filter(c => Array.isArray(c) && c.length >= 6 && c.every(Number.isFinite) && c[4] > 0 && c[5] >= 0).map(c => ({time:c[0]*1000, low:c[1], high:c[2], open:c[3], close:c[4], volume:c[5]})).sort((a,b)=>a.time-b.time);
}
