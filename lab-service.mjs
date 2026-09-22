import {createHash, randomUUID} from 'node:crypto';
import {PRODUCTS} from './analytics.mjs';
import {runExperiment} from './wind-tunnel.mjs';

const fail = (status,message) => Object.assign(new Error(message), {status});

export function createLabStore({getFeed, getSource, now=Date.now, makeId=randomUUID}) {
  const snapshots=new Map(), experiments=new Map();
  const ttlMs=30*60*1000;
  function prune(map) {
    for(const [id,item] of map) if(now()-item.createdAt>ttlMs) map.delete(id);
    while(map.size>12) map.delete(map.keys().next().value);
  }
  function remember(map,id,value) {map.set(id,{...value,createdAt:now()});prune(map);}
  function lookup(map,id,label) {
    prune(map);
    const value=map.get(id);
    if(!value) throw fail(404,`${label} expired or is unavailable. Freeze a new live window.`);
    return value;
  }
  return {
    freeze(product) {
      if(!PRODUCTS.includes(product)) throw fail(400,'Choose BTC-USD, ETH-USD, or SOL-USD.');
      const feed=getFeed(product), captured=now();
      if(!feed?.updatedAt || captured-Date.parse(feed.updatedAt)>30000 || feed.error)
        throw fail(409,'The source is waiting or stale. Wait for fresh data before freezing a window.');
      const candidates=feed.trades.filter(t=>t.timestamp>=captured-120000&&t.timestamp<=captured);
      const trades=structuredClone(candidates.slice(-1000));
      if(trades.length<3 || trades.at(-1).timestamp-trades[0].timestamp<30000)
        throw fail(409,'The sample needs at least 30 seconds of observed history. Let the collector run, then freeze again.');
      const snapshot={id:makeId(),product,source:getSource(),capturedAt:new Date(captured).toISOString(),trades,truncated:candidates.length>1000};
      remember(snapshots,snapshot.id,{snapshot});
      return {snapshot:{id:snapshot.id,product,source:snapshot.source,capturedAt:snapshot.capturedAt,count:trades.length,spanSeconds:(trades.at(-1).timestamp-trades[0].timestamp)/1000,truncated:snapshot.truncated}};
    },
    run({snapshotId,scenario,severity,thresholdPct}) {
      const {snapshot}=lookup(snapshots,snapshotId,'Snapshot');
      let result;
      try { result=runExperiment({snapshot,scenario,severity,thresholdPct}); }
      catch(error) {throw fail(400,error.message);}
      const hash=createHash('sha256').update(JSON.stringify(result.evidence)).digest('hex');
      const experimentId=hash.slice(0,24);
      const artifact={format:'marketpulse-evidence-v1',algorithm:'SHA-256',serialization:'JSON.stringify(evidence)',sha256:hash,evidence:result.evidence};
      remember(experiments,experimentId,{artifact});
      return {experimentId,hash,report:result.report};
    },
    evidence(experimentId) {return lookup(experiments,experimentId,'Experiment').artifact;}
  };
}
