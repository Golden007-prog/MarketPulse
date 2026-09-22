import {normalizeTrade, PRODUCTS} from './analytics.mjs';

// Confluent JSON Schema wire format: magic byte 0, four-byte schema ID, UTF-8 JSON.
// This decodes JSON_SR, not Avro; configure HttpSource output.data.format=JSON_SR.
export function decodeJsonMessage(value) {
  if(!value || !value.length) throw new Error('Empty Kafka value');
  const body=value[0]===0?value.subarray(5):value;
  if(value[0]===0 && value.length<6) throw new Error('Truncated schema envelope');
  return JSON.parse(body.toString('utf8'));
}
export function decodeTradeMessage(value, product) {
  const raw=decodeJsonMessage(value);
  const rows=Array.isArray(raw)?raw:[raw];
  return rows.map(row=>normalizeTrade(row,product));
}

export function normalizeSignal(raw) {
  if(!raw || !PRODUCTS.includes(raw.symbol)) throw new Error('Unknown signal symbol');
  const rawTime=raw.window_end;
  // Flink TIMESTAMP(3) has no timezone. The pipeline contract uses UTC.
  const utcTime=typeof rawTime==='string' && !/(?:Z|[+-]\d\d:\d\d)$/i.test(rawTime)?rawTime.replace(' ','T')+'Z':rawTime;
  const timestamp=Date.parse(utcTime);
  if(!Number.isFinite(timestamp)) throw new Error('Invalid signal window');
  const numeric=(key,nullable=false)=>{
    if(raw[key]===null && nullable) return null;
    if(!['string','number'].includes(typeof raw[key]) || String(raw[key]).trim()==='') throw new Error(`Invalid ${key}`);
    const value=Number(raw[key]);
    if(!Number.isFinite(value)||value<0) throw new Error(`Invalid ${key}`);
    return value;
  };
  const observed=numeric('observed_trades');
  if(!Number.isSafeInteger(observed)) throw new Error('Unsafe observed trade count');
  const signal={symbol:raw.symbol,window_end:new Date(timestamp).toISOString(),timestamp,observed_trades:observed,observed_notional:numeric('observed_notional'),sample_vwap:numeric('sample_vwap'),forecast_trades:numeric('forecast_trades',true),lower_bound:numeric('lower_bound',true),upper_bound:numeric('upper_bound',true),signal:raw.signal};
  if(!['WARMING_UP','ACTIVITY_SPIKE','NORMAL'].includes(signal.signal)) throw new Error('Unknown signal state');
  if(signal.lower_bound!==null && signal.upper_bound!==null && signal.lower_bound>signal.upper_bound) throw new Error('Inverted forecast interval');
  // These fields are optional for older forecast-only messages. Do not infer a
  // model state from the legacy WARMING_UP signal: no model is a distinct state.
  if(Object.hasOwn(raw,'model_status')) {
    if(!['NOT_CONFIGURED','WARMING_UP','READY'].includes(raw.model_status)) throw new Error('Unknown model status');
    signal.model_status=raw.model_status;
    if(raw.model_status==='NOT_CONFIGURED' && [signal.forecast_trades,signal.lower_bound,signal.upper_bound].some(value=>value!==null)) throw new Error('Unconfigured model cannot supply forecasts');
  }
  if(Object.hasOwn(raw,'operational_signal')) {
    if(!['LOW_SAMPLE','WIDE_PRICE_RANGE','BUY_PRESSURE','SELL_PRESSURE','OBSERVING'].includes(raw.operational_signal)) throw new Error('Unknown operational signal');
    signal.operational_signal=raw.operational_signal;
  }
  for(const key of ['price_range_pct','taker_buy_pct','observed_quantity','sample_span_seconds']) {
    if(Object.hasOwn(raw,key)) signal[key]=numeric(key);
  }
  if(signal.taker_buy_pct>100) throw new Error('Taker buy percentage exceeds 100');
  return signal;
}

export function decodeSignalMessage(value) {
  const raw=decodeJsonMessage(value);
  return (Array.isArray(raw)?raw:[raw]).map(normalizeSignal);
}

export async function connectSignals(status,onSignal) {
  const {Kafka,logLevel}=await import('kafkajs');
  const broker=process.env.CONFLUENT_BOOTSTRAP_SERVER;
  if(!/^[a-z0-9.-]+\.confluent\.cloud:9092$/.test(broker)) throw new Error('Expected Confluent Cloud bootstrap server on port 9092');
  const kafka=new Kafka({clientId:'marketpulse-flink-signals',brokers:[broker],ssl:true,sasl:{mechanism:'plain',username:process.env.CONFLUENT_API_KEY,password:process.env.CONFLUENT_API_SECRET},logLevel:logLevel.NOTHING,connectionTimeout:10000,retry:{initialRetryTime:500,retries:8}});
  const consumer=kafka.consumer({groupId:`${process.env.CONFLUENT_GROUP_ID||'marketpulse-dashboard-v1'}-signals`,sessionTimeout:30000});
  consumer.on(consumer.events.CRASH,event=>{status.error=event.payload.error?.message||'Signal consumer crashed';status.connected=false;});
  consumer.on(consumer.events.DISCONNECT,()=>{status.connected=false;});
  await consumer.connect();
  await consumer.subscribe({topics:[process.env.CONFLUENT_SIGNALS_TOPIC],fromBeginning:true});
  status.connected=true;
  await consumer.run({eachMessage:async({message})=>{
    try {
      const signals=decodeSignalMessage(message.value);
      for(const signal of signals) onSignal(signal);
      status.consumed+=signals.length;status.lastMessageAt=new Date().toISOString();status.error=null;
    } catch {status.rejected++;status.error='A Flink signal failed JSON validation';}
  }});
  return consumer;
}

export async function connectKafka(integration, onTrades) {
  const {Kafka,logLevel}=await import('kafkajs');
  const broker=process.env.CONFLUENT_BOOTSTRAP_SERVER;
  if(!/^[a-z0-9.-]+\.confluent\.cloud:9092$/.test(broker)) throw new Error('Expected Confluent Cloud bootstrap server on port 9092');
  const kafka=new Kafka({clientId:'marketpulse-dashboard',brokers:[broker],ssl:true,sasl:{mechanism:'plain',username:process.env.CONFLUENT_API_KEY,password:process.env.CONFLUENT_API_SECRET},logLevel:logLevel.NOTHING,connectionTimeout:10000,retry:{initialRetryTime:500,retries:8}});
  const consumer=kafka.consumer({groupId:process.env.CONFLUENT_GROUP_ID||'marketpulse-dashboard-v1',sessionTimeout:30000});
  const prefix=process.env.CONFLUENT_TOPIC_PREFIX||'marketpulse_trades_';
  const topics=Object.fromEntries(PRODUCTS.map(p=>[prefix+p,p]));
  consumer.on(consumer.events.CRASH,event=>{integration.error=event.payload.error?.message||'Kafka consumer crashed';integration.connected=false;});
  consumer.on(consumer.events.DISCONNECT,()=>{integration.connected=false;});
  await consumer.connect();
  integration.connected=true;
  await consumer.subscribe({topics:Object.keys(topics),fromBeginning:true});
  await consumer.run({partitionsConsumedConcurrently:3,eachMessage:async({topic,message})=>{
    try {
      const trades=decodeTradeMessage(message.value,topics[topic]);
      onTrades(topics[topic],trades);
      integration.consumed+=trades.length; integration.lastMessageAt=new Date().toISOString();integration.error=null;
    } catch {integration.rejected++;integration.error='A Kafka message failed JSON_SR trade validation';}
  }});
  return consumer;
}
