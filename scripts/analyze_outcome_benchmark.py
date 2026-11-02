#!/usr/bin/env python3
"""Join complete Kubernetes benchmark CSV and post-run broker evidence; no live mutations."""
import argparse,bisect,csv,hashlib,json,math
from pathlib import Path
from run_latency import distribution

def jsonlines(path):
 with path.open()as f:return[json.loads(line)for line in f if line.strip()]
def trace(path):
 with path.open(newline='')as f:return[{k:int(v)if k!='type'else v for k,v in r.items()}for r in csv.DictReader(f)]
def indexed(rows,expected,label):
 data=[r for r in rows if r['type']!='Z'];result={r['sequence']:r for r in data}
 if len(result)!=len(data)or set(result)!=set(expected):raise ValueError('missing/duplicate '+label+' samples')
 return result

def signed(values):
 if not values:raise ValueError('empty signed observations')
 ordered=sorted(values)
 return dict(count=len(values),negative_count=sum(v<0 for v in values),min_ms=min(values)/1e6,mean_ms=sum(values)/len(values)/1e6,p50_ms=ordered[math.ceil(.5*len(values))-1]/1e6,p95_ms=ordered[math.ceil(.95*len(values))-1]/1e6,p99_ms=ordered[math.ceil(.99*len(values))-1]/1e6,max_ms=max(values)/1e6)

def analyze(out):
 settings=json.loads((out/'settings.json').read_text());execution=json.loads((out/'execution.json').read_text());readback=json.loads((out/'readback-result.json').read_text())
 if not execution.get('complete')or not execution.get('exact_stage_orders')or not readback.get('complete')or not readback.get('exact_raw_payloads_and_envelopes'):raise ValueError('completed execution/correctness gates required')
 enabled=settings['enabled'];stages=settings['stages'];total=sum(s['count']for s in stages);native=range(1,total+1)
 source_rows=[]
 for i in range(1,len(stages)+1):source_rows+=trace(out/f'source-{i}.csv')
 publisher_rows=trace(out/'publisher.csv');consumer_rows=trace(out/'consumer.csv')
 sent=indexed(source_rows,native,'source');pub=indexed(publisher_rows,native,'publisher');consumer=indexed(consumer_rows,native,'consumer')
 for name,rows in [('source',source_rows),('publisher',publisher_rows),('consumer',consumer_rows)]:
  if len(rows)!=total+len(stages)or sum(r['type']=='Z'for r in rows)!=len(stages):raise ValueError('heartbeat/total mismatch '+name)
 raw=jsonlines(out/'raw-readback.jsonl');raw_data={r['source_sequence']:r for r in raw if r['type']=='N'}
 if set(raw_data)!=set(native)or len(raw)!=total+len(stages):raise ValueError('raw readback mismatch')
 for n in native:
  c,s,p,r=consumer[n],sent[n],pub[n],raw_data[n]
  if c['attempts']!=1 or c['ack_unix_ns']<=0 or c['type']!='N' or c['received_unix_ns']!=p['received_unix_ns'] or c['received_unix_ns']!=r['received_unix_ns'] or c['source_broker']!=r['broker_sequence']:raise ValueError('attempt/ack/receive provenance mismatch')
  for row in [s,p,c]:
   if row['end_unix_ns']<row['start_unix_ns']or row['duration_ns']<0:raise ValueError('negative local trace duration')
 outcomes={}
 if enabled:
  if not readback.get('exact_all_outcome_fields_and_fanout'):raise ValueError('exact outcome gate required')
  for r in jsonlines(out/'outcome-readback.jsonl'):outcomes.setdefault(r['event']['source_sequence'],[]).append(r)
  if set(outcomes)!=set(native):raise ValueError('outcome coverage missing')
 results=[];start=1
 for index,stage in enumerate(stages,1):
  end=start+stage['count']-1;seqs=range(start,end+1);ss=[sent[n]['start_unix_ns']for n in seqs];cc=[consumer[n]['end_unix_ns']for n in seqs]
  latencies={
   'send_to_commit':distribution([consumer[n]['end_unix_ns']-sent[n]['start_unix_ns']for n in seqs]),
   'send_to_input_ack':distribution([consumer[n]['ack_unix_ns']-sent[n]['start_unix_ns']for n in seqs]),
   'db_process':distribution([consumer[n]['duration_ns']for n in seqs]),
   'input_ack_duration':distribution([consumer[n]['ack_duration_ns']for n in seqs]),
   'receive_to_delivery':distribution([consumer[n]['delivery_unix_ns']-pub[n]['received_unix_ns']for n in seqs]),
   'receive_to_raw_stored':distribution([raw_data[n]['stored_unix_ns']-pub[n]['received_unix_ns']for n in seqs]),
   'send_to_raw_stored':distribution([raw_data[n]['stored_unix_ns']-sent[n]['start_unix_ns']for n in seqs]),
   'publisher_submit_to_puback_observed':distribution([pub[n]['duration_ns']for n in seqs]),
   'source_schedule_offset':signed([sent[n]['start_unix_ns']-sent[n]['scheduled_unix_ns']for n in seqs])}
  checkpoint=json.loads((out/f'checkpoint-stage-{index}.json').read_text());assert checkpoint['source']==end and checkpoint['broker']==end+index and checkpoint['pending']==0
  completion=max(cc);elapsed=completion-min(ss);sendspan=max(ss)-min(ss)
  result=dict(stage=index,first_native=start,last_native=end,events=stage['count'],offered_rate=stage['rate'],actual_send_rate=(stage['count']-1)/(sendspan/1e9),send_to_commit_completion_rate=stage['count']/(elapsed/1e9),send_span_seconds=sendspan/1e9,commit_drain_after_last_send_ms=(completion-max(ss))/1e6,max_sent_not_committed_at_send=max(k+1-bisect.bisect_right(sorted(cc),s)for k,s in enumerate(ss)),sent_not_committed_at_last_send=stage['count']-bisect.bisect_right(sorted(cc),max(ss)),latency=latencies,checkpoint=checkpoint)
  result['source_rate_fraction']=result['actual_send_rate']/stage['rate']
  result['source_rate_within_five_percent']=abs(result['source_rate_fraction']-1)<=.05
  if enabled:
   events=[r for n in seqs for r in outcomes[n]];firsts=[min(r['stored_unix_ns']for r in outcomes[n])for n in seqs];lasts=[max(r['stored_unix_ns']for r in outcomes[n])for n in seqs]
   def recorded_ns(text):
    import datetime
    # RFC3339Nano, exact integer nanoseconds (no float datetime conversion).
    whole,fraction=text[:-1].split('.')if'.'in text else(text[:-1],'')
    seconds=int(datetime.datetime.fromisoformat(whole).replace(tzinfo=datetime.timezone.utc).timestamp());return seconds*1000000000+int(fraction.ljust(9,'0'))
   result['outcomes']=dict(count=len(events),latency={
    'send_to_first_outcome_stored':distribution([t-sent[n]['start_unix_ns']for n,t in zip(seqs,firsts)]),
    'send_to_all_outcomes_stored':distribution([t-sent[n]['start_unix_ns']for n,t in zip(seqs,lasts)]),
    'send_to_each_outcome_stored':distribution([r['stored_unix_ns']-sent[r['event']['source_sequence']]['start_unix_ns']for r in events]),
    'transaction_start_to_outcome_stored':distribution([r['stored_unix_ns']-recorded_ns(r['event']['recorded_at'])for r in events])},client_commit_observation_to_outcome_stored=signed([r['stored_unix_ns']-consumer[r['event']['source_sequence']]['end_unix_ns']for r in events]),all_outcome_completion_rate=stage['count']/((max(lasts)-min(ss))/1e9),last_send_to_last_outcome_stored_ms=(max(lasts)-max(ss))/1e6,max_sent_without_all_outcomes_at_send=max(k+1-bisect.bisect_right(sorted(lasts),s)for k,s in enumerate(ss)))
  results.append(result);start=end+1
 result=dict(version=1,namespace=settings['namespace'],enabled=enabled,image_id=settings['image_id'],stages=results,validation=readback,method=dict(clock=settings['clock'],warmup_excluded=0,percentile='nearest-rank',commit='matching client observes successful PostgreSQL transaction return',outcomes='JetStream retained server timestamp, not PubAck/client delivery/effect',recorded_at='PostgreSQL transaction start, NOT commit or relay start',negative_commit_observation_lags='reported separately without clamping',instrumentation=settings['instrumentation'],caveat=settings['node_caveat']),input_sha256={p.name:hashlib.sha256(p.read_bytes()).hexdigest()for p in sorted(out.glob('*.csv'))})
 (out/'result.json').write_text(json.dumps(result,indent=2)+'\n');return result

def compare(off,on,out):
 a,b=analyze(off),analyze(on)
 if a['enabled']or not b['enabled']or a['image_id']!=b['image_id']:raise ValueError('invalid paired arms')
 pairs=[]
 if len(a['stages'])!=len(b['stages']):raise ValueError('different stage counts')
 for x,y in zip(a['stages'],b['stages']):
  if (x['events'],x['offered_rate'],x['first_native'],x['last_native'])!=(y['events'],y['offered_rate'],y['first_native'],y['last_native']):raise ValueError('nonmatching workloads')
  changes={}
  for boundary in ['send_to_commit','db_process','receive_to_delivery']:
   changes[boundary]={}
   for q in ['mean_ms','p50_ms','p95_ms','p99_ms']:
    old,new=x['latency'][boundary][q],y['latency'][boundary][q];changes[boundary][q]=dict(off=old,on=new,absolute_ms=new-old,percent=(new/old-1)*100 if old else None)
  pairs.append(dict(requested_rate=x['offered_rate'],actual_rate_off=x['actual_send_rate'],actual_rate_on=y['actual_send_rate'],both_rate_targets_met=x['source_rate_within_five_percent']and y['source_rate_within_five_percent'],events=x['events'],changes=changes))
 result=dict(baseline=str(off),outcomes=str(on),comparison=pairs,limitations='One sequential paired run in degraded shared kind VM; no statistical confidence/causal attribution/production SLA. Additional outcome path is functionality and cost, not presumed optimization.')
 out.write_text(json.dumps(result,indent=2)+'\n');return result

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--off',required=True,type=Path);p.add_argument('--on',type=Path);p.add_argument('--out',type=Path);a=p.parse_args()
 if a.on:
  if not a.out:p.error('--out required for comparison')
  print(json.dumps(compare(a.off,a.on,a.out),indent=2))
 else:print(json.dumps(analyze(a.off),indent=2))
if __name__=='__main__':main()
