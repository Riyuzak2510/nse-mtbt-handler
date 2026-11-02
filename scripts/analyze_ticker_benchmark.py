#!/usr/bin/env python3
"""Strict complete-run analysis for combined vs per-ticker benchmark; read-only inputs."""
import argparse,bisect,csv,json,math
from pathlib import Path
from analyze_outcome_benchmark import trace,indexed,jsonlines,signed
from run_latency import distribution

def overlap(rows):
 events=[]
 for r in rows:events.extend([(r['start_unix_ns'],1),(r['end_unix_ns'],-1)])
 active=maximum=busy=parallel=0;previous=None
 for at,change in sorted(events):
  if previous is not None:
   elapsed=at-previous
   if active>0:busy+=elapsed
   if active>1:parallel+=elapsed
  active+=change;maximum=max(maximum,active);previous=at
 return dict(max_overlapping_process_calls=maximum,busy_seconds=busy/1e9,two_or_more_seconds=parallel/1e9,parallel_fraction_of_busy=parallel/busy if busy else 0)

def analyze(out):
 cfg=json.loads((out/'settings.json').read_text());execution=json.loads((out/'execution.json').read_text());validation=json.loads((out/'readback-result.json').read_text())
 if not all(execution.get(k)for k in ['complete','exact_stage_orders','no_app_restarts','protected_namespace_specs_unchanged']):raise ValueError('incomplete execution')
 if not all(validation.get(k)for k in ['complete','exact_raw_payloads_and_envelopes','exact_all_outcome_fields_and_fanout','no_application_consumer_modified']):raise ValueError('incomplete exact readback')
 budget=(cfg['aggregate_order_matching_cpu_limit'],cfg['aggregate_order_matching_memory_mib'],cfg['aggregate_order_matching_cpu_request_millicores'],cfg['aggregate_order_matching_memory_request_mib'])
 if budget!=(3,1280,300,640):raise ValueError('unequal benchmark resource contract')
 mode=cfg['topology'];split=mode=='tickers';stages=cfg['stages'];total=sum(s['count']for s in stages);native=range(1,total+1)
 if split and not validation.get('exact_routing_and_digest_chains'):raise ValueError('missing routing gate')
 source=[r for i in range(1,len(stages)+1)for r in trace(out/f'source-{i}.csv')];pubrows=trace(out/'publisher.csv')
 matchrows=trace(out/'matching-service.csv')if not split else trace(out/'ticker-matcher-1.csv')+trace(out/'ticker-matcher-2.csv')
 sent=indexed(source,native,'source');pub=indexed(pubrows,native,'publisher');matched=indexed(matchrows,native,'matching commit')
 for label,rows,hb in [('source',source,len(stages)),('publisher',pubrows,len(stages)),('matcher',matchrows,len(stages)*(2 if split else 1))]:
  if len(rows)!=total+hb or sum(r['type']=='Z'for r in rows)!=hb:raise ValueError('heartbeat/count mismatch '+label)
 admission={};routes={}
 if split:
  intake=trace(out/'order-service.csv');admission=indexed(intake,native,'admission')
  if len(intake)!=total+len(stages):raise ValueError('admission coverage')
  raw_to_native={r['broker_sequence']:r['source_sequence']for r in jsonlines(out/'raw-readback.jsonl')}
  routes={raw_to_native[r['source_broker']]:r for r in jsonlines(out/'routed-readback.jsonl')if raw_to_native[r['source_broker']]!=0}
  if set(routes)!=set(native):raise ValueError('route coverage')
 raw={r['source_sequence']:r for r in jsonlines(out/'raw-readback.jsonl')if r['type']=='N'}
 if set(raw)!=set(native):raise ValueError('raw coverage')
 events={}
 for r in jsonlines(out/'outcome-readback.jsonl'):events.setdefault(r['event']['source_sequence'],[]).append(r)
 if set(events)!=set(native):raise ValueError('event coverage')
 for n in native:
  s,p,c,r=sent[n],pub[n],matched[n],raw[n]
  if p['received_unix_ns']!=r['received_unix_ns']or c['received_unix_ns']!=r['received_unix_ns']or c['source_broker']!=r['broker_sequence']:raise ValueError('correlation provenance mismatch')
  local=(n-1)//2+1;expected=1 if local%3==1 else 2
  if len(events[n])!=expected:raise ValueError('fanout mismatch')
  for row in [s,p,c]+([admission[n]]if split else[]):
   if row['end_unix_ns']<row['start_unix_ns']or row['duration_ns']<0:raise ValueError('negative monotonic duration')
  for row in [c]+([admission[n]]if split else[]):
   if row['attempts']!=1 or row['ack_unix_ns']<=0 or row['type']!='N':raise ValueError('redelivery/missing ack')
  if split:
   a=admission[n]
   if a['source_broker']!=r['broker_sequence']or a['received_unix_ns']!=r['received_unix_ns']or c['shard']!=(n-1)%2+1 or c['end_unix_ns']<a['end_unix_ns']:raise ValueError('ticker source/admission provenance')
 result=[];first=1
 for index,stage in enumerate(stages,1):
  end=first+stage['count']-1;seqs=list(range(first,end+1));ss=[sent[n]['start_unix_ns']for n in seqs];cc=[matched[n]['end_unix_ns']for n in seqs]
  checkpoint=json.loads((out/f'checkpoint-stage-{index}.json').read_text())
  if checkpoint['source']!=end or checkpoint['broker']!=end+index or checkpoint['pending']!=0:raise ValueError('not drained')
  if split and(checkpoint['routing']!=0 or not all(p['admitted']==p['published']==p['applied']for p in checkpoint['tickers'])):raise ValueError('routing backlog')
  lat={
   'send_to_matching_commit':distribution([matched[n]['end_unix_ns']-sent[n]['start_unix_ns']for n in seqs]),
   'matching_process':distribution([matched[n]['duration_ns']for n in seqs]),
   'matching_input_ack_duration':distribution([matched[n]['ack_duration_ns']for n in seqs]),
   'send_to_matching_ack':distribution([matched[n]['ack_unix_ns']-sent[n]['start_unix_ns']for n in seqs]),
   'receive_to_matching_delivery':distribution([matched[n]['delivery_unix_ns']-pub[n]['received_unix_ns']for n in seqs]),
   'send_to_raw_stored':distribution([raw[n]['stored_unix_ns']-sent[n]['start_unix_ns']for n in seqs]),
   'publisher_submit_to_puback_observed':distribution([pub[n]['duration_ns']for n in seqs])}
  if split:
   lat.update(send_to_admission_commit=distribution([admission[n]['end_unix_ns']-sent[n]['start_unix_ns']for n in seqs]),admission_process=distribution([admission[n]['duration_ns']for n in seqs]),receive_to_admission_delivery=distribution([admission[n]['delivery_unix_ns']-pub[n]['received_unix_ns']for n in seqs]),admission_commit_observed_to_matching_commit=distribution([matched[n]['end_unix_ns']-admission[n]['end_unix_ns']for n in seqs]),admission_observed_to_matching_delivery=signed([matched[n]['delivery_unix_ns']-admission[n]['end_unix_ns']for n in seqs]),admission_observed_to_routed_stored=signed([routes[n]['stored_unix_ns']-admission[n]['end_unix_ns']for n in seqs]))
  firstevents=[min(r['stored_unix_ns']for r in events[n])for n in seqs];lastevents=[max(r['stored_unix_ns']for r in events[n])for n in seqs]
  notify={'send_to_first_outcome_stored':distribution([t-sent[n]['start_unix_ns']for n,t in zip(seqs,firstevents)]),'send_to_all_outcomes_stored':distribution([t-sent[n]['start_unix_ns']for n,t in zip(seqs,lastevents)]),'commit_observation_to_each_outcome_stored':signed([r['stored_unix_ns']-matched[n]['end_unix_ns']for n in seqs for r in events[n]])}
  span=max(ss)-min(ss);actual=(len(seqs)-1)/(span/1e9);sorted_cc=sorted(cc)
  result.append(dict(stage=index,first_native=first,last_native=end,events=len(seqs),requested_rate=stage['rate'],actual_send_rate=actual,source_rate_within_five_percent=abs(actual/stage['rate']-1)<=.05,commit_completion_rate=len(seqs)/((max(cc)-min(ss))/1e9),outcome_completion_rate=len(seqs)/((max(lastevents)-min(ss))/1e9),commit_drain_after_last_send_ms=(max(cc)-max(ss))/1e6,outcome_drain_after_last_send_ms=(max(lastevents)-max(ss))/1e6,max_sent_not_committed_at_send=max(i+1-bisect.bisect_right(sorted_cc,t)for i,t in enumerate(ss)),sent_not_committed_at_last_send=len(seqs)-bisect.bisect_right(sorted_cc,max(ss)),outcome_count=sum(len(events[n])for n in seqs),matching_overlap=overlap([matched[n]for n in seqs]),per_ticker_commit={str(token):distribution([matched[n]['end_unix_ns']-sent[n]['start_unix_ns']for n in seqs if(n-1)%2+1==token])for token in(1,2)},latency=lat,notification=notify,source_schedule_offset=signed([sent[n]['start_unix_ns']-sent[n]['scheduled_unix_ns']for n in seqs])))
  first=end+1
 answer=dict(topology=mode,namespace=cfg['namespace'],image_id=cfg['image_id'],calibration_only=cfg.get('calibration_only',False),stages=result,validation=validation,method=dict(clock=cfg['clock'],percentile='nearest-rank',warmup_excluded=0,commit='client observes successful Process/ProcessCommand transaction return',notification='JetStream server storage, NOT subscriber delivery/PubAck',degraded_shared_lab=True))
 (out/'result.json').write_text(json.dumps(answer,indent=2)+'\n');return answer

def compare(a,b,out):
 if a['topology']!='combined'or b['topology']!='tickers'or a['image_id']!=b['image_id']or len(a['stages'])!=len(b['stages']):raise ValueError('incompatible arms')
 pairs=[]
 for x,y in zip(a['stages'],b['stages']):
  if(x['events'],x['requested_rate'])!=(y['events'],y['requested_rate']):raise ValueError('incompatible workload')
  delta={}
  for kind,keys in [('latency',['send_to_matching_commit','matching_process']),('notification',['send_to_all_outcomes_stored'])]:
   for key in keys:
    delta[key]={p:dict(combined=x[kind][key][p],tickers=y[kind][key][p],difference_ms=y[kind][key][p]-x[kind][key][p],change_percent=(y[kind][key][p]/x[kind][key][p]-1)*100)for p in ['mean_ms','p50_ms','p95_ms','p99_ms']}
  pairs.append(dict(requested_rate=x['requested_rate'],events=x['events'],actual_rate_combined=x['actual_send_rate'],actual_rate_tickers=y['actual_send_rate'],actual_rates_within_five_percent_of_each_other=abs(y['actual_send_rate']/x['actual_send_rate']-1)<=.05,both_rates_within_five_percent=x['source_rate_within_five_percent']and y['source_rate_within_five_percent'],deltas=delta))
 answer=dict(combined=a['namespace'],tickers=b['namespace'],pairs=pairs,negative_change_means_lower_latency=True,degraded_shared_lab=True)
 out.write_text(json.dumps(answer,indent=2)+'\n');return answer

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--combined',required=True,type=Path);p.add_argument('--tickers',required=True,type=Path);p.add_argument('--out',required=True,type=Path);a=p.parse_args();r=compare(analyze(a.combined),analyze(a.tickers),a.out);print(json.dumps(r,indent=2))
if __name__=='__main__':main()
