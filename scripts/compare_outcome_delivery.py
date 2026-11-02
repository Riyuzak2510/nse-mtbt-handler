#!/usr/bin/env python3
"""Compare fully validated same-image poll/notify outcome benchmark arms offline."""
import argparse,json
from pathlib import Path
from analyze_outcome_benchmark import analyze


def delta(old,new):
 return dict(poll=old,notify=new,absolute_ms=new-old,percent=(new/old-1)*100 if old else None)


def compare(poll,notify,out):
 if out.exists():raise ValueError('refusing to overwrite comparison evidence')
 settings=[json.loads((p/'settings.json').read_text())for p in(poll,notify)]
 a,b=settings
 if not a.get('enabled')or not b.get('enabled')or(a.get('delivery_mode'),b.get('delivery_mode'))!=('poll','notify'):raise ValueError('enabled poll and notify arms required')
 for key in('image_id','stages','raw_stream_max_bytes','outcome_stream_max_bytes','warmup_excluded','workload','source_sha256'):
  if key not in a or a[key]!=b.get(key):raise ValueError('different comparison settings: '+key)
 # Namespace/credential references differ, but workload placement, limits,
 # environment, storage and application arguments must otherwise match.
 def comparable_resources(p):
  resources=json.loads((p/'resources.json').read_text())
  ns=json.loads((p/'settings.json').read_text())['namespace']
  text=json.dumps(resources,sort_keys=True).replace(ns,'BENCH_NAMESPACE')
  text=text.replace('-delivery-mode=poll','-delivery-mode=MODE').replace('-delivery-mode=notify','-delivery-mode=MODE')
  return json.loads(text)
 if comparable_resources(poll)!=comparable_resources(notify):raise ValueError('resource specifications differ beyond delivery mode/namespace')
 results=[analyze(p)for p in(poll,notify)];pairs=[]
 for x,y in zip(results[0]['stages'],results[1]['stages']):
  changes={}
  for boundary in('send_to_commit','db_process','receive_to_delivery'):
   changes[boundary]={q:delta(x['latency'][boundary][q],y['latency'][boundary][q])for q in('mean_ms','p50_ms','p95_ms','p99_ms')}
  for boundary in('send_to_all_outcomes_stored','transaction_start_to_outcome_stored'):
   changes[boundary]={q:delta(x['outcomes']['latency'][boundary][q],y['outcomes']['latency'][boundary][q])for q in('mean_ms','p50_ms','p95_ms','p99_ms')}
  boundary='client_commit_observation_to_outcome_stored'
  changes[boundary]={q:delta(x['outcomes'][boundary][q],y['outcomes'][boundary][q])for q in('mean_ms','p50_ms','p95_ms','p99_ms')}
  pairs.append(dict(requested_rate=x['offered_rate'],actual_rate_poll=x['actual_send_rate'],actual_rate_notify=y['actual_send_rate'],both_rate_targets_met=x['source_rate_within_five_percent']and y['source_rate_within_five_percent'],events=x['events'],changes=changes))
 result=dict(poll=str(poll),notify=str(notify),image_id=a['image_id'],same_resources_except_mode_and_namespace=True,comparison=pairs,limitations='One sequential pair on degraded shared local kind VM, not randomized/repeated run-level confidence. Both matchers emit pg_notify; compares relay wake policy, not total incremental notification SQL cost. Actual rates may differ; high-load queueing is not a controlled capacity/SLA result. Broker storage endpoint is not external subscriber completion.')
 out.write_text(json.dumps(result,indent=2)+'\n');return result


def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--poll',required=True,type=Path);p.add_argument('--notify',required=True,type=Path);p.add_argument('--out',required=True,type=Path);a=p.parse_args()
 print(json.dumps(compare(a.poll,a.notify,a.out),indent=2))
if __name__=='__main__':main()
