#!/usr/bin/env python3
"""Offline stage diagnosis for bounded load JSONL; partial windows are explicit."""
import argparse,json
from pathlib import Path

def summarize(directory):
    source=[json.loads(x) for x in (directory/'source.jsonl').read_text().splitlines()]
    records=[json.loads(x) for x in (directory/'latency.jsonl').read_text().splitlines()]
    if not source or not records:raise ValueError('source and timing evidence required')
    profile=source[0]['profile'];low=1;stages=[]
    for item in profile:
        high=low+item['count']-1
        intervals=[r['interval'] for r in records if r['interval']['count'] and
                   r['interval']['first_sequence']>=low and r['interval']['last_sequence']<=high]
        count=sum(r['count'] for r in intervals)
        duration=sum((r['ended_unix_ns']-r['started_unix_ns'])/1e9 for r in intervals)
        stage={'offered_target_eps':item['rate'],'source_first':low,'source_last':high,
               'whole_completion_interval_samples':count,
               'whole_interval_service_eps':count/duration if duration>0 else None,
               'note':'Boundary-crossing completion intervals excluded; not whole-stage percentiles. Failed/pending events excluded; survivor-prefix only.',
               'boundaries':{}}
        for name in ('emitted_to_commit','received_to_delivery','db_process'):
            hs=[r['histograms'][name] for r in intervals]
            stage['boundaries'][name]={
                'mean_ms':sum(h['count']*h['mean_ms'] for h in hs)/sum(h['count'] for h in hs) if sum(h['count'] for h in hs) else None,
                'histogram_valid_count':sum(h['count'] for h in hs),
                'histogram_invalid_count':sum(h['invalid'] for h in hs),
                'last_interval_p99_upper_ms':hs[-1]['p99_ms'] if hs else None,
                'last_interval_source':intervals[-1]['last_sequence'] if intervals else None}
        stages.append(stage);low=high+1
    total=records[-1]['cumulative']
    result={'scope':'observed commits only; do not claim full20M completion or sustainable10k/s from this file',
            'source_last_report':source[-1],'final_timing_kind':records[-1]['kind'],
            'cumulative':total,'stages':stages}
    js=directory/'nats-jsz.json'
    if js.exists():
        d=json.loads(js.read_text());stream=d['account_details'][0]['stream_detail'][0]
        result['broker_observation']={'at':d['now'],'messages':stream['state']['messages'],
             'first_broker_sequence':stream['state']['first_seq'],
             'last_broker_sequence':stream['state']['last_seq'],'pending':stream['consumer_detail'][0]['num_pending'],
             'ack_floor':stream['consumer_detail'][0]['ack_floor']['stream_seq'],
             'redelivered':stream['consumer_detail'][0]['num_redelivered']}
    (directory/'diagnostic-summary.json').write_text(json.dumps(result,indent=2)+'\n')
    return result

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('directory',type=Path);a=p.parse_args()
    result=summarize(a.directory)
    print(json.dumps({k:v for k,v in result.items() if k!='cumulative'},indent=2))
if __name__=='__main__':main()
