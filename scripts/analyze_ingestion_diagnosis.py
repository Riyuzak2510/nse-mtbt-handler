#!/usr/bin/env python3
"""Reconcile a finite diagnostic source with receiver drops and retained broker."""
import argparse,json
from pathlib import Path


def analyze(out):
    ingest=[json.loads(x)for x in(out/'ingestion.jsonl').read_text().splitlines()]
    src=[json.loads(x)for x in(out/'source.jsonl').read_text().splitlines()]
    r=ingest[-1];s=src[-1]
    js=json.loads((out/'nats-jsz.json').read_text());stream=js['account_details'][0]['stream_detail'][0]
    a,b=r['start'],r['current'];socket=a['socket'];end=b['socket']
    same_socket=socket['found']and end['found']and socket['inode']==end['inode']
    drop_delta=end['drops']-socket['drops'] if same_socket else None
    snmp_delta={k:b['udp'][k]-a['udp'].get(k,0) for k in ('RcvbufErrors','InErrors','SndbufErrors','InCsumErrors','MemErrors')if k in b['udp']}
    cpu_delta={k:b['cgroup_cpu'][k]-a['cgroup_cpu'].get(k,0) for k in b['cgroup_cpu']}
    errors=[]
    if s['kind']!='complete':errors.append('source incomplete')
    if r['kind']!='final':errors.append('no final ingestion snapshot')
    if not same_socket:errors.append('socket identity not stable')
    if a.get('errors')or b.get('errors'):errors.append('OS observation errors')
    if r['publish_errors']or r['malformed']or r['reorders']:errors.append('publish/malformed/reorder observed')
    expected=s['sent']+1 if s['kind']=='complete' else None
    if expected is not None and r['published']+(drop_delta or 0)!=expected:errors.append('source/broker/socket-drop count not reconciled')
    if stream['state']['messages']!=r['published']or stream['state']['first_seq']!=1:errors.append('retention/admission differs')
    data_observed=r['emitted_to_receive']['count']+r['emitted_to_receive']['invalid']
    heartbeat_observed=r['received']-data_observed if r['malformed']==0 else None
    trailing_data=s['sent']-r['last_sequence'] if s['kind']=='complete' and not r['reorders'] else None
    trailing_heartbeat=1-heartbeat_observed if heartbeat_observed in(0,1)and s['kind']=='complete'else None
    trailing_loss=trailing_data+trailing_heartbeat if trailing_data is not None and trailing_heartbeat is not None else None
    if trailing_loss is None or r['missing_sequence_numbers']+trailing_loss!=drop_delta:
        errors.append('internal holes plus trailing data/heartbeat do not match socket drops')
    if drop_delta!=snmp_delta.get('RcvbufErrors'):errors.append('socket and namespace UDP errors differ')
    total_seconds=r['elapsed_seconds'];cpuload=cpu_delta.get('usage_usec',0)/(1e6*total_seconds) if total_seconds else None
    phase_first=1;phases=[]
    for stage in s['profile']:
        high=phase_first+stage['count']-1
        eligible=[x for x in ingest if phase_first<=x['last_sequence']<=high and x['kind']=='interval'and x['interval_published']]
        phases.append({'target_eps':stage['rate'],'first':phase_first,'last':high,
                       'snapshot_count':len(eligible),'last_snapshot':eligible[-1] if eligible else None})
        phase_first=high+1
    result={'mode':json.loads((out/'diagnostic-settings.json').read_text())['mode'],
            'reconciliation_pass':not errors,'reconciliation_errors':errors,
            'source_events':s['sent'],'source_including_heartbeat':expected,
            'source_seconds':s['elapsed_seconds'],'source_max_schedule_lag_ms':s['max_schedule_lag_ns']/1e6,
            'received':r['received'],'published':r['published'],'socket_drop_delta':drop_delta,
            'missing_sequences':r['missing_sequence_numbers'],'first_gaps':r['first_gaps'],
            'last_data_sequence':r['last_sequence'],'inferred_observed_heartbeats':heartbeat_observed,
            'trailing_data_loss':trailing_data,'trailing_heartbeat_loss':trailing_heartbeat,
            'inference_scope':'heartbeat inferred as received minus timestamped data packets; valid only with zero malformed and zero reorder, timestamp histogram covers every decoded data packet',
            'snmp_delta':snmp_delta,'actual_so_rcvbuf_bytes':r['socket_read_buffer_bytes'],
            'publisher_cpu_delta':cpu_delta,'publisher_mean_cpu_cores_over_observation':cpuload,
            'publish_client_latency_ms':r['publish'],'emitted_to_receive_ms':r['emitted_to_receive'],
            'broker_messages':stream['state']['messages'],
            'broker_consumer_pending':stream['consumer_detail'][0]['num_pending'],
            'phase_completion_snapshots':phases,
            'clock_and_overhead':'sameVM wallclock crossprocess,monotonic publish; diagnostics overhead and differentcache/systemactivity notcontrolled'}
    sql=out/'sql-pre-stop-clean.json'
    if sql.exists():
        ops=json.loads(sql.read_text())['operations']
        total=sum(v['count']*v['mean_ms'] for k,v in ops.items()if k!='other')
        result['sql_clean_cumulative']={k:dict(v,total_client_ms=v['count']*v['mean_ms'],
            share_of_labelled_client_time_percent=100*v['count']*v['mean_ms']/total if total else 0)for k,v in ops.items()}
        result['sql_scope']='all pool queries inclstartup/probes; clean cumulative snapshot before stop, not exact per-event/server lock wait'
    (out/'diagnostic-analysis.json').write_text(json.dumps(result,indent=2)+'\n')
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('out',type=Path);a=p.parse_args()
    d=analyze(a.out);print(json.dumps({k:v for k,v in d.items()if k not in ('phase_completion_snapshots','sql_clean_cumulative')},indent=2))
if __name__=='__main__':main()
