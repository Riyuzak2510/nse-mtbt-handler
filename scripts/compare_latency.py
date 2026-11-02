#!/usr/bin/env python3
"""Offline comparison of saved results. Historical v1 identities remain evidence only.

Live v2 output uses native stream_id. No traffic, SQL or deployment is executed.
Matched-workload/build/durability/correctness gates are identical for both formats.
"""
import argparse
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
SCOPE='Descriptive local architecture comparison, one run per condition; no significance/isolated-worker causality or capacity claim.'


def compare(serial,parallel):
    if serial['shards']!=0 or parallel['shards']<1:raise ValueError('requires direct serial baseline (shards=0) and routed parallel result')
    for key in ('requested_rate_per_second','seed','instruments'):
        if serial[key]!=parallel[key]:raise ValueError('noncomparable workload: '+key)
    for key in ('measured_data','warmup_data_excluded','message_mix_measured','final_active_orders'):
        if serial['diagnostics'][key]!=parallel['diagnostics'][key]:raise ValueError('noncomparable observations: '+key)
    for r in (serial,parallel):
        d=r['diagnostics']
        if not d['exact_order_oracle_match'] or d['status']!='VALID' or d['loss_or_missing_samples'] or d['redeliveries_or_duplicate_samples'] or d['processing_attempts_total']!=d['generated_data']:
            raise ValueError('failed correctness gates')
        if r['environment'].get('postgres_settings')!=dict(fsync='on',synchronous_commit='on',full_page_writes='on'):
            raise ValueError('durability differs from fixed plan')
    if serial['environment']!=parallel['environment']:raise ValueError('different build/environment; do not label this matched comparison')
    legacy='run_id' in serial
    if legacy!=('run_id' in parallel):raise ValueError('cannot mix legacy and stream-scoped observations')
    if not legacy and any(r.get('version')!=2 or type(r.get('stream_id')) is not int or not 1<=r['stream_id']<=32767 for r in (serial,parallel)):
        raise ValueError('v2 results require native stream identity')
    metrics={}
    for name in ('send_to_commit','send_to_ack','send_to_all_required_acks','db_process'):
        # Serial has one required ack; parallel has raw+shard confirmations.
        skey='send_to_ack' if name=='send_to_all_required_acks' else name
        metrics[name]={}
        for stat in ('mean_ms','p50_ms','p95_ms','p99_ms','max_ms'):
            a,b=serial['latency'][skey][stat],parallel['latency'][name][stat]
            metrics[name][stat]=dict(serial=a,parallel=b,absolute_delta_ms=b-a,ratio_parallel_over_serial=b/a if a else None,relative_change_percent=(b/a-1)*100 if a else None)
    r=dict(requested_rate_per_second=serial['requested_rate_per_second'])
    if legacy:r.update(serial_run_id=serial['run_id'],parallel_run_id=parallel['run_id'])
    else:r.update(serial_stream_id=serial['stream_id'],parallel_stream_id=parallel['stream_id'])
    r.update(measured_events_each=serial['diagnostics']['measured_data'],parallel_shards=parallel['shards'],metrics=metrics,validated_comparable_workload_build_environment=True,scope=SCOPE)
    return r


def markdown(r):
    identity=(f"Serial `{r['serial_run_id']}`; parallel `{r['parallel_run_id']}`." if 'serial_run_id' in r else
              f"Serial stream `{r['serial_stream_id']}`; parallel stream `{r['parallel_stream_id']}`.")
    lines=[f"# Serial versus {r['parallel_shards']} routed subscribers at {r['requested_rate_per_second']} requested events/s",'',
        f"{identity} {r['measured_events_each']} measured events each.",'',
        '| Metric | Serial ms | Parallel ms | Delta ms | Relative change |','| --- | ---: | ---: | ---: | ---: |']
    for name,stats in r['metrics'].items():
        for stat,v in stats.items():
            pct=f"{v['relative_change_percent']:+.2f}%" if v['relative_change_percent'] is not None else 'undefined'
            lines.append(f"| {name} {stat} | {v['serial']:.3f} | {v['parallel']:.3f} | {v['absolute_delta_ms']:+.3f} | {pct} |")
    return '\n'.join(lines+['',r['scope'],''])


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('serial','parallel','out'):p.add_argument('--'+name,type=Path,required=True)
    a=p.parse_args()
    for path in (a.serial,a.parallel,a.out):path.resolve().relative_to(ROOT)
    md=a.out.with_suffix('.md')
    if a.out.exists() or md.exists():raise FileExistsError('refusing overwrite of historical evidence')
    r=compare(json.loads(a.serial.read_text()),json.loads(a.parallel.read_text()));a.out.parent.mkdir(parents=True,exist_ok=True)
    with a.out.open('x') as f:json.dump(r,f,indent=2);f.write('\n')
    with md.open('x') as f:f.write(markdown(r))
    print(markdown(r))


if __name__=='__main__':main()
