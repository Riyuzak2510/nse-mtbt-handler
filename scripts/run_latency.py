#!/usr/bin/env python3
"""Bounded serial/parallel latency harness. Live paths require stream-scoped manifest v2.

Never resets PostgreSQL, JetStream or retained evidence. A new sequence-one source requires
fresh state; archived v1 results remain readable by offline comparison tools only.
"""
import argparse
import bisect
import collections
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import socket
import subprocess
import time
import urllib.error
import urllib.request

from parallel_profile import load_parallel, summarize_parallel
from query_profile import summarize_queries, markdown_queries

ROOT = Path(__file__).resolve().parents[1]


def command(args, **kw):
    return subprocess.check_output(args, cwd=ROOT, text=True, **kw).strip()


def sql(query):
    return command(['docker', 'compose', 'exec', '-T', 'postgres', 'psql', '-X', '-U', 'mtbt',
                    '-d', 'mtbt', '-At', '-v', 'ON_ERROR_STOP=1', '-c', query])


def distribution(values):
    if not values or any(v < 0 for v in values):
        raise ValueError('negative or empty latency observations')
    ordered = sorted(values)
    def rank(p): return ordered[max(0, math.ceil(p * len(ordered))-1)] / 1e6
    return dict(count=len(values), min_ms=min(values)/1e6, mean_ms=sum(values)/len(values)/1e6,
                p50_ms=rank(.5), p95_ms=rank(.95), p99_ms=rank(.99), p999_ms=rank(.999), max_ms=max(values)/1e6)


def read_trace(path):
    data, heartbeats = {}, []
    with path.open(newline='') as f:
        for row in csv.DictReader(f):
            item = {k:(v if k == 'type' else int(v)) for k,v in row.items()}
            seq = item['sequence']
            if seq < 0: raise ValueError('negative source sequence')
            if seq == 0: heartbeats.append(item)
            else:
                if seq in data: raise ValueError(f'duplicate sequence {seq}')
                data[seq] = item
    return data, heartbeats


def get_book(token=900001):
    try:
        with urllib.request.urlopen(f'http://127.0.0.1:19002/book?token={token}&depth=100', timeout=2) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        return e.code, json.load(e)


def wait_log(proc, path, text, seconds=30):
    end = time.monotonic()+seconds
    while time.monotonic()<end:
        if proc.poll() is not None: raise RuntimeError(f'process failed before {text}; inspect {path}')
        if path.exists() and text in path.read_text(): return
        time.sleep(.05)
    raise TimeoutError(f'waiting for {text}; inspect {path}')


def stop(proc):
    if proc.poll() is None:
        proc.terminate()
        try: proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill(); proc.wait()
            raise RuntimeError('forced kill: trace is not a valid graceful benchmark')
    if proc.returncode: raise RuntimeError(f'process exited {proc.returncode}')


def metadata():
    with urllib.request.urlopen('http://127.0.0.1:18222/varz',timeout=3) as r: nats=json.load(r)
    paths=sorted([*ROOT.glob('cmd/**/*.go'),*ROOT.glob('internal/**/*.go'),*ROOT.glob('internal/**/*.sql'),
                  *ROOT.glob('scripts/*.py'),ROOT/'go.mod',ROOT/'go.sum',ROOT/'compose.yaml'])
    return dict(platform=platform.platform(), machine=platform.machine(), go=command(['go','version']),
        postgres_version=sql('SELECT version()'), postgres_settings={k:sql('SHOW '+k) for k in
        ('fsync','synchronous_commit','full_page_writes')},
        docker_info=command(['docker','info','--format','{{json .}}']),
        nats={k:nats.get(k) for k in ('version','go','host','port','max_payload','jetstream')},
        source_sha256={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths})


def check_manifest_v2(manifest):
    if 'run_id' in manifest: raise ValueError('legacy run_id manifests are offline evidence only; live setup requires version2')
    if manifest.get('version') != 2: raise ValueError('unsupported manifest version; expected2')
    stream=manifest.get('stream_id')
    if type(stream) is not int or not 1<=stream<=32767: raise ValueError('stream_id must be a positive int16')


def check_database_state(stream_id, sql_query=None, *, require_empty=False):
    q=sql_query or sql
    if q("SELECT count(*) FROM information_schema.columns WHERE table_schema='mtbt' AND column_name='run_id'") != '0':
        raise ValueError('legacy database schema; choose fresh dependencies or an explicit migration, never automatic reset')
    exists=q("SELECT to_regclass('mtbt.feed_state') IS NOT NULL") == 't'
    if not exists: return None
    state=q(f"SELECT status||','||source_sequence||','||broker_sequence FROM mtbt.feed_state WHERE stream_id={int(stream_id)}")
    if require_empty and state: raise ValueError('stream already initialized; new source requires fresh database AND broker state')
    return state or None


def summarize(out, count, warmup, rate, manifest, environment, shards=0, sql_query=None):
    check_manifest_v2(manifest)
    q=sql_query or sql
    stream=manifest['stream_id']; total=count+warmup
    if count<1 or warmup<0: raise ValueError('invalid sample count')
    sent, sh=read_trace(out/'simulator.csv');pub, ph=read_trace(out/'publisher.csv')
    dispatch=None; parallel=None
    if shards:
        consumers,dispatch,ch,details=load_parallel(out,shards,total,read_trace)
        ch=[ch]
    else: consumers,ch=read_trace(out/'consumer.csv')
    wanted=set(range(1,total+1))
    if set(sent)!=wanted or set(pub)!=wanted or set(consumers)!=wanted: raise ValueError('missing data samples')
    if any(len(h)!=1 or h[0]['type']!='Z' for h in (sh,ph,ch)): raise ValueError('missing/extra heartbeat')
    measured=range(warmup+1,total+1); latencies=collections.defaultdict(list);mix=collections.Counter();attempts=0
    for seq in range(1,total+1):
        a,b,c=sent[seq],pub[seq],consumers[seq]
        if not (a['type']==b['type']==c['type'] and a['type'] in ('N','M','X','T')): raise ValueError('type mismatch')
        if c['attempts']!=1 or c['ack_unix_ns']<c['end_unix_ns']: raise ValueError('retry or missing confirmed ack')
        expected_received=dispatch[seq]['received_unix_ns'] if shards else c['received_unix_ns']
        if expected_received!=b['received_unix_ns']: raise ValueError('receive provenance mismatch')
        if not a['start_unix_ns']<=b['received_unix_ns']<=c['delivery_unix_ns']<=c['start_unix_ns']<=c['end_unix_ns']:
            raise ValueError('negative cross-stage interval/clock ordering')
        attempts+=c['attempts']
        if seq not in measured: continue
        mix[a['type']]+=1
        vals=dict(send_to_commit=c['end_unix_ns']-a['start_unix_ns'],
            receive_to_commit=c['end_unix_ns']-b['received_unix_ns'],send_to_receive=b['received_unix_ns']-a['start_unix_ns'],
            receiver_to_delivery=c['delivery_unix_ns']-b['received_unix_ns'],db_process=c['duration_ns'],
            publish_confirm=b['duration_ns'],consumer_ack=c['ack_duration_ns'],send_to_ack=c['ack_unix_ns']-a['start_unix_ns'])
        if shards: vals['send_to_all_required_acks']=max(c['ack_unix_ns'],dispatch[seq]['ack_unix_ns'])-a['start_unix_ns']
        for name,value in vals.items(): latencies[name].append(value)
    if ch[0]['attempts']!=1 or ch[0]['ack_unix_ns']<=0: raise ValueError('heartbeat retry/ack failure')
    if shards: parallel=summarize_parallel(sent,pub,consumers,dispatch,warmup,count,distribution,details)
    where=f'stream_id={stream}'
    actual=json.loads(q("SELECT coalesce(json_agg(json_build_object('id_hex',encode(order_id,'hex'),'token',token,'side',side,'price',price,'quantity',quantity)), '[]'::json) FROM mtbt.active_orders WHERE "+where))
    expected=json.loads((out/'expected.json').read_text())
    if expected.get('stream_id')!=stream or expected['sequence']!=total or 'run_id' in expected: raise ValueError('expected book stream identity mismatch')
    want=[dict(id_hex=int(o['id_bits']).to_bytes(8,'little').hex(),token=o['token'],side=chr(o['side']),price=o['price'],quantity=o['quantity']) for o in expected['orders']]
    if sorted(actual,key=lambda x:x['id_hex'])!=sorted(want,key=lambda x:x['id_hex']): raise ValueError('exact final order oracle mismatch')
    state=q("SELECT status||','||source_sequence||','||broker_sequence FROM mtbt.feed_state WHERE "+where)
    if state!=f'VALID,{total},{total+1}': raise ValueError('database checkpoint/heartbeat mismatch')
    if shards:
        progress=json.loads(q('SELECT json_agg(t ORDER BY shard) FROM (SELECT shard,dispatched,applied FROM mtbt.subscriber_shards WHERE '+where+') t'))
        if len(progress)!=shards or any(x['dispatched']!=x['applied'] for x in progress) or sum(x['applied'] for x in progress)!=total: raise ValueError('shard progress mismatch')
        if q('SELECT count(*) FROM mtbt.subscriber_outbox WHERE '+where)!='0': raise ValueError('outbox not drained')
    measured_sends=[sent[i]['start_unix_ns'] for i in measured]
    measured_commits=sorted(consumers[i]['end_unix_ns'] for i in measured)
    all_commits=sorted(c['end_unix_ns'] for c in consumers.values())
    firstsend,lastsend=min(measured_sends),max(measured_sends)
    finalcommit=max(measured_commits)
    windows=[]
    for low in range(warmup+1,total+1,1000):
        high=min(low+999,total);v=[consumers[i]['end_unix_ns']-sent[i]['start_unix_ns'] for i in range(low,high+1)]
        windows.append(dict(source_sequence_start=low,source_sequence_end=high,**distribution(v)))
    result=dict(version=2,stream_id=stream,mode='parallel instrument shards' if shards else 'serial direct consumer',
        shards=shards,requested_rate_per_second=rate,seed=manifest['seed'],instruments=len(manifest['instruments']),
        actual_send_rate_per_second=(count-1)/((lastsend-firstsend)/1e9),
        completed_events_per_second_send_start_to_last_commit=count/((finalcommit-firstsend)/1e9),
        latency={k:distribution(v) for k,v in latencies.items()},send_to_commit_by_event_type={kind:distribution([
            consumers[i]['end_unix_ns']-sent[i]['start_unix_ns'] for i in measured if sent[i]['type']==kind]) for kind in sorted(mix)},
        send_to_commit_windows=windows,
        diagnostics=dict(generated_data=total,published_data=len(pub),committed_data=len(consumers),measured_data=count,warmup_data_excluded=warmup,
            simulator_heartbeats=len(sh),publisher_heartbeats=len(ph),consumer_heartbeats=len(ch),processing_attempts_total=attempts,
            message_mix_measured=dict(mix),status='VALID',last_sequence=total,final_active_orders=len(actual),exact_order_oracle_match=True,
            max_sent_not_committed_observed_at_send=max(i-bisect.bisect_right(all_commits,sent[i]['start_unix_ns']) for i in measured),
            sent_not_committed_after_last_send=total-bisect.bisect_right(all_commits,lastsend),last_send_to_last_commit_ms=(finalcommit-lastsend)/1e6,
            loss_or_missing_samples=False,redeliveries_or_duplicate_samples=False),
        method=dict(percentile='nearest-rank',send_boundary='immediately before UDP send, after encode',
            commit_boundary='client-observed successful Store.Process return' if not shards else 'client-observed successful Store.ApplyShard return',
            publisher_boundary='synchronous JetStream Publish call through confirmation',queue_boundary='publisher receive timestamp to consumer delivery',
            clock='all Go processes on same host wall clock; duration_ns uses Go monotonic clock',
            durability='PostgreSQL fsync/synchronous_commit/full_page_writes must be on; NATS file stream, one replica',
            query_load='status/depth HTTP polling every250ms during drain',warmup='first events excluded without resetting the book'),environment=environment)
    if parallel:result['parallel']=parallel
    if (out/'queries.csv').exists():
        if shards:raise ValueError('parallel query tracing unsupported')
        profile=summarize_queries(out/'queries.csv',consumers,ch[0],count,warmup,distribution)
        (out/'query-profile.json').write_text(json.dumps(profile,indent=2)+'\n')
        (out/'query-profile.md').write_text(markdown_queries(profile))
    return result


def markdown(result):
    lines=[f"# Local {result['diagnostics']['measured_data']}-event latency result — {result['mode']}",'',
        f"Stream `{result['stream_id']}`; requested {result['requested_rate_per_second']} events/s, actual {result['actual_send_rate_per_second']:.2f} events/s.",'',
        '| Boundary | Mean ms | p50 ms | p95 ms | p99 ms | Maximum ms |','| --- | ---: | ---: | ---: | ---: | ---: |']
    for name,v in result['latency'].items():lines.append(f"| {name} | {v['mean_ms']:.3f} | {v['p50_ms']:.3f} | {v['p95_ms']:.3f} | {v['p99_ms']:.3f} | {v['max_ms']:.3f} |")
    lines+=['','Every data/heartbeat sample, confirmed ack, one-attempt processing and exact final-order oracle validated.','One observation, not a production capacity/SLO or causal comparison.','']
    return '\n'.join(lines)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--out',required=True,type=Path);p.add_argument('--count',type=int,default=10000)
    p.add_argument('--warmup',type=int,default=500);p.add_argument('--rate',type=int,default=100)
    p.add_argument('--seed',type=int,default=42);p.add_argument('--instruments',type=int,default=3)
    p.add_argument('--stream',type=int,default=1,help='native source stream, not a random test identity')
    p.add_argument('--shards',type=int,default=0);p.add_argument('--query-timings',action='store_true')
    a=p.parse_args()
    if not (a.count>=2 and a.warmup>=0 and 1<=a.rate<=1000000 and 1<=a.stream<=32767 and 1<=a.instruments<=10000 and 0<=a.shards<=16):p.error('invalid load parameters')
    if a.shards and a.query_timings:p.error('query timings only support serial mode')
    total=a.count+a.warmup
    if total+10>2000000 or (a.shards and (a.shards+1)*(total+10)>4000000):p.error('trace buffer bound exceeded')
    out=a.out.resolve();out.relative_to(ROOT)
    if out.exists():raise FileExistsError('refusing to overwrite evidence')
    for port,typ in ((19001,socket.SOCK_DGRAM),(19002,socket.SOCK_STREAM)):
        with socket.socket(socket.AF_INET,typ) as s:
            if typ==socket.SOCK_STREAM:s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
            s.bind(('127.0.0.1',port))
    check_database_state(a.stream,require_empty=True)
    (ROOT/'bin').mkdir(exist_ok=True)
    for name in ('simulator','publisher','book-service'):
        command(['go','build','-o',str(ROOT/'bin'/name),'./cmd/'+name])
    out.mkdir(parents=True)
    command(['go','run','./cmd/simulator','-init','-manifest',str(out/'manifest.json'),'-seed',str(a.seed),'-instruments',str(a.instruments),'-stream',str(a.stream)])
    manifest=json.loads((out/'manifest.json').read_text());check_manifest_v2(manifest)
    procs=[];logs=[]
    def start(name,args):
        f=(out/(name+'.log')).open('w');logs.append(f)
        proc=subprocess.Popen([str(ROOT/'bin'/name),*args],cwd=ROOT,stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
        procs.append(proc);return proc
    # Signal only the owned process group and require a graceful trace flush.
    def finish(proc):
        import signal
        if proc.poll() is None:os.killpg(proc.pid,signal.SIGTERM)
        try:code=proc.wait(20)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid,signal.SIGKILL);proc.wait();raise RuntimeError('forced kill invalidates traces')
        if code not in (0,-signal.SIGTERM):raise RuntimeError('service failed; inspect retained logs')
    try:
        base=['-manifest',str(out/'manifest.json'),'-timings-limit',str(total+10)]
        bookargs=['-shards',str(a.shards),'-stale-after','360s']
        bookargs+=['-parallel-timings',str(out/'parallel')] if a.shards else ['-timings',str(out/'consumer.csv')]
        if a.query_timings:bookargs+=['-query-timings',str(out/'queries.csv'),'-query-timings-limit',str(total*8+20)]
        book=start('book-service',base+bookargs);wait_log(book,out/'book-service.log','book service ready')
        pub=start('publisher',base+['-timings',str(out/'publisher.csv')]);wait_log(pub,out/'publisher.log','publisher ready')
        source=start('simulator',base+['-timings',str(out/'simulator.csv'),'-count',str(total),'-rate',str(a.rate),'-expected',str(out/'expected.json')])
        if source.wait(timeout=max(60,total/a.rate+30))!=0:raise RuntimeError('source failed')
        deadline=time.monotonic()+max(60,total/20)
        while time.monotonic()<deadline:
            if book.poll() is not None or pub.poll() is not None:raise RuntimeError('service exited')
            code,state=get_book(manifest['instruments'][0]['token'])
            if state['status']=='INVALID':raise ValueError('feed invalid')
            if code==200 and state['sequence']==total and state.get('pending_events',0)==0:break
            time.sleep(.25)
        else:raise TimeoutError('feed did not drain')
        time.sleep(.3);finish(pub);finish(book)
        for name in ('publisher','book-service'):
            text=(out/(name+'.log')).read_text().lower()
            if 'retry' in text or 'error' in text:raise ValueError('service retries/errors observed')
        result=summarize(out,a.count,a.warmup,a.rate,manifest,metadata(),a.shards)
        (out/'result.json').write_text(json.dumps(result,indent=2)+'\n');(out/'README.md').write_text(markdown(result))
        print(json.dumps(result['latency']['send_to_commit'],indent=2))
    finally:
        for proc in reversed(procs):
            if proc.poll() is None:finish(proc)
        for f in logs:f.close()


if __name__=='__main__':main()
