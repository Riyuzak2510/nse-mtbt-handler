#!/usr/bin/env python3
"""Isolated real-command durable matching test + exact oracle + kill/restart.

No existing database/broker/cluster is modified. All temporary containers use a unique prefix.
"""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

from run_latency import distribution
ROOT = Path(__file__).resolve().parents[1]


def command(args, **kw):
    return subprocess.check_output(args, cwd=ROOT, text=True, **kw).strip()


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def packet(seq, oid, side, quantity):
    # Approved local profile:38bytes TOTAL (8byte header+30byte order body), price10000.
    body = struct.pack('<cqdic ii'.replace(' ', ''), b'N', 1, float(oid), 1, side.encode(), 10000, quantity)
    return struct.pack('<Hhi', 8+len(body), 1, seq)+body


def heartbeat(sequence):
    body = struct.pack('<ci', b'Z', sequence)  # Z has no timestamp:13bytes total
    return struct.pack('<Hhi', 8+len(body), 1, 0)+body


def get(port, path):
    try:
        with urllib.request.urlopen(f'http://127.0.0.1:{port}'+path, timeout=2) as r:
            raw = r.read()
            return r.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        return e.code, json.load(e)


def wait(check, seconds=60):
    end = time.monotonic()+seconds
    while time.monotonic()<end:
        try:
            value = check()
            if value: return value
        except (OSError, ValueError, urllib.error.URLError):
            pass
        time.sleep(.05)
    raise RuntimeError('condition timed out')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--events', type=int, default=10000)
    p.add_argument('--rate', type=int, default=1000)
    args = p.parse_args()
    if args.events<10 or args.events>100000 or args.rate<1: p.error('events10..100000, rate>0 required')
    out=args.out.resolve()
    if not out.is_relative_to(ROOT):p.error('output must be inside workspace')
    out.mkdir(parents=True,exist_ok=False)
    prefix='tmp_rovo_durable_'+os.urandom(5).hex()
    net=prefix+'_net';pg=prefix+'_pg';nats=prefix+'_nats';publisher=prefix+'_publisher';matcher=prefix+'_matcher'
    pgimage='postgres:17.7-alpine';natsimage='nats:2.11.8-alpine'
    password='test-'+os.urandom(10).hex()
    api=free_port();health=free_port();udp=free_port()
    containers=[];image=prefix+':test'
    manifest=dict(version=2,profile='cm-mtbt-le64-v1',stream_id=1,seed=42,baseline='empty',created_at='2026-10-21T00:00:00Z',
                  instruments=[dict(token=1,symbol='SIM1',tick_size=5,base_price=10000)])
    (out/'manifest.json').write_text(json.dumps(manifest)+'\n')
    command(['docker','network','create',net])
    try:
        command(['docker','run','-d','--name',pg,'--network',net,'--network-alias','pg',
                 '-e','POSTGRES_USER=mtbt','-e','POSTGRES_PASSWORD='+password,'-e','POSTGRES_DB=mtbt',pgimage]);containers.append(pg)
        command(['docker','run','-d','--name',nats,'--network',net,'--network-alias','nats',natsimage,'-js']);containers.append(nats)
        wait(lambda: subprocess.run(['docker','exec',pg,'pg_isready','-U','mtbt'],capture_output=True).returncode==0)
        arch=command(['docker','info','--format','{{.Architecture}}']);arch='arm64' if arch in ('aarch64','arm64') else 'amd64'
        with tempfile.TemporaryDirectory(prefix='tmp_rovo_durable_build_',dir=ROOT) as tmp:
            tmp=Path(tmp)
            for name in ('publisher','matching-service'):
                subprocess.run(['go','build','-trimpath','-o',str(tmp/name),'./cmd/'+name],cwd=ROOT,
                               env=dict(os.environ,GOOS='linux',GOARCH=arch,CGO_ENABLED='0'),check=True)
            (tmp/'Dockerfile').write_text('FROM scratch\nCOPY publisher matching-service /app/\nUSER 10001:10001\nWORKDIR /app\n')
            command(['docker','build','-q','-t',image,str(tmp)])
        def start_matcher(trace):
            command(['docker','run','-d','--name',matcher,'--stop-timeout','60','--network',net,'--memory','512m','--memory-swap','512m',
                     '--cap-drop','ALL','--security-opt','no-new-privileges','--read-only',
                     '-p',f'127.0.0.1:{api}:19004','-e','GOMEMLIMIT=384MiB','-e','DATABASE_URL='+f'postgres://mtbt:{password}@pg:5432/mtbt?sslmode=disable',
                     '-e','NATS_URL=nats://nats:4222','-v',f'{out}:/evidence',image,'/app/matching-service',
                     '-mode=durable','-manifest=/evidence/manifest.json','-http=0.0.0.0:19004','-allow-non-loopback',
                     '-stale-after=120s',f'-timings=/evidence/{trace}','-timings-limit=110000'])
            if matcher not in containers:containers.append(matcher)
            wait(lambda:get(api,'/healthz')[0]==200)
        start_matcher('first.csv')
        command(['docker','run','-d','--name',publisher,'--network',net,'-p',f'127.0.0.1:{udp}:19001/udp',
                 '-p',f'127.0.0.1:{health}:19003','-e','NATS_URL=nats://nats:4222','-v',f'{out}:/evidence',image,'/app/publisher',
                 '-manifest=/evidence/manifest.json','-listen=0.0.0.0:19001','-allow-non-loopback','-health-http=0.0.0.0:19003'])
        containers.append(publisher);wait(lambda:get(health,'/readyz')[0]==200)
        sock=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        seq=0;sent={};schedule=time.monotonic()
        def send(oid,side,qty):
            nonlocal seq,schedule
            seq+=1;schedule+=1/args.rate;delay=schedule-time.monotonic()
            if delay>0:time.sleep(delay)
            sent[seq]=time.time_ns();sock.sendto(packet(seq,oid,side,qty),('127.0.0.1',udp))
        def until(position):
            return wait(lambda:(lambda v:v if v[1]['source_sequence']==position else None)(get(api,'/book?token=1')),90)
        # Crash with a genuinely partially filled order persisted.
        send(1,'B',100);send(2,'S',30);until(2)
        bits=lambda n:str(struct.unpack('<Q',struct.pack('<d',float(n)))[0])
        code,buy=get(api,'/orders?id='+bits(1));assert code==200 and buy['remaining_quantity']==70 and buy['filled_quantity']==30,buy
        (out/'before-crash.json').write_text(json.dumps(buy,indent=2)+'\n')
        command(['docker','kill',matcher]);inspection=json.loads(command(['docker','inspect',matcher]))[0]
        (out/'killed-container.json').write_text(json.dumps(inspection,indent=2)+'\n')
        (out/'killed.log').write_text(command(['docker','logs',matcher]))
        command(['docker','rm',matcher]);start_matcher('restarted.csv')
        code,restored=get(api,'/orders?id='+bits(1));assert code==200 and restored==buy,(restored,buy)
        schedule=time.monotonic()  # restart downtime must not become an unpaced UDP burst
        send(3,'S',70)
        # Fully filled triplets; one trailing noncrossing bid if count isn't a multiple of3.
        while seq+3<=args.events:
            send(seq+1,'B',100);send(seq+1,'S',30);send(seq+1,'S',70)
        while seq<args.events:send(seq+1,'B',100)
        sock.sendto(heartbeat(seq),('127.0.0.1',udp));sock.close()
        code,final=until(seq);assert code==200,final
        _,final = wait(lambda:(lambda response: response if response[1]['broker_sequence']==seq+1 else None)(get(api,'/book?token=1')))
        command(['docker','stop','-t','60',matcher]);command(['docker','stop','-t','30',publisher])
        inspection=json.loads(command(['docker','inspect',matcher]))[0]
        (out/'matcher-container.json').write_text(json.dumps(inspection,indent=2)+'\n')
        assert inspection['State']['ExitCode']==0 and not inspection['State']['OOMKilled'],inspection['State']
        (out/'matcher.log').write_text(command(['docker','logs',matcher]))
        query="SELECT json_agg(row_to_json(t)) FROM (SELECT encode(order_id,'hex') id,accepted_quantity,remaining_quantity,filled_quantity,cancelled_quantity,status FROM mtbt.orders WHERE stream_id=1 ORDER BY order_id) t"
        rows=json.loads(command(['docker','exec',pg,'psql','-X','-U','mtbt','-d','mtbt','-At','-v','ON_ERROR_STOP=1','-c',query]))
        actual={int.from_bytes(bytes.fromhex(row['id']),'little'):row for row in rows}
        for i in range(1,args.events+1):
            row=actual[int(bits(i))]
            if i<=args.events//3*3:
                filled=100 if i%3==1 else (30 if i%3==2 else 70);remaining=0;state='FILLED'
            else:filled=0;remaining=100;state='OPEN'
            assert row['remaining_quantity']==remaining and row['filled_quantity']==filled and row['status']==state,(i,row)
            assert row['accepted_quantity']==remaining+filled+row['cancelled_quantity'],row
        assert len(rows)==args.events
        samples=[]
        with (out/'restarted.csv').open() as f:
            for row in csv.DictReader(f):
                seqno=int(row['sequence'])
                if seqno>=3:
                    assert int(row['ack_unix_ns'])>0 and int(row['attempts'])==1,row
                    samples.append(row)
        assert len(samples)==args.events-2,len(samples)
        assert {int(r['sequence']) for r in samples}==set(range(3,args.events+1)), 'missing/duplicate timing identity'
        for r in samples:
            assert int(r['end_unix_ns'])>=int(r['start_unix_ns']) and int(r['ack_unix_ns'])>=int(r['end_unix_ns']),r
        result=dict(version=1,stream_id=1,events=args.events,rate=args.rate,exact_order_oracle=True,
                    partial_fill_crash_restore=True,checkpoint=final,final_order_rows=len(rows),
                    measured_after_restart=len(samples),
                    sender_to_db_commit=distribution([int(r['end_unix_ns'])-sent[int(r['sequence'])] for r in samples]),
                    database_processing=distribution([int(r['duration_ns']) for r in samples]),
                    sender_to_confirmed_ack=distribution([int(r['ack_unix_ns'])-sent[int(r['sequence'])] for r in samples]),
                    note='Linux Docker service; host UDP sender wall-clock timestamps. First2events crash phase excluded. Not comparable to prior host/K8s workloads.')
        (out/'orders.json').write_text(json.dumps(rows,indent=2)+'\n')
        (out/'sent.json').write_text(json.dumps(sent)+'\n')
        (out/'result.json').write_text(json.dumps(result,indent=2)+'\n')
        settings={key:command(['docker','exec',pg,'psql','-X','-U','mtbt','-d','mtbt','-At','-c','SHOW '+key]) for key in ('fsync','synchronous_commit','full_page_writes')}
        assert all(value=='on' for value in settings.values()),settings
        meta={'go':command(['go','version']),'postgres_durability':settings,'docker_info':command(['docker','info','--format','{{.OSType}} {{.Architecture}} CPUs={{.NCPU}} Memory={{.MemTotal}}']),'image_id':command(['docker','image','inspect',image,'--format','{{.Id}}']),
              'source_sha256':{str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted([*ROOT.glob('internal/**/*.go'),*ROOT.glob('internal/**/*.sql'),*ROOT.glob('cmd/matching-service/*.go')])}}
        (out/'environment.json').write_text(json.dumps(meta,indent=2)+'\n');print(json.dumps(result,indent=2))
    finally:
        for name in containers:
            # Retain diagnostic state even when a correctness gate fails.
            log=subprocess.run(['docker','logs',name],cwd=ROOT,text=True,capture_output=True)
            (out/(name.split(prefix+'_')[-1]+'.cleanup.log')).write_text(log.stdout+log.stderr)
            state=subprocess.run(['docker','inspect',name],cwd=ROOT,text=True,capture_output=True)
            (out/(name.split(prefix+'_')[-1]+'.cleanup.json')).write_text(state.stdout)
        for name in containers:
            subprocess.run(['docker','rm','-f',name],cwd=ROOT,capture_output=True,check=False)
        subprocess.run(['docker','network','rm',net],cwd=ROOT,capture_output=True,check=False)
        subprocess.run(['docker','image','rm',image],cwd=ROOT,capture_output=True,check=False)

if __name__=='__main__':main()
