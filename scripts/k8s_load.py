#!/usr/bin/env python3
"""Isolated local kind load test. No existing namespace or source state is reset."""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import re
import secrets
import subprocess
import time

import k8s_matching_lab as lab

ROOT = lab.ROOT
NAMESPACE = 'nse-mtbt-load'
IMAGE = 'nse-mtbt-handler:load-20261022-r2'
PROFILE = '5000@100,10000@1000,50000@5000,19935000@10000'
RETAINED = ('nse-mtbt-lab', 'nse-mtbt-matching')


def validate_namespace(name):
    if not re.fullmatch(r'nse-mtbt-load(?:-[a-z0-9]+)*', name) or len(name) > 63:
        raise ValueError('only isolated nse-mtbt-load[-suffix] namespaces allowed')
    return name


def profile_stages(text):
    result = []
    for part in text.split(','):
        m = re.fullmatch(r'([1-9][0-9]*)@([1-9][0-9]*)', part)
        if not m:
            raise ValueError('profile requires positive count@rate segments')
        count, rate = map(int, m.groups())
        if rate > 1_000_000:
            raise ValueError('source rate exceeds generator bounds')
        result.append({'count': count, 'rate': rate})
    if sum(x['count'] for x in result) > 2_147_483_647:
        raise ValueError('profile exceeds native sequence range')
    return result


def kube(*args, namespace=None, stdin=None, timeout=60):
    ns = NAMESPACE if namespace is None else namespace
    if ns not in RETAINED:
        validate_namespace(ns)
    return lab.command(['kubectl', '--kubeconfig', str(lab.KUBECONFIG), '--context',
                        lab.CONTEXT, '-n', ns, *args], stdin, timeout)


def create(obj):
    return kube('create', '-f', '-', stdin=json.dumps(obj))


def save(out, name, obj):
    (out / name).write_text(json.dumps(obj, indent=2) + '\n')


def sql(query):
    return kube('exec', 'postgres-0', '--', 'psql', '-X', '-U', 'mtbt', '-d', 'mtbt',
                '-At', '-v', 'ON_ERROR_STOP=1', '-c', query).strip()


def checkpoint():
    raw = sql('SELECT owner_mode,status,source_sequence,broker_sequence,reason '
              'FROM mtbt.feed_state WHERE stream_id=1')
    mode, status, source, broker, reason = raw.split('|', 4)
    return dict(mode=mode, status=status, source=int(source), broker=int(broker), reason=reason)


def snapshots():
    result = {}
    for ns in RETAINED:
        result[ns] = {}
        for kind in ('deployments', 'statefulsets', 'persistentvolumeclaims'):
            items = json.loads(kube('get', kind, '-o', 'json', namespace=ns))['items']
            result[ns][kind] = [dict(name=o['metadata']['name'], uid=o['metadata']['uid'],
                                     generation=o['metadata'].get('generation'), spec=o['spec'])
                                for o in items]
    return result


def pod(app):
    items = json.loads(kube('get', 'pods', '-l', 'app=' + app, '-o', 'json'))['items']
    return items[0] if len(items) == 1 else None


def restarts(item):
    return sum(x.get('restartCount', 0) for x in (item or {}).get('status', {}).get('containerStatuses', []))


def monitor_object(name='load-monitor'):
    return {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': name}, 'spec': {
        'nodeSelector': {'mtbt.lane': 'b'}, 'automountServiceAccountToken': False,
        'restartPolicy': 'Never', 'securityContext': {'runAsNonRoot': True, 'runAsUser': 10001,
                                                      'runAsGroup': 10001, 'fsGroup': 10001},
        'containers': [{'name': 'monitor', 'image': 'nats:2.11.8-alpine',
                        'command': ['sleep', '7200'],
                        'resources': {'requests': {'cpu': '10m', 'memory': '16Mi'},
                                      'limits': {'cpu': '100m', 'memory': '64Mi'}},
                        'securityContext': {'allowPrivilegeEscalation': False,
                                            'readOnlyRootFilesystem': True, 'capabilities': {'drop': ['ALL']}},
                        'volumeMounts': [{'name': 'metrics', 'mountPath': '/metrics', 'readOnly': True},
                                         {'name': 'source', 'mountPath': '/source', 'readOnly': True}]}],
        'volumes': [{'name': 'metrics', 'persistentVolumeClaim': {'claimName': 'matching-evidence'}},
                    {'name': 'source', 'persistentVolumeClaim': {'claimName': 'source-evidence'}}]}}


def ensure_monitor():
    if not kube('get', 'pod', 'load-monitor', '--ignore-not-found').strip():
        create(monitor_object())
    kube('wait', '--for=condition=Ready', 'pod/load-monitor', '--timeout=120s', timeout=140)


def deploy(out, build):
    lab.guard()
    if kube('get', 'namespace', NAMESPACE, '--ignore-not-found').strip():
        raise ValueError('namespace exists: refusing reset/overwrite')
    out.mkdir(parents=True, exist_ok=False)
    save(out, 'retained-before.json', snapshots())
    if build:
        with (out / 'image-build.log').open('w') as f:
            subprocess.run(['docker', 'build', '-t', IMAGE, '.'], cwd=ROOT, stdout=f,
                           stderr=subprocess.STDOUT, check=True)
    (out / 'image-load.log').write_text(lab.command(
        ['kind', 'load', 'docker-image', IMAGE, '--name', lab.CLUSTER], timeout=600))
    create({'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': NAMESPACE,
            'labels': {'app.kubernetes.io/part-of': 'nse-mtbt-load'}}})
    pgpw, npw = secrets.token_hex(24), secrets.token_hex(24)
    create({'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': 'mtbt-credentials'},
            'stringData': {'POSTGRES_PASSWORD': pgpw,
                           'DATABASE_URL': f'postgres://mtbt:{pgpw}@postgres:5432/mtbt?sslmode=disable',
                           'NATS_URL': f'nats://mtbt:{npw}@nats:4222',
                           'nats.conf': 'port:4222\nhttp:8222\njetstream { store_dir:"/data/jetstream", '
                                        'max_mem_store:64MB, max_file_store:12GB }\n'
                                        f'authorization {{ user:mtbt, password:"{npw}" }}\n'}})
    manifest = dict(version=2, profile='cm-mtbt-le64-v1', stream_id=1, seed=42, baseline='empty',
                    created_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    instruments=[dict(token=n, symbol=f'SIM{n}', tick_size=5,
                                      base_price=10000 + 5*n) for n in range(1, 101)])
    create({'apiVersion': 'v1', 'kind': 'ConfigMap', 'immutable': True,
            'metadata': {'name': 'mtbt-source'}, 'data': {'manifest.json': json.dumps(manifest)}})
    save(out, 'manifest.json', manifest)
    install(out)


def install(out):
    if json.loads(kube('get', 'deployments,statefulsets', '-o', 'json'))['items']:
        raise ValueError('install refuses existing deployments/statefulsets')
    config = json.loads(kube('get', 'configmap', 'mtbt-source', '-o', 'json'))
    if json.loads(config['data']['manifest.json']) != json.loads((out/'manifest.json').read_text()):
        raise ValueError('manifest differs from staged source')
    rendered = lab.command(['kubectl', 'kustomize', 'deploy/matching']).replace(
        'namespace: nse-mtbt-matching', 'namespace: ' + NAMESPACE)
    resources = lab.resource_list(kube('create', '--dry-run=client', '-f', '-', '-o', 'json', stdin=rendered))
    for obj in resources['items']:
        obj['metadata']['namespace'] = NAMESPACE
        if obj['kind'] == 'PersistentVolumeClaim':
            size = {'postgres-data': '20Gi', 'nats-data': '12Gi',
                    'matching-evidence': '256Mi', 'source-evidence': '256Mi'}.get(obj['metadata']['name'])
            if size:
                obj['spec']['resources']['requests']['storage'] = size
        if obj['kind'] in ('Deployment', 'StatefulSet'):
            template = obj['spec']['template']['spec']
            for container in template['containers']:
                if container['image'] == 'nse-mtbt-handler:lab':
                    container['image'] = IMAGE
            if obj['kind'] == 'Deployment':
                obj['spec']['replicas'] = 0  # stage dependencies first; book-service remains zero
                if obj['metadata']['name'] != 'book-service':
                    container = template['containers'][0]
                    container.setdefault('env', []).append({'name': 'MTBT_STREAM_MAX_BYTES', 'value': str(8 << 30)})
                    if obj['metadata']['name'] == 'matching-service':
                        container['env'].append({'name': 'MTBT_LOAD_METRICS', 'value': '/evidence/latency.jsonl'})
    save(out, 'resources.json', resources)
    kube('apply', '--dry-run=server', '-f', '-', stdin=json.dumps(resources))
    kube('apply', '-f', '-', stdin=json.dumps(resources))
    for name in ('postgres', 'nats'):
        kube('rollout', 'status', 'statefulset/' + name, '--timeout=240s', timeout=270)
    kube('scale', 'deployment/publisher', '--replicas=1')
    kube('rollout', 'status', 'deployment/publisher', '--timeout=120s', timeout=150)
    kube('scale', 'deployment/matching-service', '--replicas=1')
    lab.wait(lambda: 'restored through broker=0' in kube('logs', 'deployment/matching-service', '--tail=20'),
             'matching process startup')
    ensure_monitor()
    source_files = list((ROOT/'cmd/load-source').glob('*.go')) + [ROOT/'cmd/matching-service/durable.go',
                   ROOT/'internal/loadmetrics/recorder.go', ROOT/'internal/loadmetrics/histogram.go']
    save(out, 'settings.json', dict(namespace=NAMESPACE, image=IMAGE,
         image_id=lab.command(['docker','image','inspect',IMAGE,'--format','{{.Id}}']).strip(),
         stream_max_bytes=8 << 30, clock='same Linux VM, source generation before encode/send',
         histogram='32 subdivisions per power of two; conservative bucket upper bound quantiles',
         source_sha256={str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in source_files},
         workload='100 instruments,N-only buy100/sell30/sell70,at most100active',
         publisher='unchanged synchronous publish perUDPpacket',
         matcher='single writer,transaction plus confirmed ack perpacket;no sharding/batching',
         pvc_note='localpath size requests are not verified disk quotas',
         sleep_note='caffeinate used by caller; wall/monotonic interruption aborts validity'))
    print('Isolated load namespace deployed.', flush=True)


def source(profile):
    stages = profile_stages(profile)
    seconds = sum(x['count']/x['rate'] for x in stages)
    create({'apiVersion': 'batch/v1', 'kind': 'Job', 'metadata': {'name': 'load-source'}, 'spec': {
        'backoffLimit': 0, 'activeDeadlineSeconds': int(seconds + 600), 'template': {
        'metadata': {'labels': {'app': 'load-source'}}, 'spec': {
        'nodeSelector': {'mtbt.lane': 'b'}, 'restartPolicy': 'Never', 'automountServiceAccountToken': False,
        'terminationGracePeriodSeconds': 10,
        'securityContext': {'runAsNonRoot': True, 'runAsUser': 10001, 'runAsGroup': 10001, 'fsGroup': 10001},
        'containers': [{'name': 'source', 'image': IMAGE, 'command': ['/app/load-source'],
        'args': ['-manifest=/config/manifest.json','-addr=publisher:19001','-allow-non-loopback',
                 '-profile='+profile,'-report=/evidence/source.jsonl','-max-schedule-lag=2s'],
        'resources': {'requests': {'cpu': '100m','memory':'32Mi'},'limits':{'cpu':'1','memory':'128Mi'}},
        'securityContext': {'allowPrivilegeEscalation': False, 'readOnlyRootFilesystem': True,
                            'capabilities': {'drop': ['ALL']}},
        'volumeMounts': [{'name':'config','mountPath':'/config','readOnly':True},
                         {'name':'evidence','mountPath':'/evidence'}]}],
        'volumes':[{'name':'config','configMap':{'name':'mtbt-source'}},
                   {'name':'evidence','persistentVolumeClaim':{'claimName':'source-evidence'}}]}}}})
    return sum(x['count'] for x in stages), seconds


def live_metrics():
    text = kube('exec','load-monitor','--','sh','-c',
                'tail -1 /source/source.jsonl 2>/dev/null || true; '
                'tail -1 /metrics/latency.jsonl 2>/dev/null || true', timeout=15)
    lines = [json.loads(x) for x in text.splitlines() if x.startswith('{')]
    source_row = next((x for x in lines if 'sent' in x), None)
    latency_row = next((x for x in lines if 'cumulative' in x), None)
    return source_row, latency_row


def capture_failure(out):
    for app in ('publisher','matching-service','load-source'):
        for previous in (False, True):
            try:
                args=['logs','-l','app='+app,'--tail=1000']
                if previous:args.append('--previous')
                (out/(app+('-previous' if previous else '')+'.log')).write_text(kube(*args))
            except RuntimeError:
                pass
    save(out, 'pods-before-stop.json', json.loads(kube('get','pods','-o','json')))
    save(out, 'jobs-before-stop.json', json.loads(kube('get','jobs','-o','json')))
    (out/'events-before-stop.txt').write_text(kube('get','events','--sort-by=.lastTimestamp'))


def oracle(out, state):
    text=kube('exec','-i','postgres-0','--','psql','-X','-U','mtbt','-d','mtbt','-At',
              '-v','ON_ERROR_STOP=1','-v','count='+str(state['source']),'-v','instruments=100',
              stdin=(ROOT/'scripts/verify_load.sql').read_text(),timeout=180).strip()
    (out/'oracle.txt').write_text(text+'\n')
    fields=text.split('|')
    if len(fields)!=5:raise ValueError('unexpected oracle result')
    rows,mismatches,first,last,active=fields
    return dict(rows=int(rows),mismatches=int(mismatches),first=int(first) if first else None,
                last=int(last) if last else None,active=int(active))


def observe(out, profile, drain_seconds):
    if checkpoint()['source'] != 0:
        raise ValueError('pristine stream required; refusing replay')
    if (out/'outcome.json').exists():raise ValueError('existing outcome; refusing overwrite')
    stages=profile_stages(profile)
    baseline={app:restarts(pod(app)) for app in ('matching-service','publisher')}
    if not pod('matching-service') or not pod('publisher'):raise ValueError('services not running')
    save(out,'restart-baseline.json',baseline)
    (out/'profile.txt').write_text(profile+'\n')
    total,seconds=source(profile)
    started,last_mono,last_wall=time.monotonic(),time.monotonic(),time.time()
    status,reason='RUNNING',''
    try:
        with (out/'progress.jsonl').open('x') as f:
            while time.monotonic()-started < seconds+drain_seconds+60:
                mono,wall=time.monotonic(),time.time()
                if abs((wall-last_wall)-(mono-last_mono))>2 or mono-last_mono>30:
                    status,reason='ENVIRONMENT_INTERRUPTED','host/VM scheduling or sleep interruption';break
                last_mono,last_wall=mono,wall
                state=checkpoint();mp,pp,sp=pod('matching-service'),pod('publisher'),pod('load-source')
                src,metrics=live_metrics()
                event=dict(at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                           elapsed_seconds=time.monotonic()-started,state=state,
                           matcher_restarts=restarts(mp),publisher_restarts=restarts(pp),
                           source_phase=(sp or {}).get('status',{}).get('phase'),
                           source=src,latency=metrics)
                f.write(json.dumps(event)+'\n');f.flush()
                print(json.dumps({k:v for k,v in event.items() if k!='latency'}),flush=True)
                if state['status']=='INVALID':status,reason='FAILED',state['reason'];break
                if restarts(mp)>baseline['matching-service'] or restarts(pp)>baseline['publisher']:
                    status,reason='FAILED','application restarted during load';break
                if event['source_phase']=='Failed' or src and src['kind'] in ('failed','interrupted'):
                    status,reason='FAILED','source failed: '+str((src or {}).get('reason','inspect source logs'));break
                if state['source']==total and state['broker']==total+1 and event['source_phase']=='Succeeded':
                    status='CANDIDATE_COMPLETE';break
                time.sleep(5)
            else:status,reason='DRAIN_TIMEOUT','source plus bounded drain timeout'
    except Exception as exc:
        status,reason='HARNESS_ERROR',str(exc)
    capture_failure(out)  # preserve pod terminations/reasons before anything is stopped
    if status!='CANDIDATE_COMPLETE':
        # Keep Job and source PVC evidence; terminate only its process, do not delete history.
        item=pod('load-source')
        if item and item['status'].get('phase') in ('Running','Pending'):
            kube('patch','job','load-source','--type=merge','-p','{"spec":{"suspend":true}}')
    kube('scale','deployment/matching-service','deployment/publisher','--replicas=0')
    lab.wait(lambda:not json.loads(kube('get','pods','-l','app=matching-service','-o','json'))['items'],
             'metrics final flush',timeout=120)
    state=checkpoint();verified=oracle(out,state)
    outcome=dict(status=status,reason=reason,requested_events=total,profile=stages,
                 nominal_source_seconds=seconds,elapsed_seconds=time.monotonic()-started,
                 final_checkpoint=state,oracle=verified,complete_workload_latency_claim=False)
    save(out,'outcome.json',outcome)
    collect(out)
    analyze(out)
    return json.loads((out/'analysis.json').read_text())


def collect(out):
    ensure_monitor()
    for path,name in [('/metrics/latency.jsonl','latency.jsonl'),('/source/source.jsonl','source.jsonl')]:
        try:(out/name).write_text(kube('exec','load-monitor','--','cat',path,timeout=30))
        except RuntimeError as exc:(out/(name+'.error')).write_text(str(exc)+'\n')
    try:(out/'nats-jsz.json').write_text(kube('exec','load-monitor','--','wget','-qO-',
                                          'http://nats:8222/jsz?streams=true&consumers=true',timeout=15))
    except RuntimeError as exc:(out/'nats-error.txt').write_text(str(exc)+'\n')
    (out/'db-counts.txt').write_text(sql("SELECT count(*),count(*)FILTER(WHERE remaining_quantity>0),"
        "sum(accepted_quantity),sum(remaining_quantity),sum(filled_quantity),sum(cancelled_quantity) "
        "FROM mtbt.orders; SELECT pg_total_relation_size('mtbt.orders'); SHOW fsync; "
        "SHOW synchronous_commit; SHOW full_page_writes;")+'\n')
    save(out,'pods-final.json',json.loads(kube('get','pods','-o','json')))
    after=snapshots();save(out,'retained-after.json',after)
    if json.loads((out/'retained-before.json').read_text())!=after:
        raise ValueError('retained namespaces changed')
    kube('delete','pod','load-monitor','--grace-period=0','--wait=false')


def analyze(out):
    outcome=json.loads((out/'outcome.json').read_text())
    sources=[json.loads(x) for x in (out/'source.jsonl').read_text().splitlines()]
    metrics=[json.loads(x) for x in (out/'latency.jsonl').read_text().splitlines()]
    failures=[];target=outcome['requested_events']
    state=outcome.get('final_checkpoint',{})
    oracle_result=outcome.get('oracle',{})
    cumulative=metrics[-1]['cumulative'] if metrics else {}
    src=sources[-1] if sources else {}
    if outcome['status'] not in ('CANDIDATE_COMPLETE','COMPLETE'):failures.append('workload did not reach completion candidate')
    if src.get('kind')!='complete' or src.get('sent')!=target:failures.append('source completion/count missing')
    if state.get('status')!='VALID' or state.get('source')!=target or state.get('broker')!=target+1:
        failures.append('full contiguous database checkpoint missing')
    if oracle_result.get('rows')!=target or oracle_result.get('mismatches')!=0 or oracle_result.get('first')!=1 or oracle_result.get('last')!=target:
        failures.append('complete exact-state oracle missing')
    if not metrics or metrics[-1].get('kind')!='final' or cumulative.get('count')!=target:
        failures.append('final complete timing count missing')
    if cumulative.get('ack_failures',0) or cumulative.get('retried_samples',0) or cumulative.get('missing_timestamps',0):
        failures.append('ack/retry/timestamp gate failed')
    for name in ('emitted_to_commit','received_to_delivery','db_process','ack','emitted_to_ack'):
        h=cumulative.get('histograms',{}).get(name,{})
        if h.get('count')!=target or h.get('invalid',0):failures.append(name+' histogram incomplete/invalid')
    result=dict(full_target_pass=not failures,failures=failures,committed_prefix=cumulative.get('count'),
                source_last_reported=src.get('sent'),source_report_kind=src.get('kind'),
                timing_scope='all observed commits only; not full-target latency unless full_target_pass',
                cumulative=cumulative)
    save(out,'analysis.json',result)
    if not failures:
        outcome['status']='COMPLETE';outcome['complete_workload_latency_claim']=True
        save(out,'outcome.json',outcome)
    print(json.dumps({k:v for k,v in result.items() if k!='cumulative'}),flush=True)
    return result


def main():
    global NAMESPACE,IMAGE
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['deploy','install','run','collect','analyze'])
    parser.add_argument('--out',required=True,type=Path)
    parser.add_argument('--namespace',default=NAMESPACE)
    parser.add_argument('--image',default=IMAGE)
    parser.add_argument('--profile',default=PROFILE)
    parser.add_argument('--skip-build',action='store_true')
    parser.add_argument('--drain-seconds',default=600,type=int)
    args=parser.parse_args()
    NAMESPACE=validate_namespace(args.namespace);IMAGE=args.image
    out=args.out.resolve()
    if not out.is_relative_to(ROOT):parser.error('output must be within project')
    profile_stages(args.profile)
    if args.action=='analyze':analyze(out);return
    lab.guard()
    if args.action=='deploy':deploy(out,not args.skip_build)
    elif args.action=='install':install(out)
    elif args.action=='run':observe(out,args.profile,args.drain_seconds)
    else:collect(out)

if __name__=='__main__':main()
