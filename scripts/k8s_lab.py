#!/usr/bin/env python3
"""Isolated stream-scoped kind lab. No default context, automatic reset, or legacy migration."""
import argparse
import json
import os
from pathlib import Path
import secrets
import subprocess
import time

from run_latency import check_manifest_v2

ROOT=Path(__file__).resolve().parents[1]
KUBECONFIG=ROOT/'run/k8s-lab/kubeconfig'
CLUSTER='nse-mtbt-lab'
NAMESPACE='nse-mtbt-lab'
IMAGE='nse-mtbt-handler:lab'


def command(argv, *, stdin=None, timeout=240):
    proc=subprocess.run(argv,cwd=ROOT,input=stdin,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=timeout)
    if proc.returncode:raise RuntimeError(f'{argv[0]} failed: {proc.stderr.strip()}')
    return proc.stdout


def kubectl(*argv, stdin=None, timeout=240):
    return command(['kubectl','--kubeconfig',str(KUBECONFIG),'--context','kind-'+CLUSTER,
                    '-n',NAMESPACE,*argv],stdin=stdin,timeout=timeout)


def guard_cluster():
    if not KUBECONFIG.is_file():raise RuntimeError('create isolated cluster with deploy/kind.yaml first')
    cfg=json.loads(command(['kubectl','--kubeconfig',str(KUBECONFIG),'--context','kind-'+CLUSTER,'config','view','--minify','-o','json']))
    if not cfg['clusters'][0]['cluster']['server'].startswith('https://127.0.0.1:'):
        raise RuntimeError('refusing non-loopback API server; this harness is kind-only')
    nodes=json.loads(kubectl('get','nodes','-o','json'))['items']
    if {n['metadata']['name'] for n in nodes}!={CLUSTER+'-control-plane',CLUSTER+'-worker',CLUSTER+'-worker2'}:
        raise RuntimeError('unexpected nodes; refusing a different cluster')


def create(obj):return kubectl('create','-f','-',stdin=json.dumps(obj))


def sql(query):
    return kubectl('exec','postgres-0','--','psql','-X','-v','ON_ERROR_STOP=1','-U','mtbt','-d','mtbt','-At','-c',query).strip()


def wait_for(test, description, timeout=240):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        if test():return
        time.sleep(1)
    raise TimeoutError(description)


def healthy_start(name):
    try:logs=kubectl('logs','deployment/'+name,timeout=10)
    except (RuntimeError,subprocess.TimeoutExpired):return False
    return name.replace('-',' ')+' ready' in logs


def load(out):
    manifest=json.loads((out/'manifest.json').read_text());check_manifest_v2(manifest)
    settings=json.loads((out/'settings.json').read_text())
    return manifest,settings


def same_manifest(manifest):
    live=json.loads(kubectl('get','configmap','mtbt-source','-o','json'))
    if json.loads(live['data']['manifest.json'])!=manifest:raise ValueError('local and deployed configuration differ')


def app_args(name, shards, limit=None):
    if name=='publisher':
        argv=['-manifest=/config/manifest.json','-listen=0.0.0.0:19001','-allow-non-loopback','-health-http=0.0.0.0:19003']
        if limit:argv+=['-timings=/evidence/publisher.csv']
    else:
        argv=['-manifest=/config/manifest.json','-http=0.0.0.0:19002','-allow-non-loopback','-shards='+str(shards)]
        if limit:argv+=['-parallel-timings=/evidence/parallel'] if shards else ['-timings=/evidence/consumer.csv']
    if limit:argv+=['-timings-limit='+str(limit)]
    return argv


def up(args,out):
    if kubectl('get','namespace',NAMESPACE,'--ignore-not-found').strip():
        raise RuntimeError('namespace exists; refusing overwrite, reset or legacy cut-over')
    out.mkdir(parents=True,exist_ok=False)
    if not args.skip_build:(out/'image-build.log').write_text(command(['docker','build','-t',IMAGE,'.'],timeout=600))
    command(['kind','load','docker-image',IMAGE,'postgres:17.7','nats:2.11.8-alpine','--name',CLUSTER],timeout=600)
    command(['docker','run','--rm','--user',f'{os.getuid()}:{os.getgid()}',
        '--mount',f'type=bind,source={out},target=/output','--entrypoint','/app/simulator',IMAGE,
        '-init','-manifest','/output/manifest.json','-seed',str(args.seed),'-instruments','3','-stream',str(args.stream)])
    manifest=json.loads((out/'manifest.json').read_text());check_manifest_v2(manifest)
    settings=dict(count=args.count,warmup=args.warmup,rate=args.rate,shards=args.shards,
        stream_id=args.stream,image=IMAGE,image_id=command(['docker','image','inspect',IMAGE,'--format','{{.Id}}']).strip(),
        namespace=NAMESPACE,cluster=CLUSTER,trace_limit=args.count+args.warmup+10)
    (out/'settings.json').write_text(json.dumps(settings,indent=2)+'\n')
    create(dict(apiVersion='v1',kind='Namespace',metadata=dict(name=NAMESPACE,labels={'app.kubernetes.io/part-of':'nse-mtbt'})))
    pg_password,nats_password=secrets.token_hex(24),secrets.token_hex(24)
    config=('port: 4222\nhttp: 8222\njetstream { store_dir: "/data/jetstream", max_mem_store: 64MB, max_file_store: 512MB }\n'
            f'authorization {{ user: mtbt, password: "{nats_password}" }}\n')
    create(dict(apiVersion='v1',kind='Secret',metadata=dict(name='mtbt-credentials'),type='Opaque',
        stringData={'POSTGRES_PASSWORD':pg_password,'DATABASE_URL':f'postgres://mtbt:{pg_password}@postgres:5432/mtbt?sslmode=disable',
                    'NATS_URL':f'nats://mtbt:{nats_password}@nats:4222','nats.conf':config}))
    create(dict(apiVersion='v1',kind='ConfigMap',immutable=True,metadata=dict(name='mtbt-source'),data={'manifest.json':json.dumps(manifest)}))
    rendered=command(['kubectl','kustomize','deploy/kubernetes']);(out/'rendered.yaml').write_text(rendered)
    resources=json.loads(kubectl('create','--dry-run=client','-f','-','-o','json',stdin=rendered))
    for obj in resources['items']:
        if obj['kind']=='Deployment':obj['spec']['replicas']=0
    staged=json.dumps(resources);(out/'staged-resources.json').write_text(json.dumps(resources,indent=2)+'\n')
    kubectl('apply','--dry-run=server','-f','-',stdin=staged);kubectl('apply','-f','-',stdin=staged)
    for name in ('postgres','nats'):kubectl('rollout','status','statefulset/'+name,'--timeout=240s',timeout=270)
    for name in ('publisher','book-service'):
        patch={'spec':{'replicas':1,'template':{'spec':{'containers':[{'name':name,'args':app_args(name,args.shards,settings['trace_limit'])}]}}}}
        kubectl('patch','deployment',name,'--type=strategic','-p',json.dumps(patch))
    start_source(out,args.timeout)


def start_source(out,timeout):
    manifest,settings=load(out);same_manifest(manifest)
    if kubectl('get','job','simulator','--ignore-not-found').strip():raise ValueError('source Job already exists; refusing sequence-one replay')
    # Schema/run setup belongs to book-service, never a duplicate hand-written schema here.
    kubectl('rollout','status','deployment/publisher','--timeout=240s',timeout=270)
    wait_for(lambda:healthy_start('book-service'),'book process did not initialize')
    state=sql(f"SELECT status||','||source_sequence||','||broker_sequence FROM mtbt.feed_state WHERE stream_id={manifest['stream_id']}")
    if state!='UNINITIALIZED,0,0':raise ValueError('source requires pristine initialized database state')
    job=json.loads(kubectl('create','--dry-run=client','-f','deploy/kubernetes/simulator-job.yaml','-o','json'))
    total=settings['count']+settings['warmup']
    job['spec']['template']['spec']['containers'][0]['args']=[
        '-manifest=/config/manifest.json','-addr=publisher:19001','-allow-non-loopback','-rate='+str(settings['rate']),
        '-count='+str(total),'-expected=/evidence/expected.json','-timings=/evidence/simulator.csv','-timings-limit='+str(settings['trace_limit'])]
    job['spec']['activeDeadlineSeconds']=timeout;create(job)
    kubectl('wait','--for=condition=complete','job/simulator','--timeout='+str(timeout)+'s',timeout=timeout+30)
    verify(out,timeout)


def read_evidence(name):
    pod='evidence-'+name
    if not kubectl('get','pod',pod,'--ignore-not-found').strip():
        create({'apiVersion':'v1','kind':'Pod','metadata':{'name':pod,'labels':{'app.kubernetes.io/part-of':'nse-mtbt'}},'spec':{
            'restartPolicy':'Never','automountServiceAccountToken':False,'securityContext':{'runAsNonRoot':True,'runAsUser':10001,'runAsGroup':10001},
            'containers':[{'name':'reader','image':'nats:2.11.8-alpine','command':['sleep','3600'],
                'securityContext':{'allowPrivilegeEscalation':False,'readOnlyRootFilesystem':True,'capabilities':{'drop':['ALL']}},
                'resources':{'requests':{'cpu':'10m','memory':'16Mi'},'limits':{'cpu':'100m','memory':'32Mi'}},
                'volumeMounts':[{'name':'evidence','mountPath':'/evidence','readOnly':True}]}],
            'volumes':[{'name':'evidence','persistentVolumeClaim':{'claimName':name+'-evidence'}}]}})
    kubectl('wait','--for=condition=Ready','pod/'+pod,'--timeout=90s',timeout=100)
    return pod


def verify(out,timeout=240):
    manifest,settings=load(out);same_manifest(manifest)
    stream=manifest['stream_id'];where=f'stream_id={stream}';total=settings['count']+settings['warmup']
    def drained():
        state=sql("SELECT status||','||source_sequence||','||broker_sequence FROM mtbt.feed_state WHERE "+where)
        if state.startswith('INVALID'):raise ValueError('feed invalid')
        if state!=f'VALID,{total},{total+1}':return False
        return not settings['shards'] or sql('SELECT coalesce(sum(dispatched-applied),0) FROM mtbt.subscriber_shards WHERE '+where)=='0'
    wait_for(drained,'database did not drain',timeout)
    reader=read_evidence('source')
    try:expected=json.loads(kubectl('exec',reader,'--','cat','/evidence/expected.json'))
    finally:kubectl('delete','pod',reader,'--wait=true')
    if expected.get('stream_id')!=stream or 'run_id' in expected or expected['sequence']!=total:raise ValueError('expected source identity mismatch')
    (out/'expected.json').write_text(json.dumps(expected,indent=2)+'\n')
    actual=json.loads(sql("SELECT coalesce(json_agg(json_build_object('id_hex',encode(order_id,'hex'),'token',token,'side',side,'price',price,'quantity',quantity)), '[]'::json) FROM mtbt.active_orders WHERE "+where))
    (out/'actual-orders.json').write_text(json.dumps(actual,indent=2)+'\n')
    want=[dict(id_hex=int(o['id_bits']).to_bytes(8,'little').hex(),token=o['token'],side=chr(o['side']),price=o['price'],quantity=o['quantity']) for o in expected['orders']]
    if sorted(actual,key=lambda o:o['id_hex'])!=sorted(want,key=lambda o:o['id_hex']):raise ValueError('exact final order oracle mismatch')
    durability=','.join(sql('SHOW '+k) for k in ('fsync','synchronous_commit','full_page_writes'))
    if durability!='on,on,on':raise ValueError('durability not enabled')
    if settings['shards'] and sql('SELECT count(*) FROM mtbt.subscriber_outbox WHERE '+where)!='0':raise ValueError('outbox not empty')
    pods=json.loads(kubectl('get','pods','-o','json'))
    for name in ('publisher','book-service'):
        logs=kubectl('logs','deployment/'+name);(out/(name+'.log')).write_text(logs)
        if 'retry' in logs.lower() or 'error' in logs.lower():raise ValueError('service retries/errors observed')
    (out/'simulator.log').write_text(kubectl('logs','job/simulator'))
    result=dict(version=2,stream_id=stream,count=settings['count'],warmup=settings['warmup'],source_events=total,
        source_sequence=total,broker_sequence=total+1,status='VALID',all_orders_match_oracle=True,active_orders=len(actual),
        postgres_durability=durability,pod_restarts=sum(c.get('restartCount',0) for p in pods['items'] for c in p['status'].get('containerStatuses',[])),
        shards=settings['shards'],clock_scope='one shared Docker Desktop VM',latency_report='pending graceful trace collection')
    (out/'verification.json').write_text(json.dumps(result,indent=2)+'\n');(out/'pods.json').write_text(json.dumps(pods,indent=2)+'\n')
    (out/'nodes.json').write_text(kubectl('get','nodes','-o','json'));print(json.dumps(result,indent=2))


def pause(out):
    manifest,settings=load(out);same_manifest(manifest)
    if json.loads(kubectl('get','job','simulator','-o','json')).get('status',{}).get('succeeded')!=1:
        raise ValueError('source must complete before pause')
    kubectl('scale','deployment/publisher','deployment/book-service','--replicas=0')
    for name in ('publisher','book-service'):kubectl('wait','--for=delete','pod','-l','app='+name,'--timeout=60s',timeout=70)
    groups=[('source',['simulator.csv']),('publisher',['publisher.csv']),('book',
        ['parallel/dispatcher.csv']+['parallel/shard-'+str(i)+'.csv' for i in range(settings['shards'])] if settings['shards'] else ['consumer.csv'])]
    for name,files in groups:
        reader=read_evidence(name)
        try:
            for filename in files:
                p=out/filename;p.parent.mkdir(parents=True,exist_ok=True);p.write_text(kubectl('exec',reader,'--','cat','/evidence/'+filename))
        finally:kubectl('delete','pod',reader,'--wait=true')
    from run_latency import summarize,markdown
    environment=dict(deployment='kind cross-node single-instance lab',image_id=settings['image_id'],
        kubernetes=json.loads(kubectl('version','-o','json')),clock='same Docker Desktop Linux VM; no independent-node clock validation')
    result=summarize(out,settings['count'],settings['warmup'],settings['rate'],manifest,environment,settings['shards'],sql_query=sql)
    result['method']['clock']=environment['clock'];result['method']['query_load']='SQL drain/oracle checks after source completion; no measured book-read load'
    (out/'result.json').write_text(json.dumps(result,indent=2)+'\n');(out/'README.md').write_text(markdown(result))
    print('Application paused; data and infrastructure retained. Source is not replayed.')


def resume(out):
    manifest,settings=load(out);same_manifest(manifest)
    for name in ('publisher','book-service'):
        if json.loads(kubectl('get','deployment',name,'-o','json'))['spec']['replicas']!=0:
            raise ValueError('both app deployments must be paused; no partial resume')
    for name in ('publisher','book-service'):
        patch={'spec':{'replicas':1,'template':{'spec':{'containers':[{'name':name,'args':app_args(name,settings['shards'])}]}}}}
        kubectl('patch','deployment',name,'--type=strategic','-p',json.dumps(patch))
    kubectl('rollout','status','deployment/publisher','--timeout=120s',timeout=130)
    wait_for(lambda:healthy_start('book-service'),'book service did not resume')
    print('Publisher/book resumed without tracing; source Job NOT replayed. Finite source will remain stale.')


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['up','start','verify','pause','resume'])
    p.add_argument('--out',type=Path,required=True);p.add_argument('--count',type=int,default=10000);p.add_argument('--warmup',type=int,default=500)
    p.add_argument('--rate',type=int,default=100);p.add_argument('--seed',type=int,default=42);p.add_argument('--stream',type=int,default=1)
    p.add_argument('--shards',type=int,default=0);p.add_argument('--timeout',type=int,default=300);p.add_argument('--skip-build',action='store_true')
    a=p.parse_args()
    if not(2<=a.count<=1000000 and 0<=a.warmup<=100000 and 1<=a.rate<=1000000 and 1<=a.stream<=32767 and 0<=a.shards<=16 and a.timeout>0):p.error('invalid parameters')
    if a.shards and (a.shards+1)*(a.count+a.warmup+10)>4000000:p.error('aggregate trace buffer exceeds bound')
    out=a.out.resolve();out.relative_to(ROOT);guard_cluster()
    if a.action=='up':up(a,out)
    elif a.action=='start':start_source(out,a.timeout)
    elif a.action=='verify':verify(out,a.timeout)
    elif a.action=='pause':pause(out)
    else:resume(out)


if __name__=='__main__':main()
