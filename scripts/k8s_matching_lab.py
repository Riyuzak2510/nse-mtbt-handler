#!/usr/bin/env python3
"""Explicit-context kind durable matching deployment and in-cluster benchmark. Never resets retained state."""
import argparse
import csv
import datetime
import hashlib
import json
from pathlib import Path
import secrets
import subprocess
import time

from run_latency import distribution

ROOT=Path(__file__).resolve().parents[1]
KUBECONFIG=ROOT/'run/k8s-lab/kubeconfig'
CLUSTER='nse-mtbt-lab'
CONTEXT='kind-'+CLUSTER
NAMESPACE='nse-mtbt-matching'
OLD_NAMESPACE='nse-mtbt-lab'
IMAGE='nse-mtbt-handler:matching-k8s-20261022'


def command(argv,stdin=None,timeout=240):
    p=subprocess.run(argv,cwd=ROOT,input=stdin,text=True,capture_output=True,timeout=timeout)
    if p.returncode:raise RuntimeError(f'{argv[0]} failed: {p.stderr.strip()}')
    return p.stdout


def kube(*args,stdin=None,timeout=240,namespace=NAMESPACE):
    if namespace not in (NAMESPACE,OLD_NAMESPACE):raise ValueError('unsupported namespace')
    return command(['kubectl','--kubeconfig',str(KUBECONFIG),'--context',CONTEXT,'-n',namespace,*args],stdin,timeout)


def guard():
    cfg=json.loads(command(['kubectl','--kubeconfig',str(KUBECONFIG),'--context',CONTEXT,'config','view','--minify','-o','json']))
    if not cfg['clusters'][0]['cluster']['server'].startswith('https://127.0.0.1:'):raise ValueError('kind loopback API required')
    nodes=json.loads(kube('get','nodes','-o','json'))['items']
    if {n['metadata']['name'] for n in nodes}!={CLUSTER+'-control-plane',CLUSTER+'-worker',CLUSTER+'-worker2'}:raise ValueError('unexpected cluster nodes')


def create(obj):return kube('create','-f','-',stdin=json.dumps(obj))


def resource_list(text):
    decoder=json.JSONDecoder();items=[];remaining=text.strip()
    while remaining:
        obj,end=decoder.raw_decode(remaining)
        items.extend(obj['items'] if obj.get('kind')=='List' else [obj])
        remaining=remaining[end:].strip()
    return {'apiVersion':'v1','kind':'List','items':items}

def sql(query):return kube('exec','postgres-0','--','psql','-X','-U','mtbt','-d','mtbt','-At','-v','ON_ERROR_STOP=1','-c',query).strip()

def wait(test,label,timeout=240):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        try:
            value=test()
            if value:return value
        except (RuntimeError,json.JSONDecodeError):pass
        time.sleep(1)
    raise TimeoutError(label)


def retained_snapshot():
    result={}
    for kind in ('deployments','statefulsets','persistentvolumeclaims'):
        items=json.loads(kube('get',kind,'-o','json',namespace=OLD_NAMESPACE))['items']
        result[kind]=[{k:o.get(k) for k in ('apiVersion','kind','spec')}|{'name':o['metadata']['name'],'uid':o['metadata']['uid'],'generation':o['metadata'].get('generation')} for o in items]
    return result


def pod(name):
    items=json.loads(kube('get','pods','-l','app='+name,'-o','json'))['items']
    live=[p for p in items if not p['metadata'].get('deletionTimestamp')]
    return live[0] if len(live)==1 else None


def observer():
    if kube('get','pod','matching-observer','--ignore-not-found').strip():return
    create({'apiVersion':'v1','kind':'Pod','metadata':{'name':'matching-observer','labels':{'app':'matching-observer'}},'spec':{
        'nodeSelector':{'mtbt.lane':'b'},'restartPolicy':'Never','automountServiceAccountToken':False,
        'securityContext':{'runAsNonRoot':True,'runAsUser':10001,'runAsGroup':10001,'fsGroup':10001},
        'containers':[{'name':'reader','image':'nats:2.11.8-alpine','command':['sleep','3600'],
                       'securityContext':{'allowPrivilegeEscalation':False,'readOnlyRootFilesystem':True,'capabilities':{'drop':['ALL']}},
                       'resources':{'requests':{'cpu':'10m','memory':'16Mi'},'limits':{'cpu':'100m','memory':'32Mi'}},
                       'volumeMounts':[{'name':'evidence','mountPath':'/evidence','readOnly':True}]}],
        'volumes':[{'name':'evidence','persistentVolumeClaim':{'claimName':'matching-evidence'}}]}})
    kube('wait','--for=condition=Ready','pod/matching-observer','--timeout=120s',timeout=140)


def http(path):
    current=pod('matching-service')
    if not current or not current.get('status',{}).get('podIP'):raise RuntimeError('matching pod not started')
    text=kube('exec','matching-observer','--','wget','-qO-','http://'+current['status']['podIP']+':19004'+path,timeout=10)
    return json.loads(text)


def matching_args(trace):
    args=['-manifest=/config/manifest.json','-http=0.0.0.0:19004','-allow-non-loopback','-mode=durable','-max-active=100000','-stale-after=5s']
    if trace:args+=['-timings=/evidence/consumer-$(POD_UID).csv','-timings-limit=11000']
    return args


def pub_args(trace):
    args=['-manifest=/config/manifest.json','-listen=0.0.0.0:19001','-allow-non-loopback','-health-http=0.0.0.0:19003']
    if trace:args+=['-timings=/evidence/publisher.csv','-timings-limit=11000']
    return args


def patch_apps(trace,replicas=1):
    for name,args in [('publisher',pub_args(trace)),('matching-service',matching_args(trace))]:
        kube('patch','deployment',name,'--type=strategic','-p',json.dumps({'spec':{'replicas':replicas,'template':{'spec':{'containers':[{'name':name,'args':args}]}}}}))


def up(out,count,rate,skip_build=False):
    if kube('get','namespace',NAMESPACE,'--ignore-not-found').strip():raise ValueError('namespace already exists; refusing overwrite/reset')
    out.mkdir(parents=True,exist_ok=False)
    before=retained_snapshot();(out/'retained-before.json').write_text(json.dumps(before,indent=2)+'\n')
    if not skip_build:(out/'image-build.log').write_text(command(['docker','build','-t',IMAGE,'.'],timeout=600))
    (out/'image-load.log').write_text(command(['kind','load','docker-image',IMAGE,'postgres:17.7','nats:2.11.8-alpine','--name',CLUSTER],timeout=600))
    manifest={'version':2,'profile':'cm-mtbt-le64-v1','stream_id':1,'seed':42,'baseline':'empty','created_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'instruments':[{'token':1,'symbol':'SIM1','tick_size':5,'base_price':10000}]}
    (out/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    settings={'count':count,'rate':rate,'image':IMAGE,'image_id':command(['docker','image','inspect',IMAGE,'--format','{{.Id}}']).strip(),'namespace':NAMESPACE,'cluster':CLUSTER}
    (out/'settings.json').write_text(json.dumps(settings,indent=2)+'\n')
    create({'apiVersion':'v1','kind':'Namespace','metadata':{'name':NAMESPACE,'labels':{'app.kubernetes.io/part-of':'nse-mtbt-matching'}}})
    pgpw,natspw=secrets.token_hex(24),secrets.token_hex(24)
    config='port:4222\nhttp:8222\njetstream { store_dir:"/data/jetstream", max_mem_store:64MB, max_file_store:512MB }\n'+f'authorization {{ user:mtbt, password:"{natspw}" }}\n'
    create({'apiVersion':'v1','kind':'Secret','metadata':{'name':'mtbt-credentials'},'type':'Opaque','stringData':{
        'POSTGRES_PASSWORD':pgpw,'DATABASE_URL':f'postgres://mtbt:{pgpw}@postgres:5432/mtbt?sslmode=disable','NATS_URL':f'nats://mtbt:{natspw}@nats:4222','nats.conf':config}})
    create({'apiVersion':'v1','kind':'ConfigMap','immutable':True,'metadata':{'name':'mtbt-source'},'data':{'manifest.json':json.dumps(manifest)}})
    rendered=command(['kubectl','kustomize','deploy/matching']);(out/'rendered.yaml').write_text(rendered)
    install(out)


def install(out):
    load(out)
    if json.loads(kube('get','deployments,statefulsets','-o','json'))['items']:raise ValueError('install only resumes empty staging, not an existing deployment')
    rendered=(out/'rendered.yaml').read_text()
    resources=resource_list(kube('create','--dry-run=client','-f','-','-o','json',stdin=rendered))
    for obj in resources['items']:
        if obj['kind'] in ('Deployment','StatefulSet'):
            for c in obj['spec']['template']['spec']['containers']:
                if c['image']=='nse-mtbt-handler:lab':c['image']=IMAGE
        if obj['kind']=='Deployment':
            obj['spec']['replicas']=0
            if obj['metadata']['name']=='matching-service':
                obj['spec']['template']['spec']['containers'][0]['env'].append({'name':'POD_UID','valueFrom':{'fieldRef':{'fieldPath':'metadata.uid'}}})
    (out/'applied-resources.json').write_text(json.dumps(resources,indent=2)+'\n')
    kube('apply','--dry-run=server','-f','-',stdin=json.dumps(resources));kube('apply','-f','-',stdin=json.dumps(resources))
    for name in ('postgres','nats'):kube('rollout','status','statefulset/'+name,'--timeout=240s',timeout=270)
    patch_apps(True)
    kube('rollout','status','deployment/publisher','--timeout=240s',timeout=270)
    observer();wait(lambda:http('/healthz'),'matching process startup')
    state=sql('SELECT owner_mode,status,source_sequence,broker_sequence FROM mtbt.feed_state WHERE stream_id=1')
    if state!='matching|UNINITIALIZED|0|0':raise ValueError('matching stream not pristine: '+state)
    (out/'startup-state.txt').write_text(state+'\n');print('Isolated matching namespace deployed; pristine input state, source not started.',flush=True)


def source_job(name,start,end,total,rate,expected=False,heartbeat=True,image=None):
    args=['-manifest=/config/manifest.json','-addr=publisher:19001','-allow-non-loopback',f'-start={start}',f'-end={end}',f'-total={total}',f'-rate={rate}',f'-heartbeat={str(heartbeat).lower()}',f'-timings=/evidence/{name}.csv']
    if expected:args+=['-expected=/evidence/'+(expected if isinstance(expected,str) else 'expected.json')]
    create({'apiVersion':'batch/v1','kind':'Job','metadata':{'name':name,'labels':{'app.kubernetes.io/part-of':'nse-mtbt-matching'}},'spec':{'backoffLimit':0,'activeDeadlineSeconds':600,'template':{'metadata':{'labels':{'app':name}},'spec':{
        'nodeSelector':{'mtbt.lane':'b'},'restartPolicy':'Never','automountServiceAccountToken':False,'terminationGracePeriodSeconds':60,
        'securityContext':{'runAsNonRoot':True,'runAsUser':10001,'runAsGroup':10001,'fsGroup':10001,'seccompProfile':{'type':'RuntimeDefault'}},
        'containers':[{'name':'source','image':image or IMAGE,'command':['/app/matching-workload'],'args':args,'imagePullPolicy':'IfNotPresent',
        'securityContext':{'allowPrivilegeEscalation':False,'readOnlyRootFilesystem':True,'capabilities':{'drop':['ALL']}},
        'resources':{'requests':{'cpu':'100m','memory':'64Mi'},'limits':{'cpu':'1','memory':'128Mi'}},
        'volumeMounts':[{'name':'config','mountPath':'/config','readOnly':True},{'name':'evidence','mountPath':'/evidence'}]}],
        'volumes':[{'name':'config','configMap':{'name':'mtbt-source'}},{'name':'evidence','persistentVolumeClaim':{'claimName':'source-evidence'}}]}}}})
    kube('wait','--for=condition=complete','job/'+name,'--timeout=600s',timeout=630)


def load(out):
    settings=json.loads((out/'settings.json').read_text())
    if settings['namespace']!=NAMESPACE or settings['cluster']!=CLUSTER:raise ValueError('wrong deployment evidence')
    live=json.loads(kube('get','configmap','mtbt-source','-o','json'))
    if json.loads(live['data']['manifest.json'])!=json.loads((out/'manifest.json').read_text()):raise ValueError('manifest differs from deployed configuration')
    return settings


def verify_run(out):
    settings=load(out);count=settings['count'];rate=settings['rate'];observer()
    if kube('get','job','matching-prefix','--ignore-not-found').strip():raise ValueError('source already released; refuse sequence-one replay')
    if sql('SELECT source_sequence FROM mtbt.feed_state WHERE stream_id=1')!='0':raise ValueError('source requires empty matching state')
    source_job('matching-prefix',1,2,count,rate,heartbeat=False)
    wait(lambda:sql('SELECT source_sequence FROM mtbt.feed_state WHERE stream_id=1')=='2','partial fill checkpoint')
    before=http('/orders?id=4607182418800017408')
    if before['remaining_quantity']!=70 or before['filled_quantity']!=30 or before['status']!='PARTIALLY_FILLED':raise ValueError('partial fill not persisted')
    old=pod('matching-service');(out/'before-restart.json').write_text(json.dumps({'order':before,'pod':old},indent=2)+'\n')
    (out/'before-restart.log').write_text(kube('logs',old['metadata']['name']))
    kube('delete','pod',old['metadata']['name'],'--grace-period=0','--force','--wait=true')
    wait(lambda:(lambda p:p and p['metadata']['uid']!=old['metadata']['uid'])(pod('matching-service')),'replacement pod')
    wait(lambda:http('/healthz'),'replacement process')
    after=http('/orders?id=4607182418800017408')
    if before!=after:raise ValueError('database restoration differs after pod replacement')
    current=pod('matching-service');(out/'after-restart.json').write_text(json.dumps({'order':after,'pod':current},indent=2)+'\n')
    print('Partial fill restored exactly in replacement Kubernetes pod.',flush=True)
    source_job('matching-source',3,count,count,rate,expected=True)
    wait(lambda:sql('SELECT status,source_sequence,broker_sequence FROM mtbt.feed_state WHERE stream_id=1')==f'VALID|{count}|{count+1}','final matching checkpoint',timeout=240)
    verify_orders(out,count)
    for name in ('publisher','matching-service'):(out/(name+'.log')).write_text(kube('logs','deployment/'+name))
    (out/'source.log').write_text(kube('logs','job/matching-source'))
    (out/'pods-measured.json').write_text(kube('get','pods','-o','json'));(out/'nodes.json').write_text(kube('get','nodes','-o','json'))
    patch_apps(True,0)
    for name in ('publisher','matching-service'):kube('wait','--for=delete','pod','-l','app='+name,'--timeout=120s',timeout=140)
    collect(out)
    analyze(out)
    patch_apps(False)
    kube('rollout','status','deployment/publisher','--timeout=120s',timeout=140)
    wait(lambda:http('/healthz'),'matching resumed without tracing')
    after=retained_snapshot();(out/'retained-after.json').write_text(json.dumps(after,indent=2)+'\n')
    if after!=json.loads((out/'retained-before.json').read_text()):raise ValueError('retained reconstruction specs changed')
    (out/'pods-final.json').write_text(kube('get','pods','-o','json'))
    kube('delete','pod','matching-observer','--wait=true')
    print('Matching deployment resumed; finite source intentionally not replayed. Finite feed becomes STALE, health remains live.',flush=True)


def evidence_reader(claim):
    name='reader-'+claim
    create({'apiVersion':'v1','kind':'Pod','metadata':{'name':name},'spec':{'restartPolicy':'Never','automountServiceAccountToken':False,
        'securityContext':{'runAsNonRoot':True,'runAsUser':10001,'runAsGroup':10001},'containers':[{'name':'reader','image':'nats:2.11.8-alpine','command':['sleep','600'],
        'securityContext':{'allowPrivilegeEscalation':False,'readOnlyRootFilesystem':True,'capabilities':{'drop':['ALL']}},
        'volumeMounts':[{'name':'evidence','mountPath':'/evidence','readOnly':True}]}],
        'volumes':[{'name':'evidence','persistentVolumeClaim':{'claimName':claim}}]}})
    kube('wait','--for=condition=Ready','pod/'+name,'--timeout=120s',timeout=140);return name


def verify_orders(out,count,expected_file="expected.json",broker_position=None,restart_evidence=None):
    reader=evidence_reader('source-evidence')
    try:expected=json.loads(kube('exec',reader,'--','cat','/evidence/'+expected_file))
    finally:kube('delete','pod',reader,'--wait=true')
    query="SELECT coalesce(json_agg(row_to_json(t)),'[]') FROM (SELECT encode(order_id,'hex') id,token,side,price,accepted_quantity,remaining_quantity,filled_quantity,cancelled_quantity,status FROM mtbt.orders WHERE stream_id=1 ORDER BY order_id) t"
    actual=json.loads(sql(query))
    if len(expected)!=count or sorted(actual,key=lambda r:r['id'])!=sorted(expected,key=lambda r:r['id']):raise ValueError('exact full order oracle mismatch')
    if sql('SELECT count(*) FROM mtbt.active_orders WHERE stream_id=1')!='0':raise ValueError('redundant reconstructed business rows appeared')
    (out/'orders.json').write_text(json.dumps(actual,indent=2)+'\n');(out/'expected.json').write_text(json.dumps(expected,indent=2)+'\n')
    durability={k:sql('SHOW '+k) for k in ('fsync','synchronous_commit','full_page_writes')}
    if any(v!='on' for v in durability.values()):raise ValueError('durability disabled')
    (out/'verification.json').write_text(json.dumps({'exact_order_oracle':True,'partial_fill_pod_restart':True,'partial_fill_restart_evidence':restart_evidence or 'same-window before-restart.json and after-restart.json','source_sequence':count,'broker_sequence':broker_position or count+1,'order_rows':len(actual),'postgres_durability':durability,'duplicate_business_rows':False},indent=2)+'\n')


def collect(out):
    for claim,files in [('source-evidence',['matching-prefix.csv','matching-source.csv']),('publisher-evidence',['publisher.csv'])]:
        reader=evidence_reader(claim)
        try:
            for file in files:(out/file).write_text(kube('exec',reader,'--','cat','/evidence/'+file))
        finally:kube('delete','pod',reader,'--wait=true')
    files=kube('exec','matching-observer','--','sh','-c','ls /evidence/consumer-*.csv').splitlines()
    for file in files:(out/Path(file).name).write_text(kube('exec','matching-observer','--','cat',file))



def continue_window(out,previous):
    prior=load(previous)
    state=sql('SELECT status,source_sequence,broker_sequence FROM mtbt.feed_state WHERE stream_id=1')
    if state!='VALID|10000|10001':raise ValueError('second window requires exact committed first benchmark; no reset')
    if kube('get','job','matching-source2','--ignore-not-found').strip():raise ValueError('second source already exists')
    out.mkdir(parents=True,exist_ok=False)
    (out/'manifest.json').write_text((previous/'manifest.json').read_text())
    image='nse-mtbt-handler:matching-k8s-20261022-r2'
    settings=dict(prior,count=20000,rate=100,first_measured=10001,source_trace='matching-source2.csv',publisher_trace='publisher2.csv',
                  image=image,image_id=command(['docker','image','inspect',image,'--format','{{.Id}}']).strip(),
                  previous_evidence=str(previous.relative_to(ROOT)),clock='same Linux kind VM',note='continuation, not fresh state; prior10000orders retained; no builds during this window')
    (out/'settings.json').write_text(json.dumps(settings,indent=2)+'\n')
    (out/'retained-before.json').write_text(json.dumps(retained_snapshot(),indent=2)+'\n')
    for name,args in [('publisher',pub_args(True)),('matching-service',matching_args(True))]:
        args=[x.replace('/evidence/publisher.csv','/evidence/publisher2.csv') for x in args]
        kube('patch','deployment',name,'--type=strategic','-p',json.dumps({'spec':{'replicas':1,'template':{'spec':{'containers':[{'name':name,'image':image,'args':args}]}}}}))
    kube('rollout','status','deployment/publisher','--timeout=120s',timeout=140)
    observer();wait(lambda:http('/healthz'),'continued matcher startup')
    source_job('matching-source2',10001,20000,20000,100,expected='expected2.json',image=image)
    wait(lambda:sql('SELECT status,source_sequence,broker_sequence FROM mtbt.feed_state WHERE stream_id=1')=='VALID|20000|20002','continuation drain',240)
    verify_orders(out,20000,'expected2.json',20002,str(previous.relative_to(ROOT)))
    for name in ('publisher','matching-service'):(out/(name+'.log')).write_text(kube('logs','deployment/'+name))
    (out/'source.log').write_text(kube('logs','job/matching-source2'))
    (out/'pods-measured.json').write_text(kube('get','pods','-o','json'))
    for name in ('publisher','matching-service'):kube('scale','deployment/'+name,'--replicas=0')
    for name in ('publisher','matching-service'):kube('wait','--for=delete','pod','-l','app='+name,'--timeout=120s',timeout=140)
    for claim,file in [('source-evidence','matching-source2.csv'),('publisher-evidence','publisher2.csv')]:
        reader=evidence_reader(claim)
        try:(out/file).write_text(kube('exec',reader,'--','cat','/evidence/'+file))
        finally:kube('delete','pod',reader,'--grace-period=0','--wait=true')
    current_files=kube('exec','matching-observer','--','sh','-c','ls /evidence/consumer-*.csv').splitlines()
    for file in current_files:
        text=kube('exec','matching-observer','--','cat',file)
        lines=text.splitlines()
        if len(lines)>1 and any(int(r['sequence'])>=10001 for r in csv.DictReader(lines)):(out/Path(file).name).write_text(text)
    analyze(out)
    patch_apps(False)
    kube('rollout','status','deployment/publisher','--timeout=120s',timeout=140)
    wait(lambda:http('/healthz'),'continued matcher resumed')
    (out/'retained-after.json').write_text(json.dumps(retained_snapshot(),indent=2)+'\n')
    if (out/'retained-before.json').read_text()!=(out/'retained-after.json').read_text():raise ValueError('old namespace specs changed')
    (out/'pods-final.json').write_text(kube('get','pods','-o','json'))
    kube('delete','pod','matching-observer','--grace-period=0','--wait=true')
    print('Continuation complete:20000committedsourceevents,20002brokerpositions,matchingdeploymentretained.',flush=True)


def read_csv(path):
    with path.open() as f:return list(csv.DictReader(f))


def analyze(out):
    settings=json.loads((out/'settings.json').read_text());count=settings['count'];first=settings.get('first_measured',3);measured=count-first+1
    sent=read_csv(out/settings.get('source_trace','matching-source.csv'));pub=read_csv(out/settings.get('publisher_trace','publisher.csv'))
    verification=json.loads((out/'verification.json').read_text())
    if not verification.get('exact_order_oracle') or not verification.get('partial_fill_pod_restart'):raise ValueError('correctness/restart gates missing')
    consumer=[]
    for p in out.glob('consumer-*.csv'):
        if p.stat().st_size:consumer+=read_csv(p)
    data=[r for r in sent if int(r['sequence'])>=first]
    publisher_rows=[r for r in pub if int(r['sequence'])>=first]
    if len(publisher_rows)!=measured or {int(r['sequence']) for r in publisher_rows}!=set(range(first,count+1)):raise ValueError('missing/duplicate publisher trace')
    if sum(r['type']=='Z' for r in pub)!=1 or sum(r['type']=='Z' for r in sent)!=1 or sum(r['type']=='Z' for r in consumer)!=1:raise ValueError('heartbeat trace mismatch')
    pub={int(r['sequence']):r for r in publisher_rows}
    rows=[r for r in consumer if int(r['sequence'])>=first]
    if any(r['type']!='N' for r in rows):raise ValueError('unexpected workload event type')
    if len(data)!=measured or len(rows)!=measured:raise ValueError('incomplete data trace')
    if {int(r['sequence']) for r in data}!=set(range(first,count+1)) or {int(r['sequence']) for r in rows}!=set(range(first,count+1)):raise ValueError('missing/duplicate trace')
    sent={int(r['sequence']):r for r in data}
    if any(int(r['attempts'])!=1 or int(r['ack_unix_ns'])<=0 for r in rows):raise ValueError('retry/unconfirmed ack')
    durations={'send_to_commit':[],'send_to_ack':[],'db_process':[],'publisher_to_delivery':[]}
    for r in rows:
        seq=int(r['sequence']);start=int(sent[seq]['start_unix_ns']);p=pub[seq]
        if int(r['received_unix_ns'])!=int(p['received_unix_ns']):raise ValueError('receive provenance mismatch')
        durations['send_to_commit'].append(int(r['end_unix_ns'])-start)
        durations['send_to_ack'].append(int(r['ack_unix_ns'])-start)
        durations['db_process'].append(int(r['duration_ns']))
        durations['publisher_to_delivery'].append(int(r['delivery_unix_ns'])-int(p['received_unix_ns']))
    elapsed=max(int(r['end_unix_ns']) for r in rows)-min(int(r['start_unix_ns']) for r in data)
    result={'version':1,'deployment':'Kubernetes kind durable matching','namespace':NAMESPACE,'stream_id':1,'events':measured,'source_total':count,'first_measured':first,'rate':settings['rate'],'measured':measured,
        'latency':{k:distribution(v) for k,v in durations.items()},'processing_window_events_per_second':(measured)/(elapsed/1e9),
        'validation':json.loads((out/'verification.json').read_text()),
        'method':{'source':'Go Job inside same kind/Docker Linux VM','clock':'all pod clocks share local VM kernel; not independently synchronized distributed machines','warmup':0,'excluded':'first2events before forced pod replacement' if first==3 else 'prior committed history; this continuation measures every new event','postgres':'17.7 fsync/synchronous_commit/full_page_writes on','resources':'matcher1CPU/512Mi,Go384MiB,default100k active cap','workload':'N-only one-instrument buy100/sell30/sell70 triplets; observed final heartbeat','matching_shards':0,'runtime_trace_overhead':'bounded in-memory traces flushed on graceful pause; traced prefix pod force-deleted'},
        'input_sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted([out/settings.get('source_trace','matching-source.csv'),out/settings.get('publisher_trace','publisher.csv'),*out.glob('consumer-*.csv')])}}
    (out/'result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result['latency'],indent=2),flush=True)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['up','install','run','continue','analyze']);p.add_argument('--out',type=Path,required=True);p.add_argument('--count',type=int,default=10000);p.add_argument('--rate',type=int,default=100);p.add_argument('--skip-build',action='store_true');p.add_argument('--previous',type=Path);args=p.parse_args()
    if not 3<=args.count<=100000 or not 1<=args.rate<=1000000:p.error('count3..100000,rate1..1000000 required')
    out=args.out.resolve()
    if not out.is_relative_to(ROOT):p.error('output must be within project')
    if args.action=='analyze':analyze(out);return
    guard()
    if args.action=='up':up(out,args.count,args.rate,args.skip_build)
    elif args.action=='install':install(out)
    elif args.action=='continue':
        if args.previous is None:p.error('--previous required')
        previous=args.previous.resolve()
        if not previous.is_relative_to(ROOT):p.error('previous evidence must be inside project')
        continue_window(out,previous)
    else:verify_run(out)

if __name__=='__main__':main()
