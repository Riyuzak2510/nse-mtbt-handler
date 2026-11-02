#!/usr/bin/env python3
"""Fresh isolated kind A/B matching/outcome benchmark; never resets retained histories."""
import argparse,base64,datetime,hashlib,json,os,re,secrets,select,subprocess,time
from pathlib import Path
import k8s_matching_lab as lab
import k8s_load as load

ROOT=lab.ROOT
IMAGE='nse-mtbt-handler:outcome-notify-20261101'
PROTECTED=('nse-mtbt-lab','nse-mtbt-matching','nse-mtbt-outcomes')

def validate_namespace(ns):
 if not re.fullmatch(r'nse-mtbt-load-outcome-bench-(off|on)-[a-z0-9]+(?:-[a-z0-9]+)*',ns)or len(ns)>63:raise ValueError('fresh isolated outcome benchmark namespace required')
 return ns

def plan(text):
 result=load.profile_stages(text)
 if any(s['count']<2 for s in result)or sum(s['count']for s in result)>99999:raise ValueError('benchmark requires >=2 inputs per stage and <=99999 total')
 return result

class Lab:
 def __init__(self,namespace,out,delivery_mode='notify'):
  if delivery_mode not in('notify','poll'):raise ValueError('delivery mode must be notify or poll')
  self.namespace=validate_namespace(namespace);self.out=out;self.delivery_mode=delivery_mode
 def k(self,*args,stdin=None,timeout=60):return lab.command(['kubectl','--kubeconfig',str(lab.KUBECONFIG),'--context',lab.CONTEXT,'-n',self.namespace,*args],stdin,timeout)
 def create(self,obj):return self.k('create','-f','-',stdin=json.dumps(obj))
 def save(self,name,obj):self.out.joinpath(name).write_text(json.dumps(obj,indent=2)+'\n')
 def sql(self,q):return self.k('exec','postgres-0','--','psql','-X','-U','mtbt','-d','mtbt','-At','-v','ON_ERROR_STOP=1','-c',q).strip()
 def state(self):
  row=self.sql("SELECT owner_mode,status,source_sequence,broker_sequence,reason,(SELECT count(*) FROM mtbt.order_outbox WHERE stream_id=1) FROM mtbt.feed_state WHERE stream_id=1")
  mode,status,seq,broker,reason,pending=row.split('|');return dict(mode=mode,status=status,source=int(seq),broker=int(broker),reason=reason,pending=int(pending))
 def snapshot(self):
  result={}
  for ns in PROTECTED:
   result[ns]={}
   for kind in ['deployments','statefulsets','persistentvolumeclaims']:
    items=json.loads(lab.command(['kubectl','--kubeconfig',str(lab.KUBECONFIG),'--context',lab.CONTEXT,'-n',ns,'get',kind,'-o','json']))['items']
    result[ns][kind]=[dict(name=o['metadata']['name'],uid=o['metadata']['uid'],generation=o['metadata'].get('generation'),spec=o['spec'])for o in items]
  return result
 def wait(self,check,label,seconds=180):
  deadline=time.monotonic()+seconds
  while time.monotonic()<deadline:
   try:
    value=check()
    if value:return value
   except (RuntimeError,ValueError):pass
   time.sleep(1)
  raise RuntimeError(label+' timed out; inspect retained evidence before any continuation')
 def install(self,enabled,stages):
  lab.guard()
  if self.k('get','namespace',self.namespace,'--ignore-not-found').strip():raise ValueError('refusing existing namespace/history')
  self.out.mkdir(parents=True,exist_ok=False)
  self.save('retained-before.json',self.snapshot());self.save('nodes-before.json',json.loads(self.k('get','nodes','-o','json')))
  self.create({'apiVersion':'v1','kind':'Namespace','metadata':{'name':self.namespace,'labels':{'app.kubernetes.io/part-of':'nse-mtbt-outcome-benchmark'}}})
  pgpw,npw=secrets.token_hex(24),secrets.token_hex(24)
  self.create({'apiVersion':'v1','kind':'Secret','metadata':{'name':'mtbt-credentials'},'stringData':{'POSTGRES_PASSWORD':pgpw,'DATABASE_URL':f'postgres://mtbt:{pgpw}@postgres:5432/mtbt?sslmode=disable','NATS_URL':f'nats://mtbt:{npw}@nats:4222','nats.conf':'port:4222\nhttp:8222\njetstream {store_dir:"/data/jetstream",max_mem_store:64MB,max_file_store:2GB}\nauthorization {user:mtbt,password:"'+npw+'"}\n'}})
  manifest=dict(version=2,profile='cm-mtbt-le64-v1',stream_id=1,seed=42,baseline='empty',created_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),instruments=[dict(token=1,symbol='SIM1',tick_size=5,base_price=10000)])
  self.save('manifest.json',manifest);self.create({'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':'mtbt-source'},'immutable':True,'data':{'manifest.json':json.dumps(manifest)}})
  overlay='deploy/outcomes'if enabled else'deploy/matching';origin='nse-mtbt-outcomes'if enabled else'nse-mtbt-matching'
  rendered=lab.command(['kubectl','kustomize',overlay]).replace('namespace: '+origin,'namespace: '+self.namespace)
  resources=lab.resource_list(self.k('create','--dry-run=client','-f','-','-o','json',stdin=rendered))
  for obj in resources['items']:
   obj['metadata']['namespace']=self.namespace
   if obj['kind']=='PersistentVolumeClaim':obj['spec']['resources']['requests']['storage']='2Gi'if obj['metadata']['name']in('postgres-data','nats-data')else'128Mi'
   if obj['kind']in('Deployment','StatefulSet'):
    pod=obj['spec']['template']['spec']
    for c in pod['containers']:
     if c['image']=='nse-mtbt-handler:lab':c['image']=IMAGE
    if obj['kind']=='Deployment':
     if obj['metadata']['name']=='outcome-relay':
      c=pod['containers'][0];c['args']=[a for a in c['args']if not a.startswith(('-delivery-mode=','-reconcile-interval='))]+['-delivery-mode='+self.delivery_mode,'-reconcile-interval=5s']
     obj['spec']['replicas']=0
     if obj['metadata']['name']in('publisher','matching-service'):
      c=pod['containers'][0];c.setdefault('env',[]).append({'name':'POD_UID','valueFrom':{'fieldRef':{'fieldPath':'metadata.uid'}}})
      c['env']=[e for e in c['env']if e['name']not in('MTBT_LOAD_METRICS','MTBT_SQL_METRICS','MTBT_INGEST_METRICS')]
      c['args']+=['-timings=/evidence/trace-$(POD_UID).csv','-timings-limit='+str(sum(s['count']for s in stages)+len(stages)+10)]
  self.save('resources.json',resources);self.k('apply','--dry-run=server','-f','-',stdin=json.dumps(resources));self.k('apply','-f','-',stdin=json.dumps(resources))
  for app in('postgres','nats'):self.k('rollout','status','statefulset/'+app,'--timeout=240s',timeout=260)
  self.k('scale','deployment/publisher','--replicas=1');self.k('rollout','status','deployment/publisher','--timeout=120s',timeout=140)
  self.k('scale','deployment/matching-service','--replicas=1');self.wait(lambda:'restored through broker=0' in self.k('logs','deployment/matching-service','--tail=25'),'matcher initialization')
  if enabled:
   self.k('scale','deployment/outcome-relay','--replicas=1');self.k('rollout','status','deployment/outcome-relay','--timeout=120s',timeout=140)
  monitor=load.monitor_object('benchmark-reader');monitor['spec']['volumes']=[{'name':n,'persistentVolumeClaim':{'claimName':claim}}for n,claim in [('metrics','matching-evidence'),('source','source-evidence')]];monitor['spec']['containers'][0]['volumeMounts']=[{'name':n,'mountPath':'/'+n,'readOnly':True}for n in('metrics','source')]
  self.create(monitor);self.k('wait','--for=condition=Ready','pod/benchmark-reader','--timeout=120s',timeout=140)
  self.save('settings.json',dict(namespace=self.namespace,image=IMAGE,image_id=lab.command(['docker','image','inspect',IMAGE,'--format','{{.Id}}']).strip(),enabled=enabled,delivery_mode=self.delivery_mode if enabled else None,reconcile_interval_seconds=5 if enabled else None,stages=stages,raw_stream_max_bytes=64<<20,outcome_stream_max_bytes=(64<<20)if enabled else None,clock='same Linux VM; not synchronized independent hosts',workload='N-only one instrument buy100/sell30/sell70 at10000; same absolute native history per arm',warmup_excluded=0,instrumentation='bounded in-memory source/publisher/matcher CSV; no synchronous JSONL diagnostics',node_caveat='control-plane NotReady observed; two workers Ready',source_sha256={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest()for p in [ROOT/'cmd/matching-workload/main.go',ROOT/'internal/durable/outcomes.go',ROOT/'internal/outcomerelay/relay.go']}))
  print(self.namespace+': deployed fresh benchmark',flush=True)
 def broker(self):return json.loads(self.k('exec','benchmark-reader','--','wget','-qO-','http://nats:8222/jsz?streams=true&consumers=true'))
 def app_pods(self):
  pods=json.loads(self.k('get','pods','-o','json'))['items'];return{p['metadata']['labels']['app']:dict(uid=p['metadata']['uid'],restarts=sum(c.get('restartCount',0)for c in p.get('status',{}).get('containerStatuses',[])),phase=p['status']['phase'])for p in pods if p['metadata'].get('labels',{}).get('app')in('publisher','matching-service','outcome-relay','nats','postgres')}
 def make_job(self,index,start,end,total,rate):
  name='benchmark-source-'+str(index)
  return {'apiVersion':'batch/v1','kind':'Job','metadata':{'name':name},'spec':{'backoffLimit':0,'activeDeadlineSeconds':max(120,int((end-start+1)/rate*3)+60),'template':{'metadata':{'labels':{'app':name}},'spec':{'nodeSelector':{'mtbt.lane':'b'},'restartPolicy':'Never','automountServiceAccountToken':False,'terminationGracePeriodSeconds':60,'securityContext':{'runAsNonRoot':True,'runAsUser':10001,'runAsGroup':10001,'fsGroup':10001,'seccompProfile':{'type':'RuntimeDefault'}},'containers':[{'name':'source','image':IMAGE,'command':['/app/matching-workload'],'args':['-manifest=/config/manifest.json','-addr=publisher:19001','-allow-non-loopback',f'-start={start}',f'-end={end}',f'-total={total}',f'-rate={rate}',f'-timings=/evidence/source-{index}.csv',f'-expected=/evidence/expected-{index}.json'],'resources':{'requests':{'cpu':'100m','memory':'64Mi'},'limits':{'cpu':'1','memory':'128Mi'}},'securityContext':{'allowPrivilegeEscalation':False,'readOnlyRootFilesystem':True,'capabilities':{'drop':['ALL']}},'volumeMounts':[{'name':'config','mountPath':'/config','readOnly':True},{'name':'evidence','mountPath':'/evidence'}]}],'volumes':[{'name':'config','configMap':{'name':'mtbt-source'}},{'name':'evidence','persistentVolumeClaim':{'claimName':'source-evidence'}}]}}}}
 def run(self,drain):
  lab.guard();settings=json.loads((self.out/'settings.json').read_text());stages=settings['stages'];enabled=settings['enabled'];total=sum(s['count']for s in stages)
  if self.state()['source']!=0 or json.loads(self.k('get','jobs','-o','json'))['items']:raise ValueError('run requires fresh source history and no jobs')
  before=self.app_pods();self.save('pods-before.json',before);first=1
  with (self.out/'progress.jsonl').open('x')as progress:
   for index,stage in enumerate(stages,1):
    end=first+stage['count']-1;obj=self.make_job(index,first,end,total,stage['rate']);self.save(f'source-job-{index}.json',obj);self.create(obj)
    deadline=time.monotonic()+max(180,stage['count']/stage['rate']*3+60)+drain;drain_deadline=None
    while time.monotonic()<deadline:
     state=self.state();job=json.loads(self.k('get','job','benchmark-source-'+str(index),'-o','json'));status=job.get('status',{})
     broker=self.broker();row=dict(observed_unix_ns=time.time_ns(),stage=index,state=state,job=status,broker=broker);progress.write(json.dumps(row,separators=(',',':'))+'\n');progress.flush()
     if state['status']=='INVALID' or status.get('failed',0):raise RuntimeError('source/matching failed: '+json.dumps(state))
     done=bool(status.get('succeeded',0))
     if done and drain_deadline is None:drain_deadline=time.monotonic()+drain
     if done and state['source']==end and state['broker']==end+index and state['pending']==0:break
     if drain_deadline and time.monotonic()>drain_deadline:raise RuntimeError('drain deadline exceeded')
     time.sleep(2)
    else:raise RuntimeError('source/commit window exceeded deadline')
    self.verify_orders(index,end);self.save(f'broker-stage-{index}.json',self.broker());self.save(f'checkpoint-stage-{index}.json',self.state())
    now=self.app_pods()
    if now!=before:raise RuntimeError('application restarted or replaced during measurement')
    print(f'{self.namespace}: {stage["count"]} events at requested {stage["rate"]}/s committed; pending=0',flush=True);first=end+1
  self.save('pods-after-traffic.json',self.app_pods());self.save('nodes-after.json',json.loads(self.k('get','nodes','-o','json')))
  for app in['publisher','matching-service']+(['outcome-relay']if enabled else[]):(self.out/(app+'.log')).write_text(self.k('logs','deployment/'+app))
  for app in ['publisher','matching-service']+(['outcome-relay']if enabled else[]):self.k('scale','deployment/'+app,'--replicas=0')
  for app in ['publisher','matching-service']+(['outcome-relay']if enabled else[]):self.k('wait','--for=delete','pod','-l','app='+app,'--timeout=90s',timeout=110)
  reader=load.monitor_object('publisher-reader');reader['spec']['nodeSelector']={'mtbt.lane':'a'};reader['spec']['volumes']=[{'name':'publisher','persistentVolumeClaim':{'claimName':'publisher-evidence'}}];reader['spec']['containers'][0]['volumeMounts']=[{'name':'publisher','mountPath':'/publisher','readOnly':True}]
  self.create(reader);self.k('wait','--for=condition=Ready','pod/publisher-reader','--timeout=120s',timeout=140)
  try:
   for pod,mount,prefix in [('benchmark-reader','metrics','consumer'),('publisher-reader','publisher','publisher')]:
    paths=self.k('exec',pod,'--','sh','-c',f'ls /{mount}/trace-*.csv').splitlines()
    if len(paths)!=1:raise ValueError('exactly one trace per non-restarted app expected')
    (self.out/(prefix+'.csv')).write_text(self.k('exec',pod,'--','cat',paths[0]))
  finally:self.k('delete','pod','publisher-reader','--wait=false')
  for index in range(1,len(stages)+1):(self.out/f'source-{index}.csv').write_text(self.k('exec','benchmark-reader','--','cat',f'/source/source-{index}.csv'))
  self.save('broker-final.json',self.broker());self.save('retained-after.json',self.snapshot())
  if json.loads((self.out/'retained-before.json').read_text())!=self.snapshot():raise ValueError('protected namespace specification changed')
  self.save('execution.json',dict(complete=True,source_total=total,raw_packets=total+len(stages),outcomes_enabled=enabled,no_app_restarts=True,exact_stage_orders=True,paused_after_measurement=True,protected_namespace_specs_unchanged=True))
 def verify_orders(self,index,end):
  # Source writes final total, even for a prefix Job: compute prefix independently.
  import struct
  oracle=[]
  for n in range(1,end+1):
   mod=n%3;q=100 if mod==1 else 30 if mod==2 else 70;remaining=100 if mod==1 and n==end else 70 if mod==1 and n+1==end else 0
   oracle.append(dict(id=struct.pack('<d',float(n)).hex(),token=1,side='B'if mod==1 else'S',price=10000,accepted_quantity=q,remaining_quantity=remaining,filled_quantity=q-remaining,cancelled_quantity=0,status='OPEN'if remaining==100 else'PARTIALLY_FILLED'if remaining else'FILLED'))
  actual=json.loads(self.sql("SELECT coalesce(json_agg(row_to_json(t)),'[]') FROM (SELECT encode(order_id,'hex') id,token,side,price,accepted_quantity,remaining_quantity,filled_quantity,cancelled_quantity,status FROM mtbt.orders WHERE stream_id=1 ORDER BY order_id) t"))
  if sorted(actual,key=lambda r:r['id'])!=sorted(oracle,key=lambda r:r['id']):raise ValueError('exact independent DB order oracle mismatch')
  if self.sql('SELECT count(*) FROM mtbt.active_orders WHERE stream_id=1')!='0':raise ValueError('duplicate reconstruction order rows')
  flags={key:self.sql('SHOW '+key)for key in('fsync','synchronous_commit','full_page_writes')}
  if any(v!='on'for v in flags.values()):raise ValueError('PostgreSQL durability weakened')
  self.save(f'orders-stage-{index}.json',actual);self.save(f'oracle-stage-{index}.json',oracle);self.save('postgres-durability.json',flags)
 def readback(self):
  secret=json.loads(self.k('get','secret','mtbt-credentials','-o','json'))['data'];url=base64.b64decode(secret['NATS_URL']).decode();port=14226
  url=url.replace('@nats:4222',f'@127.0.0.1:{port}')
  p=subprocess.Popen(['kubectl','--kubeconfig',str(lab.KUBECONFIG),'--context',lab.CONTEXT,'-n',self.namespace,'port-forward','service/nats',f'{port}:4222'],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
  try:
   deadline=time.monotonic()+20
   while time.monotonic()<deadline:
    if p.poll()is not None:raise RuntimeError('NATS port-forward exited')
    if select.select([p.stdout],[],[],.2)[0]and'Forwarding from 127.0.0.1:'in p.stdout.readline():break
   else:raise RuntimeError('port-forward not ready')
   settings=json.loads((self.out/'settings.json').read_text());ends=[];total=0
   for s in settings['stages']:total+=s['count'];ends.append(str(total))
   subprocess.run(['bin/outcome-readback','-manifest',str(self.out/'manifest.json'),'-windows',','.join(ends),'-out',str(self.out),'-outcomes='+str(settings['enabled']).lower()],env={**os.environ,'NATS_URL':url},check=True,timeout=210)
  finally:
   p.terminate()
   try:p.wait(10)
   except subprocess.TimeoutExpired:p.kill();p.wait()
  self.k('delete','pod','benchmark-reader','--wait=false')


def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['build','deploy','run','readback']);p.add_argument('--out',required=True,type=Path);p.add_argument('--namespace');p.add_argument('--enabled',action='store_true');p.add_argument('--delivery-mode',choices=['notify','poll'],default='notify');p.add_argument('--profile',default='10000@100,10000@1000');p.add_argument('--drain',default=240,type=int);a=p.parse_args();out=a.out.resolve()
 if not out.is_relative_to(ROOT):p.error('output must be inside project')
 if a.action=='build':
  lab.guard()
  out.mkdir(parents=True,exist_ok=False)
  with(out/'image-build.log').open('w')as f:subprocess.run(['docker','build','-t',IMAGE,'.'],cwd=ROOT,stdout=f,stderr=subprocess.STDOUT,check=True)
  (out/'image-load.log').write_text(lab.command(['kind','load','docker-image',IMAGE,'--name',lab.CLUSTER],timeout=600));return
 if not a.namespace:p.error('--namespace required')
 lab.guard();l=Lab(a.namespace,out,a.delivery_mode)
 if a.action=='deploy':l.install(a.enabled,plan(a.profile))
 elif a.action=='run':l.run(a.drain)
 else:l.readback()
if __name__=='__main__':main()
