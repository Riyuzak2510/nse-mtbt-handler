#!/usr/bin/env python3
"""Fresh-only, equal aggregate budget, combined vs dedicated-ticker Kubernetes benchmark."""
import argparse,base64,datetime,hashlib,json,os,re,secrets,select,struct,subprocess,time
from pathlib import Path
import k8s_matching_lab as lab
import k8s_load as load
from k8s_outcome_benchmark import Lab as Base
ROOT=lab.ROOT
IMAGE='nse-mtbt-handler:ticker-bench-20261102'
PROTECTED=('nse-mtbt-lab','nse-mtbt-matching','nse-mtbt-outcomes','nse-mtbt-tickers')

def validate_namespace(ns):
 if not re.fullmatch(r'nse-mtbt-ticker-bench-(combined|tickers)-[a-z0-9]+(?:-[a-z0-9]+)*',ns)or len(ns)>63:raise ValueError('fresh benchmark-only namespace required')
 return ns

def stages(text):
 result=load.profile_stages(text)
 if sum(x['count']for x in result)>99999 or any(x['count']<2 or x['rate']>1000 for x in result):raise ValueError('bounded stages:2..99999 total, rate<=1000')
 return result

def expected_orders(end):
 result=[]
 for n in range(1,end+1):
  token=(n-1)%2+1;local=(n-1)//2+1;last=(end-token)//2+1;mod=local%3
  q=100 if mod==1 else 30 if mod==2 else 70
  remain=(100 if last==local else 70 if last==local+1 else 0)if mod==1 else 0
  result.append(dict(id=struct.pack('<d',float(n)).hex(),token=token,side='B'if mod==1 else'S',price=10000,accepted_quantity=q,remaining_quantity=remain,filled_quantity=q-remain,cancelled_quantity=0,status='OPEN'if remain==100 else'PARTIALLY_FILLED'if remain else'FILLED'))
 return sorted(result,key=lambda r:r['id'])

class Bench(Base):
 def __init__(self,ns,out):self.namespace=validate_namespace(ns);self.out=out;self.topology='tickers'if'-tickers-'in ns else'combined'
 def snapshot(self):
  result={}
  for ns in PROTECTED:
   result[ns]={}
   for kind in('deployments','statefulsets','persistentvolumeclaims'):
    items=json.loads(lab.command(['kubectl','--kubeconfig',str(lab.KUBECONFIG),'--context',lab.CONTEXT,'-n',ns,'get',kind,'-o','json']))['items']
    result[ns][kind]=[dict(name=o['metadata']['name'],uid=o['metadata']['uid'],generation=o['metadata'].get('generation'),spec=o['spec'])for o in items]
  return result
 def apps(self):return ['publisher','outcome-relay']+(['matching-service']if self.topology=='combined'else['order-service','ticker-matcher-1','ticker-matcher-2'])
 def app_pods(self):
  pods=json.loads(self.k('get','pods','-o','json'))['items']
  return{p['metadata']['labels']['app']:dict(uid=p['metadata']['uid'],restarts=sum(c.get('restartCount',0)for c in p.get('status',{}).get('containerStatuses',[])),phase=p['status']['phase'])for p in pods if p['metadata'].get('labels',{}).get('app')in self.apps()+['nats','postgres']}
 def state(self):
  result=super().state()
  if self.topology=='tickers':
   result['tickers']=json.loads(self.sql("SELECT json_agg(row_to_json(t)) FROM (SELECT p.token,p.admitted,p.published,a.applied FROM mtbt.ticker_progress p JOIN mtbt.ticker_applied a USING(stream_id,token) WHERE stream_id=1 ORDER BY p.token)t"))
   result['routing']=int(self.sql('SELECT count(*) FROM mtbt.match_work WHERE stream_id=1'))
  return result
 def install(self,profile):
  lab.guard()
  if self.k('get','namespace',self.namespace,'--ignore-not-found').strip():raise ValueError('refusing existing namespace/history')
  self.out.mkdir(parents=True,exist_ok=False)
  self.save('retained-before.json',self.snapshot());self.save('nodes-before.json',json.loads(self.k('get','nodes','-o','json')))
  self.create({'apiVersion':'v1','kind':'Namespace','metadata':{'name':self.namespace}})
  pw,npw=secrets.token_hex(24),secrets.token_hex(24)
  self.create({'apiVersion':'v1','kind':'Secret','metadata':{'name':'mtbt-credentials'},'stringData':{'POSTGRES_PASSWORD':pw,'DATABASE_URL':f'postgres://mtbt:{pw}@postgres:5432/mtbt?sslmode=disable','NATS_URL':f'nats://mtbt:{npw}@nats:4222','nats.conf':'port:4222\nhttp:8222\njetstream {store_dir:"/data/jetstream",max_mem_store:64MB,max_file_store:2GB}\nauthorization {user:mtbt,password:"'+npw+'"}\n'}})
  m=dict(version=2,profile='cm-mtbt-le64-v1',stream_id=1,seed=42,baseline='empty',created_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),instruments=[dict(token=i,symbol='SIM'+str(i),tick_size=5,base_price=10000)for i in(1,2)])
  self.save('manifest.json',m);self.create({'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':'mtbt-source'},'immutable':True,'data':{'manifest.json':json.dumps(m)}})
  overlay='deploy/tickers'if self.topology=='tickers'else'deploy/outcomes'
  origin='nse-mtbt-tickers'if self.topology=='tickers'else'nse-mtbt-outcomes'
  rendered=lab.command(['kubectl','kustomize',overlay]).replace('namespace: '+origin,'namespace: '+self.namespace)
  resources=lab.resource_list(self.k('create','--dry-run=client','-f','-','-o','json',stdin=rendered))
  total=sum(s['count']for s in profile)
  for obj in resources['items']:
   obj['metadata']['namespace']=self.namespace
   if obj['kind']=='PersistentVolumeClaim':obj['spec']['resources']['requests']['storage']='2Gi'if obj['metadata']['name']in('postgres-data','nats-data')else'128Mi'
   if obj['kind']not in('Deployment','StatefulSet'):continue
   pod=obj['spec']['template']['spec'];app=obj['metadata']['name']
   for c in pod['containers']:
    if c['image']=='nse-mtbt-handler:lab':c['image']=IMAGE
    c['env']=[e for e in c.get('env',[])if e['name']not in('MTBT_LOAD_METRICS','MTBT_SQL_METRICS','MTBT_INGEST_METRICS')]
   if obj['kind']=='Deployment':
    obj['spec']['replicas']=0;c=pod['containers'][0]
    if app in('matching-service','order-service','ticker-matcher-1','ticker-matcher-2'):
     # Match placement and aggregate admission+matching limits:3CPU/1280Mi; requests300m/640Mi.
     pod['nodeSelector']={'mtbt.lane':'b'}
     if app=='matching-service':
      c['resources']={'requests':{'cpu':'300m','memory':'640Mi'},'limits':{'cpu':'3','memory':'1280Mi'}}
      c['env']=[e for e in c['env']if e['name']!='GOMEMLIMIT']+[{'name':'GOMEMLIMIT','value':'960MiB'}]
     if app in self.apps():
      c['args']+=['-timings=/evidence/'+app+'.csv','-timings-limit='+str(total+2*len(profile)+10)]
      if app!='matching-service':
       pod['securityContext']['fsGroup']=10001
       pod['volumes'].append({'name':'evidence','persistentVolumeClaim':{'claimName':'matching-evidence'}})
       c['volumeMounts'].append({'name':'evidence','mountPath':'/evidence'})
    if app=='publisher':c['args']+=['-timings=/evidence/publisher.csv','-timings-limit='+str(total+len(profile)+10)]
  self.save('resources.json',resources)
  self.k('apply','--dry-run=server','-f','-',stdin=json.dumps(resources));self.k('apply','-f','-',stdin=json.dumps(resources))
  for app in('postgres','nats'):self.k('rollout','status','statefulset/'+app,'--timeout=240s',timeout=260)
  self.k('scale','deployment/publisher','--replicas=1');self.k('rollout','status','deployment/publisher','--timeout=120s',timeout=140)
  primary='order-service'if self.topology=='tickers'else'matching-service'
  self.k('scale','deployment/'+primary,'--replicas=1')
  if self.topology=='tickers':
   self.wait(lambda:self.sql('SELECT count(*) FROM mtbt.ticker_topology')=='1','intake initialization')
   for app in('ticker-matcher-1','ticker-matcher-2'):self.k('scale','deployment/'+app,'--replicas=1')
  else:self.wait(lambda:'restored through broker=0'in self.k('logs','deployment/matching-service','--tail=30'),'combined initialization')
  self.k('scale','deployment/outcome-relay','--replicas=1');self.k('rollout','status','deployment/outcome-relay','--timeout=120s',timeout=140)
  for app in self.apps():self.wait(lambda app=app:any(p['status'].get('phase')=='Running'and all(x.get('started',False)for x in p['status'].get('containerStatuses',[]))for p in json.loads(self.k('get','pods','-l','app='+app,'-o','json'))['items']),app+' startup')
  monitor=load.monitor_object('benchmark-reader');monitor['spec']['volumes']=[{'name':n,'persistentVolumeClaim':{'claimName':claim}}for n,claim in[('metrics','matching-evidence'),('source','source-evidence')]];monitor['spec']['containers'][0]['volumeMounts']=[{'name':n,'mountPath':'/'+n,'readOnly':True}for n in('metrics','source')]
  self.create(monitor);self.k('wait','--for=condition=Ready','pod/benchmark-reader','--timeout=120s',timeout=140)
  self.save('settings.json',dict(namespace=self.namespace,topology=self.topology,image=IMAGE,image_id=lab.command(['docker','image','inspect',IMAGE,'--format','{{.Id}}']).strip(),enabled=True,stages=profile,clock='same Linux VM, no synchronized independent hosts',warmup_excluded=0,aggregate_order_matching_cpu_limit=3,aggregate_order_matching_memory_mib=1280,aggregate_order_matching_cpu_request_millicores=300,aggregate_order_matching_memory_request_mib=640,placement='all order/matching processes lane b; PG lane a; NATS/relay/source lane b; publisher lane a',delivery_mode='notify',benchmark_degraded_control_plane=True,workload='two equally interleaved tickers, local triplet buy100/sell30/sell70; N-only plus one heartbeat per stage'))
  print(self.namespace+': deployed fresh',flush=True)
 def make_job(self,index,start,end,total,rate):
  o=super().make_job(index,start,end,total,rate);c=o['spec']['template']['spec']['containers'][0];c['image']=IMAGE;c['command']=['/app/ticker-workload'];c['args']=['-manifest=/config/manifest.json','-addr=publisher:19001','-allow-non-loopback','-start='+str(start),'-end='+str(end),'-rate='+str(rate),'-timings=/evidence/source-'+str(index)+'.csv'];return o
 def verify_orders(self,index,end):
  actual=json.loads(self.sql("SELECT coalesce(json_agg(row_to_json(t)),'[]') FROM (SELECT encode(order_id,'hex') id,token,side,price,accepted_quantity,remaining_quantity,filled_quantity,cancelled_quantity,status FROM mtbt.orders WHERE stream_id=1 ORDER BY order_id)t"));expected=expected_orders(end)
  if sorted(actual,key=lambda x:x['id'])!=expected:raise ValueError('all-field order oracle mismatch')
  if self.sql('SELECT count(*) FROM mtbt.active_orders')!='0':raise ValueError('redundant reconstruction rows')
  flags={n:self.sql('SHOW '+n)for n in('fsync','synchronous_commit','full_page_writes')}
  if set(flags.values())!={'on'}:raise ValueError('SQL durability weakened')
  self.save(f'orders-stage-{index}.json',actual);self.save(f'oracle-stage-{index}.json',expected);self.save('postgres-durability.json',flags)
 def run(self,drain):
  lab.guard();cfg=json.loads((self.out/'settings.json').read_text());profile=cfg['stages'];total=sum(s['count']for s in profile)
  if self.state()['source']!=0 or json.loads(self.k('get','jobs','-o','json'))['items']:raise ValueError('fresh source/no jobs required; no replay')
  before=self.app_pods();self.save('pods-before.json',before);first=1
  with(self.out/'progress.jsonl').open('x')as f:
   for index,stage in enumerate(profile,1):
    end=first+stage['count']-1;obj=self.make_job(index,first,end,total,stage['rate']);self.save(f'source-job-{index}.json',obj);self.create(obj)
    deadline=time.monotonic()+max(180,stage['count']/stage['rate']*3+60)+drain;drain_until=None
    while time.monotonic()<deadline:
     job=json.loads(self.k('get','job/benchmark-source-'+str(index),'-o','json'))['status'];state=self.state()
     f.write(json.dumps(dict(stage=index,observed_unix_ns=time.time_ns(),job=job,state=state,broker=self.broker()),separators=(',',':'))+'\n');f.flush()
     if state['status']=='INVALID'or job.get('failed'):raise RuntimeError('invalid input/failed source')
     if job.get('succeeded')and drain_until is None:drain_until=time.monotonic()+drain
     drained=state['pending']==0
     if self.topology=='tickers':drained=drained and state['routing']==0 and all(p['admitted']==p['published']==p['applied']for p in state['tickers'])
     if job.get('succeeded')and state['source']==end and state['broker']==end+index and drained:break
     if drain_until and time.monotonic()>drain_until:raise RuntimeError('bounded drain deadline exceeded')
     time.sleep(2)
    else:raise RuntimeError('stage deadline exceeded')
    if self.app_pods()!=before:raise RuntimeError('process replaced or restarted during measurement')
    self.verify_orders(index,end);self.save(f'checkpoint-stage-{index}.json',self.state());self.save(f'broker-stage-{index}.json',self.broker());print(self.namespace+f': stage{index} exact {stage["count"]} inputs, queues drained',flush=True);first=end+1
  self.save('pods-after-traffic.json',self.app_pods());self.save('nodes-after.json',json.loads(self.k('get','nodes','-o','json')))
  for app in self.apps():(self.out/(app+'.log')).write_text(self.k('logs','deployment/'+app))
  for app in self.apps():self.k('scale','deployment/'+app,'--replicas=0')
  for app in self.apps():self.k('wait','--for=delete','pod','-l','app='+app,'--timeout=90s',timeout=110)
  reader=load.monitor_object('publisher-reader');reader['spec']['nodeSelector']={'mtbt.lane':'a'};reader['spec']['volumes']=[{'name':'publisher','persistentVolumeClaim':{'claimName':'publisher-evidence'}}];reader['spec']['containers'][0]['volumeMounts']=[{'name':'publisher','mountPath':'/publisher','readOnly':True}]
  self.create(reader);self.k('wait','--for=condition=Ready','pod/publisher-reader','--timeout=120s',timeout=140)
  try:
   for app in self.apps():
    if app=='outcome-relay':continue
    pod,mount=('publisher-reader','publisher')if app=='publisher'else('benchmark-reader','metrics')
    (self.out/(app+'.csv')).write_text(self.k('exec',pod,'--','cat','/'+mount+'/'+app+'.csv'))
  finally:self.k('delete','pod/publisher-reader','--wait=false')
  for index in range(1,len(profile)+1):(self.out/f'source-{index}.csv').write_text(self.k('exec','benchmark-reader','--','cat',f'/source/source-{index}.csv'))
  self.save('broker-final.json',self.broker());self.save('retained-after.json',self.snapshot())
  if self.snapshot()!=json.loads((self.out/'retained-before.json').read_text()):raise ValueError('protected state changed')
  self.save('execution.json',dict(complete=True,source_total=total,raw_packets=total+len(profile),outcomes_enabled=True,no_app_restarts=True,exact_stage_orders=True,paused_after_measurement=True,protected_namespace_specs_unchanged=True,topology=self.topology))
 def readback(self):
  if not json.loads((self.out/'execution.json').read_text()).get('complete'):raise ValueError('completed run required')
  secret=json.loads(self.k('get','secret','mtbt-credentials','-o','json'))['data'];url=base64.b64decode(secret['NATS_URL']).decode().replace('@nats:4222','@127.0.0.1:14228')
  p=subprocess.Popen(['kubectl','--kubeconfig',str(lab.KUBECONFIG),'--context',lab.CONTEXT,'-n',self.namespace,'port-forward','service/nats','14228:4222'],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
  try:
   deadline=time.monotonic()+20
   while time.monotonic()<deadline:
    if p.poll()is not None:raise RuntimeError('port forward exited')
    if select.select([p.stdout],[],[],.2)[0]and'Forwarding from 127.0.0.1:'in p.stdout.readline():break
   else:raise RuntimeError('port forward not ready')
   profile=json.loads((self.out/'settings.json').read_text())['stages'];n=0;ends=[]
   for stage in profile:n+=stage['count'];ends.append(str(n))
   subprocess.run(['bin/ticker-benchmark-readback','-manifest',str(self.out/'manifest.json'),'-windows',','.join(ends),'-topology',self.topology,'-out',str(self.out)],env={**os.environ,'NATS_URL':url},check=True,timeout=360)
  finally:
   p.terminate()
   try:p.wait(5)
   except subprocess.TimeoutExpired:p.kill();p.wait()
  self.k('delete','pod/benchmark-reader','--wait=false')

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['build','deploy','run','readback']);p.add_argument('--out',required=True,type=Path);p.add_argument('--namespace');p.add_argument('--profile',default='10000@100,10000@1000');p.add_argument('--drain',type=int,default=240);a=p.parse_args();out=a.out.resolve()
 if out==ROOT or ROOT not in out.parents:p.error('project output required')
 lab.guard()
 if a.action=='build':
  out.mkdir(parents=True,exist_ok=False)
  for name,args in [('image-build.log',['docker','build','-t',IMAGE,'.']),('image-load.log',['kind','load','docker-image',IMAGE,'--name',lab.CLUSTER])]:
   with(out/name).open('x')as f:subprocess.run(args,cwd=ROOT,stdout=f,stderr=subprocess.STDOUT,check=True)
  (out/'image.json').write_text(json.dumps(dict(image=IMAGE,id=lab.command(['docker','image','inspect',IMAGE,'--format','{{.Id}}']).strip()),indent=2)+'\n')
  (out/'source-sha256.json').write_text(json.dumps({str(p):hashlib.sha256(p.read_bytes()).hexdigest()for p in sorted([Path('Dockerfile'),Path('go.mod'),Path('go.sum'),*Path('cmd').rglob('*.go'),*Path('internal').rglob('*.go'),*Path('internal/store').glob('*.sql')])},indent=2)+'\n');return
 if not a.namespace:p.error('--namespace required')
 b=Bench(a.namespace,out)
 if a.action=='deploy':b.install(stages(a.profile))
 elif a.action=='run':b.run(a.drain)
 else:b.readback()
if __name__=='__main__':main()
