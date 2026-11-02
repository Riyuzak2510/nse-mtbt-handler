#!/usr/bin/env python3
"""Guarded fresh-only two-ticker Kubernetes correctness proof, not a latency benchmark."""
import argparse,base64,datetime,hashlib,json,os,secrets,select,subprocess,time
from pathlib import Path
import k8s_matching_lab as lab
from k8s_outcome_benchmark import Lab

ROOT=lab.ROOT;NAMESPACE='nse-mtbt-tickers';IMAGE='nse-mtbt-handler:tickers-20261102'
def kube(*args,stdin=None,timeout=120):return lab.command(['kubectl','--kubeconfig',str(lab.KUBECONFIG),'--context',lab.CONTEXT,'-n',NAMESPACE,*args],stdin,timeout)
def sql(q):return kube('exec','postgres-0','--','psql','-X','-U','mtbt','-d','mtbt','-At','-v','ON_ERROR_STOP=1','-c',q).strip()
def save(out,name,v):out.joinpath(name).write_text(json.dumps(v,indent=2)+'\n')
def snapshot():return Lab('nse-mtbt-load-outcome-bench-on-snapshot',Path('.')).snapshot()
def wait(check,label,seconds=180):
 deadline=time.monotonic()+seconds
 while time.monotonic()<deadline:
  try:
   value=check()
   if value:return value
  except (RuntimeError,ValueError,KeyError):pass
  time.sleep(1)
 raise RuntimeError(label+' timed out; inspect retained namespace, do not replay/reset')
def build(out):
 lab.guard();out.mkdir(parents=True,exist_ok=False)
 for name,args in [('image-build.log',['docker','build','-t',IMAGE,'.']),('image-load.log',['kind','load','docker-image',IMAGE,'--name','nse-mtbt-lab'])]:
  with(out/name).open('x')as f:subprocess.run(args,cwd=ROOT,stdout=f,stderr=subprocess.STDOUT,check=True)
 save(out,'image.json',dict(image=IMAGE,id=lab.command(['docker','image','inspect',IMAGE,'--format','{{.Id}}']).strip()))
 save(out,'source-sha256.json',{str(p):hashlib.sha256(p.read_bytes()).hexdigest()for p in sorted([Path('Dockerfile'),Path('go.mod'),Path('go.sum'),*Path('cmd').rglob('*.go'),*Path('internal').rglob('*.go'),*Path('internal/store').glob('*.sql')])})
def setup(out):
 lab.guard()
 if kube('get','namespace',NAMESPACE,'--ignore-not-found').strip():raise ValueError('refusing existing ticker namespace/history')
 out.mkdir(parents=True,exist_ok=False);save(out,'retained-before.json',snapshot());save(out,'nodes-before.json',json.loads(kube('get','nodes','-o','json')))
 kube('create','-f','-',stdin=json.dumps({'apiVersion':'v1','kind':'Namespace','metadata':{'name':NAMESPACE}}))
 pw,npw=secrets.token_hex(24),secrets.token_hex(24)
 kube('create','-f','-',stdin=json.dumps({'apiVersion':'v1','kind':'Secret','metadata':{'name':'mtbt-credentials'},'stringData':{'POSTGRES_PASSWORD':pw,'DATABASE_URL':f'postgres://mtbt:{pw}@postgres:5432/mtbt?sslmode=disable','NATS_URL':f'nats://mtbt:{npw}@nats:4222','nats.conf':'port:4222\nhttp:8222\njetstream {store_dir:"/data/jetstream",max_mem_store:64MB,max_file_store:2GB}\nauthorization {user:mtbt,password:"'+npw+'"}\n'}}))
 manifest=dict(version=2,profile='cm-mtbt-le64-v1',stream_id=1,seed=42,baseline='empty',created_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),instruments=[dict(token=i,symbol='SIM'+str(i),tick_size=5,base_price=10000)for i in(1,2)])
 save(out,'manifest.json',manifest);kube('create','-f','-',stdin=json.dumps({'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':'mtbt-source'},'immutable':True,'data':{'manifest.json':json.dumps(manifest)}}))
 rendered=lab.command(['kubectl','kustomize','deploy/tickers']);resources=lab.resource_list(kube('create','--dry-run=client','-f','-','-o','json',stdin=rendered))
 for obj in resources['items']:
  if obj['kind']=='PersistentVolumeClaim':obj['spec']['resources']['requests']['storage']='2Gi'if obj['metadata']['name']in('postgres-data','nats-data')else'128Mi'
  if obj['kind']in('Deployment','StatefulSet'):
   pod=obj['spec']['template']['spec']
   for c in pod['containers']:
    if c['image']=='nse-mtbt-handler:lab':c['image']=IMAGE
    c['env']=[e for e in c.get('env',[])if e['name']not in('MTBT_LOAD_METRICS','MTBT_SQL_METRICS','MTBT_INGEST_METRICS')]
   if obj['kind']=='Deployment':obj['spec']['replicas']=0
 save(out,'resources.json',resources);kube('apply','--dry-run=server','-f','-',stdin=json.dumps(resources));kube('apply','-f','-',stdin=json.dumps(resources))
 for app in('postgres','nats'):kube('rollout','status','statefulset/'+app,'--timeout=240s',timeout=260)
 for app in('publisher','order-service'):
  kube('scale','deployment/'+app,'--replicas=1')
  wait(lambda:bool(json.loads(kube('get','pods','-l','app='+app,'-o','json'))['items'])and all(o['status'].get('phase')=='Running'for o in json.loads(kube('get','pods','-l','app='+app,'-o','json'))['items']),app+' running')
 wait(lambda:sql('SELECT count(*) FROM mtbt.ticker_topology WHERE stream_id=1')=='1','intake topology initialized')
 for app in('ticker-matcher-1','outcome-relay'):kube('scale','deployment/'+app,'--replicas=1')
 wait(lambda:sql('SELECT count(*) FROM mtbt.ticker_applied WHERE stream_id=1')=='2','ticker progress')
 save(out,'settings.json',dict(namespace=NAMESPACE,image=IMAGE,image_id=lab.command(['docker','image','inspect',IMAGE,'--format','{{.Id}}']).strip(),fixture='two interleaved instruments,12 native NEW,2 heartbeat,20 order outcomes',token2_initially_paused=True))
def job(name,start,end):
 if kube('get','job',name,'--ignore-not-found').strip():raise ValueError('refusing existing source job')
 obj={'apiVersion':'batch/v1','kind':'Job','metadata':{'name':name},'spec':{'backoffLimit':0,'activeDeadlineSeconds':60,'template':{'metadata':{'labels':{'app':name}},'spec':{'restartPolicy':'Never','nodeSelector':{'mtbt.lane':'b'},'automountServiceAccountToken':False,'securityContext':{'runAsNonRoot':True,'runAsUser':10001,'runAsGroup':10001},'containers':[{'name':'source','image':IMAGE,'command':['/app/ticker-workload'],'args':['-manifest=/config/manifest.json','-addr=publisher:19001','-allow-non-loopback','-start='+str(start),'-end='+str(end),'-rate=10'],'resources':{'requests':{'cpu':'50m','memory':'32Mi'},'limits':{'cpu':'1','memory':'128Mi'}},'volumeMounts':[{'name':'config','mountPath':'/config','readOnly':True}]}],'volumes':[{'name':'config','configMap':{'name':'mtbt-source'}}]}}}}
 kube('create','-f','-',stdin=json.dumps(obj));kube('wait','--for=condition=complete','job/'+name,'--timeout=90s',timeout=110)
def orders():return json.loads(sql("SELECT coalesce(json_agg(row_to_json(t)),'[]') FROM (SELECT encode(order_id,'hex') id,token,side,price,accepted_quantity,remaining_quantity,filled_quantity,cancelled_quantity,status FROM mtbt.orders WHERE stream_id=1 ORDER BY order_id)t"))
def progress():return json.loads(sql("SELECT json_agg(row_to_json(t)) FROM (SELECT p.token,p.admitted,p.published,a.applied,a.applied_digest,a.source_sequence,a.source_broker FROM mtbt.ticker_progress p JOIN mtbt.ticker_applied a USING(stream_id,token) WHERE stream_id=1 ORDER BY token)t"))
def restart(app,out):
 old=json.loads(kube('get','pods','-l','app='+app,'-o','json'))['items'][0]['metadata']['uid']
 kube('scale','deployment/'+app,'--replicas=0');wait(lambda:not json.loads(kube('get','pods','-l','app='+app,'-o','json'))['items'],app+' stopped')
 kube('scale','deployment/'+app,'--replicas=1');new=wait(lambda:next((p['metadata']['uid']for p in json.loads(kube('get','pods','-l','app='+app,'-o','json'))['items']if p['metadata']['uid']!=old and p['status'].get('phase')=='Running'),None),app+' replacement')
 save(out,app+'-replacement.json',dict(before=old,after=new))
def run(out):
 lab.guard()
 if sql('SELECT source_sequence FROM mtbt.feed_state WHERE stream_id=1')!='0' or json.loads(kube('get','jobs','-o','json'))['items']:raise ValueError('fresh stream/no jobs required; refusing replay')
 job('ticker-prefix',1,4)
 wait(lambda:sql('SELECT source_sequence::text||\'|\'||broker_sequence::text FROM mtbt.feed_state WHERE stream_id=1')=='4|5'and sql('SELECT applied FROM mtbt.ticker_applied WHERE stream_id=1 AND token=1')=='3','prefix applied only ticker1')
 rows=orders();save(out,'prefix-orders.json',rows);save(out,'prefix-progress.json',progress())
 byid={r['id']:r for r in rows};import struct
 a=byid[struct.pack('<d',1.0).hex()];assert(a['status'],a['remaining_quantity'],a['filled_quantity'])==('PARTIALLY_FILLED',70,30)
 assert [r['status']for r in rows if r['token']==2]==['PENDING','PENDING'];assert sql('SELECT applied FROM mtbt.ticker_applied WHERE stream_id=1 AND token=2')=='0'
 restart('order-service',out);restart('ticker-matcher-1',out)
 assert orders()==rows
 kube('scale','deployment/ticker-matcher-2','--replicas=1');wait(lambda:sql('SELECT applied FROM mtbt.ticker_applied WHERE stream_id=1 AND token=2')=='3','pending ticker2 recovery')
 save(out,'recovered-prefix-orders.json',orders());job('ticker-rest',5,12)
 wait(lambda:sql('SELECT source_sequence::text||\'|\'||broker_sequence::text FROM mtbt.feed_state WHERE stream_id=1')=='12|14'and sql('SELECT min(applied) FROM mtbt.ticker_applied WHERE stream_id=1')=='8','complete input')
 wait(lambda:sql('SELECT count(*) FROM mtbt.match_work WHERE stream_id=1')=='0'and sql('SELECT count(*) FROM mtbt.order_outbox WHERE stream_id=1')=='0','routing and outcome drain')
 rows=orders();expected=[]
 for n in range(1,13):
  local=(n-1)//2+1;q=100 if local%3==1 else 30 if local%3==2 else 70
  expected.append(dict(id=struct.pack('<d',float(n)).hex(),token=(n-1)%2+1,side='B'if local%3==1 else'S',price=10000,accepted_quantity=q,remaining_quantity=0,filled_quantity=q,cancelled_quantity=0,status='FILLED'))
 assert sorted(rows,key=lambda r:r['id'])==sorted(expected,key=lambda r:r['id'])
 save(out,'orders-final.json',rows);save(out,'oracle-final.json',expected);save(out,'progress-final.json',progress())
 broker=json.loads(kube('exec','nats-0','--','wget','-qO-','http://127.0.0.1:8222/jsz?streams=true&consumers=true'));save(out,'broker-final.json',broker)
 counts={s['name']:s['state']['messages']for a in broker.get('account_details',[])for s in a.get('stream_detail',[])};assert counts=={'MTBT_1':14,'MATCH_1':16,'MTBT_OUTCOMES_1':20},counts
 for app in('publisher','order-service','ticker-matcher-1','ticker-matcher-2','outcome-relay'):(out/(app+'.log')).write_text(kube('logs','deployment/'+app))
 after=snapshot();save(out,'retained-after.json',after);assert after==json.loads((out/'retained-before.json').read_text())
 save(out,'nodes-after.json',json.loads(kube('get','nodes','-o','json')));save(out,'pods-after.json',json.loads(kube('get','pods','-o','json')))
 save(out,'execution.json',dict(complete=True,native_inputs=12,raw_packets=14,routed_commands=16,outcomes=20,exact_final_orders=True,token2_pending_while_token1_matched=True,order_and_token1_pods_replaced=True,partial_fill_restored=True,protected_namespace_specs_unchanged=True,latency_benchmark=False))
def readback(out):
 lab.guard()
 if not json.loads((out/'execution.json').read_text()).get('complete'):raise ValueError('complete smoke required')
 secret=json.loads(kube('get','secret','mtbt-credentials','-o','json'))['data'];url=base64.b64decode(secret['NATS_URL']).decode().replace('@nats:4222','@127.0.0.1:14227')
 args=['kubectl','--kubeconfig',str(lab.KUBECONFIG),'--context',lab.CONTEXT,'-n',NAMESPACE,'port-forward','service/nats','14227:4222']
 p=subprocess.Popen(args,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
 try:
  deadline=time.monotonic()+20
  while time.monotonic()<deadline:
   if p.poll()is not None:raise RuntimeError('port-forward exited')
   if select.select([p.stdout],[],[],.2)[0]and'Forwarding from 127.0.0.1:'in p.stdout.readline():break
  else:raise RuntimeError('port-forward timed out')
  subprocess.run([str(ROOT/'bin/ticker-readback'),'-manifest',str(out/'manifest.json'),'-out',str(out)],env={**os.environ,'NATS_URL':url},check=True)
 finally:
  p.terminate()
  try:p.wait(timeout=5)
  except subprocess.TimeoutExpired:p.kill();p.wait()
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['build','up','smoke','readback']);p.add_argument('--out',required=True,type=Path);a=p.parse_args();out=a.out.resolve()
 if out==ROOT or ROOT not in out.parents:raise ValueError('project output required')
 if a.action=='build':build(out)
 elif a.action=='up':setup(out)
 elif a.action=='smoke':run(out)
 else:readback(out)
if __name__=='__main__':main()
