#!/usr/bin/env python3
"""Fresh-namespace outcome integration smoke, never alters retained stream data."""
import argparse,json,time
from pathlib import Path
import k8s_load as load

NAMESPACE='nse-mtbt-outcomes'
# Current overlay passes notify/reconciliation flags; older cached images do not support them.
IMAGE='nse-mtbt-handler:outcome-notify-20261101'

def kube(*args,**kwargs):
 return load.lab.command(['kubectl','--kubeconfig',str(load.lab.KUBECONFIG),'--context',load.lab.CONTEXT,'-n',NAMESPACE,*args],kwargs.get('stdin'),kwargs.get('timeout',60))

def setup(out):
 load.lab.guard()
 if kube('get','namespace',NAMESPACE,'--ignore-not-found').strip():raise ValueError('refusing existingoutcome namespace')
 load.NAMESPACE=NAMESPACE;load.IMAGE=IMAGE
 original_kube,original_command=load.kube,load.lab.command
 def command(args,stdin=None,timeout=240):
  if args==['kubectl','kustomize','deploy/matching']:args=['kubectl','kustomize','deploy/outcomes']
  if args and args[0]=='kubectl' and 'apply' in args and stdin:
   data=json.loads(stdin)
   for o in data.get('items',[]):
    if o['kind']=='Deployment'and o['metadata']['name']=='matching-service':
     c=o['spec']['template']['spec']['containers'][0];c['env']=[e for e in c['env']if e.get('name')!='MTBT_LOAD_METRICS']
   stdin=json.dumps(data)
  return original_command(args,stdin,timeout)
 load.lab.command=command
 # loadhelper cannot allow arbitrarynamespaces; this wrapper pins exactfreshnamespace.
 load.kube=lambda *a,**kw:command(['kubectl','--kubeconfig',str(load.lab.KUBECONFIG),'--context',load.lab.CONTEXT,'-n',kw.get('namespace',NAMESPACE),*a],kw.get('stdin'),kw.get('timeout',240))
 try:
  load.deploy(out,True)
  # Restore a localobserver onlywhenreadingmetrics; source/evidencePVCusedforjobs.
 finally:load.lab.command=original_command;load.kube=original_kube

def sql(q):return kube('exec','postgres-0','--','psql','-X','-U','mtbt','-d','mtbt','-At','-v','ON_ERROR_STOP=1','-c',q).strip()
def wait(check,label,seconds=90):
 deadline=time.monotonic()+seconds
 while time.monotonic()<deadline:
  try:
   if check():return
  except (RuntimeError,ValueError):pass
  time.sleep(.25)
 raise RuntimeError(label+' timedout')
def job(name,start,end):
 if not kube('get','configmap','mtbt-smoke-source','--ignore-not-found').strip():
  m=json.loads(kube('get','configmap','mtbt-source','-o','json'))
  manifest=json.loads(m['data']['manifest.json']);manifest['instruments']=manifest['instruments'][:1]
  kube('create','-f','-',stdin=json.dumps({'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':'mtbt-smoke-source'},'immutable':True,'data':{'manifest.json':json.dumps(manifest)}}))
 obj={'apiVersion':'batch/v1','kind':'Job','metadata':{'name':name},'spec':{'backoffLimit':0,'activeDeadlineSeconds':60,'template':{'metadata':{'labels':{'app':name}},'spec':{'restartPolicy':'Never','nodeSelector':{'mtbt.lane':'b'},'automountServiceAccountToken':False,'securityContext':{'runAsNonRoot':True,'runAsUser':10001,'runAsGroup':10001,'fsGroup':10001},'containers':[{'name':'source','image':IMAGE,'command':['/app/matching-workload'],'args':['-manifest=/config/manifest.json','-addr=publisher:19001','-allow-non-loopback',f'-start={start}',f'-end={end}','-total=12','-rate=50',f'-timings=/evidence/{name}.csv',f'-expected=/evidence/{name}-expected.json']+(['-heartbeat=false']if end<12 else []),'resources':{'requests':{'cpu':'50m','memory':'16Mi'},'limits':{'cpu':'500m','memory':'64Mi'}},'volumeMounts':[{'name':'source','mountPath':'/config','readOnly':True},{'name':'evidence','mountPath':'/evidence'}]}],'volumes':[{'name':'source','configMap':{'name':'mtbt-smoke-source'}},{'name':'evidence','persistentVolumeClaim':{'claimName':'source-evidence'}}]}}}}
 kube('create','-f','-',stdin=json.dumps(obj));kube('wait','--for=condition=complete','job/'+name,'--timeout=60s',timeout=75)
def brokers():
 return json.loads(kube('exec','load-monitor','--','wget','-qO-','http://nats:8222/jsz?streams=true&consumers=true'))
def outcome_messages():
 for a in brokers().get('account_details',[]):
  for stream in a.get('stream_detail',[]):
   if stream['name']=='MTBT_OUTCOMES_1':return stream['state']['messages']
 return 0

def run(out):
 if sql('SELECT source_sequence FROM mtbt.feed_state WHERE stream_id=1')!='0':raise ValueError('smoke requires fresh stream, refusingreplay')
 job('outcome-prefix-v2',1,2)
 wait(lambda:sql('SELECT source_sequence FROM mtbt.feed_state WHERE stream_id=1')=='2','partialmatching')
 prefix=sql("SELECT encode(order_id,'hex'),remaining_quantity,filled_quantity,status FROM mtbt.orders WHERE stream_id=1 ORDER BY order_id")
 assert '70|30|PARTIALLY_FILLED' in prefix,prefix
 assert sql('SELECT count(*) FROM mtbt.order_outbox WHERE stream_id=1')=='3'
 (out/'before-restart-orders.txt').write_text(prefix+'\n')
 # Actualpodreplacement with pending outcomes; no new native runidentity/reset.
 old=json.loads(kube('get','pods','-l','app=matching-service','-o','json'))['items'][0]['metadata']['uid']
 kube('scale','deployment/matching-service','--replicas=0')
 wait(lambda:not json.loads(kube('get','pods','-l','app=matching-service','-o','json'))['items'],'matcherstop')
 kube('scale','deployment/matching-service','--replicas=1');wait(lambda:'restored through broker=2' in kube('logs','deployment/matching-service','--tail=20'),'matcherrestored',seconds=120)
 new=json.loads(kube('get','pods','-l','app=matching-service','-o','json'))['items'][0]['metadata']['uid'];assert old!=new
 assert sql('SELECT count(*) FROM mtbt.order_outbox WHERE stream_id=1')=='3'
 assert sql("SELECT encode(order_id,'hex'),remaining_quantity,filled_quantity,status FROM mtbt.orders WHERE stream_id=1 ORDER BY order_id")==prefix
 kube('scale','deployment/outcome-relay','--replicas=1');kube('rollout','status','deployment/outcome-relay','--timeout=120s',timeout=150)
 wait(lambda:sql('SELECT count(*) FROM mtbt.order_outbox WHERE stream_id=1')=='0'and outcome_messages()==3,'firstoutcomedelivery')
 job('outcome-rest-v2',3,12)
 wait(lambda:sql('SELECT source_sequence::text||\'|\'||broker_sequence::text FROM mtbt.feed_state WHERE stream_id=1')=='12|13','completeinput')
 wait(lambda:sql('SELECT count(*) FROM mtbt.order_outbox WHERE stream_id=1')=='0'and outcome_messages()==20,'completeoutcomedelivery')
 rows=sql("SELECT encode(order_id,'hex'),token,side,price,accepted_quantity,remaining_quantity,filled_quantity,cancelled_quantity,status FROM mtbt.orders WHERE stream_id=1 ORDER BY order_id")
 assert len(rows.splitlines())==12
 (out/'final-orders.txt').write_text(rows+'\n');(out/'broker-final.json').write_text(json.dumps(brokers(),indent=2)+'\n')
 for app in ['matching-service','publisher','outcome-relay']:(out/(app+'.log')).write_text(kube('logs','deployment/'+app,'--tail=200'))
 before=json.loads((out/'retained-before.json').read_text());after=load.snapshots();assert before==after
 (out/'retained-after.json').write_text(json.dumps(after,indent=2)+'\n')
 (out/'smoke-result.json').write_text(json.dumps({'source_sequence':12,'broker_sequence':13,'orders':12,'outcomes':20,'pending':0,'partial_fill_restart':{'old_pod_uid':old,'new_pod_uid':new,'remaining':70,'filled':30,'pending_preserved':3},'old_namespace_specs_unchanged':True,'scope':'small actualKubernetes functional test,not10k/20Mlatency','deployment':'freshisolated namespace; existingnativehistoriesuntouched'},indent=2)+'\n')
 kube('delete','pod','load-monitor','--grace-period=0','--wait=false')
 print('OutcomeKubernetessmoke passed:12orders20outcomes,partialrestart,3pendingrecovered',flush=True)
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['deploy','run']);p.add_argument('--out',required=True,type=Path);a=p.parse_args();out=a.out.resolve()
 if not out.is_relative_to(load.ROOT):raise ValueError('outputoutsideproject')
 load.lab.guard()
 if a.action=='deploy':setup(out)
 else:run(out)
if __name__=='__main__':main()
