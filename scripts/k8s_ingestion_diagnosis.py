#!/usr/bin/env python3
"""Fresh-namespace matched publisher-only/full-path diagnosis, unchanged packet logic."""
import argparse,json,time,subprocess
from pathlib import Path
import k8s_load as load

IMAGE='nse-mtbt-handler:diagnostics-20261022-r2'
PROFILE='1000@100,10000@1000,50000@5000,100000@10000'

def deploy(out,mode,build):
 original=load.kube
 def intercepted(*args,**kwargs):
  if args[:2]==('apply','-f') and kwargs.get('stdin'):
   data=json.loads(kwargs['stdin'])
   for o in data['items']:
    if o['kind']=='Deployment' and o['metadata']['name']=='publisher':
     c=o['spec']['template']['spec']['containers'][0]
     c.setdefault('env',[]).append({'name':'MTBT_INGEST_METRICS','value':'/evidence/ingestion.jsonl'})
     o['spec']['template']['spec']['containers'].append({'name':'diagnostics-reader','image':'nats:2.11.8-alpine','command':['sleep','7200'], 'resources':{'requests':{'cpu':'10m','memory':'16Mi'},'limits':{'cpu':'100m','memory':'64Mi'}},'securityContext':{'runAsNonRoot':True,'runAsUser':10001,'allowPrivilegeEscalation':False,'readOnlyRootFilesystem':True,'capabilities':{'drop':['ALL']}},'volumeMounts':[{'name':'evidence','mountPath':'/evidence','readOnly':True}]})
    if o['kind']=='Deployment' and o['metadata']['name']=='matching-service':
     o['spec']['template']['spec']['containers'][0]['env'].append({'name':'MTBT_SQL_METRICS','value':'/evidence/sql.jsonl'})
   kwargs['stdin']=json.dumps(data)
  if mode=='publisher-only' and args[:3]==('scale','deployment/matching-service','--replicas=1'):return 'skipped matcher for publisher-only control'
  if mode=='publisher-only' and args[:3]==('logs','deployment/matching-service','--tail=20'):return 'restored through broker=0 (not launched, publisher-only)'
  return original(*args,**kwargs)
 load.kube=intercepted
 try:load.deploy(out,build)
 finally:load.kube=original
 load.save(out,'diagnostic-settings.json',{'mode':mode,'image':load.IMAGE,'profile':PROFILE,'no_tuning':'NoCPU/socketbuffer/publishing/SQL/ack/ordering/durability changes; same diagnostic image in both variants','sql_scope':'all pool queries including initialization and reads; cumulative operations, not server execution time','udp_scope':'exactport/proc/net/udp drops plus podnetworkSNMP RcvbufErrors, read-onlycgroupstats'})

def ingestion_tail():
 text=load.kube('exec','deployment/publisher','-c','diagnostics-reader','--','sh','-c','tail -1 /evidence/ingestion.jsonl')
 return json.loads(text.strip())

def run(out,mode,profile):
 if (out/'diagnostic-outcome.json').exists():raise ValueError('existing diagnostics outcome')
 if mode=='full-path' and load.checkpoint()['source']!=0:raise ValueError('pristine diagnostic state required')
 if mode=='publisher-only':
  monitor=json.loads(load.kube('exec','load-monitor','--','wget','-qO-','http://nats:8222/jsz?streams=true&consumers=true'))
  assert monitor['messages']==0,'control requires empty stream'
 baseline={a:load.restarts(load.pod(a)) for a in ('publisher','matching-service')}
 load.save(out,'restart-baseline.json',baseline)
 total,seconds=load.source(profile);started=time.monotonic();last=time.monotonic();wall=time.time();reason='';source_done=None;stable=0;previous=-1
 with (out/'diagnostic-progress.jsonl').open('x') as f:
  while time.monotonic()-started<seconds+90:
   now=time.monotonic();newwall=time.time()
   if now-last>30 or abs((newwall-wall)-(now-last))>2:reason='environmentinterruption';break
   last,wall=now,newwall
   sp=load.pod('load-source');phase=(sp or {}).get('status',{}).get('phase')
   ing=ingestion_tail();cp=load.checkpoint() if mode=='full-path' else None
   event={'elapsed_seconds':now-started,'source_phase':phase,'ingestion':ing,'checkpoint':cp,'publisher_restarts':load.restarts(load.pod('publisher')),'matcher_restarts':load.restarts(load.pod('matching-service'))}
   f.write(json.dumps(event)+'\n');f.flush();print(json.dumps({'elapsed':event['elapsed_seconds'],'phase':phase,'published':ing['published'],'socketdrops':ing['current']['socket']['drops'],'gaps':ing['missing_sequence_numbers'],'checkpoint':cp}),flush=True)
   if phase=='Failed':reason='sourcefailed';break
   if event['publisher_restarts']>baseline['publisher']:reason='publisherrestart';break
   if phase=='Succeeded':
    if source_done is None:source_done=now
    if ing['published']==previous:stable+=1
    else:stable=0
    if stable>=2 and now-source_done>=10:break
   previous=ing['published'];time.sleep(3)
  else:reason='timeout'
 # Stop source only if unfinished; capture reasons before stopping application.
 load.capture_failure(out)
 if load.pod('load-source') and load.pod('load-source')['status'].get('phase')not in('Succeeded','Failed'):
  load.kube('patch','job','load-source','--type=merge','-p','{"spec":{"suspend":true}}')
 # Publisher loop exits gracefully and writes final metrics before sidecar pod stops.
 load.kube('scale','deployment/publisher','deployment/matching-service','--replicas=0')
 load.lab.wait(lambda:not json.loads(load.kube('get','pods','-l','app=publisher','-o','json'))['items'],'publisher flush',timeout=120)
 load.lab.wait(lambda:not json.loads(load.kube('get','pods','-l','app=matching-service','-o','json'))['items'],'matcher flush',timeout=120)
 state=load.checkpoint() if mode=='full-path' else None
 verified=load.oracle(out,state) if state else None
 load.save(out,'diagnostic-outcome.json',{'mode':mode,'requested':total,'nominal_source_seconds':seconds,'reason':reason,'elapsed':time.monotonic()-started,'checkpoint':state,'oracle':verified})
 # Read persisted ingestion evidence on the correct local-PVC node.
 r=load.monitor_object('ingest-reader');r['spec']['nodeSelector']={'mtbt.lane':'a'}
 r['spec']['volumes']=[{'name':'pub','persistentVolumeClaim':{'claimName':'publisher-evidence'}}]
 r['spec']['containers'][0]['volumeMounts']=[{'name':'pub','mountPath':'/pub','readOnly':True}]
 load.create(r);load.kube('wait','--for=condition=Ready','pod/ingest-reader','--timeout=120s',timeout=140)
 (out/'ingestion.jsonl').write_text(load.kube('exec','ingest-reader','--','cat','/pub/ingestion.jsonl'))
 try:(out/'pipeline.jsonl').write_text(load.kube('exec','ingest-reader','--','cat','/pub/ingestion.jsonl.pipeline.jsonl'))
 except RuntimeError:pass
 load.kube('delete','pod','ingest-reader','--grace-period=0','--wait=false')
 if mode=='full-path':(out/'sql.jsonl').write_text(load.kube('exec','load-monitor','--','cat','/metrics/sql.jsonl'))
 if mode=='full-path':load.collect(out)
 else:
  (out/'source.jsonl').write_text(load.kube('exec','load-monitor','--','cat','/source/source.jsonl'))
  (out/'nats-jsz.json').write_text(load.kube('exec','load-monitor','--','wget','-qO-','http://nats:8222/jsz?streams=true&consumers=true'))
  after=load.snapshots();load.save(out,'retained-after.json',after)
  assert after==json.loads((out/'retained-before.json').read_text()),'retained namespaces changed'
  load.save(out,'pods-final.json',json.loads(load.kube('get','pods','-o','json')))
  load.kube('delete','pod','load-monitor','--grace-period=0','--wait=false')
 print('Diagnostic evidence collected',flush=True)

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['deploy','run']);p.add_argument('--mode',choices=['full-path','publisher-only'],required=True);p.add_argument('--namespace',required=True);p.add_argument('--out',required=True,type=Path);p.add_argument('--skip-build',action='store_true');p.add_argument('--profile',default=PROFILE);a=p.parse_args()
 load.NAMESPACE=load.validate_namespace(a.namespace);load.IMAGE=IMAGE;out=a.out.resolve()
 if not out.is_relative_to(load.ROOT):raise ValueError('output outsideproject')
 load.lab.guard();load.profile_stages(a.profile)
 if a.action=='deploy':deploy(out,a.mode,not a.skip_build)
 else:run(out,a.mode,a.profile)
if __name__=='__main__':main()
