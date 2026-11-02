#!/usr/bin/env python3
"""Fresh isolated async-vs-sync publisher comparison; no retained deployment upgrade."""
import argparse,json
from pathlib import Path
import k8s_ingestion_diagnosis as diagnostic
import k8s_load as load

IMAGE='nse-mtbt-handler:async-20261024'

def deploy(out,mode,publish_mode,build):
 original=load.kube
 def intercepted(*args,**kwargs):
  if args[:2]==('apply','-f')and kwargs.get('stdin'):
   data=json.loads(kwargs['stdin'])
   for item in data['items']:
    if item['kind']=='Deployment'and item['metadata']['name']=='publisher':
     item['spec']['template']['spec']['containers'][0]['args'].append('-publish-mode='+publish_mode)
   kwargs['stdin']=json.dumps(data)
  return original(*args,**kwargs)
 load.kube=intercepted
 try:diagnostic.deploy(out,mode,build)
 finally:load.kube=original
 load.save(out,'async-settings.json',{'publish_mode':publish_mode,'image':IMAGE,'scope':'publisher only; matching transaction/ack unchanged; no business status events','queue_capacity':4096,'payload_bytes':16<<20,'max_inflight':256,'ack_timeout_seconds':3,'drain_timeout_seconds':10})

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['deploy','run']);p.add_argument('--out',type=Path,required=True);p.add_argument('--namespace',required=True);p.add_argument('--publish-mode',choices=['async','sync'],default='async');p.add_argument('--mode',choices=['publisher-only','full-path'],default='publisher-only');p.add_argument('--skip-build',action='store_true');p.add_argument('--profile',default=diagnostic.PROFILE);a=p.parse_args()
 load.NAMESPACE=load.validate_namespace(a.namespace);load.IMAGE=IMAGE;out=a.out.resolve()
 if not out.is_relative_to(load.ROOT):p.error('output outside project')
 load.lab.guard();load.profile_stages(a.profile)
 if a.action=='deploy':deploy(out,a.mode,a.publish_mode,not a.skip_build)
 else:
  stored=json.loads((out/'async-settings.json').read_text())
  if stored['publish_mode']!=a.publish_mode:raise ValueError('requested mode differs from deployment')
  diagnostic.run(out,a.mode,a.profile)
if __name__=='__main__':main()
