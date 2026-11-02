#!/usr/bin/env python3
"""Read-only retained feed verification using an independent temporary consumer."""
import argparse,base64,json,os,select,subprocess
from pathlib import Path
import k8s_load as load

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--namespace',required=True);p.add_argument('--out',type=Path,required=True);p.add_argument('--count',type=int,required=True);p.add_argument('--port',type=int,default=14224);p.add_argument('--integration',action='store_true');p.add_argument('--integration-only',action='store_true');a=p.parse_args()
 load.NAMESPACE=load.validate_namespace(a.namespace);load.lab.guard();out=a.out.resolve()
 if not out.is_relative_to(load.ROOT):p.error('outputoutsideproject')
 secret=json.loads(load.kube('get','secret','mtbt-credentials','-o','json'))
 url=base64.b64decode(secret['data']['NATS_URL']).decode().replace('@nats:4222',f'@127.0.0.1:{a.port}')
 command=['kubectl','--kubeconfig',str(load.lab.KUBECONFIG),'--context',load.lab.CONTEXT,'-n',load.NAMESPACE,'port-forward','service/nats',f'{a.port}:4222']
 proc=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
 try:
  if not select.select([proc.stdout],[],[],15)[0]:raise RuntimeError('portforwardstartupfailed')
  if 'Forwarding'not in proc.stdout.readline():raise RuntimeError('portforwardnotready')
  env=os.environ.copy();env['NATS_URL']=url;env['MTBT_STREAM_MAX_BYTES']=str(8<<30)
  report=out/'ordered-readback.json'
  if report.exists() and not a.integration_only:raise ValueError('refusingoverwrite existingreadback')
  text=''
  if not a.integration_only:
   text=subprocess.check_output(['go','run','scripts/verify_published_feed.go',str(out/'manifest.json'),str(a.count)],cwd=load.ROOT,env=env,text=True,timeout=150)
   report.write_text(text)
  if a.integration or a.integration_only:
   env['MTBT_TEST_NATS_URL']=url
   env['MTBT_STREAM_MAX_BYTES']=str(64<<20)
   result=subprocess.run(['go','test','-race','-count=1','-v','./internal/broker','-run','TestAsyncReal'],cwd=load.ROOT,env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=90)
   (out/'real-nats-integration.log').write_text(result.stdout)
   if result.returncode:raise RuntimeError('real integrationfailed;see savedlog')
  print(text)
 finally:
  proc.terminate()
  try:proc.wait(timeout=10)
  except subprocess.TimeoutExpired:proc.kill();proc.wait()
if __name__=='__main__':main()
