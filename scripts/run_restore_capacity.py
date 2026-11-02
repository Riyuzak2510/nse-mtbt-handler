#!/usr/bin/env python3
"""Isolated real PostgreSQL -> matching RAM hydration in fresh limited Linux processes."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time

ROOT=Path(__file__).resolve().parents[1]
def cmd(args,**kw):return subprocess.check_output(args,cwd=ROOT,text=True,**kw).strip()
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--out',type=Path,required=True);p.add_argument('--counts',default='100000,500000,1000000');args=p.parse_args()
 counts=[int(x) for x in args.counts.split(',')]
 if any(n<1 or n>1000000 for n in counts):p.error('count1..1000000')
 out=args.out.resolve()
 if not out.is_relative_to(ROOT):p.error('output inside project required')
 out.mkdir(parents=True,exist_ok=False);prefix='tmp_rovo_restore_'+os.urandom(6).hex();network=prefix+'_net';pg=prefix+'_pg';image=prefix+':test';password='test-'+os.urandom(8).hex()
 cmd(['docker','network','create',network])
 try:
  cmd(['docker','run','-d','--name',pg,'--network',network,'--network-alias','pg','-e','POSTGRES_USER=mtbt','-e','POSTGRES_PASSWORD='+password,'-e','POSTGRES_DB=mtbt','postgres:17.7-alpine'])
  for _ in range(100):
   if subprocess.run(['docker','exec',pg,'pg_isready','-U','mtbt'],capture_output=True).returncode==0:break
   time.sleep(.1)
  else:raise RuntimeError('PostgreSQL not ready')
  arch=cmd(['docker','info','--format','{{.Architecture}}']);arch='arm64' if arch in ('aarch64','arm64') else 'amd64'
  with tempfile.TemporaryDirectory(prefix='tmp_rovo_restore_build_',dir=ROOT) as tmp:
   tmp=Path(tmp);subprocess.run(['go','test','-c','-o',str(tmp/'restore.test'),'./internal/durable'],cwd=ROOT,env=dict(os.environ,CGO_ENABLED='0',GOOS='linux',GOARCH=arch),check=True)
   (tmp/'Dockerfile').write_text('FROM scratch\nCOPY restore.test /app\nUSER 10001:10001\nENTRYPOINT ["/app"]\n');cmd(['docker','build','-q','-t',image,str(tmp)])
  results=[]
  for count in counts:
   name=prefix+'_'+str(count);dest=out/str(count);dest.mkdir()
   try:
    cmd(['docker','create','--name',name,'--network',network,'--memory','512m','--memory-swap','512m','--read-only','--cap-drop','ALL',
         '-e','GOMEMLIMIT=384MiB','-e','MTBT_TEST_RESTORE_COUNT='+str(count),'-e',f'MTBT_TEST_DATABASE_URL=postgres://mtbt:{password}@pg:5432/mtbt?sslmode=disable',image,
         '-test.run=^TestDatabaseRestoreCapacity$','-test.v','-test.timeout=240s'])
    proc=subprocess.run(['docker','start','-a',name],cwd=ROOT,text=True,capture_output=True,timeout=270)
    (dest/'test.log').write_text(proc.stdout+proc.stderr)
    inspect=json.loads(cmd(['docker','inspect',name]))[0];(dest/'container.json').write_text(json.dumps(inspect,indent=2)+'\n')
    if proc.returncode or inspect['State']['OOMKilled'] or inspect['State']['ExitCode']:raise ValueError(inspect['State'])
    line=next(x for x in proc.stdout.splitlines() if 'RESTORE_CAPACITY ' in x)
    result=json.loads(line.split('RESTORE_CAPACITY ',1)[1]);assert result['count']==count and result['oracle']
    (dest/'result.json').write_text(json.dumps(result,indent=2)+'\n');results.append(result);print(json.dumps(result),flush=True)
   finally:subprocess.run(['docker','rm','-f',name],cwd=ROOT,capture_output=True)
  (out/'summary.json').write_text(json.dumps(results,indent=2)+'\n')
 finally:
  subprocess.run(['docker','rm','-f',pg],cwd=ROOT,capture_output=True);subprocess.run(['docker','network','rm',network],cwd=ROOT,capture_output=True);subprocess.run(['docker','image','rm',image],cwd=ROOT,capture_output=True)
if __name__=='__main__':main()
