import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import k8s_load as load

class LoadTest(unittest.TestCase):
 def test_explicit_kube_context(self):
  with patch.object(load.lab,'command',return_value='ok') as c:
   load.kube('get','pods');a=c.call_args.args[0]
   self.assertIn('--kubeconfig',a);self.assertIn('kind-nse-mtbt-lab',a);self.assertIn('nse-mtbt-load',a)
  for name in ('default','nse-mtbt-matching-other','nse-mtbt-load-','nse-mtbt-load-../default'):
   with self.assertRaises(ValueError):load.validate_namespace(name)
  self.assertEqual(load.validate_namespace('nse-mtbt-load-r2'),'nse-mtbt-load-r2')
 def test_reject_existing_namespace(self):
  with patch.object(load.lab,'guard'),patch.object(load,'kube',return_value='existing'),patch.object(load,'snapshots') as s:
   with self.assertRaises(ValueError):load.deploy(Path('not-created'),False)
   s.assert_not_called()
 def test_profile_twenty_million_peak(self):
  stages=load.profile_stages(load.PROFILE)
  self.assertEqual(sum(x['count'] for x in stages),20000000)
  self.assertEqual(max(x['rate'] for x in stages),10000)
  for invalid in ('','0@2','1@0','1@1000001','2147483647@1,1@1'):
   with self.assertRaises(ValueError):load.profile_stages(invalid)
 def fixture(self,path,**override):
  total=10
  state={'status':'VALID','source':10,'broker':11}
  oracle={'rows':10,'mismatches':0,'first':1,'last':10}
  out={'status':'CANDIDATE_COMPLETE','requested_events':10,'final_checkpoint':state,'oracle':oracle}
  out.update(override)
  (path/'outcome.json').write_text(json.dumps(out))
  (path/'source.jsonl').write_text(json.dumps({'kind':'complete','sent':total})+'\n')
  c={'count':10,'ack_failures':0,'retried_samples':0,'missing_timestamps':0,'histograms':{name:{'count':10,'invalid':0} for name in ['emitted_to_commit','received_to_delivery','db_process','ack','emitted_to_ack']}}
  (path/'latency.jsonl').write_text(json.dumps({'kind':'final','cumulative':c})+'\n')
 def test_full_gate_success(self):
  with tempfile.TemporaryDirectory() as d:
   p=Path(d);self.fixture(p);self.assertTrue(load.analyze(p)['full_target_pass']);self.assertTrue(load.analyze(p)['full_target_pass'])
 def test_no_success_from_prefix_bad_ack_or_oracle(self):
  for bad in ('ack','count','oracle','source','invalid','missing'):
   with tempfile.TemporaryDirectory() as d:
    p=Path(d);self.fixture(p)
    r=json.loads((p/'latency.jsonl').read_text());o=json.loads((p/'outcome.json').read_text())
    if bad=='ack':r['cumulative']['ack_failures']=1
    elif bad=='count':r['cumulative']['count']=9
    elif bad=='oracle':o['oracle']['mismatches']=1
    elif bad=='source':(p/'source.jsonl').write_text(json.dumps({'kind':'interval','sent':9})+'\n')
    elif bad=='missing':r['cumulative']['missing_timestamps']=1
    else:o['final_checkpoint']['status']='INVALID'
    (p/'latency.jsonl').write_text(json.dumps(r)+'\n');(p/'outcome.json').write_text(json.dumps(o))
    self.assertFalse(load.analyze(p)['full_target_pass'])
