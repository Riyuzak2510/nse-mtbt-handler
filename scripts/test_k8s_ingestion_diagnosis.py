import json
from pathlib import Path
import unittest
from unittest.mock import patch
import k8s_ingestion_diagnosis as d

class DiagnosisTests(unittest.TestCase):
 def test_profile_and_namespace_guards(self):
  s=d.load.profile_stages(d.PROFILE)
  self.assertEqual(sum(x['count'] for x in s),161000)
  self.assertEqual(sum(x['count']/x['rate'] for x in s),40)
  self.assertEqual(max(x['rate'] for x in s),10000)
  for n in ('nse-mtbt-matching','default','nse-mtbt-load-../x'):
   with self.assertRaises(ValueError):d.load.validate_namespace(n)
 def test_deploy_only_new_resources_and_control_omits_matcher(self):
  original=d.load.kube;calls=[]
  def fake_deploy(out,build):
   resource={'items':[{'kind':'Deployment','metadata':{'name':'publisher'},'spec':{'template':{'spec':{'containers':[{'name':'publisher','env':[]}]}}}},{'kind':'Deployment','metadata':{'name':'matching-service'},'spec':{'template':{'spec':{'containers':[{'env':[]}]}}}}]}
   d.load.kube('apply','-f','-',stdin=json.dumps(resource))
   self.assertEqual(d.load.kube('scale','deployment/matching-service','--replicas=1'),'skipped matcher for publisher-only control')
  with patch.object(d.load,'kube',side_effect=lambda *a,**kw:calls.append((a,kw))or'ok'),patch.object(d.load,'deploy',side_effect=fake_deploy),patch.object(d.load,'save'):
   d.deploy(Path('unused'),'publisher-only',False)
  data=json.loads(calls[0][1]['stdin'])
  pub=data['items'][0]['spec']['template']['spec']['containers']
  self.assertEqual(pub[0]['env'][0]['name'],'MTBT_INGEST_METRICS')
  self.assertEqual(pub[1]['name'],'diagnostics-reader')
  self.assertEqual(data['items'][1]['spec']['template']['spec']['containers'][0]['env'][0]['name'],'MTBT_SQL_METRICS')
  self.assertIs(d.load.kube,original)
