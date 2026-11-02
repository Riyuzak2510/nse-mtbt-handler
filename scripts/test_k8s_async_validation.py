import json
from pathlib import Path
from unittest.mock import patch
import unittest
import k8s_async_validation as a
class AsyncHarnessTests(unittest.TestCase):
 def test_new_publisher_flag_only(self):
  observed=[];old=a.load.kube
  def fake_deploy(*args):
   data={'items':[{'kind':'Deployment','metadata':{'name':'publisher'},'spec':{'template':{'spec':{'containers':[{'args':['-listen=0.0.0.0:19001']}]}}}},{'kind':'Deployment','metadata':{'name':'matching-service'},'spec':{'template':{'spec':{'containers':[{'args':['-mode=durable']}]}}}}]}
   a.load.kube('apply','-f','-',stdin=json.dumps(data))
  with patch.object(a.load,'kube',side_effect=lambda *args,**kw:observed.append(kw.get('stdin'))),patch.object(a.diagnostic,'deploy',side_effect=fake_deploy),patch.object(a.load,'save'):
   a.deploy(Path('unused'),'publisher-only','async',False)
  data=json.loads(observed[0]);self.assertIn('-publish-mode=async',data['items'][0]['spec']['template']['spec']['containers'][0]['args'])
  self.assertEqual(data['items'][1]['spec']['template']['spec']['containers'][0]['args'],['-mode=durable'])
  self.assertIs(a.load.kube,old)
