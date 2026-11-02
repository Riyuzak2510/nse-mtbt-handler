import unittest
from pathlib import Path
from unittest.mock import patch
import k8s_outcomes_smoke as smoke

class OutcomeKubernetesGuards(unittest.TestCase):
 def test_explicit_context_fixed_namespace(self):
  with patch.object(smoke.load.lab,'command',return_value='ok')as c:
   smoke.kube('get','pods')
   args=c.call_args.args[0]
   self.assertIn('--kubeconfig',args);self.assertIn('kind-nse-mtbt-lab',args);self.assertIn('nse-mtbt-outcomes',args)
 def test_existing_namespace_refused(self):
  with patch.object(smoke.load.lab,'guard'),patch.object(smoke,'kube',return_value='existing'),patch.object(smoke.load,'deploy')as deploy:
   with self.assertRaises(ValueError):smoke.setup(Path('notcreated'))
   deploy.assert_not_called()
 def test_deploy_strips_exclusive_trace_with_explicit_kubeconfig(self):
  resources={'items':[{'kind':'Deployment','metadata':{'name':'matching-service'},'spec':{'template':{'spec':{'containers':[{'env':[{'name':'MTBT_LOAD_METRICS','value':'/evidence/exclusive.jsonl'},{'name':'DATABASE_URL','value':'kept'}]}]}}}}]}
  import json
  def deployment(out,build):
   smoke.load.lab.command(['kubectl','--kubeconfig','project-only','--context','kind-nse-mtbt-lab','-n',smoke.NAMESPACE,'apply','-f','-'],json.dumps(resources))
  old_namespace,old_image=smoke.load.NAMESPACE,smoke.load.IMAGE
  try:
   with patch.object(smoke.load.lab,'guard'),patch.object(smoke,'kube',return_value=''),patch.object(smoke.load.lab,'command',return_value='ok')as command,patch.object(smoke.load,'deploy',side_effect=deployment):
    smoke.setup(Path('notcreated'))
    applied=json.loads(command.call_args.args[1])
    self.assertEqual(applied['items'][0]['spec']['template']['spec']['containers'][0]['env'],[{'name':'DATABASE_URL','value':'kept'}])
  finally:
   smoke.load.NAMESPACE,smoke.load.IMAGE=old_namespace,old_image
 def test_existing_source_refused(self):
  with patch.object(smoke,'sql',return_value='2'),patch.object(smoke,'job')as job:
   with self.assertRaises(ValueError):smoke.run(Path('notcreated'))
   job.assert_not_called()
