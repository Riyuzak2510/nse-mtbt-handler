import unittest
from pathlib import Path
from unittest.mock import patch
import k8s_tickers_smoke as s
class TickerKubernetesGuards(unittest.TestCase):
 def test_context_namespace(self):
  with patch.object(s.lab,'command',return_value='ok')as c:
   s.kube('get','pods');args=c.call_args.args[0];self.assertIn('--kubeconfig',args);self.assertIn('kind-nse-mtbt-lab',args);self.assertIn('nse-mtbt-tickers',args)
 def test_existing_namespace_refused(self):
  with patch.object(s.lab,'guard'),patch.object(s,'kube',return_value='existing'),patch.object(s,'snapshot')as snapshot:
   with self.assertRaises(ValueError):s.setup(Path('notcreated'))
   snapshot.assert_not_called()
 def test_existing_source_refused(self):
  with patch.object(s.lab,'guard'),patch.object(s,'sql',return_value='1'),patch.object(s,'job')as job:
   with self.assertRaises(ValueError):s.run(Path('notcreated'))
   job.assert_not_called()
 def test_existing_job_refused(self):
  with patch.object(s,'kube',return_value='existing'):
   with self.assertRaises(ValueError):s.job('prefix',1,4)
if __name__=='__main__':unittest.main()
