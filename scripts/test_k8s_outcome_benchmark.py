import unittest
from pathlib import Path
from unittest.mock import patch
import k8s_outcome_benchmark as bench

class OutcomeBenchmarkTests(unittest.TestCase):
 def test_namespaces(self):
  for ns in ['default','nse-mtbt-outcomes','nse-mtbt-matching','nse-mtbt-load-r2','nse-mtbt-load-outcome-bench-on-']:
   with self.assertRaises(ValueError):bench.validate_namespace(ns)
  self.assertEqual(bench.validate_namespace('nse-mtbt-load-outcome-bench-on-r1'),'nse-mtbt-load-outcome-bench-on-r1')
 def test_delivery_mode(self):
  self.assertEqual(bench.Lab('nse-mtbt-load-outcome-bench-on-push',Path('notcreated')).delivery_mode,'notify')
  self.assertEqual(bench.Lab('nse-mtbt-load-outcome-bench-on-poll',Path('notcreated'),'poll').delivery_mode,'poll')
  with self.assertRaises(ValueError):bench.Lab('nse-mtbt-load-outcome-bench-on-bad',Path('notcreated'),'bad')
 def test_profile_bounds(self):
  self.assertEqual(bench.plan('10000@100,10000@1000'),[{'count':10000,'rate':100},{'count':10000,'rate':1000}])
  with self.assertRaises(ValueError):bench.plan('100000@100')
  with self.assertRaises(ValueError):bench.plan('1@100')
 def test_explicit_context(self):
  obj=bench.Lab('nse-mtbt-load-outcome-bench-off-r1',Path('notcreated'))
  with patch.object(bench.lab,'command',return_value='ok')as c:
   obj.k('get','pods');args=c.call_args.args[0]
  self.assertEqual(args[args.index('--context')+1],'kind-nse-mtbt-lab')
  self.assertIn('--kubeconfig',args);self.assertEqual(args[args.index('-n')+1],obj.namespace)
 def test_refuse_existing_namespace_before_writes(self):
  obj=bench.Lab('nse-mtbt-load-outcome-bench-off-r1',Path('notcreated'))
  with patch.object(bench.lab,'guard'),patch.object(obj,'k',return_value='exists'),patch.object(obj,'create')as create:
   with self.assertRaises(ValueError):obj.install(False,bench.plan('10@100'))
   create.assert_not_called()
 def test_continuation_job(self):
  obj=bench.Lab('nse-mtbt-load-outcome-bench-on-r1',Path('notcreated'));job=obj.make_job(2,10001,20000,20000,1000)
  args=job['spec']['template']['spec']['containers'][0]['args']
  self.assertIn('-start=10001',args);self.assertIn('-end=20000',args)
  self.assertEqual(job['spec']['backoffLimit'],0)

if __name__=='__main__':unittest.main()
