import unittest
from pathlib import Path
from unittest.mock import patch
from k8s_ticker_benchmark import Bench,stages,validate_namespace,expected_orders
class Tests(unittest.TestCase):
 def test_guards(self):
  for n in ['nse-mtbt-tickers','default','nse-mtbt-matching','nse-mtbt-ticker-bench-combined-','other']:
   with self.assertRaises(ValueError):validate_namespace(n)
  for mode in ['combined','tickers']:self.assertEqual(validate_namespace('nse-mtbt-ticker-bench-'+mode+'-r1'),'nse-mtbt-ticker-bench-'+mode+'-r1')
 def test_plans(self):
  self.assertEqual(len(stages('10000@100,10000@1000')),2)
  for p in ['100000@100','2@1001','1@100']:
   with self.assertRaises(ValueError):stages(p)
 def test_prefix_oracle(self):
  rows=expected_orders(4);self.assertEqual(len(rows),4)
  self.assertEqual(sorted(r['remaining_quantity']for r in rows),[0,0,70,70])
  self.assertTrue(all(r['status']=='FILLED'for r in expected_orders(6)))
  self.assertEqual(sum(r['remaining_quantity']for r in expected_orders(8)),200)
 def test_existing_namespace_refused(self):
  b=Bench('nse-mtbt-ticker-bench-combined-r1',Path('/unused'))
  with patch('k8s_ticker_benchmark.lab.guard'),patch.object(b,'k',return_value='exists'):
   with self.assertRaises(ValueError):b.install(stages('10000@100'))
 def test_no_replay(self):
  import tempfile,json
  with tempfile.TemporaryDirectory()as d:
   p=Path(d);(p/'settings.json').write_text(json.dumps({'stages':stages('10000@100')}))
   b=Bench('nse-mtbt-ticker-bench-tickers-r1',p)
   with patch('k8s_ticker_benchmark.lab.guard'),patch.object(b,'state',return_value={'source':1}):
    with self.assertRaises(ValueError):b.run(10)
if __name__=='__main__':unittest.main()
