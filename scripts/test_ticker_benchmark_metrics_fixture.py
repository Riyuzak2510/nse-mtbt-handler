"""Exercise analyzer against the exact retained calibration inputs once present."""
import unittest
from pathlib import Path
from analyze_ticker_benchmark import analyze
ROOT=Path(__file__).resolve().parents[1]
class Tests(unittest.TestCase):
 def test_retained_combined_trace(self):
  p=ROOT/'verification/20261102-ticker-bench-combined-r1'
  if not(p/'readback-result.json').exists():self.skipTest('retained run not present')
  r=analyze(p);self.assertEqual([s['events']for s in r['stages']],[10000,10000]);self.assertEqual(sum(s['outcome_count']for s in r['stages']),33332);self.assertTrue(r['calibration_only'])
if __name__=='__main__':unittest.main()
