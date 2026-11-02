import unittest
import analyze_outcome_benchmark as a
class OutcomeAnalysisTests(unittest.TestCase):
 def test_strict_samples(self):
  with self.assertRaises(ValueError):a.indexed([{'type':'N','sequence':1}],[1,2],'test')
  with self.assertRaises(ValueError):a.indexed([{'type':'N','sequence':1}]*2,[1],'test')
 def test_signed_cross_process_difference(self):
  r=a.signed([-100,0,100]);self.assertEqual(r['negative_count'],1);self.assertEqual(r['mean_ms'],0)
 def test_schedule_offset_remains_signed(self):
  r=a.signed([-275843,0,1234]);self.assertLess(r['min_ms'],0);self.assertEqual(r['negative_count'],1)
 def test_no_negative_real_duration(self):
  with self.assertRaises(ValueError):a.distribution([1,-1])
 def test_nearest_rank(self):self.assertEqual(a.distribution(list(range(1,101)))['p99_ms'],99/1e6)
if __name__=='__main__':unittest.main()
