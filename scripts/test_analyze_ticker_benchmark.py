import json,tempfile,unittest
from pathlib import Path
from analyze_ticker_benchmark import compare,analyze,overlap
class Tests(unittest.TestCase):
 def arm(self,mode,value):
  stat={k:value for k in ['mean_ms','p50_ms','p95_ms','p99_ms']}
  return dict(topology=mode,namespace=mode,image_id='same',stages=[dict(events=10000,requested_rate=100,actual_send_rate=99,source_rate_within_five_percent=True,latency={'send_to_matching_commit':stat,'matching_process':stat},notification={'send_to_all_outcomes_stored':stat})])
 def test_interval_overlap(self):
  r=overlap([{'start_unix_ns':0,'end_unix_ns':10},{'start_unix_ns':5,'end_unix_ns':15}]);self.assertEqual(r['max_overlapping_process_calls'],2);self.assertAlmostEqual(r['parallel_fraction_of_busy'],1/3)
 def test_delta_direction(self):
  with tempfile.TemporaryDirectory()as d:
   r=compare(self.arm('combined',4),self.arm('tickers',6),Path(d)/'result.json')
   self.assertEqual(r['pairs'][0]['deltas']['send_to_matching_commit']['p99_ms']['change_percent'],50)
 def test_incomparable_image(self):
  b=self.arm('tickers',3);b['image_id']='other'
  with tempfile.TemporaryDirectory()as d:
   with self.assertRaises(ValueError):compare(self.arm('combined',4),b,Path(d)/'result.json')
 def test_failed_run_not_percentiles(self):
  with tempfile.TemporaryDirectory()as d:
   p=Path(d)
   for name,obj in [('settings.json',{}),('execution.json',{'complete':False}),('readback-result.json',{})]:(p/name).write_text(json.dumps(obj))
   with self.assertRaises(ValueError):analyze(p)
if __name__=='__main__':unittest.main()
