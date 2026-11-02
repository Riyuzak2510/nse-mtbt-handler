import json
from pathlib import Path
import tempfile
import unittest
from analyze_load_history import summarize

class LoadHistoryTests(unittest.TestCase):
 def test_boundary_intervals_and_invalid_duration_denominator(self):
  with tempfile.TemporaryDirectory() as d:
   p=Path(d);src={'profile':[{'count':10,'rate':100},{'count':10,'rate':1000}],'sent':20,'kind':'complete'}
   (p/'source.jsonl').write_text(json.dumps(src)+'\n')
   def window(first,last,count,valid,mean):
    h={'count':valid,'invalid':count-valid,'mean_ms':mean,'p99_ms':mean+1}
    return {'first_sequence':first,'last_sequence':last,'count':count,'started_unix_ns':1_000_000_000,'ended_unix_ns':2_000_000_000,'histograms':{name:h for name in ['emitted_to_commit','received_to_delivery','db_process']}}
   rows=[{'interval':window(1,9,9,8,2),'cumulative':{'count':9}},
         {'interval':window(10,11,2,2,20),'cumulative':{'count':11}},
         {'kind':'final','interval':window(12,20,9,9,4),'cumulative':{'count':20}}]
   (p/'latency.jsonl').write_text('\n'.join(json.dumps(x) for x in rows)+'\n')
   r=summarize(p)
   self.assertEqual(r['stages'][0]['whole_completion_interval_samples'],9)
   self.assertEqual(r['stages'][0]['boundaries']['db_process']['mean_ms'],2)
   self.assertEqual(r['stages'][0]['boundaries']['db_process']['histogram_invalid_count'],1)
   self.assertEqual(r['stages'][1]['whole_completion_interval_samples'],9)
