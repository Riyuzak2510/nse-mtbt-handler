import csv,json,tempfile,unittest
from pathlib import Path
import analyze_outcome_benchmark as a

class FullOutcomeAnalysisTests(unittest.TestCase):
 def test_full_join_counts_and_broker_storage_boundary(self):
  with tempfile.TemporaryDirectory(prefix='tmp_rovo_outcome_analysis_')as d:
   p=Path(d);save=lambda n,v:(p/n).write_text(json.dumps(v))
   save('settings.json',dict(enabled=True,stages=[dict(count=3,rate=100)],namespace='fixture',image_id='fixture',clock='fixture',instrumentation='fixture',node_caveat='fixture'))
   save('execution.json',dict(complete=True,exact_stage_orders=True));save('readback-result.json',dict(complete=True,exact_raw_payloads_and_envelopes=True,exact_all_outcome_fields_and_fanout=True));save('checkpoint-stage-1.json',dict(source=3,broker=4,pending=0))
   base=1800000000000000000;source=[];publisher=[];consumer=[];raw=[];outcomes=[]
   fields=['sequence','type','start_unix_ns','end_unix_ns','duration_ns','received_unix_ns','scheduled_unix_ns','delivery_unix_ns','ack_unix_ns','ack_duration_ns','attempts','source_broker']
   for n in range(1,4):
    t=base+n*10000000
    s=dict.fromkeys(fields,0);s.update(sequence=n,type='N',start_unix_ns=t,end_unix_ns=t+1000,duration_ns=1000,scheduled_unix_ns=t);source.append(s)
    v=dict(s);v.update(start_unix_ns=t+10000,end_unix_ns=t+20000,duration_ns=10000,received_unix_ns=t+10000);publisher.append(v)
    v=dict(s);v.update(start_unix_ns=t+50000,end_unix_ns=t+1000000,duration_ns=950000,received_unix_ns=t+10000,delivery_unix_ns=t+40000,ack_unix_ns=t+1100000,ack_duration_ns=100000,attempts=1,source_broker=n);consumer.append(v)
    raw.append(dict(source_sequence=n,type='N',broker_sequence=n,received_unix_ns=t+10000,stored_unix_ns=t+15000))
    for _ in range(1 if n==1 else 2):outcomes.append(dict(stored_unix_ns=t+50000000,event=dict(source_sequence=n,recorded_at='2026-01-01T00:00:00Z')))
   # recorded_at fixture merely anchors a nonnegative transaction-start comparison.
   raw.append(dict(source_sequence=0,type='Z'))
   hb=dict.fromkeys(fields,0);hb.update(type='Z')
   for name,rows in [('source-1.csv',source),('publisher.csv',publisher),('consumer.csv',consumer)]:
    with(p/name).open('w',newline='')as f:
     writer=csv.DictWriter(f,fieldnames=fields);writer.writeheader();writer.writerows(rows+[hb])
   for name,rows in [('raw-readback.jsonl',raw),('outcome-readback.jsonl',outcomes)]:(p/name).write_text(''.join(json.dumps(r)+'\n'for r in rows))
   r=a.analyze(p)['stages'][0]
   self.assertEqual(r['events'],3);self.assertEqual(r['latency']['send_to_commit']['mean_ms'],1)
   self.assertEqual(r['outcomes']['count'],5);self.assertEqual(r['outcomes']['latency']['send_to_all_outcomes_stored']['mean_ms'],50)
   self.assertAlmostEqual(r['actual_send_rate'],100)
   consumer[1]['source_broker']=99
   with(p/'consumer.csv').open('w',newline='')as f:
    writer=csv.DictWriter(f,fieldnames=fields);writer.writeheader();writer.writerows(consumer+[hb])
   with self.assertRaises(ValueError):a.analyze(p)
if __name__=='__main__':unittest.main()
