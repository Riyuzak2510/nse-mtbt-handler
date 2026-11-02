import json
from pathlib import Path
import tempfile
import unittest
from analyze_ingestion_diagnosis import analyze

class DiagnosisAnalysisTests(unittest.TestCase):
 def fixture(self,p,trailing=False):
  osstate={'socket':{'found':True,'rx_queue_bytes':0,'drops':0,'inode':'100'},'udp':{'RcvbufErrors':0,'InErrors':0},'cgroup_cpu':{'usage_usec':100,'throttled_usec':0}}
  end=json.loads(json.dumps(osstate));end['socket']['drops']=5;end['udp']['RcvbufErrors']=5;end['udp']['InErrors']=5
  r={'kind':'final','start':osstate,'current':end,'publish_errors':0,'malformed':0,'reorders':0,'published':16,'received':16,'missing_sequence_numbers':3 if trailing else 5,'last_sequence':19 if trailing else 20,'first_gaps':[],'socket_read_buffer_bytes':425984,'elapsed_seconds':1,'publish':{'count':16,'mean_ms':.1},'emitted_to_receive':{'count':16 if trailing else 15,'invalid':0},'interval_published':16,'interval_seconds':1}
  s={'kind':'complete','sent':20,'elapsed_seconds':1,'max_schedule_lag_ns':1000,'profile':[{'count':20,'rate':1000}]}
  js={'account_details':[{'stream_detail':[{'state':{'messages':16,'first_seq':1},'consumer_detail':[{'num_pending':16}]}]}]}
  for name,d in [('ingestion.jsonl',r),('source.jsonl',s),('nats-jsz.json',js),('diagnostic-settings.json',{'mode':'publisher-only'})]:(p/name).write_text(json.dumps(d)+'\n')
 def test_internal_holes_and_trailing_heartbeat_reconcile(self):
  for trailing in (False,True):
   with tempfile.TemporaryDirectory() as tmp:
    p=Path(tmp);self.fixture(p,trailing);r=analyze(p);self.assertTrue(r['reconciliation_pass'],r)
    self.assertEqual(r['trailing_data_loss'],1 if trailing else 0)
    self.assertEqual(r['trailing_heartbeat_loss'],1 if trailing else 0)
 def test_changed_socket_missing_counters_and_publish_error_refused(self):
  for mode in ('inode','drop','publish','incomplete'):
   with tempfile.TemporaryDirectory() as tmp:
    p=Path(tmp);self.fixture(p)
    r=json.loads((p/'ingestion.jsonl').read_text())
    if mode=='inode':r['current']['socket']['inode']='200'
    elif mode=='drop':r['current']['socket']['drops']=6
    elif mode=='publish':r['publish_errors']=1
    else:
     s=json.loads((p/'source.jsonl').read_text());s['kind']='interval';(p/'source.jsonl').write_text(json.dumps(s))
    (p/'ingestion.jsonl').write_text(json.dumps(r))
    self.assertFalse(analyze(p)['reconciliation_pass'])
