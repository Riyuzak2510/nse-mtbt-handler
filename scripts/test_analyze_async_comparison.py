import json
from pathlib import Path
import tempfile
import unittest
from analyze_async_comparison import compare

class AsyncComparisonTests(unittest.TestCase):
 def fixture(self,a,s):
  base={'reconciliation_pass':True,'source_events':10,'source_including_heartbeat':11,'socket_drop_delta':0,'missing_sequences':0,'published':11,'emitted_to_receive_ms':{},'publish_client_latency_ms':{}}
  for p in (a,s):
   (p/'diagnostic-analysis.json').write_text(json.dumps(base));(p/'settings.json').write_text(json.dumps({'image_id':'same'}));(p/'diagnostic-outcome.json').write_text(json.dumps({'reason':''}))
  stats={k:11 for k in ['received','submitted','confirmed','last_confirmed_broker']};stats.update({k:0 for k in ['unconfirmed','overflow','ack_errors','queued','in_flight','owned_bytes']})
  (a/'pipeline.jsonl').write_text(json.dumps(stats)+'\n');(a/'ordered-readback.json').write_text(json.dumps({'verified_messages':11,'exact_payload_and_envelope':True,'redeliveries':0}))
 def test_success_and_reject_incomplete(self):
  for broken in (False,True):
   with tempfile.TemporaryDirectory() as d:
    a,s=Path(d)/'a',Path(d)/'s';a.mkdir();s.mkdir();self.fixture(a,s)
    if broken:
     p=json.loads((a/'pipeline.jsonl').read_text());p['confirmed']=10;(a/'pipeline.jsonl').write_text(json.dumps(p))
    self.assertEqual(compare(a,s)['ingestion_acceptance_pass'],not broken)
 def test_no_matched_claim_when_images_differ(self):
  with tempfile.TemporaryDirectory() as d:
   a,s=Path(d)/'a',Path(d)/'s';a.mkdir();s.mkdir();self.fixture(a,s);(s/'settings.json').write_text(json.dumps({'image_id':'other'}))
   self.assertFalse(compare(a,s)['ingestion_acceptance_pass'])
