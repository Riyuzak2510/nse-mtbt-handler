import json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import compare_outcome_delivery as compare

class DeliveryComparisonTests(unittest.TestCase):
 def test_settings_resources_and_deltas(self):
  with tempfile.TemporaryDirectory(prefix='tmp_rovo_delivery_',dir=Path(__file__).resolve().parents[1])as tmp:
   root=Path(tmp);poll=root/'poll';notify=root/'notify';poll.mkdir();notify.mkdir()
   for path,mode in((poll,'poll'),(notify,'notify')):
    settings=dict(namespace='ns-'+mode,enabled=True,delivery_mode=mode,image_id='same',stages=[dict(count=10,rate=100)],raw_stream_max_bytes=1,outcome_stream_max_bytes=1,warmup_excluded=0,workload='same',source_sha256={'same':'hash'})
    (path/'settings.json').write_text(json.dumps(settings));(path/'resources.json').write_text(json.dumps(dict(namespace=settings['namespace'],args=['-delivery-mode='+mode],cpu=1)))
   def result(value):
    metric={k:value for k in('mean_ms','p50_ms','p95_ms','p99_ms')}
    return dict(stages=[dict(events=10,offered_rate=100,actual_send_rate=100,source_rate_within_five_percent=True,latency={k:metric for k in('send_to_commit','db_process','receive_to_delivery')},outcomes=dict(latency={k:metric for k in('send_to_all_outcomes_stored','transaction_start_to_outcome_stored')},client_commit_observation_to_outcome_stored=metric))])
   with patch.object(compare,'analyze',side_effect=[result(50),result(2)]):
    r=compare.compare(poll,notify,root/'comparison.json')
   self.assertEqual(r['comparison'][0]['changes']['send_to_all_outcomes_stored']['mean_ms']['percent'],-96)
   with self.assertRaises(ValueError):compare.compare(poll,notify,root/'comparison.json')
   resources=json.loads((notify/'resources.json').read_text());resources['cpu']=2;(notify/'resources.json').write_text(json.dumps(resources))
   with self.assertRaisesRegex(ValueError,'resource specifications'):compare.compare(poll,notify,root/'different.json')
   settings=json.loads((notify/'settings.json').read_text());settings['image_id']='other';(notify/'settings.json').write_text(json.dumps(settings))
   with self.assertRaisesRegex(ValueError,'image_id'):compare.compare(poll,notify,root/'different-image.json')

if __name__=='__main__':unittest.main()
