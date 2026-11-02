import copy
import csv
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import k8s_matching_lab as lab

class MatchingLabTests(unittest.TestCase):
    def test_explicit_context_and_namespace(self):
        with patch.object(lab,'command',return_value='') as run:
            lab.kube('get','pods')
            args=run.call_args[0][0]
            self.assertIn('--kubeconfig',args)
            self.assertEqual(args[args.index('--context')+1],'kind-nse-mtbt-lab')
            self.assertEqual(args[args.index('-n')+1],'nse-mtbt-matching')
        with self.assertRaises(ValueError):lab.kube('get','pods',namespace='other')
    def test_guard_remote_and_nodes(self):
        with patch.object(lab,'command',return_value=json.dumps({'clusters':[{'cluster':{'server':'https://remote'}}]})):
            with self.assertRaises(ValueError):lab.guard()
        with patch.object(lab,'command',return_value=json.dumps({'clusters':[{'cluster':{'server':'https://127.0.0.1:123'}}]})),patch.object(lab,'kube',return_value=json.dumps({'items':[]})):
            with self.assertRaises(ValueError):lab.guard()
    def test_up_refuses_existing_namespace_before_writes(self):
        with patch.object(lab,'kube',return_value='namespace exists'),patch.object(lab,'create') as create:
            with self.assertRaises(ValueError):lab.up(Path('not-created'),100,100)
            create.assert_not_called()
    def test_resource_parser_handles_concatenated_objects(self):
        first={'apiVersion':'v1','kind':'Service','metadata':{'name':'a'}}
        second={'apiVersion':'v1','kind':'List','items':[{'kind':'Deployment','metadata':{'name':'b'}}]}
        parsed=lab.resource_list(json.dumps(first)+'\n'+json.dumps(second))
        self.assertEqual([x['metadata']['name'] for x in parsed['items']],['a','b'])
    def test_install_refuses_existing_workload(self):
        with patch.object(lab,'load'),patch.object(lab,'kube',return_value=json.dumps({'items':[{'kind':'Deployment'}]})),patch.object(lab,'create') as create:
            with self.assertRaises(ValueError):lab.install(Path('ignored'))
            create.assert_not_called()
    def test_source_uses_absolute_restart_segment(self):
        with patch.object(lab,'create') as create,patch.object(lab,'kube'):
            lab.source_job('matching-source',3,10000,10000,100,True)
            obj=create.call_args[0][0];c=obj['spec']['template']['spec']['containers'][0]
            self.assertEqual(c['command'],['/app/matching-workload'])
            self.assertIn('-start=3',c['args']);self.assertIn('-end=10000',c['args'])
            self.assertIn('-total=10000',c['args'])
            self.assertEqual(obj['spec']['backoffLimit'],0)
    def fixture(self,out):
        (out/'settings.json').write_text(json.dumps({'count':5,'rate':100}))
        (out/'verification.json').write_text(json.dumps({'exact_order_oracle':True,'partial_fill_pod_restart':True}))
        fields=['sequence','type','start_unix_ns','end_unix_ns','duration_ns','received_unix_ns','delivery_unix_ns','ack_unix_ns','attempts']
        def write(name,stage):
            with (out/name).open('w',newline='') as f:
                w=csv.DictWriter(f,fieldnames=fields);w.writeheader()
                for seq in range(3,6):
                    base=seq*10000000
                    row=dict(sequence=seq,type='N',start_unix_ns=base,end_unix_ns=base+300,duration_ns=100,
                             received_unix_ns=base+100,delivery_unix_ns=base+200,ack_unix_ns=base+400,attempts=1)
                    if stage=='source':row['end_unix_ns']=base+10
                    w.writerow(row)
                row.update(sequence=0,type='Z');w.writerow(row)
        write('matching-source.csv','source');write('publisher.csv','publisher');write('consumer-test.csv','consumer')
    def test_analyze_exact_samples_and_retry_rejection(self):
        with tempfile.TemporaryDirectory(prefix='tmp_rovo_k8smatch_',dir=lab.ROOT) as d:
            out=Path(d);self.fixture(out);lab.analyze(out)
            r=json.loads((out/'result.json').read_text());self.assertEqual(r['measured'],3)
            self.assertEqual(r['latency']['send_to_commit']['p99_ms'],.0003)
            rows=lab.read_csv(out/'consumer-test.csv');rows[0]['attempts']='2'
            with (out/'consumer-test.csv').open('w',newline='') as f:
                w=csv.DictWriter(f,fieldnames=rows[0].keys());w.writeheader();w.writerows(rows)
            with self.assertRaises(ValueError):lab.analyze(out)

if __name__=='__main__':unittest.main()
