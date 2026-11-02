import json
from pathlib import Path
import unittest
from unittest.mock import patch
import k8s_lab as lab


class LabTests(unittest.TestCase):
    def test_every_api_call_has_explicit_kubeconfig_context_namespace(self):
        with patch.object(lab,'command',return_value='ok') as command:
            self.assertEqual(lab.kubectl('get','pods'),'ok')
        self.assertEqual(command.call_args.args[0][:7],['kubectl','--kubeconfig',str(lab.KUBECONFIG),'--context','kind-nse-mtbt-lab','-n','nse-mtbt-lab'])

    def test_guard_refuses_remote_server(self):
        cfg={'clusters':[{'cluster':{'server':'https://remote.example:6443'}}]}
        with patch.object(Path,'is_file',return_value=True),patch.object(lab,'command',return_value=json.dumps(cfg)),patch.object(lab,'kubectl') as kube:
            with self.assertRaisesRegex(RuntimeError,'non-loopback'):lab.guard_cluster()
            kube.assert_not_called()

    def test_guard_refuses_unexpected_nodes(self):
        cfg={'clusters':[{'cluster':{'server':'https://127.0.0.1:32100'}}]}
        with patch.object(Path,'is_file',return_value=True),patch.object(lab,'command',return_value=json.dumps(cfg)),patch.object(lab,'kubectl',return_value=json.dumps({'items':[{'metadata':{'name':'prod'}}]})):
            with self.assertRaisesRegex(RuntimeError,'unexpected nodes'):lab.guard_cluster()

    def test_existing_namespace_refuses_setup_writes(self):
        with patch.object(lab,'kubectl',return_value='nse-mtbt-lab'),patch.object(lab,'create') as create:
            with self.assertRaisesRegex(RuntimeError,'namespace exists'):lab.up(None,None)
            create.assert_not_called()

    def test_resume_checks_both_owners_before_mutating(self):
        m={'version':2,'stream_id':1}
        def read(path,*args,**kwargs):return json.dumps(m if path.name=='manifest.json' else {'shards':0})
        def kube(*args,**kwargs):
            if args[:2]==('get','configmap'):return json.dumps({'data':{'manifest.json':json.dumps(m)}})
            return json.dumps({'spec':{'replicas':0 if args[2]=='publisher' else 1}})
        with patch.object(Path,'read_text',read),patch.object(lab,'kubectl',side_effect=kube) as calls:
            with self.assertRaisesRegex(ValueError,'both app deployments'):lab.resume(lab.ROOT)
            self.assertTrue(all(c.args[0]=='get' for c in calls.call_args_list))

    def test_legacy_configuration_refused_before_api_calls(self):
        with patch.object(Path,'read_text',return_value=json.dumps({'version':1,'run_id':'old','stream_id':1})),patch.object(lab,'kubectl') as kube:
            with self.assertRaises(ValueError):lab.resume(lab.ROOT)
            kube.assert_not_called()

    def test_source_is_non_retrying_and_not_in_base(self):
        root=lab.ROOT/'deploy/kubernetes';base=(root/'kustomization.yaml').read_text();job=(root/'simulator-job.yaml').read_text()
        self.assertNotIn('  - simulator-job.yaml',base)
        self.assertIn('backoffLimit: 0',job);self.assertIn('restartPolicy: Never',job);self.assertIn('-addr=publisher:19001',job)
        self.assertIn('mtbt-source',job);self.assertNotIn('mtbt-run',job)
        text=(root/'applications.yaml').read_text()
        self.assertEqual(text.count('replicas: 1'),2);self.assertEqual(text.count('strategy: {type: Recreate}'),2)
        self.assertEqual(text.count('httpGet: {path: /readyz'),2);self.assertEqual(text.count('httpGet: {path: /healthz'),4)


if __name__=='__main__':unittest.main()
