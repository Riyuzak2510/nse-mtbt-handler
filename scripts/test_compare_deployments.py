import copy
import json
from pathlib import Path
import unittest
from unittest.mock import patch
from compare_deployments import load_v1_run,compare,markdown
from run_latency import read_trace
ROOT=Path(__file__).resolve().parents[1]


@unittest.skipUnless(all((ROOT / p).is_file() for p in (
    'benchmarks/20261020-compare-serial-100eps/result.json',
    'benchmarks/20261020-k8s-serial-100eps/result.json',
    'benchmarks/20261020-host-vs-k8s/serial-100eps.json',
    'benchmarks/20261020-host-vs-k8s/serial-100eps.md',
)), 'Private historical traces/rendered Markdown are intentionally not part of the source export')
class DeploymentComparisonTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.before=load_v1_run(ROOT/'benchmarks/20261020-compare-serial-100eps')
        cls.after=load_v1_run(ROOT/'benchmarks/20261020-k8s-serial-100eps')

    def test_exact_saved_comparison_and_markdown(self):
        r=compare(self.before,self.after);stem=ROOT/'benchmarks/20261020-host-vs-k8s/serial-100eps'
        self.assertEqual(r,json.loads(stem.with_suffix('.json').read_text()))
        self.assertEqual(markdown(r),stem.with_suffix('.md').read_text())
        self.assertFalse(r['matched_environment']);self.assertFalse(r['causal_attribution'])

    def test_reject_workload_catalog_boundary_core_and_oracle(self):
        def rate(x):x['result']['requested_rate_per_second']+=1
        def mix(x):x['result']['diagnostics']['message_mix_measured']['N']+=1
        def catalog(x):x['manifest']['instruments'][0]['tick_size']+=1
        def boundary(x):x['result']['method']['commit_boundary']='dispatcher ack'
        def oracle(x):x['expected']['orders'][0]['quantity']+=1
        def core(x):x['source_hashes']['internal/store/store.go']='changed'
        for mutate in (rate,mix,catalog,boundary,oracle,core):
            with self.subTest(mutation=mutate.__name__):
                bad=copy.deepcopy(self.after);mutate(bad)
                with self.assertRaises(ValueError):compare(self.before,bad)

    def test_reject_mutated_raw_evidence(self):
        def missing(r,h):r.pop(501)
        def retry(r,h):r[501]['attempts']=2
        def ack(r,h):r[501]['ack_unix_ns']=0
        def time(r,h):r[501]['end_unix_ns']+=1000
        def heartbeat(r,h):h.clear()
        for mutate in (missing,retry,ack,time,heartbeat):
            def altered(path):
                r,h=read_trace(path)
                if path.name=='consumer.csv':mutate(r,h)
                return r,h
            with self.subTest(mutation=mutate.__name__),patch('compare_deployments.read_trace',side_effect=altered):
                with self.assertRaises(ValueError):load_v1_run(ROOT/self.after['directory'])


if __name__=='__main__':unittest.main()
