import copy
import json
from pathlib import Path
import unittest
from compare_latency import compare,markdown
ROOT=Path(__file__).resolve().parents[1]


@unittest.skipUnless(all((ROOT / f'benchmarks/20261020-latency-comparison/{rate}eps{extension}').is_file()
                        for rate in (100, 1000) for extension in ('.json', '.md')),
                     'Historical trace fixtures/rendered Markdown intentionally excluded from source export')
class CompareLatencyTests(unittest.TestCase):
    def pair(self,rate=100):
        def load(mode):return json.loads((ROOT/f'benchmarks/20261020-compare-{mode}-{rate}eps/result.json').read_text())
        return load('serial'),load('parallel3')

    def test_exact_archived_results_and_markdown(self):
        for rate in (100,1000):
            a,b=self.pair(rate);r=compare(a,b);stem=ROOT/f'benchmarks/20261020-latency-comparison/{rate}eps'
            self.assertEqual(r,json.loads(stem.with_suffix('.json').read_text()))
            self.assertEqual(markdown(r),stem.with_suffix('.md').read_text())

    def test_v2_uses_stream_identity_without_lifecycle_id(self):
        a,b=self.pair()
        for r in (a,b):r.pop('run_id');r.update(version=2,stream_id=1)
        r=compare(a,b);self.assertEqual(r['serial_stream_id'],1);self.assertNotIn('serial_run_id',r)

    def test_reject_workload_build_durability_oracle_retry(self):
        a,b=self.pair()
        def rate(r):r['requested_rate_per_second']+=1
        def catalog(r):r['instruments']+=1
        def mix(r):r['diagnostics']['message_mix_measured']['N']+=1
        def source(r):r['environment']['source_sha256']['go.mod']='changed'
        def durability(r):r['environment']['postgres_settings']['fsync']='off'
        def oracle(r):r['diagnostics']['exact_order_oracle_match']=False
        def retry(r):r['diagnostics']['processing_attempts_total']+=1
        def loss(r):r['diagnostics']['loss_or_missing_samples']=True
        for mutate in (rate,catalog,mix,source,durability,oracle,retry,loss):
            with self.subTest(mutation=mutate.__name__):
                bad=copy.deepcopy(b);mutate(bad)
                with self.assertRaises(ValueError):compare(a,bad)


if __name__=='__main__':unittest.main()
