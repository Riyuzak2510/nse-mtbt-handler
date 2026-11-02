import unittest
from pathlib import Path
from analyze_matching_handoff import slices, analyze
ROOT = Path(__file__).resolve().parents[1]

class Tests(unittest.TestCase):
    def test_same_input_components_telescope(self):
        s = {'start_unix_ns': 10}
        a = {'end_unix_ns': 110}
        m = {'delivery_unix_ns': 140, 'start_unix_ns': 145, 'end_unix_ns': 200}
        r = {'stored_unix_ns': 130}
        x = slices(s,a,m,r)
        self.assertEqual(x['admission_commit_observed_to_matching_delivery'], 30)
        self.assertEqual(x['admission_commit_observed_to_matching_commit_observed'], 90)
        self.assertEqual(x['send_to_matching_commit_observed'], 190)
        self.assertEqual(sum(x[k] for k in ('send_to_admission_commit_observed', 'admission_commit_observed_to_routed_timestamp', 'routed_timestamp_to_matching_delivery', 'matching_delivery_to_process_start', 'matching_process_wall_interval')), 190)

    def test_client_commit_observation_not_actual_commit(self):
        x = slices({'start_unix_ns': 0}, {'end_unix_ns': 30}, {'delivery_unix_ns': 25, 'start_unix_ns': 27, 'end_unix_ns': 50}, {'stored_unix_ns': 20})
        self.assertEqual(x['admission_commit_observed_to_routed_timestamp'], -10)
        self.assertEqual(x['admission_commit_observed_to_matching_delivery'], -5)
        self.assertEqual(x['admission_commit_observed_to_matching_commit_observed'], 20)

    def test_retained_exact_fixture(self):
        p = ROOT/'verification/20261102-ticker-bench-tickers-r2'
        h = ROOT/'verification/20261102-ticker-benchmark-evidence-sha256.json'
        if not p.exists() or not h.exists(): self.skipTest('retained benchmark not present')
        x = analyze(p,h)
        self.assertTrue(x['complete'])
        self.assertEqual([s['count'] for s in x['stages']], [10000,10000])
        for s in x['stages']:
            self.assertEqual(s['slowest_one_percent_by_end_to_end']['count'],100)
            self.assertAlmostEqual(sum(s['mean_share_of_total_percent'].values()),100,places=9)
            self.assertEqual(s['distributions']['routed_timestamp_to_matching_delivery']['negative_count'],0)
        self.assertAlmostEqual(x['stages'][1]['distributions']['admission_commit_observed_to_matching_delivery']['mean_ms'], .7694760753,places=8)

if __name__ == '__main__': unittest.main()
