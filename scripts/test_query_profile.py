import csv
import tempfile
import unittest
from pathlib import Path
import run_latency
from query_profile import summarize_queries

class QueryProfileTests(unittest.TestCase):
    def fixture(self, path):
        rows = []
        consumers = {}
        for broker in (1, 2, 3):
            kind = 'Z' if broker == 3 else 'N'
            seq = 0 if kind == 'Z' else broker
            start = broker * 10000
            operations = ['begin', 'checkpoint_select_for_update']
            if kind != 'Z':
                operations += ['order_select', 'order_upsert']
            operations += ['checkpoint_update', 'commit']
            for i, op in enumerate(operations):
                rows.append([broker, seq, kind, 1, op, start + i * 100, start + i * 100 + 80, 80, 'true'])
            consumers[broker] = {'type': kind, 'start_unix_ns': start - 10, 'end_unix_ns': start + len(operations) * 100, 'duration_ns': len(operations) * 100 + 10}
        self.write(path, rows)
        return rows, consumers
    def write(self, path, rows):
        with path.open('w', newline='') as f:
            w = csv.writer(f)
            w.writerow(['broker_sequence', 'sequence', 'type', 'attempt', 'operation', 'start_unix_ns', 'end_unix_ns', 'duration_ns', 'success'])
            w.writerows(rows)
    def test_additive_contribution_and_exclusions(self):
        with tempfile.TemporaryDirectory(prefix='tmp_rovo_query_', dir=run_latency.ROOT) as tmp:
            path = Path(tmp) / 'queries.csv'; rows, c = self.fixture(path)
            p = summarize_queries(path, {1: c[1], 2: c[2]}, c[3], 1, 1, run_latency.distribution)
            self.assertEqual(p['query_calls_measured'], 6)
            self.assertEqual(p['query_calls_all_including_warmup_heartbeat'], 16)
            self.assertEqual(p['calls_per_event_min'], 6)
            self.assertAlmostEqual(p['process_mean_ms'], .000610)
            self.assertAlmostEqual(p['sum_operation_mean_ms_per_event'], .000480)
            self.assertAlmostEqual(p['residual_non_query']['mean_ms'], .000130)
            self.assertAlmostEqual(sum(x['share_of_process_percent'] for x in p['operation_ranked_by_total_time']) + p['residual_non_query']['share_of_process_percent'], 100)
    def test_invalid_trace_rejected(self):
        for change in ('missing', 'failed', 'retry', 'overlap'):
            with self.subTest(change=change), tempfile.TemporaryDirectory(prefix='tmp_rovo_query_', dir=run_latency.ROOT) as tmp:
                path = Path(tmp) / 'queries.csv'; rows, c = self.fixture(path)
                if change == 'missing':
                    rows.pop(6)
                elif change == 'failed':
                    rows[6][-1] = 'false'
                elif change == 'retry':
                    rows[6][3] = 2
                else:
                    rows[7][5] = rows[6][5]
                self.write(path, rows)
                with self.assertRaises(ValueError):
                    summarize_queries(path, {1: c[1], 2: c[2]}, c[3], 1, 1, run_latency.distribution)

if __name__ == '__main__':
    unittest.main()
