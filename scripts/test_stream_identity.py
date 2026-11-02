"""Live v2 preflight and exact SQL/oracle summary contracts, with no live database."""
import csv
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock
import run_latency


class StreamIdentityTests(unittest.TestCase):
    def test_legacy_manifest_rejected_before_sql(self):
        query = Mock(side_effect=AssertionError('SQL must not execute'))
        with self.assertRaises(ValueError):
            run_latency.summarize(Path('.'), 2, 0, 100,
                                  {'version': 1, 'run_id': 'archived', 'stream_id': 1}, {}, sql_query=query)
        query.assert_not_called()

    def test_database_preflight_never_resets(self):
        queries = []
        def query(text):
            queries.append(text)
            if 'information_schema' in text: return '0'
            if 'to_regclass' in text: return 't'
            return 'VALID,5,6'
        with self.assertRaisesRegex(ValueError, 'already initialized'):
            run_latency.check_database_state(1, query, require_empty=True)
        self.assertTrue(all(x.startswith('SELECT ') for x in queries))
        self.assertIn('WHERE stream_id=1', queries[-1])
        with self.assertRaisesRegex(ValueError, 'legacy database'):
            run_latency.check_database_state(1, lambda _: '1', require_empty=True)

    def test_small_sample_nearest_rank_and_zero(self):
        stats = run_latency.distribution([0, 1000000, 2000000])
        self.assertEqual(stats['p50_ms'], 1)
        self.assertEqual(stats['p95_ms'], 2)
        self.assertEqual(stats['p999_ms'], 2)
        self.assertEqual(stats['min_ms'], 0)

    def test_stream_scoped_summary_and_oracle(self):
        manifest = dict(version=2, stream_id=1, seed=42, instruments=[{'token': 900001}])
        with tempfile.TemporaryDirectory(prefix='tmp_rovo_identity_', dir=run_latency.ROOT) as temp:
            out = Path(temp)
            fields = ['sequence', 'type', 'start_unix_ns', 'received_unix_ns', 'delivery_unix_ns',
                      'end_unix_ns', 'duration_ns', 'attempts', 'ack_unix_ns', 'ack_duration_ns']
            for filename in ['simulator.csv', 'publisher.csv', 'consumer.csv']:
                with (out / filename).open('w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=fields)
                    writer.writeheader()
                    for seq, kind in [(1, 'N'), (2, 'X'), (0, 'Z')]:
                        t = (seq or 3) * 10000000
                        row = dict(sequence=seq, type=kind, start_unix_ns=t,
                                   received_unix_ns=t+100, delivery_unix_ns=t+200,
                                   end_unix_ns=t+400, duration_ns=100, attempts=1,
                                   ack_unix_ns=t+500, ack_duration_ns=100)
                        if filename == 'consumer.csv': row['start_unix_ns'] = t+300
                        writer.writerow(row)
            expected = dict(stream_id=1, sequence=2, orders=[])
            (out / 'expected.json').write_text(json.dumps(expected))
            def query(text):
                self.assertNotIn('run_id', text)
                self.assertIn('stream_id=1', text)
                return '[]' if 'active_orders' in text else 'VALID,2,3'
            result = run_latency.summarize(out, 2, 0, 100, manifest, {}, sql_query=query)
            self.assertEqual(result['version'], 2)
            self.assertEqual(result['stream_id'], 1)
            self.assertNotIn('run_id', result)
            self.assertTrue(result['diagnostics']['exact_order_oracle_match'])
            expected['orders'] = [dict(id_bits=1, token=900001, side=66, price=100, quantity=1)]
            (out / 'expected.json').write_text(json.dumps(expected))
            with self.assertRaisesRegex(ValueError, 'oracle mismatch'):
                run_latency.summarize(out, 2, 0, 100, manifest, {}, sql_query=query)


if __name__ == '__main__':
    unittest.main()
