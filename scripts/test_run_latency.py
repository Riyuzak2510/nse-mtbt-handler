"""Unit tests for percentile math, trace validation, and v2 manifest checks; no infrastructure required."""
import csv
import tempfile
import unittest
from pathlib import Path
import run_latency


class LatencyTests(unittest.TestCase):
    def test_nearest_rank(self):
        stats = run_latency.distribution([i * 1000000 for i in range(1, 101)])
        self.assertEqual(stats['p50_ms'], 50)
        self.assertEqual(stats['p95_ms'], 95)
        self.assertEqual(stats['p99_ms'], 99)
        self.assertEqual(stats['mean_ms'], 50.5)
    
    def test_reject_invalid_latency(self):
        for values in ([], [-1, 2]):
            with self.assertRaises(ValueError):
                run_latency.distribution(values)
    
    def test_duplicate_samples_rejected(self):
        with tempfile.TemporaryDirectory(prefix='tmp_rovo_latency_', dir=run_latency.ROOT) as tmp:
            path = Path(tmp) / 'trace.csv'
            with path.open('w', newline='') as f:
                w = csv.writer(f)
                w.writerow(['sequence', 'type', 'start_unix_ns'])
                w.writerow([1, 'N', 10])
                w.writerow([1, 'N', 11])
            with self.assertRaises(ValueError):
                run_latency.read_trace(path)
    
    def test_heartbeat_excluded(self):
        with tempfile.TemporaryDirectory(prefix='tmp_rovo_latency_', dir=run_latency.ROOT) as tmp:
            path = Path(tmp) / 'trace.csv'
            path.write_text('sequence,type,start_unix_ns\n1,N,10\n0,Z,20\n')
            data, heartbeats = run_latency.read_trace(path)
            self.assertEqual(set(data), {1})
            self.assertEqual(len(heartbeats), 1)
    
    def test_reject_v1_manifest_with_run_id(self):
        """Reject v1 manifests that have run_id field."""
        v1_manifest = {
            'version': 1,
            'run_id': 'test-run-id',
            'stream_id': 1,
            'profile': 'cm-mtbt-le64-v1',
        }
        with self.assertRaises(ValueError) as cm:
            run_latency.check_manifest_v2(v1_manifest)
        self.assertIn('legacy run_id', str(cm.exception))
    
    def test_accept_v2_manifest(self):
        """Accept valid v2 manifests without run_id."""
        v2_manifest = {
            'version': 2,
            'stream_id': 1,
            'profile': 'cm-mtbt-le64-v1',
            'seed': 42,
            'baseline': 'empty',
            'instruments': [
                {'token': 900001, 'symbol': 'SIM0001', 'tick_size': 5, 'base_price': 100000}
            ]
        }
        # Should not raise
        run_latency.check_manifest_v2(v2_manifest)
    
    def test_reject_v2_manifest_without_stream_id(self):
        """Reject v2 manifests that lack stream_id."""
        manifest = {
            'version': 2,
            'profile': 'cm-mtbt-le64-v1',
        }
        with self.assertRaises(ValueError) as cm:
            run_latency.check_manifest_v2(manifest)
        self.assertIn('stream_id', str(cm.exception))
    
    def test_reject_v2_manifest_with_invalid_stream_id(self):
        """Reject v2 manifests with non-positive stream_id."""
        for stream_id in (0, -1, -100, 32768, True, 1.5, "1"):
            manifest = {
                'version': 2,
                'stream_id': stream_id,
                'profile': 'cm-mtbt-le64-v1',
            }
            with self.assertRaises(ValueError) as cm:
                run_latency.check_manifest_v2(manifest)
            self.assertIn('stream_id', str(cm.exception).lower())
    
    def test_reject_unsupported_version(self):
        """Reject unsupported manifest versions."""
        manifest = {
            'version': 99,
            'stream_id': 1,
        }
        with self.assertRaises(ValueError) as cm:
            run_latency.check_manifest_v2(manifest)
        self.assertIn('version', str(cm.exception))


if __name__ == '__main__':
    unittest.main()
