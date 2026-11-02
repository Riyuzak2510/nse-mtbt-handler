import copy
import csv
import tempfile
import unittest
from pathlib import Path
from parallel_profile import load_parallel, summarize_parallel
import run_latency


class ParallelProfileTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='tmp_rovo_parallel_profile_', dir=run_latency.ROOT)
        self.addCleanup(self.tmp.cleanup)
        self.out = Path(self.tmp.name)
        (self.out / 'parallel').mkdir()
        self.dispatch = []
        self.workers = [[], []]
        # Worker seq1 finishes AFTER seq2; workers can commit before route end.
        for seq, shard, start, end in [(1, 0, 13_000, 45_000), (2, 1, 23_000, 26_000), (3, 0, 47_000, 50_000), (4, 1, 43_000, 48_000)]:
            base = seq * 10_000
            d = dict(sequence=seq, type='N', received_unix_ns=base-1000, delivery_unix_ns=base, start_unix_ns=base+100, end_unix_ns=base+1000, duration_ns=900, ack_unix_ns=base+9000, ack_duration_ns=100, attempts=1, shard=-1, shard_sequence=0, source_broker=seq, route_start_unix_ns=base+1100, route_end_unix_ns=base+8000, route_duration_ns=6900)
            self.dispatch.append(d)
            w = dict(d, shard=shard, shard_sequence=len(self.workers[shard])+1, delivery_unix_ns=start, start_unix_ns=start+100, end_unix_ns=end, duration_ns=end-start-100, ack_unix_ns=end+100, route_start_unix_ns=0, route_end_unix_ns=0, route_duration_ns=0)
            self.workers[shard].append(w)
        self.hb = dict(self.dispatch[-1], sequence=0, type='Z', source_broker=5, received_unix_ns=50000, delivery_unix_ns=51000, start_unix_ns=51100, end_unix_ns=52000, route_start_unix_ns=52100, route_end_unix_ns=53000, ack_unix_ns=54000)
        self.write()

    def write(self):
        def emit(name, rows):
            with (self.out / 'parallel' / name).open('w', newline='') as f:
                w = csv.DictWriter(f, fieldnames=list(self.dispatch[0])); w.writeheader(); w.writerows(rows)
        emit('dispatcher.csv', self.dispatch + [self.hb])
        for i, rows in enumerate(self.workers): emit(f'shard-{i}.csv', rows)

    def load(self):
        return load_parallel(self.out, 2, 4, run_latency.read_trace)

    def test_overlap_and_out_of_order_completion_valid(self):
        workers, dispatch, hb, details = self.load()
        self.assertGreater(workers[1]['end_unix_ns'], workers[2]['end_unix_ns'])
        sent = {seq: dict(row, start_unix_ns=row['received_unix_ns']-100) for seq,row in dispatch.items()}
        pub = {seq: dict(row) for seq,row in dispatch.items()}
        r = summarize_parallel(sent,pub,workers,dispatch,1,3,run_latency.distribution,details)
        # Nanoseconds go into distribution unchanged: 900ns == .0009ms.
        self.assertEqual(r['stage_latency']['dispatcher_db']['mean_ms'], .0009)
        self.assertEqual(r['stage_latency']['dispatcher_db']['count'], 3)
        self.assertEqual(r['max_overlapping_worker_process_calls_measured'],2)
        self.assertGreater(r['worker_commits_before_routing_span_end_measured'],0)
        self.assertEqual(hb['source_broker'],5)

    def test_reject_mutations(self):
        original = copy.deepcopy((self.dispatch,self.workers,self.hb))
        mutations = [
            lambda: self.workers[0].pop(),
            lambda: self.workers[1][0].update(shard_sequence=2),
            lambda: self.workers[1][0].update(source_broker=1),
            lambda: self.workers[1][0].update(attempts=2),
            lambda: self.workers[1][0].update(ack_unix_ns=0),
            lambda: self.workers[1][0].update(shard=0),
            lambda: self.workers[1][0].update(type='M'),
            lambda: self.workers[1][0].update(received_unix_ns=1),
            lambda: self.workers[0][1].update(delivery_unix_ns=44000),
            lambda: self.dispatch[0].update(route_start_unix_ns=1),
            lambda: self.dispatch[0].update(route_end_unix_ns=100000),
            lambda: self.hb.update(source_broker=4),
            lambda: self.dispatch[0].update(attempts=2),
        ]
        for i,mutate in enumerate(mutations):
            with self.subTest(i=i):
                self.dispatch,self.workers,self.hb=copy.deepcopy(original);mutate();self.write()
                with self.assertRaises(ValueError):self.load()

    def test_missing_and_unexpected_files_rejected(self):
        extra=self.out/'parallel'/'shard-2.csv';extra.write_text('bad')
        with self.assertRaises(ValueError):self.load()
        extra.unlink();(self.out/'parallel'/'shard-1.csv').unlink()
        with self.assertRaises(ValueError):self.load()

    def test_receiver_microsecond_precision(self):
        self.workers[0][0]['received_unix_ns'] += 999
        self.write();self.load()
        self.workers[0][0]['received_unix_ns'] += 1
        self.write()
        with self.assertRaises(ValueError):self.load()


if __name__ == '__main__': unittest.main()
