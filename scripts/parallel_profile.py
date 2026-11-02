"""Validate per-loop parallel CSVs without treating overlapping stages as serial.

Returned observations use the exact row dictionaries from run_latency.read_trace;
all durations passed to distribution stay in nanoseconds. No inferred timestamps.
"""
from pathlib import Path


def _row(row, label, previous_ack=0):
    required = ('received_unix_ns', 'delivery_unix_ns', 'start_unix_ns',
                'end_unix_ns', 'duration_ns', 'ack_unix_ns', 'ack_duration_ns', 'attempts')
    if any(k not in row for k in required):
        raise ValueError(f'{label}: missing timing fields')
    received, delivery, start, end, ack = [row[k] for k in
        ('received_unix_ns', 'delivery_unix_ns', 'start_unix_ns', 'end_unix_ns', 'ack_unix_ns')]
    if not (0 < received <= delivery <= start <= end <= ack):
        raise ValueError(f'{label}: invalid delivery/process/ack ordering')
    if row['duration_ns'] < 0 or row['ack_duration_ns'] < 0 or row['attempts'] != 1:
        raise ValueError(f'{label}: failed/retried or invalid duration')
    if delivery < previous_ack:
        raise ValueError(f'{label}: per-loop deliveries overlap previous acknowledgement')


def load_parallel(out: Path, shards: int, total: int, read_trace):
    """Return (workers_by_source_sequence, dispatcher_by_source_sequence, raw_hb, details).

    Exactly one loop owns each CSV. Normal benchmark requires no redeliveries,
    retries, duplicates or missing positions; no success-only censoring.
    """
    if not 1 <= shards <= 16 or total < 1:
        raise ValueError('invalid shard count or total')
    folder = out / 'parallel'
    expected_files = {'dispatcher.csv', *(f'shard-{i}.csv' for i in range(shards))}
    if not folder.is_dir() or {p.name for p in folder.iterdir()} != expected_files:
        raise ValueError('missing or unexpected parallel trace files')
    dispatch, heartbeat = read_trace(folder / 'dispatcher.csv')
    expected = set(range(1, total + 1))
    if set(dispatch) != expected or len(heartbeat) != 1:
        raise ValueError('incomplete dispatcher data/heartbeat')
    previous_ack = 0
    for seq, row in [*dispatch.items(), (0, heartbeat[0])]:
        _row(row, f'dispatcher {seq}', previous_ack)
        if row['sequence'] != seq or row['source_broker'] != (seq if seq else total + 1):
            raise ValueError('dispatcher source identity differs from normal benchmark stream')
        if seq and row['type'] not in ('N', 'M', 'X', 'T'):
            raise ValueError('unsupported dispatcher type')
        if not (row['end_unix_ns'] <= row['route_start_unix_ns'] <= row['route_end_unix_ns'] <= row['ack_unix_ns']):
            raise ValueError('routing span outside dispatch-to-ack interval')
        if row['route_duration_ns'] < 0:
            raise ValueError('negative routing duration')
        previous_ack = row['ack_unix_ns']
    workers = {}
    counts = {}
    for shard in range(shards):
        rows, hbs = read_trace(folder / f'shard-{shard}.csv')
        if hbs:
            raise ValueError('heartbeat must not be sent to a shard worker')
        last_ack = 0
        for local, (seq, row) in enumerate(rows.items(), 1):
            _row(row, f'shard {shard}/{seq}', last_ack)
            if seq not in dispatch or seq in workers:
                raise ValueError('unexpected/duplicate worker source position')
            if row['shard'] != shard or row['shard_sequence'] != local or row['source_broker'] != seq:
                raise ValueError('shard-local sequence or source provenance gap')
            d = dispatch[seq]
            if row['type'] != d['type'] or row['received_unix_ns'] // 1000 != d['received_unix_ns'] // 1000:
                raise ValueError('worker packet type/receive provenance mismatch')
            if not d['route_start_unix_ns'] <= row['delivery_unix_ns']:
                raise ValueError('worker delivery precedes committed dispatch/routing start')
            # NO dependency on route_end or raw ack: workers can finish first.
            last_ack = row['ack_unix_ns']
            workers[seq] = row
        if list(rows) != sorted(rows):
            raise ValueError('source positions not increasing within a shard')
        counts[str(shard)] = len(rows)
    if set(workers) != expected:
        raise ValueError('incomplete worker source positions')
    return workers, dispatch, heartbeat[0], {
        'shards': shards, 'dispatcher_data': len(dispatch), 'worker_data': len(workers),
        'worker_rows_per_shard': counts, 'raw_heartbeat': 1, 'worker_heartbeats': 0,
        'all_raw_and_shard_acks_confirmed': True, 'all_processing_attempts_one': True,
    }


def summarize_parallel(sent, pub, consume, dispatch, warmup, count, distribution, details):
    """Measured distributions, excluding warmup; overlapping spans are NOT summed."""
    series = {k: [] for k in ('dispatcher_db', 'dispatcher_route', 'dispatcher_raw_ack',
        'receive_to_raw_delivery', 'raw_delivery_to_shard_delivery',
        'route_start_to_shard_delivery', 'dispatch_commit_to_shard_delivery', 'send_to_raw_ack')}
    per_shard = {}
    intervals = []
    finished_before_route_end = 0
    for seq in range(warmup + 1, warmup + count + 1):
        d, w = dispatch[seq], consume[seq]
        if d['received_unix_ns'] != pub[seq]['received_unix_ns']:
            raise ValueError('dispatcher receiver metadata mismatch')
        if w['received_unix_ns'] // 1000 != pub[seq]['received_unix_ns'] // 1000:
            raise ValueError('worker receiver metadata mismatch')
        series['dispatcher_db'].append(d['duration_ns'])
        series['dispatcher_route'].append(d['route_duration_ns'])
        series['dispatcher_raw_ack'].append(d['ack_duration_ns'])
        series['receive_to_raw_delivery'].append(d['delivery_unix_ns'] - pub[seq]['received_unix_ns'])
        series['raw_delivery_to_shard_delivery'].append(w['delivery_unix_ns'] - d['delivery_unix_ns'])
        series['route_start_to_shard_delivery'].append(w['delivery_unix_ns'] - d['route_start_unix_ns'])
        series['dispatch_commit_to_shard_delivery'].append(w['delivery_unix_ns'] - d['end_unix_ns'])
        series['send_to_raw_ack'].append(d['ack_unix_ns'] - sent[seq]['start_unix_ns'])
        key = str(w['shard'])
        per_shard.setdefault(key, []).append(w['duration_ns'])
        intervals.extend(((w['start_unix_ns'], 1), (w['end_unix_ns'], -1)))
        finished_before_route_end += w['end_unix_ns'] < d['route_end_unix_ns']
    active = maximum = 0
    for _, delta in sorted(intervals):  # ends (-1) before starts (+1) on ties
        active += delta
        maximum = max(maximum, active)
    return {
        'validation': details, 'stage_latency': {k: distribution(v) for k, v in series.items()},
        'worker_processing_by_shard': {k: distribution(v) for k, v in per_shard.items()},
        'max_overlapping_worker_process_calls_measured': maximum,
        'worker_commits_before_routing_span_end_measured': finished_before_route_end,
        'overlap_note': 'Client processing intervals can include waits; overlap is not proof of simultaneous CPU/server execution. Routing and worker stages overlap and are not additive.',
    }
