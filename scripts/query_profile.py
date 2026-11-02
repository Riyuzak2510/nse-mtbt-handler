"""Analyze client-observed database-operation spans, not PostgreSQL executor time."""
import collections
import csv


def summarize_queries(path, consumers, heartbeat, count, warmup, distribution):
    with path.open(newline='') as f:
        rows = list(csv.DictReader(f))
    allowed = {'begin', 'checkpoint_select_for_update', 'order_select', 'order_upsert', 'order_delete', 'checkpoint_update', 'commit'}
    events = collections.defaultdict(list)
    for row in rows:
        item = {k: (v if k in ('operation', 'type', 'success') else int(v)) for k, v in row.items()}
        if item['operation'] not in allowed or item['success'] != 'true' or item['attempt'] != 1:
            raise ValueError('query trace contains unsupported, failed or retried operation; inspect raw evidence')
        if item['duration_ns'] < 0 or item['end_unix_ns'] < item['start_unix_ns']:
            raise ValueError('negative query interval')
        events[item['broker_sequence']].append(item)
    total = count + warmup
    if set(events) != set(range(1, total + 2)):
        raise ValueError('query trace missing or duplicated broker event identity')
    metrics = collections.defaultdict(list)
    by_kind = collections.defaultdict(lambda: collections.defaultdict(list))
    residual = []
    calls_per_event = []
    event_durations = []
    dominant_calls = collections.Counter()
    measured_rows = 0
    for broker in range(1, total + 2):
        event = events[broker]
        is_heartbeat = broker == total + 1
        c = heartbeat if is_heartbeat else consumers[broker]
        seq = 0 if is_heartbeat else broker
        if any(r['sequence'] != seq or r['type'] != c['type'] for r in event):
            raise ValueError('query/source/consumer correlation mismatch')
        for row in event:
            if row['start_unix_ns'] < c['start_unix_ns'] or row['end_unix_ns'] > c['end_unix_ns']:
                raise ValueError('query span is outside measured Process interval')
        for before, after in zip(event, event[1:]):
            if before['end_unix_ns'] > after['start_unix_ns']:
                raise ValueError('SQL spans overlap; do not sum them')
        ops = collections.Counter(r['operation'] for r in event)
        for name in ('begin', 'checkpoint_select_for_update', 'checkpoint_update', 'commit'):
            if ops[name] != 1:
                raise ValueError('missing/duplicate transaction operation ' + name)
        if event[0]['operation'] != 'begin' or event[-1]['operation'] != 'commit':
            raise ValueError('invalid transaction operation order')
        reads = ops['order_select']
        writes = ops['order_upsert'] + ops['order_delete']
        if is_heartbeat:
            if reads or writes:
                raise ValueError('heartbeat mutated/looked up orders')
        elif c['type'] in ('N', 'M', 'X'):
            # The generated fixture uses known cancellations; native permitted missing X can be a no-op.
            if reads != 1 or writes != 1:
                raise ValueError('unexpected generated order operation count')
        elif c['type'] == 'T':
            if reads > 2 or writes > reads:
                raise ValueError('unexpected execution operation count')
        total_ops = sum(r['duration_ns'] for r in event)
        remainder = c['duration_ns'] - total_ops
        if remainder < 0:
            raise ValueError('operation sum exceeds measured Process duration')
        if is_heartbeat or broker <= warmup:
            continue
        measured_rows += len(event)
        calls_per_event.append(len(event))
        event_durations.append(c['duration_ns'])
        residual.append(remainder)
        grouped = collections.Counter()
        for row in event:
            op, duration = row['operation'], row['duration_ns']
            metrics[op].append(duration)
            by_kind[c['type']][op].append(duration)
            grouped[op] += duration
        dominant_calls[max(grouped, key=grouped.get)] += 1
    denominator = sum(event_durations)
    operations = []
    for op, values in metrics.items():
        operations.append({'operation': op, **distribution(values), 'total_ms': sum(values) / 1e6,
                           'share_of_process_percent': 100 * sum(values) / denominator,
                           'mean_contribution_ms_per_event': sum(values) / count / 1e6,
                           'calls_per_event': len(values) / count})
    operations.sort(key=lambda row: row['total_ms'], reverse=True)
    return {'measured_events': count, 'warmup_events_excluded': warmup, 'query_calls_measured': measured_rows,
            'query_calls_all_including_warmup_heartbeat': len(rows),
            'process_mean_ms': denominator / count / 1e6,
            'sum_operation_mean_ms_per_event': sum(sum(v) for v in metrics.values()) / count / 1e6,
            'residual_non_query': {**distribution(residual), 'share_of_process_percent': 100 * sum(residual) / denominator},
            'operation_ranked_by_total_time': operations,
            'largest_operation_for_event_counts': dict(dominant_calls),
            'calls_per_event_min': min(calls_per_event), 'calls_per_event_max': max(calls_per_event),
            'by_event_type': {kind: {op: distribution(values) for op, values in groups.items()} for kind, groups in by_kind.items()},
            'method': 'monotonic client-observed call spans; QueryRow through Scan; begin includes pool acquire; commit includes pool return; excludes startup/HTTP queries; warmup and heartbeat excluded; per-event additive totals, not sums of percentiles; residual includes application/driver instrumentation between calls; no server execution/lock/fsync attribution'}


def markdown_queries(profile):
    lines = ['# Database-operation profile', '', f"Measured {profile['measured_events']} events, {profile['query_calls_measured']} operation calls. Client-observed latency, not PostgreSQL executor time.", '',
             '| Operation | Calls | Mean ms/call | p50 ms | p95 ms | p99 ms | Share of Process time |',
             '| --- | ---: | ---: | ---: | ---: | ---: | ---: |']
    for row in profile['operation_ranked_by_total_time']:
        lines.append(f"| {row['operation']} | {row['count']} | {row['mean_ms']:.4f} | {row['p50_ms']:.4f} | {row['p95_ms']:.4f} | {row['p99_ms']:.4f} | {row['share_of_process_percent']:.2f}% |")
    lines += ['', f"Mean Process time/event: {profile['process_mean_ms']:.4f}ms; sum of operation mean contributions: {profile['sum_operation_mean_ms_per_event']:.4f}ms; non-query residual: {profile['residual_non_query']['mean_ms']:.4f}ms ({profile['residual_non_query']['share_of_process_percent']:.2f}%).", '',
              'Raw queries.csv and query-profile.json include per-event correlation and exact aggregation. Percentile columns are per call; do not add p99s. Durability, SQL and transaction semantics unchanged.']
    return '\n'.join(lines) + '\n'
