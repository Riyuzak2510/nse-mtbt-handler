#!/usr/bin/env python3
"""Read-only reanalysis of saved, exactly verified ticker benchmark traces.

No broker/database/cluster access, runtime changes or rewrites of archived results.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path
from analyze_outcome_benchmark import indexed, jsonlines, signed, trace

INPUTS = ('settings.json', 'execution.json', 'readback-result.json',
          'source-1.csv', 'source-2.csv', 'publisher.csv', 'order-service.csv',
          'ticker-matcher-1.csv', 'ticker-matcher-2.csv', 'raw-readback.jsonl',
          'routed-readback.jsonl')


def slices(source, admission, matcher, routed):
    """Same-input wall-clock slices telescope; not independent percentile sums."""
    return {
        'send_to_admission_commit_observed': admission['end_unix_ns'] - source['start_unix_ns'],
        'admission_commit_observed_to_routed_timestamp': routed['stored_unix_ns'] - admission['end_unix_ns'],
        'routed_timestamp_to_matching_delivery': matcher['delivery_unix_ns'] - routed['stored_unix_ns'],
        'matching_delivery_to_process_start': matcher['start_unix_ns'] - matcher['delivery_unix_ns'],
        'matching_process_wall_interval': matcher['end_unix_ns'] - matcher['start_unix_ns'],
        'admission_commit_observed_to_matching_delivery': matcher['delivery_unix_ns'] - admission['end_unix_ns'],
        'admission_commit_observed_to_matching_process_start': matcher['start_unix_ns'] - admission['end_unix_ns'],
        'admission_commit_observed_to_matching_commit_observed': matcher['end_unix_ns'] - admission['end_unix_ns'],
        'send_to_matching_commit_observed': matcher['end_unix_ns'] - source['start_unix_ns'],
    }


def analyze(root, evidence_manifest):
    checksums = json.loads(evidence_manifest.read_text())
    project = Path(__file__).resolve().parents[1]
    provenance = {}
    for name in INPUTS:
        path = root / name
        key = str(path.resolve().relative_to(project))
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if checksums.get(key) != actual:
            raise ValueError('saved evidence hash mismatch: ' + key)
        provenance[key] = actual
    settings = json.loads((root / 'settings.json').read_text())
    execution = json.loads((root / 'execution.json').read_text())
    validation = json.loads((root / 'readback-result.json').read_text())
    if settings['topology'] != 'tickers' or len(settings['stages']) != 2:
        raise ValueError('two-stage split topology required')
    if not all(execution.get(k) for k in ('complete', 'no_app_restarts', 'exact_stage_orders', 'protected_namespace_specs_unchanged')):
        raise ValueError('incomplete execution gates')
    if not all(validation.get(k) for k in ('complete', 'exact_raw_payloads_and_envelopes', 'exact_routing_and_digest_chains', 'exact_all_outcome_fields_and_fanout')):
        raise ValueError('incomplete exact readback gates')
    total = sum(stage['count'] for stage in settings['stages'])
    expected = range(1, total + 1)
    source = indexed(trace(root / 'source-1.csv') + trace(root / 'source-2.csv'), expected, 'source')
    admission = indexed(trace(root / 'order-service.csv'), expected, 'admission')
    matching = indexed(trace(root / 'ticker-matcher-1.csv') + trace(root / 'ticker-matcher-2.csv'), expected, 'matching')
    raw = jsonlines(root / 'raw-readback.jsonl')
    native_by_broker = {row['broker_sequence']: row for row in raw}
    routes = {}
    for row in jsonlines(root / 'routed-readback.jsonl'):
        original = native_by_broker[row['source_broker']]
        if original['type'] == 'Z':
            continue
        n = original['source_sequence']
        if n in routes:
            raise ValueError('duplicate routed native input')
        routes[n] = row
    if set(routes) != set(expected):
        raise ValueError('incomplete routed inputs')
    observations = {}
    for n in expected:
        a, m, r = admission[n], matching[n], routes[n]
        if a['attempts'] != 1 or m['attempts'] != 1 or a['ack_unix_ns'] <= 0 or m['ack_unix_ns'] <= 0:
            raise ValueError('redelivery or missing confirmed ACK')
        original = native_by_broker[r['source_broker']]
        if (a['source_broker'] != m['source_broker'] or m['source_broker'] != r['source_broker']
                or a['received_unix_ns'] != original['received_unix_ns']
                or m['received_unix_ns'] != original['received_unix_ns']
                or r['token'] != m['shard'] or r['local_sequence'] != m['shard_sequence']):
            raise ValueError('raw/ticker identity mismatch')
        row = slices(source[n], a, m, r)
        parts = ('send_to_admission_commit_observed', 'admission_commit_observed_to_routed_timestamp',
                 'routed_timestamp_to_matching_delivery', 'matching_delivery_to_process_start',
                 'matching_process_wall_interval')
        if sum(row[k] for k in parts) != row['send_to_matching_commit_observed']:
            raise ValueError('non-telescoping timestamps')
        if row['send_to_matching_commit_observed'] <= 0 or row['matching_process_wall_interval'] < 0:
            raise ValueError('invalid processing chronology')
        row['matching_process_monotonic_duration'] = m['duration_ns']
        row['matching_process_wall_minus_monotonic'] = row['matching_process_wall_interval'] - m['duration_ns']
        row['matching_ack_after_commit_duration'] = m['ack_duration_ns']
        observations[n] = row
    stages = []
    first = 1
    for index, stage in enumerate(settings['stages'], 1):
        numbers = list(range(first, first + stage['count']))
        distributions = {key: signed([observations[n][key] for n in numbers]) for key in observations[first]}
        total_ns = sum(observations[n]['send_to_matching_commit_observed'] for n in numbers)
        shares = {key: 100 * sum(observations[n][key] for n in numbers) / total_ns for key in (
            'send_to_admission_commit_observed', 'admission_commit_observed_to_matching_delivery',
            'matching_delivery_to_process_start', 'matching_process_wall_interval')}
        # Attribute the same slowest 1% inputs; never add/subtract marginal p99s.
        slowest = sorted(numbers, key=lambda n: observations[n]['send_to_matching_commit_observed'])[-max(1, math.ceil(len(numbers) * .01)):]
        cohort = {key: signed([observations[n][key] for n in slowest]) for key in observations[first]}
        starts = [source[n]['start_unix_ns'] for n in numbers]
        stages.append(dict(stage=index, count=len(numbers), requested_rate=stage['rate'],
                           actual_send_rate=(len(starts)-1)/((max(starts)-min(starts))/1e9),
                           distributions=distributions, mean_share_of_total_percent=shares,
                           slowest_one_percent_by_end_to_end=dict(count=len(slowest), distributions=cohort),
                           per_ticker={str(token): {
                               'handoff': signed([observations[n]['admission_commit_observed_to_matching_delivery'] for n in numbers if matching[n]['shard'] == token]),
                               'matching_process': signed([observations[n]['matching_process_monotonic_duration'] for n in numbers if matching[n]['shard'] == token])}
                               for token in sorted({matching[n]['shard'] for n in numbers})}))
        first += stage['count']
    return dict(complete=True, analysis='offline saved benchmark handoff decomposition', namespace=settings['namespace'],
                runtime_changed=False, benchmark_rerun=False, source_hashes=provenance, stages=stages,
                boundaries=dict(admission='client observes successful admission transaction return',
                                routed='JetStream retained message timestamp, not exact disk-fsync instant',
                                delivery='matching Fetch(1) returned; includes transport, consumer wait and scheduling',
                                process='ProcessCommand: validation, engine work, SQL transaction and client commit response',
                                ack='confirmed input ACK after successful matching method return; not in send-to-commit latency'),
                limitations=['Same shared/degraded Linux-VM environment as saved benchmark; source missed requested rates.',
                             'Admission client commit observation can follow dispatcher activity; signed negatives retained, not clamped.',
                             'No dispatcher-step/PubAck timing in saved traces; cannot isolate pure network RTT or dispatch SQL latency.',
                             'Per-input wall-clock slices telescope; their marginal percentiles must not be added/subtracted.',
                             'Monotonic method duration differs slightly from wall-clock endpoints; both retained.',
                             'No new worker design, runtime deployment or production capacity inference.'])


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input', type=Path, required=True)
    p.add_argument('--evidence-manifest', type=Path, required=True)
    p.add_argument('--out', type=Path, required=True)
    args = p.parse_args()
    result = analyze(args.input, args.evidence_manifest)
    with args.out.open('x') as f:
        json.dump(result, f, indent=2)
        f.write('\n')
    for stage in result['stages']:
        print('stage', stage['stage'], 'actual rate', round(stage['actual_send_rate'], 2), 'mean shares', stage['mean_share_of_total_percent'])


if __name__ == '__main__':
    main()
