#!/usr/bin/env python3
"""Descriptive historical serial-deployment comparison; never a matched-environment speedup.

Only reads archived evidence; no SQL, cluster operations, traffic, or runtime changes.
Supports v1 archived runs via explicit offline loader (not live SQL).
The strict same-environment serial/parallel comparator remains separate and unchanged.
"""
import argparse
import collections
import hashlib
import json
import math
from pathlib import Path

from run_latency import distribution, read_trace

ROOT = Path(__file__).resolve().parents[1]
METRICS = ('send_to_commit', 'receive_to_commit', 'send_to_receive', 'receiver_to_delivery',
           'db_process', 'publish_confirm', 'consumer_ack', 'send_to_ack')
STATS = ('mean_ms', 'p50_ms', 'p95_ms', 'p99_ms', 'p999_ms', 'max_ms')
SCOPE = ('Descriptive archived before/after observations, one run per deployment. Workload and '
         'logical timing boundaries match, but build/environment/read-load differ. '
         'No isolated Kubernetes effect, statistical significance, maximum capacity, or production SLO claim.')


def load_v1_run(directory):
    """Load archived v1 run (with run_id) for offline comparison only."""
    directory = Path(directory).resolve()
    directory.relative_to(ROOT)  # Archived inputs must stay within this workspace.
    result = json.loads((directory/'result.json').read_text())
    manifest = json.loads((directory/'manifest.json').read_text())
    expected = json.loads((directory/'expected.json').read_text())
    
    # Verify this is v1 (has run_id, not stream_id-only)
    if 'run_id' not in result:
        raise ValueError('expected archived v1 run with run_id; use compare_latency for live v2 runs')
    
    d = result['diagnostics']
    if result['shards'] != 0 or result['mode'] != 'serial direct consumer':
        raise ValueError('only direct serial observations have comparable processing endpoints here')
    if (not d['exact_order_oracle_match'] or d['status'] != 'VALID' or
            d['loss_or_missing_samples'] or d['redeliveries_or_duplicate_samples'] or
            d['processing_attempts_total'] != d['generated_data']):
        raise ValueError('failed archived correctness gates')
    total = d['measured_data'] + d['warmup_data_excluded']
    if any(d[key] != total for key in ('generated_data', 'published_data', 'committed_data', 'last_sequence')):
        raise ValueError('inconsistent generated/published/committed checkpoint counts')
    if (result['run_id'] != manifest['run_id'] or expected['run_id'] != result['run_id'] or
            expected['sequence'] != total or len(expected['orders']) != d['final_active_orders']):
        raise ValueError('manifest/oracle identity or checkpoint mismatch')
    if result['seed'] != manifest['seed'] or result['instruments'] != len(manifest['instruments']):
        raise ValueError('manifest workload disagrees with summary')
    if result['method']['commit_boundary'] != 'client-observed successful Store.Process return':
        raise ValueError('different commit boundary')
    
    traces = [read_trace(directory/f) for f in ('simulator.csv', 'publisher.csv', 'consumer.csv')]
    source, pub, consumer = [x[0] for x in traces]
    positions = set(range(1, total+1))
    for rows, hb in traces:
        if set(rows) != positions or len(hb) != 1 or hb[0]['sequence'] != 0:
            raise ValueError('missing data or heartbeat samples')
    if traces[2][1][0]['attempts'] != 1 or traces[2][1][0]['ack_unix_ns'] <= 0:
        raise ValueError('heartbeat retry or unconfirmed ack')
    
    values = collections.defaultdict(list)
    kinds = collections.Counter()
    for seq in range(1, total+1):
        a, b, c = source[seq], pub[seq], consumer[seq]
        if not (a['type'] == b['type'] == c['type'] and a['type'] in ('N', 'M', 'X', 'T')):
            raise ValueError('message identity mismatch')
        if (c['attempts'] != 1 or c['ack_unix_ns'] < c['end_unix_ns'] or
                b['received_unix_ns'] != c['received_unix_ns']):
            raise ValueError('retry, unconfirmed ack, or changed receive provenance')
        if not (a['start_unix_ns'] <= b['received_unix_ns'] <= c['delivery_unix_ns'] <=
                c['start_unix_ns'] <= c['end_unix_ns']):
            raise ValueError('invalid cross-stage ordering/clock')
        if seq <= d['warmup_data_excluded']:
            continue
        kinds[a['type']] += 1
        for key, value in zip(METRICS, (
            c['end_unix_ns']-a['start_unix_ns'], c['end_unix_ns']-b['received_unix_ns'],
            b['received_unix_ns']-a['start_unix_ns'], c['delivery_unix_ns']-b['received_unix_ns'],
            c['duration_ns'], b['duration_ns'], c['ack_duration_ns'],
            c['ack_unix_ns']-a['start_unix_ns'])):
            values[key].append(value)
    
    if dict(kinds) != d['message_mix_measured']:
        raise ValueError('raw measured event mix disagrees with summary')
    for key in METRICS:
        actual = distribution(values[key])
        saved = result['latency'][key]
        if actual.keys() != saved.keys() or any(
                not math.isclose(actual[k], saved[k], rel_tol=1e-12, abs_tol=1e-9) for k in actual):
            raise ValueError('raw trace distribution disagrees with saved summary: '+key)
    
    files = ('result.json', 'manifest.json', 'expected.json', 'simulator.csv', 'publisher.csv', 'consumer.csv')
    hashes = result['environment'].get('source_sha256')
    evidence_files = list(files)
    if hashes is None:
        hashes = json.loads((directory/'source-sha256.json').read_text())
        evidence_files.append('source-sha256.json')
    durability = result['environment'].get('postgres_settings')
    if durability is None:
        verification = json.loads((directory/'verification.json').read_text())
        evidence_files.append('verification.json')
        if (verification['run_id'] != result['run_id'] or not verification['all_orders_match_oracle'] or
                verification['source_sequence'] != total or verification['broker_sequence'] != total+1 or
                verification['status'] != 'VALID' or verification['postgres_durability'] != 'on,on,on'):
            raise ValueError('archived deployment verification disagrees with run')
        durability = dict(fsync='on', synchronous_commit='on', full_page_writes='on')
    if durability != dict(fsync='on', synchronous_commit='on', full_page_writes='on'):
        raise ValueError('all three PostgreSQL durability settings must be on')
    
    return dict(
        result=result, manifest=manifest, expected=expected, source_hashes=hashes,
        directory=str(directory.relative_to(ROOT)),
        evidence_sha256={f: hashlib.sha256((directory/f).read_bytes()).hexdigest() for f in evidence_files}
    )


def compare(before, after):
    """Compare two archived v1 runs (offline loader only)."""
    a, b = before['result'], after['result']
    for key in ('requested_rate_per_second', 'seed', 'instruments', 'shards'):
        if a[key] != b[key]:
            raise ValueError('different workload: '+key)
    for key in ('measured_data', 'warmup_data_excluded', 'message_mix_measured', 'final_active_orders'):
        if a['diagnostics'][key] != b['diagnostics'][key]:
            raise ValueError('different observations: '+key)
    for key in ('version', 'profile', 'stream_id', 'seed', 'baseline', 'instruments'):
        if before['manifest'][key] != after['manifest'][key]:
            raise ValueError('different catalog/profile: '+key)
    for key in ('percentile', 'send_boundary', 'commit_boundary', 'publisher_boundary', 'queue_boundary'):
        if a['method'][key] != b['method'][key]:
            raise ValueError('different measurement boundary: '+key)
    if sorted(before['expected']['orders'], key=lambda o: o['id_bits']) != sorted(
            after['expected']['orders'], key=lambda o: o['id_bits']):
        raise ValueError('different independent expected final book')
    
    core = sorted(k for k in before['source_hashes'] if k.startswith((
        'internal/book/', 'internal/feed/', 'internal/broker/', 'internal/store/', 'internal/timingtrace/')))
    if not core or any(before['source_hashes'][k] != after['source_hashes'].get(k) for k in core):
        raise ValueError('missing or changed core protocol/book/broker/store/timing source')
    
    metrics = {}
    for key in METRICS:
        metrics[key] = {}
        for stat in STATS:
            old, new = a['latency'][key][stat], b['latency'][key][stat]
            metrics[key][stat] = dict(
                before_ms=old, after_ms=new, delta_ms=new-old,
                relative_change_percent=(new/old-1)*100 if old else None
            )
    
    return dict(
        scope=SCOPE,
        workload_validated=True,
        raw_trace_distributions_recomputed=True,
        matched_environment=False,
        causal_attribution=False,
        unchanged_core_source_files=core,
        requested_rate_per_second=a['requested_rate_per_second'],
        measured_events_each=a['diagnostics']['measured_data'],
        before={k: before[k] for k in ('directory', 'evidence_sha256')},
        after={k: after[k] for k in ('directory', 'evidence_sha256')},
        run_ids={'before': a['run_id'], 'after': b['run_id']},
        actual_send_rates={'before': a['actual_send_rate_per_second'], 'after': b['actual_send_rate_per_second']},
        method_differences={k: dict(before=a['method'].get(k), after=b['method'].get(k))
                            for k in sorted(a['method'].keys() | b['method'].keys())
                            if a['method'].get(k) != b['method'].get(k)},
        environments={'before': a['environment'], 'after': b['environment']},
        metrics=metrics
    )


def markdown(result):
    lines = [
        '# Archived host-local versus Kubernetes serial observations',
        '',
        result['scope'],
        '',
        f"Before `{result['run_ids']['before']}`; after `{result['run_ids']['after']}`. "
        f"{result['measured_events_each']} measured events each, "
        f"{result['requested_rate_per_second']} requested events/s. Negative delta means lower latency.",
        '',
        '| Boundary / statistic | Before ms | Kubernetes ms | Delta ms | Relative change |',
        '| --- | ---: | ---: | ---: | ---: |',
    ]
    
    for key, stats in result['metrics'].items():
        for stat, v in stats.items():
            pct = f"{v['relative_change_percent']:+.2f}%" if v['relative_change_percent'] is not None else 'undefined'
            lines.append(f"| {key} {stat} | {v['before_ms']:.3f} | {v['after_ms']:.3f} | {v['delta_ms']:+.3f} | {pct} |")
    
    lines.extend([
        '',
        'Workload and raw distributions validated; **environment not matched, no causal attribution**.',
        'See docs/kubernetes-latency-comparison-20261020.md for environment changes and interpretation.',
        ''
    ])
    return '\n'.join(lines)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--before', type=Path, required=True, help='archived v1 serial run directory')
    p.add_argument('--after', type=Path, required=True, help='archived v1 serial run directory')
    p.add_argument('--out', type=Path, required=True, help='new comparison JSON, Markdown alongside')
    args = p.parse_args()
    args.out.resolve().relative_to(ROOT)
    md = args.out.with_suffix('.md')
    if args.out.exists() or md.exists():
        raise FileExistsError('refusing to overwrite historical comparison')
    
    result = compare(load_v1_run(args.before), load_v1_run(args.after))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open('x') as f:
        json.dump(result, f, indent=2)
        f.write('\n')
    with md.open('x') as f:
        f.write(markdown(result))
    
    print(markdown(result))


if __name__ == '__main__':
    main()
