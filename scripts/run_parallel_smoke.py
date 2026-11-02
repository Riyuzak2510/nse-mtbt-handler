#!/usr/bin/env python3
"""Finite actual-command correctness smoke for v2 manifests (stream_id only).

Requires make build and local Compose services. Own process ports must be unused.
Retains evidence and native-stream state; never resets database or broker state.
Rejects v1 manifests before executing SQL.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request

from run_latency import check_manifest_v2, check_database_state

ROOT = Path(__file__).resolve().parents[1]


def sql(query):
    return subprocess.check_output(
        ['docker', 'compose', 'exec', '-T', 'postgres', 'psql', '-X', '-U', 'mtbt', '-d', 'mtbt', '-At', '-v', 'ON_ERROR_STOP=1', '-c', query],
        cwd=ROOT, text=True).strip()



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', required=True)
    parser.add_argument('--count', type=int, default=300)
    parser.add_argument('--shards', type=int, default=3)
    parser.add_argument('--stream', type=int, default=1, help='native stream selector, not a test identity')
    args = parser.parse_args()
    if not 1 <= args.shards <= 16 or not 1 <= args.count <= 100000 or not 1 <= args.stream <= 32767:
        parser.error('shards 1..16, count 1..100000 and native stream 1..32767 required')
    # Refuse occupied ports before starting or sending synthetic UDP packets.
    for kind, port in [(socket.SOCK_DGRAM, 19001), (socket.SOCK_STREAM, 19002)]:
        with socket.socket(socket.AF_INET, kind) as check:
            check.bind(('127.0.0.1', port))
    out = Path(args.out).resolve()
    out.relative_to(ROOT)
    check_database_state(args.stream, sql_query=sql, require_empty=True)
    out.mkdir(parents=True, exist_ok=False)
    manifest, expected = out / 'manifest.json', out / 'expected.json'
    env = os.environ.copy()
    env['DATABASE_URL'] = 'postgres://mtbt:local-dev-only@127.0.0.1:15432/mtbt?sslmode=disable'
    env['NATS_URL'] = 'nats://127.0.0.1:14222'
    processes = []
    logs = []

    def command(*argv):
        return [str(ROOT / 'bin' / argv[0]), *argv[1:]]

    def start(name, argv):
        log = (out / f'{name}.log').open('x')
        logs.append(log)
        p = subprocess.Popen(argv, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
        processes.append(p)
        return p

    def stop(p):
        if p.poll() is None:
            p.send_signal(signal.SIGINT)
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
                raise RuntimeError('owned process required forced kill')
        if p.returncode != 0:
            raise RuntimeError(f'owned process exited {p.returncode}; see logs')

    def wait_log(name, text, p):
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if text in (out / f'{name}.log').read_text():
                return
            if p.poll() is not None:
                raise RuntimeError(f'{name} exited before readiness')
            time.sleep(.025)
        raise TimeoutError(f'{name} readiness log absent')

    def get_book():
        try:
            with urllib.request.urlopen('http://127.0.0.1:19002/book?token=900001&depth=100', timeout=3) as r:
                return r.status, json.load(r)
        except urllib.error.HTTPError as e:
            return e.code, json.load(e)

    try:
        subprocess.run(command('simulator', '-init', '-manifest', str(manifest), '-seed', '42', '-instruments', '3', '-stream', str(args.stream)), cwd=ROOT, env=env, check=True)
        manifest_obj = json.loads(manifest.read_text())
        check_manifest_v2(manifest_obj)
        stream_id = manifest_obj['stream_id']
        
        book_args = command('book-service', '-manifest', str(manifest), '-shards', str(args.shards), '-stale-after', '120s')
        service = start('book-service', book_args)
        wait_log('book-service', 'book service ready', service)
        publisher = start('publisher', command('publisher', '-manifest', str(manifest)))
        wait_log('publisher', 'publisher ready', publisher)
        source = start('simulator', command('simulator', '-manifest', str(manifest), '-count', str(args.count), '-rate', '100', '-expected', str(expected)))
        source.wait(timeout=args.count / 50 + 20)
        stop(source)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            code, snap = get_book()
            if code == 200 and snap['sequence'] == args.count and snap.get('pending_events', 0) == 0:
                break
            if service.poll() is not None:
                raise RuntimeError('service failed during drain')
            time.sleep(.025)
        else:
            raise TimeoutError('parallel run did not drain')
        (out / 'snapshot.json').write_text(json.dumps(snap, indent=2) + '\n')
        orders_query = f"SELECT COALESCE(json_agg(t ORDER BY id_hex),'[]'::json) FROM (SELECT encode(order_id,'hex') AS id_hex,token,side,price,quantity FROM mtbt.active_orders WHERE stream_id={stream_id}) t"
        actual = json.loads(sql(orders_query))
        want = [dict(id_hex=int(o['id_bits']).to_bytes(8, 'little').hex(), token=o['token'], side=chr(o['side']), price=o['price'], quantity=o['quantity']) for o in json.loads(expected.read_text())['orders']]
        if actual != sorted(want, key=lambda x: x['id_hex']):
            raise AssertionError('full active orders differ from independent generator')
        progress = json.loads(sql(f"SELECT json_agg(t ORDER BY shard) FROM (SELECT shard,dispatched,applied FROM mtbt.subscriber_shards WHERE stream_id={stream_id}) t"))
        if sum(s['applied'] for s in progress) != args.count or any(s['applied'] != s['dispatched'] for s in progress):
            raise AssertionError('shard progress differs from input')
        stop(publisher)
        stop(service)
        restarted = start('book-service-restarted', book_args)
        wait_log('book-service-restarted', 'book service ready', restarted)
        code, after = get_book()
        if code != 200 or after['sequence'] != args.count or json.loads(sql(orders_query)) != actual:
            raise AssertionError('restart changed book/progress')
        stop(restarted)
        # Mode/topology guards must reject startup before consuming any messages.
        guard_results = {}
        for label, flags in [('serial-mode', []), ('different-shards', ['-shards', str(2 if args.shards != 2 else 3)])]:
            p = start(label, command('book-service', '-manifest', str(manifest), *flags))
            p.wait(timeout=15)
            if p.returncode == 0:
                raise AssertionError(f'{label} guard accepted incompatible topology')
            guard_results[label] = 'rejected'
        result = dict(stream_id=stream_id, source_events=args.count, shards=args.shards, active_orders=len(actual), shard_progress=progress, exact_order_oracle=True, restart_stable=True, topology_guards=guard_results, latency_benchmark=False, note='100/s finite correctness smoke; stale-after120s is diagnostic, not a freshness SLA. Native-stream state retained; a new sequence-one simulation requires clean dependencies.')
        (out / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps(result, indent=2))
    finally:
        for p in reversed(processes):
            if p.poll() is None:
                p.send_signal(signal.SIGINT)
                try:
                    p.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    p.kill()
                    p.wait()
        for log in logs:
            log.close()


if __name__ == '__main__':
    main()
