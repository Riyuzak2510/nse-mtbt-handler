#!/usr/bin/env python3
"""Fresh cgroup per engine scenario; retains failures/OOM metadata; no live cluster changes."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def command(args, **kw):
    return subprocess.check_output(args, cwd=ROOT, text=True, **kw).strip()


def validate(result, count, layout):
    if result.get('version') != 1 or result.get('count') != count or result.get('layout') != layout:
        raise ValueError('scenario identity mismatch')
    if not result.get('correctness_verified') or not result.get('database_style_restore_verified'):
        raise ValueError('order oracle/restore not verified')
    samples = {s['phase']: s for s in result['samples']}
    for phase in ('loaded_gc', 'churn_gc', 'restored_gc'):
        s = samples[phase]
        if s['active_orders'] != count or s['heap_alloc_bytes'] <= 0 or s['rss_bytes'] <= 0:
            raise ValueError('invalid memory sample or active count')
    if samples['final_drained_gc']['active_orders'] or samples['released']['active_orders']:
        raise ValueError('orders were not drained')
    if result['os'] != 'linux':
        raise ValueError('container measurement must be Linux')
    return result


def scenario(image, count, layout, rounds, limit, gomemlimit, out):
    # Unique container handle is temporary tooling, never order/feed identity.
    name = 'tmp_rovo_capacity_' + os.urandom(6).hex()
    args = ['docker', 'create', '--name', name, '--memory', limit, '--memory-swap', limit,
            '--network', 'none', '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
            '-e', 'GOMEMLIMIT=' + gomemlimit, image, '-count', str(count), '-layout', layout, '-rounds', str(rounds)]
    command(args)
    error = None
    try:
        proc = subprocess.run(['docker', 'start', '-a', name], cwd=ROOT, text=True, capture_output=True, timeout=600)
        (out/'stdout.json').write_text(proc.stdout)
        (out/'stderr.log').write_text(proc.stderr)
        inspection = json.loads(command(['docker', 'inspect', name]))[0]
        (out/'container.json').write_text(json.dumps(inspection, indent=2)+'\n')
        state = inspection['State']
        if state['OOMKilled'] or state['ExitCode'] != 0 or proc.returncode != 0:
            raise ValueError(f"container failed: OOMKilled={state['OOMKilled']} exit={state['ExitCode']}")
        result = validate(json.loads(proc.stdout), count, layout)
        (out/'result.json').write_text(json.dumps(result, indent=2)+'\n')
        return {'count': count, 'layout': layout, 'passed': True,
                'peak_rss_bytes': max(s['peak_rss_bytes'] for s in result['samples']),
                'peak_cgroup_bytes': max(s['cgroup_peak_bytes'] for s in result['samples']),
                'loaded_heap_bytes': next(s['heap_alloc_bytes'] for s in result['samples'] if s['phase'] == 'loaded_gc')}
    except Exception as e:
        error = str(e)
        (out/'error.txt').write_text(error+'\n')
        return {'count': count, 'layout': layout, 'passed': False, 'error': error}
    finally:
        # Own only this newly-created container. Inspect before remove, even after timeout.
        try:
            data = command(['docker', 'inspect', name])
            if not (out/'container.json').exists(): (out/'container.json').write_text(data+'\n')
        finally:
            subprocess.run(['docker', 'rm', '-f', name], cwd=ROOT, capture_output=True, check=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--counts', default='100000,500000,1000000')
    parser.add_argument('--layouts', default='dense,sparse')
    parser.add_argument('--rounds', type=int, default=2)
    parser.add_argument('--limit', default='512m')
    parser.add_argument('--gomemlimit', default='384MiB')
    args = parser.parse_args()
    counts = [int(n) for n in args.counts.split(',')]
    layouts = args.layouts.split(',')
    if any(n < 1 or n > 1000000 for n in counts) or any(x not in ('dense', 'sparse') for x in layouts) or not 1 <= args.rounds <= 10:
        parser.error('counts1..1000000, layouts dense,sparse and rounds1..10 required')
    out = args.out.resolve()
    if not out.is_relative_to(ROOT): parser.error('evidence directory must be inside project')
    out.mkdir(parents=True, exist_ok=False)
    osinfo = command(['docker', 'info', '--format', '{{.OSType}} {{.Architecture}}'])
    arch = 'arm64' if 'aarch64' in osinfo or 'arm64' in osinfo else 'amd64'
    env = dict(os.environ, GOOS='linux', GOARCH=arch, CGO_ENABLED='0')
    with tempfile.TemporaryDirectory(prefix='tmp_rovo_capacity_', dir=ROOT) as temp:
        temp = Path(temp)
        subprocess.run(['go', 'build', '-trimpath', '-o', str(temp/'matching-capacity'), './cmd/matching-capacity'], cwd=ROOT, env=env, check=True)
        (temp/'Dockerfile').write_text('FROM scratch\nCOPY matching-capacity /app\nUSER 10001:10001\nENTRYPOINT ["/app"]\n')
        image = 'tmp_rovo_matching_capacity:' + os.urandom(6).hex()
        command(['docker', 'build', '-q', '-t', image, str(temp)])
        metadata = {'time': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'platform': platform.platform(),
                    'docker_info': osinfo, 'image_id': command(['docker', 'image', 'inspect', image, '--format', '{{.Id}}']),
                    'go': command(['go', 'version']), 'memory_limit': args.limit, 'gomemlimit': args.gomemlimit,
                    'source_sha256': {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                                      for p in sorted([*ROOT.glob('internal/matching/*.go'), *ROOT.glob('cmd/matching-capacity/*.go'), ROOT/'scripts/run_matching_capacity.py'])}}
        (out/'environment.json').write_text(json.dumps(metadata, indent=2)+'\n')
        results = []
        try:
            for count in counts:
                for layout in layouts:
                    dest = out/f'{count}-{layout}'; dest.mkdir()
                    summary = scenario(image, count, layout, args.rounds, args.limit, args.gomemlimit, dest)
                    results.append(summary)
                    (out/'summary.json').write_text(json.dumps(results, indent=2)+'\n')
                    print(json.dumps(summary), flush=True)
        finally:
            subprocess.run(['docker', 'image', 'rm', image], cwd=ROOT, capture_output=True, check=False)
    if not all(r['passed'] for r in results): raise SystemExit(1)


if __name__ == '__main__':
    main()
