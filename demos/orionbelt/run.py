"""Portable demo lifecycle; Podman locally, Docker only on GitHub Actions."""
import argparse
import os
from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['up', 'verify', 'stop'])
    parser.add_argument('--skip-build', action='store_true', help='Reuse an already-built local driver jar')
    args = parser.parse_args()
    compose = ['docker', 'compose'] if os.environ.get('GITHUB_ACTIONS') == 'true' else ['podman-compose']
    stack = [*compose, '-p', 'flightsql-orionbelt', '-f', str(HERE / 'compose.yaml')]

    def run(command):
        subprocess.run(command, cwd=ROOT, check=True)

    if args.command == 'stop':
        run([*stack, 'stop'])  # preserve all data and credentials
    elif args.command == 'verify':
        run([sys.executable, '-u', str(HERE / 'demo.py'), 'verify'])
    else:
        if not args.skip_build:
            run([*compose, '-p', 'flightsql-orionbelt-build', '-f', 'docker-compose.compat.yaml',
                 'run', '--rm', 'builder'])
        jar = ROOT / 'target/metabase-flightsql-driver-0.1.0-SNAPSHOT-standalone.jar'
        if not jar.is_file():
            raise SystemExit('Build the driver first (omit --skip-build).')
        run([*stack, 'run', '--rm', 'seed'])
        run([*stack, 'up', '-d', 'orionbelt', 'mcp', 'metabase'])
        # Reload mounted model/shim edits and discard pooled JDBC connections
        # whose JVM DNS cache may retain a recreated OrionBelt container's IP.
        run([*stack, 'restart', 'orionbelt', 'mcp', 'metabase'])
        for command in ['provision', 'dashboard', 'verify']:
            run([sys.executable, '-u', str(HERE / 'demo.py'), command])


if __name__ == '__main__':
    main()
