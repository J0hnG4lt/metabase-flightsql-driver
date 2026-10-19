"""Portable local workflows shared by Claude Code, Codex, and maintainers."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]


def read_env(path):
    """Read simple KEY=value records without evaluating shell expressions."""
    result = {}
    if Path(path).exists():
        for line in Path(path).read_text(encoding="utf-8").splitlines():
            if line.strip() and not line.lstrip().startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                result[key.strip()] = value.strip().strip("\"'")
    return result


def run(command, **kwargs):
    return subprocess.run(command, cwd=ROOT, check=True, **kwargs)


def wait_ready(url, timeout=300):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url.rstrip("/") + "/api/health", timeout=5) as response:
                if response.status == 200 and json.load(response).get("status") == "ok":
                    return
        except (OSError, ValueError):
            pass
        time.sleep(2)
    raise RuntimeError(f"Metabase did not become healthy at {url} within {timeout}s")


def compose_command(args):
    executable = shutil.which("podman-compose")
    if not executable:
        raise RuntimeError("Install pip's podman-compose and start Podman first")
    command = [executable]
    if args.project:
        command += ["-p", args.project]
    for compose_file in args.file or ["docker-compose.yaml"]:
        command += ["-f", compose_file]
    return command


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["up", "rebuild", "test", "reset", "health"])
    parser.add_argument("-f", "--file", action="append", help="compose file (repeatable)")
    parser.add_argument("-p", "--project", help="compose project name")
    parser.add_argument("--url", default=os.environ.get("METABASE_URL", "http://localhost:3000"))
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--delete-data", action="store_true", help="explicitly authorize reset's volume deletion")
    parser.add_argument("--pytest", nargs=argparse.REMAINDER, help="pytest paths/options (default: tests/e2e -v)")
    args = parser.parse_args(argv)
    if args.action == "reset" and (not args.delete_data or not args.project):
        parser.error("reset requires --project NAME --delete-data; it deletes that compose project's volumes")
    env = dict(os.environ, METABASE_URL=args.url)
    if args.action == "health":
        wait_ready(args.url, args.timeout)
        print(f"Metabase is healthy at {args.url}")
        return
    compose = compose_command(args)
    if args.action == "reset":
        run(compose + ["down", "--volumes"], env=env)
        print("Project volumes removed. Credentials were preserved; bootstrap again for the new app database.")
        return
    if args.action == "up":
        run(compose + ["up", "-d"], env=env)
    elif args.action == "rebuild":
        run(compose + ["stop", "metabase"], env=env)
        # Foreground run: a non-zero compiler exit prevents restart with a stale jar.
        run(compose + ["run", "--rm", "--no-deps", "builder"], env=env)
        run(compose + ["up", "-d", "--no-deps", "metabase"], env=env)
    wait_ready(args.url, args.timeout)
    if args.action == "test":
        run([sys.executable, "scripts/metabase_setup.py"], env=env)
        run([sys.executable, "-m", "pytest"] + (args.pytest or ["tests/e2e", "-v"]), env=env)
    else:
        run(compose + ["logs", "--tail=80", "metabase"], env=env)
        print("Check the plugin-load log, then run the compatibility smoke or E2E tests.")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, subprocess.CalledProcessError) as error:
        sys.exit(str(error))
