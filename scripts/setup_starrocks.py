"""Wait for the StarRocks all-in-one container to be ready, then load the
sample schemas from starrocks/init.sql via the FE MySQL port (9030).

Usage: python scripts/setup_starrocks.py
Requires the starrocks service running (docker-compose.starrocks.yaml).

NOTE: StarRocks' BE comes alive fine (unlike Doris), but the all-in-one image
wires FE<->BE over 127.0.0.1, so the FE advertises its Flight SQL data
endpoint as 127.0.0.1:9408 — unreachable from a *separate* Metabase container
under podman-on-Windows. Works when the client shares the host/namespace
(localhost) or on native Linux. Only writes STARROCKS_READY when a live BE +
loaded data are confirmed; test_starrocks.py additionally requires the Flight
SQL endpoint to actually be reachable, so it self-skips where it isn't.
"""
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

CONTAINER = "starrocks"
CLI = os.environ.get("CONTAINER_CLI") or ("podman" if shutil.which("podman") else "docker")


def mysql(sql, stdin=None, timeout=180):
    args = [CLI, "exec", "-i", CONTAINER,
            "mysql", "-uroot", "-P9030", "-h127.0.0.1"]
    if stdin is None:
        args += ["-e", sql]
    return subprocess.run(args, input=stdin, capture_output=True, text=True, timeout=timeout)


def main():
    print("waiting for a live StarRocks BE (usually <30s)...")
    deadline = time.time() + 240
    alive = False
    while time.time() < deadline:
        r = mysql("SHOW BACKENDS\\G")
        if r.returncode == 0 and "Alive: true" in r.stdout:
            alive = True
            break
        time.sleep(5)
    if not alive:
        print("StarRocks BE never became alive. Last output:")
        print((r.stdout + r.stderr)[-600:])
        sys.exit(1)
    print("BE alive; loading sample schemas...")

    # StarRocks' parser rejects leading `--` comments the mysql client would
    # forward, so DO NOT pass --comments.
    init = (Path(__file__).resolve().parent.parent / "starrocks" / "init.sql").read_text()
    r = mysql(None, stdin=init)
    if r.returncode != 0:
        print("init.sql failed:")
        print((r.stdout + r.stderr)[-800:])
        sys.exit(1)

    check = mysql("SELECT COUNT(*) FROM sales.orders")
    print("sales.orders rows:", check.stdout.strip().splitlines()[-1] if check.returncode == 0 else check.stderr[:200])

    _set_env_flag("STARROCKS_READY", "1")
    print("StarRocks ready: host=starrocks port=9408 (FE Flight SQL), user=root, no password")
    print("  (STARROCKS_READY=1 written to .env)")


def _set_env_flag(key, value):
    env_path = Path(__file__).resolve().parent.parent / ".env"
    lines = env_path.read_text().splitlines() if env_path.exists() else []
    lines = [l for l in lines if not l.startswith(f"{key}=")]
    lines.append(f"{key}={value}")
    env_path.write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
