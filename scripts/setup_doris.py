"""Wait for the Doris all-in-one container to be ready, then load the sample
schemas from doris/init.sql via the FE MySQL port (inside the container).

Usage: python scripts/setup_doris.py
Requires the doris service running (docker-compose.doris.yaml).
"""
import subprocess
import sys
import time

CONTAINER = "doris"


def sh(*args, timeout=120):
    return subprocess.run(args, capture_output=True, text=True, timeout=timeout)


def mysql(sql, timeout=120):
    # the image bundles a mysql client; FE speaks MySQL on 9030
    return sh("podman", "exec", "-i", CONTAINER,
              "mysql", "-uroot", "-P9030", "-h127.0.0.1", "--comments",
              "-e", sql, timeout=timeout)


def be_alive():
    r = mysql("SHOW BACKENDS")
    return r.returncode == 0 and "\ttrue\t" in r.stdout.replace(" ", "\t") \
        or ("Alive" in r.stdout and "true" in r.stdout.lower())


def main():
    print("waiting for Doris FE + a live BE (can take 1-2 min)...")
    deadline = time.time() + 300
    ready = False
    while time.time() < deadline:
        r = mysql("SHOW BACKENDS")
        if r.returncode == 0 and "true" in r.stdout.lower():
            ready = True
            break
        time.sleep(5)
    if not ready:
        print("Doris did not become ready in time. Last output:")
        print((r.stdout + r.stderr)[-600:])
        sys.exit(1)
    print("Doris is up; loading sample schemas...")

    init = open("doris/init.sql", encoding="utf-8").read()
    r = subprocess.run(
        ["podman", "exec", "-i", CONTAINER,
         "mysql", "-uroot", "-P9030", "-h127.0.0.1", "--comments"],
        input=init, capture_output=True, text=True, timeout=180)
    if r.returncode != 0:
        print("init.sql failed:")
        print((r.stdout + r.stderr)[-800:])
        sys.exit(1)

    check = mysql("SELECT COUNT(*) FROM sales.orders")
    print("sales.orders rows:", check.stdout.strip().splitlines()[-1] if check.returncode == 0 else check.stderr[:200])
    dbs = mysql("SHOW DATABASES")
    print("databases:", [l for l in dbs.stdout.split() if l in ("sales", "hr", "analytics")])

    # Mark Doris usable for the pytest suite only when a BE is alive AND data
    # loaded — the FE Flight SQL port opens before the BE is ready, so
    # port-open alone is not a safe gate (see tests/e2e/test_doris.py).
    _set_env_flag("DORIS_READY", "1")
    print("Doris ready: host=doris port=8070 (FE Flight SQL), user=root, no password,")
    print("  additional-options useServerPrepStmts=false  (DORIS_READY=1 written to .env)")


def _set_env_flag(key, value):
    from pathlib import Path
    env_path = Path(__file__).resolve().parent.parent / ".env"
    lines = env_path.read_text().splitlines() if env_path.exists() else []
    lines = [l for l in lines if not l.startswith(f"{key}=")]
    lines.append(f"{key}={value}")
    env_path.write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
