"""Seed the influxdb3 compose service for driver testing.

Creates the initial admin token (once), stores it in .env as INFLUXDB3_TOKEN,
and writes sample weather data into the `demo` database.

Usage: python scripts/setup_influxdb3.py
"""
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
DATABASE = "demo"

LINES = "\n".join(
    [f"weather,city={city} temp={temp},humidity={hum}"
     for city, temp, hum in [
         ("Madrid", 31.5, 28), ("Berlin", 22.1, 55), ("Caracas", 27.9, 74),
         ("Tokyo", 29.4, 61), ("Austin", 35.2, 40)]]
    # extra measurements so the iox schema has several tables (schema-filter
    # and multi-table sync tests)
    + [f"cpu,host=host{n} usage={u},cores=8i" for n, u in [(1, 42.5), (2, 71.2), (3, 12.9)]]
    + [f"sensors,device=dev{n} reading={r}" for n, r in [(1, 0.42), (2, 0.87)]]
)


CLI = os.environ.get("CONTAINER_CLI") or ("podman" if shutil.which("podman") else "docker")


def podman_exec(*args, check=True):
    return subprocess.run([CLI, "exec", "influxdb3", *args],
                          capture_output=True, text=True, check=check)


def read_env():
    if not ENV_PATH.exists():
        return {}
    out = {}
    for line in ENV_PATH.read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            out[k] = v
    return out


def save_token(token):
    env = read_env()
    env["INFLUXDB3_TOKEN"] = token
    ENV_PATH.write_text("".join(f"{k}={v}\n" for k, v in env.items()))


def main():
    env = read_env()
    token = env.get("INFLUXDB3_TOKEN")

    # wait for server
    for _ in range(30):
        r = podman_exec("influxdb3", "show", "databases", "--token", token or "x",
                        check=False)
        if r.returncode == 0 or "unauthorized" in (r.stdout + r.stderr).lower() \
           or "Unauthorized" in (r.stdout + r.stderr):
            break
        time.sleep(2)

    if not token:
        r = podman_exec("influxdb3", "create", "token", "--admin", check=False)
        out = r.stdout + r.stderr
        m = re.search(r"(apiv3_\S+)", out)
        if not m:
            print("Could not create admin token (already created in a previous "
                  "run without .env?). Reset with: podman-compose down -v")
            print(out[-500:])
            sys.exit(1)
        token = m.group(1)
        save_token(token)
        print("Admin token created and saved to .env (INFLUXDB3_TOKEN)")

    r = podman_exec("influxdb3", "write", "--database", DATABASE,
                    "--token", token, LINES, check=False)
    if r.returncode != 0:
        print("write failed:", (r.stdout + r.stderr)[-400:])
        sys.exit(1)
    print(f"Wrote sample rows into database '{DATABASE}'")

    r = podman_exec("influxdb3", "query", "--database", DATABASE,
                    "--token", token, "SELECT COUNT(*) FROM weather", check=False)
    print(r.stdout.strip()[-200:] or r.stderr.strip()[-200:])
    print("influxdb3 ready: host=influxdb3 port=8181, bearer token in .env, "
          f"additional-options database={DATABASE}")


if __name__ == "__main__":
    main()
