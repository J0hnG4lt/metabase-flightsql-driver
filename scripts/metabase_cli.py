"""Run the pinned official CLI against this checkout's development Metabase."""
import os
import shutil
import subprocess
import sys

from dev import ROOT, read_env

CLI_PACKAGE = "@metabase/cli@0.5.1"


def cli_environment(environ, saved):
    env = dict(environ)
    env["MB_URL"] = environ.get("METABASE_URL") or saved.get("METABASE_URL", "http://127.0.0.1:3000")
    env["MB_API_KEY"] = environ.get("METABASE_API_KEY") or saved.get("METABASE_API_KEY", "")
    return env


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    env = cli_environment(os.environ, read_env(ROOT / ".env"))
    if not env["MB_API_KEY"] and not ("--help" in argv or "-h" in argv or argv[:1] == ["skills"] or not argv):
        sys.exit("Bootstrap this checkout's .env or set METABASE_API_KEY. Use mb directly for saved user profiles.")
    executable = shutil.which("npx.cmd" if os.name == "nt" else "npx")
    if not executable:
        sys.exit("Install Node.js 20.6+ (including npm/npx) to use the official Metabase CLI")
    return subprocess.call([executable, "--yes", "--package=" + CLI_PACKAGE, "--", "mb", *argv],
                           cwd=ROOT, env=env)


if __name__ == "__main__":
    sys.exit(main())
