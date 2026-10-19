"""Optional legacy Metabase MCP launcher; credentials never go on the command line."""
import os
import shutil
import subprocess
import sys

from dev import ROOT, read_env


def main():
    saved = read_env(ROOT / ".env")
    env = os.environ.copy()
    env.setdefault("METABASE_URL", saved.get("METABASE_URL", "http://localhost:3000"))
    env.setdefault("METABASE_API_KEY", saved.get("METABASE_API_KEY", ""))
    if not env["METABASE_API_KEY"]:
        sys.exit("Set METABASE_API_KEY or bootstrap this checkout's ignored .env first")
    command = shutil.which("npx.cmd" if os.name == "nt" else "npx")
    if not command:
        sys.exit("Node.js/npm is required for the optional community MCP server")
    return subprocess.call([command, "-y", "@imlewc/metabase-server"], cwd=ROOT, env=env)


if __name__ == "__main__":
    sys.exit(main())
