"""Minimal Metabase bootstrap for CI: create the admin user and an API key,
write it to .env. No backend connections or dashboards — backend-specific
test modules register their own connections via the db_factory fixture.

Used by the heavy-backend CI jobs (Doris/StarRocks) so they don't have to
spin up Spice/GizmoSQL just to get an API key. Run from the repo root.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from metabase_setup import MetabaseClient, MetabaseConfig, save_env_file  # noqa: E402


def main():
    client = MetabaseClient(MetabaseConfig())
    if not client.wait_for_ready(timeout=180):
        print("Metabase did not become ready")
        sys.exit(1)
    if client.is_setup_complete():
        if not client.login():
            print("login failed")
            sys.exit(1)
    elif not client.setup():
        print("setup failed")
        sys.exit(1)
    key = client.create_api_key("ci-key")
    if not key:
        print("could not create API key")
        sys.exit(1)
    # write .env at the repo root (where conftest reads it)
    save_env_file(key, str(Path(__file__).resolve().parent.parent / ".env"))
    print("CI bootstrap complete: admin user + API key written to .env")


if __name__ == "__main__":
    main()
