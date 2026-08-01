"""kamu / Open Data Fabric backend (docker-compose.kamu.yaml) — a verifiable,
bitemporal data lakehouse that speaks Arrow Flight SQL via embedded DataFusion.

Why this backend matters in the matrix — it is the only source where every
dataset is an append-only **changelog**: each row carries the ODF system
columns `offset`, `op` (0=append/1=retract/2=correct-from/3=correct-to),
`system_time` (transaction time) and `event_time` (valid time). That lets
Metabase do things a plain warehouse table can't:
  - **audit trail**: see every append/correction/retraction behind a number
  - **current state**: collapse the log with the `to_table()` UDTF
  - **time-travel**: reconstruct state as of any past transaction time

The kamu container self-seeds `account.balances` (a Snapshot dataset) with two
batches — the second corrects one balance (50->75), adds one account, and drops
another — so all four op codes appear. Seeding runs *before* the Flight SQL
server binds :50050, so an open port implies the data is ready.

No driver changes are needed: kamu is DataFusion over Flight SQL (same shape as
Spice), reached with anonymous/anonymous over a plaintext connection. kamu's
server is slower to accept new gRPC handshakes than the other backends, so these
tests share ONE module-scoped connection (a realistic single-connection
deployment) and raise the connect timeout rather than churning connections.
"""
import pytest

from conftest import port_open

requires_kamu = pytest.mark.skipif(
    not port_open("localhost", 50050),
    reason="kamu not running (docker-compose.kamu.yaml)")

pytestmark = requires_kamu

KAMU = {"host": "kamu", "port": 50050, "use-token": False,
        "username": "anonymous", "password": "anonymous",
        "useEncryption": False, "disableCertificateVerification": True,
        # kamu's cold-start handshake can exceed the 10s driver default
        "connect-timeout-millis": "30000"}

TABLE = '"account.balances"'

# reconstruct state as of a transaction-time cut: for each PK take the latest
# changelog row (by offset) at or before the cut, keeping only value-setting ops
# (0=append, 3=correct-to); 1=retract and 2=correct-from remove a value.
FOLD = ("SELECT account_id, balance FROM ("
        "SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY offset DESC) rn "
        f"FROM {TABLE} WHERE system_time <= {{cut}}) WHERE rn = 1 AND op IN (0, 3)")


@pytest.fixture(scope="module")
def kamu_db(mb):
    """One shared kamu connection for the whole module (kamu is slow to accept
    repeated new connections). Synced and yielding its db id; cleaned up after."""
    status, res = mb.post("/api/database",
                          {"name": "kamu-e2e", "engine": "arrow-flight-sql", "details": KAMU})
    assert status == 200 and res.get("id"), f"kamu connection rejected: {status} {str(res)[:300]}"
    db_id = res["id"]
    mb.wait_synced_tables(
        db_id, predicate=lambda ts: any(t["name"] == "account.balances" for t in ts))
    yield db_id
    mb.req("DELETE", f"/api/database/{db_id}")


def _rows(mb, db_id, sql):
    res = mb.native(db_id, sql)
    assert res.get("status") == "completed", res.get("error")
    return res["data"]["rows"]


def test_kamu_anonymous_connects_and_queries(mb, kamu_db):
    """anonymous/anonymous over the both-present auth branch, plaintext."""
    assert _rows(mb, kamu_db, f"SELECT COUNT(*) FROM {TABLE}")[0][0] == 7


def test_kamu_system_columns_synced(mb, kamu_db):
    """The ODF system columns must surface to Metabase — they are the hook for
    every temporal/audit query."""
    _, meta = mb.get(f"/api/database/{kamu_db}/metadata")
    t = next(t for t in meta["tables"] if t["name"] == "account.balances")
    cols = {f["name"] for f in t["fields"]}
    assert {"offset", "op", "system_time", "event_time", "account_id", "balance"} <= cols, cols


def test_kamu_changelog_has_all_op_codes(mb, kamu_db):
    """Audit trail: the append-only log records every change — append(0),
    correct-from(2)/correct-to(3) for the 50->75 fix, and retract(1)."""
    ops = {r[0] for r in _rows(mb, kamu_db, f"SELECT DISTINCT op FROM {TABLE}")}
    assert {0, 1, 2, 3} <= ops, ops
    # the correction pair for acc-2 is present: a -C(2)@50 and a +C(3)@75
    pair = _rows(mb, kamu_db,
                 f"SELECT op, balance FROM {TABLE} WHERE account_id = 'acc-2' "
                 "AND op IN (2, 3) ORDER BY op")
    assert pair == [[2, 50], [3, 75]], pair


def test_kamu_to_table_collapses_to_current_state(mb, kamu_db):
    """to_table() folds the changelog to the live current state (acc-3 retracted)."""
    rows = _rows(mb, kamu_db,
                 f"SELECT account_id, balance FROM to_table({TABLE}) ORDER BY account_id")
    assert rows == [["acc-1", 100], ["acc-2", 75], ["acc-4", 200]], rows
    assert _rows(mb, kamu_db, f"SELECT SUM(balance) FROM to_table({TABLE})")[0][0] == 375


def test_kamu_time_travel_as_of_transaction_time(mb, kamu_db):
    """Reconstruct state as originally booked (first ingest) vs now."""
    as_booked = FOLD.format(cut=f"(SELECT MIN(system_time) FROM {TABLE})")
    rows = _rows(mb, kamu_db, as_booked + " ORDER BY account_id")
    assert rows == [["acc-1", 100], ["acc-2", 50], ["acc-3", 30]], rows
    total = _rows(mb, kamu_db, f"SELECT SUM(balance) FROM ({as_booked})")[0][0]
    assert total == 180, total  # vs 375 today — the delta is the audit trail
