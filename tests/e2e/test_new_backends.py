"""Anonymous auth (spiced-anon) and InfluxDB 3 Core (bearer-only + database
gRPC header via additional-options)."""
from conftest import ENV, requires_influxdb3, requires_spiced_anon


@requires_spiced_anon
def test_anonymous_connection(mb, db_factory):
    db_id = db_factory("t-anon", {"host": "spiced-anon", "port": 50051,
                                  "use-token": False, "useEncryption": False,
                                  "disableCertificateVerification": True})
    res = mb.native(db_id, "SELECT COUNT(*) FROM yellow_taxis")
    assert res.get("status") == "completed" and res["data"]["rows"][0][0] > 0


@requires_influxdb3
def test_influxdb3_bearer_and_database_header(mb, db_factory):
    db_id = db_factory("t-influx", {"host": "influxdb3", "port": 8181,
                                    "use-token": True,
                                    "token-value": ENV["INFLUXDB3_TOKEN"],
                                    "additional-options": "database=demo",
                                    "useEncryption": False,
                                    "disableCertificateVerification": True})
    res = mb.native(db_id, "SELECT COUNT(*) AS n FROM weather")
    assert res.get("status") == "completed" and res["data"]["rows"][0][0] == 5

    tables = mb.wait_synced_tables(
        db_id, predicate=lambda ts: any(t["name"] == "weather" for t in ts))
    assert any(t["name"] == "weather" for t in tables)


@requires_influxdb3
def test_influxdb3_rejects_anonymous(mb, db_factory):
    db_id = db_factory("t-influx-anon", {"host": "influxdb3", "port": 8181,
                                         "use-token": False,
                                         "additional-options": "database=demo",
                                         "useEncryption": False,
                                         "disableCertificateVerification": True},
                       expect_ok=False)
    assert db_id is None
