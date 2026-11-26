import copy
import io
import json
import os
import socket
import ssl
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest
import requests
from hotglue_singer_sdk.exceptions import FatalAPIError
import hotglue_singer_sdk.target_sdk.target as sdk_target

from target_sap_b1.sinks import BuyOrders
from target_sap_b1.target import TargetSAPB1


STREAM = "UpdateBuyOrders"
ROOT = Path(__file__).resolve().parents[1]


def config_for(folder, api_url="https://sap.example.invalid/b1s/v2/"):
    return {
        "api_url": api_url, "company_db": "SYNTHETIC_COMPANY",
        "username": "SYNTHETIC_USER", "password": "synthetic-password",
        "state_path": str(folder / "target-state.json"),
    }


def singer_input(records, stream=STREAM):
    messages = [{"type": "SCHEMA", "stream": stream, "schema": {
        "type": "object", "properties": {key: {} for record in records for key in record},
    }, "key_properties": []}]
    messages.extend({"type": "RECORD", "stream": stream, "record": record} for record in records)
    messages.append({"type": "STATE", "value": {}})
    return "".join(json.dumps(message) + "\n" for message in messages)


@pytest.fixture
def workdir(tmp_path, monkeypatch):
    run = tmp_path / "run"
    run.mkdir()
    (tmp_path / ".secrets").mkdir()
    monkeypatch.chdir(run)
    monkeypatch.delenv("JOB_ID", raising=False)
    monkeypatch.delenv("STREAMING_JOB", raising=False)
    monkeypatch.setenv("FLOW", "synthetic-flow")
    monkeypatch.setattr(sdk_target, "SNAPSHOT_DIR", str(tmp_path / "snapshots"))
    return run


@pytest.fixture
def harness(workdir, monkeypatch):
    state = SimpleNamespace(config=config_for(workdir), calls=[], replies=[])

    def send(session, method, url, **kwargs):
        assert url.startswith(state.config["api_url"])
        endpoint = url.removeprefix(state.config["api_url"])
        state.calls.append((method.upper(), endpoint, kwargs))
        if endpoint == "Login":
            assert method.upper() == "POST"
            status, body = 200, {"SessionId": "synthetic-session", "SessionTimeout": 30}
        else:
            assert method.upper() == "PATCH" and endpoint.startswith("PurchaseOrders(")
            assert kwargs["headers"]["Cookie"] == "B1SESSION=synthetic-session"
            assert kwargs["timeout"] == (5, 30)
            assert kwargs["allow_redirects"] is False
            reply = state.replies.pop(0) if state.replies else (204, None)
            if isinstance(reply, Exception):
                raise reply
            status, body = reply
        response = requests.Response()
        response.status_code = status
        response._content = body if isinstance(body, bytes) else json.dumps(body).encode() if body is not None else b""
        return response

    monkeypatch.setattr(requests.sessions.Session, "request", send)

    def run(records=None, stream=STREAM, text=None, before=None):
        target = TargetSAPB1(config=state.config)
        try:
            if before:
                before(target)
            target.listen(io.StringIO(text if text is not None else singer_input(records or [], stream)))
        finally:
            target._shutdown_requested.set()
            target.signal_listener_thread.join(timeout=1)
        return json.loads(Path(state.config["state_path"]).read_text())

    state.run = run
    state.patches = lambda: [row for row in state.calls if row[0] == "PATCH"]
    return state


@pytest.mark.parametrize("remote_id", [5001, "0005001"])
def test_header_only_patch_accepts_204_and_warm_replay_skips_it(harness, remote_id):
    record = {"id": "41", "remoteId": remote_id, "DocumentLines": [{"untrusted": True}]}
    result = harness.run([record])
    assert [(method, endpoint, kwargs["json"]) for method, endpoint, kwargs in harness.patches()] == [
        ("PATCH", "PurchaseOrders(5001)", {"U_OPT_PO_HeaderID": "41"}),
    ]
    assert set(result["bookmarks"]) == {STREAM}
    assert result["bookmarks"][STREAM][0]["id"] == 5001
    assert result["bookmarks"][STREAM][0]["externalId"] == "41"
    assert result["bookmarks"][STREAM][0]["success"] is True
    assert result["summary"][STREAM]["updated"] == 1
    calls = copy.deepcopy(harness.calls)
    harness.run([record])
    assert harness.calls == calls


@pytest.mark.parametrize("field,value", [
    ("id", None), ("id", ""), ("id", True), ("id", " 41"),
    ("remoteId", None), ("remoteId", ""), ("remoteId", 0), ("remoteId", -1),
    ("remoteId", True), ("remoteId", 1.5), ("remoteId", "5001|6001"),
    ("remoteId", "PO_5001"), ("remoteId", "PI_5001"), ("remoteId", "1)/Orders(2"),
])
def test_bad_identifiers_fail_before_any_sap_call(harness, field, value, capsys):
    record = {"id": "41", "remoteId": "5001", field: value}
    with pytest.raises((FatalAPIError, ValueError)):
        harness.run([record])
    assert not harness.calls
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("missing", ["id", "remoteId"])
def test_absent_identifier_fails_before_any_sap_call(harness, missing, capsys):
    record = {"id": "41", "remoteId": "5001"}
    del record[missing]
    with pytest.raises((FatalAPIError, ValueError)):
        harness.run([record])
    assert not harness.calls
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("reply", [
    (400, {"error": {"code": -1}}), (401, {"error": {"code": -1}}),
    (403, {"error": {"code": -1}}), (404, {"error": {"code": -1}}),
    (429, {"error": {"code": -1}}), (503, {"error": {"code": -1}}),
    (200, {"error": {"code": -1}}), (200, []), (200, b"not-json"),
    requests.exceptions.Timeout("synthetic timeout"),
    requests.exceptions.ConnectionError("synthetic lost response"),
])
def test_failure_is_not_retried_or_acknowledged_and_can_resume(harness, reply, capsys):
    record = {"id": "41", "remoteId": "5001"}
    harness.replies.append(reply)
    with pytest.raises(FatalAPIError):
        harness.run([record])
    assert len(harness.patches()) == 1
    assert capsys.readouterr().out == ""
    failed = json.loads(Path(harness.config["state_path"]).read_text())
    assert failed["bookmarks"][STREAM][0]["success"] is False
    assert failed["bookmarks"][STREAM][0]["externalId"] == "41"
    recovered = harness.run([record])
    assert len(harness.patches()) == 2
    assert len(recovered["bookmarks"][STREAM]) == 1
    assert recovered["bookmarks"][STREAM][0]["success"] is True


@pytest.mark.parametrize("reply", [
    (200, {}), (200, {"DocEntry": 5001}), (201, {}), (202, None), (202, {"accepted": True}),
])
def test_successful_response_is_accepted_without_polling(harness, reply):
    harness.replies.append(reply)
    result = harness.run([{"id": "41", "remoteId": "5001"}])
    assert result["bookmarks"][STREAM][0]["success"] is True


def test_mixed_failure_keeps_success_without_ack_and_retries_only_failed_record(harness, capsys):
    first = {"id": "41", "remoteId": "5001"}
    second = {"id": "42", "remoteId": None}
    with pytest.raises(FatalAPIError):
        harness.run([first, second])
    assert capsys.readouterr().out == ""
    second["remoteId"] = "5002"
    result = harness.run([first, second])
    assert [endpoint for _, endpoint, _ in harness.patches()] == ["PurchaseOrders(5001)", "PurchaseOrders(5002)"]
    assert [(row["externalId"], row["success"]) for row in result["bookmarks"][STREAM]] == [("41", True), ("42", True)]


@pytest.mark.parametrize("patch_first", [False, True])
def test_buy_orders_and_id_exports_keep_separate_bookmarks_for_same_source_id(harness, monkeypatch, patch_first):
    normal = {"id": "41", "externalid": "41", "supplier_remoteId": "SUPPLIER", "created_at": "2026-06-02", "line_items": [
        {"line_id": "51", "sku": "SKU", "quantity": 1, "sub_total_price": 5},
    ]}
    monkeypatch.setattr(BuyOrders, "upsert_record", lambda self, record, context: (6001, True, {}))
    inputs = [singer_input([normal], "BuyOrders"), singer_input([{"id": "41", "remoteId": "5001"}])]
    if patch_first:
        inputs.reverse()
    result = harness.run(text="".join(inputs))
    assert set(result["bookmarks"]) == {"BuyOrders", STREAM}
    assert result["bookmarks"]["BuyOrders"][0]["id"] == 6001
    assert result["bookmarks"][STREAM][0]["id"] == 5001
    assert result["bookmarks"]["BuyOrders"][0]["externalId"] == result["bookmarks"][STREAM][0]["externalId"] == "41"
    assert len(harness.patches()) == 1
    rerun = harness.run(text="".join(inputs))
    assert rerun["bookmarks"] == result["bookmarks"]
    assert len(harness.patches()) == 1


def test_initializing_buy_orders_preserves_failed_id_export_until_replayed(harness, monkeypatch, capsys):
    patch = {"id": "41", "remoteId": "5001"}
    harness.replies.append((400, {"error": {"code": -1}}))
    with pytest.raises(FatalAPIError):
        harness.run([patch])
    normal = {"id": "41", "externalid": "41", "supplier_remoteId": "SUPPLIER", "created_at": "2026-06-02", "line_items": [
        {"line_id": "51", "sku": "SKU", "quantity": 1, "sub_total_price": 5},
    ]}
    monkeypatch.setattr(BuyOrders, "upsert_record", lambda self, record, context: (6001, True, {}))
    with pytest.raises(FatalAPIError, match="Unresolved SAP orders"):
        harness.run([normal], stream="BuyOrders")
    assert capsys.readouterr().out == ""
    assert json.loads(Path(harness.config["state_path"]).read_text())["bookmarks"][STREAM][0]["success"] is False
    recovered = harness.run([patch])
    assert recovered["bookmarks"][STREAM][0]["success"] is True
    assert recovered["bookmarks"]["BuyOrders"][0]["success"] is True
    assert len(harness.patches()) == 2


def test_patch_identity_does_not_hydrate_ids_from_any_sdk_snapshot(harness, monkeypatch):
    def no_snapshot(target):
        def reject_snapshot(*args, **kwargs):
            raise AssertionError("ID export must use payload remoteId, not an SDK snapshot")
        monkeypatch.setattr(target, "read_snapshot", reject_snapshot)
    harness.run([{"id": "41", "remoteId": "5001"}], before=no_snapshot)
    assert harness.patches()[0][1] == "PurchaseOrders(5001)"


@pytest.mark.parametrize("stream", ["ExportOptiplyBOID", "patchBuyOrder", "boExportOptiplyId", "Products"])
def test_unknown_or_previous_stream_name_is_rejected(harness, stream, capsys):
    with pytest.raises(FatalAPIError):
        harness.run([{"id": "41", "remoteId": "5001"}], stream=stream)
    assert not harness.calls
    assert capsys.readouterr().out == ""


def test_failure_in_id_export_blocks_final_state_even_if_buy_orders_succeeded(harness, capsys):
    def seed_failure(target):
        target._state = target._latest_state = {"bookmarks": {
            "BuyOrders": [{"success": True}], STREAM: [{"success": False}],
        }, "summary": {}, "sap_company": target.company_scope}
    with pytest.raises(FatalAPIError, match="Unresolved SAP orders"):
        harness.run(before=seed_failure)
    assert capsys.readouterr().out == ""
    assert not harness.calls


@pytest.fixture
def https_sap(workdir, tmp_path, monkeypatch):
    cert, key = tmp_path / "trust-bundle", tmp_path / "tls-material"
    subprocess.run([
        "openssl", "req", "-x509", "-newkey", "rsa:2048", "-sha256", "-days", "1", "-nodes",
        "-keyout", str(key), "-out", str(cert), "-subj", "/CN=localhost",
        "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
    ], check=True, capture_output=True)
    state = SimpleNamespace(calls=[], header=None, drop_response=False)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            length = int(self.headers["Content-Length"])
            body = json.loads(self.rfile.read(length))
            state.calls.append(("POST", self.path, body))
            assert self.path == "/b1s/v2/Login"
            response = json.dumps({"SessionId": "synthetic-session", "SessionTimeout": 30}).encode()
            self.send_response(200)
            self.send_header("Set-Cookie", "ROUTEID=.node1; Path=/")
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

        def do_PATCH(self):
            length = int(self.headers["Content-Length"])
            body = json.loads(self.rfile.read(length))
            state.calls.append(("PATCH", self.path, body))
            assert self.path == "/b1s/v2/PurchaseOrders(5001)"
            assert self.headers["Cookie"] == "B1SESSION=synthetic-session; ROUTEID=.node1"
            assert self.headers["Content-Type"] == "application/json"
            state.header = body
            if state.drop_response:
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(str(cert), str(key))
    server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", str(cert))
    state.config = config_for(workdir, f"https://localhost:{server.server_port}/b1s/v2/")
    config_path = workdir / "config.json"
    config_path.write_text(json.dumps(state.config))

    def run():
        return subprocess.run([
            str(Path(sys.executable).with_name("target-sap-b1")), "--config", str(config_path),
        ], input=singer_input([{"id": "41", "remoteId": "5001"}]), text=True,
            capture_output=True, cwd=workdir, env=os.environ.copy(), timeout=30)

    state.run = run
    try:
        yield state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_real_singer_cli_header_patch_and_warm_and_cold_replay(https_sap):
    first = https_sap.run()
    assert first.returncode == 0, first.stderr
    assert json.loads(first.stdout)["bookmarks"][STREAM][0]["id"] == 5001
    assert https_sap.header == {"U_OPT_PO_HeaderID": "41"}
    assert https_sap.run().returncode == 0
    assert len([call for call in https_sap.calls if call[0] == "PATCH"]) == 1
    Path(https_sap.config["state_path"]).unlink()
    cold = https_sap.run()
    assert cold.returncode == 0, cold.stderr
    assert [call[2] for call in https_sap.calls if call[0] == "PATCH"] == [{"U_OPT_PO_HeaderID": "41"}] * 2


def test_real_cli_lost_patch_response_fails_without_ack_and_safe_replay_recovers(https_sap):
    https_sap.drop_response = True
    failed = https_sap.run()
    assert failed.returncode != 0 and failed.stdout == ""
    assert len([call for call in https_sap.calls if call[0] == "PATCH"]) == 1
    assert https_sap.header == {"U_OPT_PO_HeaderID": "41"}
    https_sap.drop_response = False
    recovered = https_sap.run()
    assert recovered.returncode == 0, recovered.stderr
    assert json.loads(recovered.stdout)["bookmarks"][STREAM][0]["success"] is True
    assert len([call for call in https_sap.calls if call[0] == "PATCH"]) == 2
