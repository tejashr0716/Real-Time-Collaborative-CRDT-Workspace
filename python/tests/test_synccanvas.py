import json
import os
import random
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

from python.api.main import app
from python.services.cpp_bridge import SyncCanvasEngine
from python.database.connection import close_db_pool
from python.services.redis_service import init_redis, update_presence, get_room_presence, close_redis
from python.spark.spark_crdt_convergence_verifier import run_crdt_convergence_audit

@pytest.fixture(scope="module")
def cpp_engine():
    return SyncCanvasEngine()

@pytest_asyncio.fixture
async def async_client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
    await close_db_pool()
    await close_redis()

def test_cpp_crdt_merge_in_order(cpp_engine):
    chars = [
        {"lamport_ts": 1, "pos_fractional": 0.1, "char_val": "W", "is_deleted": False, "client_id": "C1"},
        {"lamport_ts": 2, "pos_fractional": 0.2, "char_val": "o", "is_deleted": False, "client_id": "C1"},
        {"lamport_ts": 3, "pos_fractional": 0.3, "char_val": "r", "is_deleted": False, "client_id": "C1"},
        {"lamport_ts": 4, "pos_fractional": 0.4, "char_val": "l", "is_deleted": False, "client_id": "C1"},
        {"lamport_ts": 5, "pos_fractional": 0.5, "char_val": "d", "is_deleted": False, "client_id": "C1"}
    ]
    res = cpp_engine.merge_characters(chars)
    assert res["compiled_text"] == "World"
    assert res["length"] == 5

def test_cpp_crdt_merge_out_of_order_commutativity(cpp_engine):
    # Same characters shuffled in reverse / random order
    chars = [
        {"lamport_ts": 5, "pos_fractional": 0.5, "char_val": "d", "is_deleted": False, "client_id": "C1"},
        {"lamport_ts": 2, "pos_fractional": 0.2, "char_val": "o", "is_deleted": False, "client_id": "C1"},
        {"lamport_ts": 4, "pos_fractional": 0.4, "char_val": "l", "is_deleted": False, "client_id": "C1"},
        {"lamport_ts": 1, "pos_fractional": 0.1, "char_val": "W", "is_deleted": False, "client_id": "C1"},
        {"lamport_ts": 3, "pos_fractional": 0.3, "char_val": "r", "is_deleted": False, "client_id": "C1"}
    ]
    res = cpp_engine.merge_characters(chars)
    assert res["compiled_text"] == "World"
    assert res["length"] == 5

def test_cpp_crdt_tombstone_deletion(cpp_engine):
    chars = [
        {"lamport_ts": 1, "pos_fractional": 0.1, "char_val": "A", "is_deleted": False, "client_id": "C1"},
        {"lamport_ts": 2, "pos_fractional": 0.2, "char_val": "B", "is_deleted": True, "client_id": "C1"}, # Deleted
        {"lamport_ts": 3, "pos_fractional": 0.3, "char_val": "C", "is_deleted": False, "client_id": "C1"}
    ]
    res = cpp_engine.merge_characters(chars)
    assert res["compiled_text"] == "AC"
    assert res["total_tombstones"] == 1

def test_cpp_fractional_position_alloc(cpp_engine):
    mid = cpp_engine.allocate_position(0.1, 0.2)
    assert 0.1 < mid < 0.2
    assert mid == 0.15

def test_cpp_lamport_clock_advance(cpp_engine):
    next_ts = cpp_engine.advance_clock(10, 25)
    assert next_ts == 26

@pytest.mark.asyncio
async def test_redis_presence():
    await init_redis()
    await update_presence("doc-test-1", "user-alice", cursor_pos=42, color="#ff0000", ttl_seconds=10)
    members = await get_room_presence("doc-test-1")
    assert len(members) == 1
    assert members[0]["client_id"] == "user-alice"
    assert members[0]["cursor_pos"] == 42
    await close_redis()

@pytest.mark.asyncio
async def test_api_health(async_client):
    resp = await async_client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "HEALTHY"

@pytest.mark.asyncio
async def test_api_list_and_create_document(async_client):
    # List documents
    list_resp = await async_client.get("/api/v1/documents")
    assert list_resp.status_code == 200
    docs = list_resp.json()
    assert len(docs) >= 2

    # Create new document
    new_doc_payload = {
        "workspace_id": "WS_ENG_CORP",
        "title": "Quantum Mesh Consensus RFC",
        "initial_text": "Draft"
    }
    create_resp = await async_client.post("/api/v1/documents", json=new_doc_payload)
    assert create_resp.status_code == 201
    assert "doc_id" in create_resp.json()

@pytest.mark.asyncio
async def test_api_get_document_compiled(async_client):
    resp = await async_client.get("/api/v1/documents/DOC_DISTRIBUTED_SPECS")
    assert resp.status_code == 200
    doc = resp.json()
    assert doc["doc_id"] == "DOC_DISTRIBUTED_SPECS"
    assert "Hello" in doc["compiled_text"]
    assert doc["total_characters"] >= 5

@pytest.mark.asyncio
async def test_api_apply_operations_and_convergence(async_client):
    # Apply characters '!' at pos 0.6
    ops_payload = {
        "operations": [
            {
                "client_id": "BOB",
                "lamport_ts": 6,
                "op_type": "INSERT",
                "char_val": "!",
                "pos_fractional": 0.6
            }
        ]
    }
    resp = await async_client.post("/api/v1/documents/DOC_DISTRIBUTED_SPECS/operations", json=ops_payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["operations_applied"] == 1
    assert data["compiled_text"].endswith("!")

@pytest.mark.asyncio
async def test_api_presence_endpoints(async_client):
    payload = {
        "client_id": "CHARLIE_DEV",
        "cursor_pos": 15,
        "color_hex": "#10b981"
    }
    pres_resp = await async_client.post("/api/v1/documents/DOC_DISTRIBUTED_SPECS/presence", json=payload)
    assert pres_resp.status_code == 200

    collab_resp = await async_client.get("/api/v1/documents/DOC_DISTRIBUTED_SPECS/collaborators")
    assert collab_resp.status_code == 200
    collabs = collab_resp.json()["active_collaborators"]
    assert any(c["client_id"] == "CHARLIE_DEV" for c in collabs)

def test_spark_crdt_convergence(tmp_path):
    input_file = str(tmp_path / "crdt_ops.json")
    output_report = str(tmp_path / "convergence_report.json")

    mock_ops = [
        {"op_id": 1, "doc_id": "DOC_TEST", "client_id": "C1", "lamport_ts": 1, "op_type": "INSERT", "char_val": "C", "pos_fractional": 0.1, "is_deleted": False},
        {"op_id": 2, "doc_id": "DOC_TEST", "client_id": "C2", "lamport_ts": 2, "op_type": "INSERT", "char_val": "R", "pos_fractional": 0.2, "is_deleted": False},
        {"op_id": 3, "doc_id": "DOC_TEST", "client_id": "C1", "lamport_ts": 3, "op_type": "INSERT", "char_val": "X", "pos_fractional": 0.25, "is_deleted": True}, # Tombstone
        {"op_id": 4, "doc_id": "DOC_TEST", "client_id": "C2", "lamport_ts": 4, "op_type": "INSERT", "char_val": "D", "pos_fractional": 0.3, "is_deleted": False},
        {"op_id": 5, "doc_id": "DOC_TEST", "client_id": "C1", "lamport_ts": 5, "op_type": "INSERT", "char_val": "T", "pos_fractional": 0.4, "is_deleted": False}
    ]

    with open(input_file, "w") as f:
        for op in mock_ops:
            f.write(json.dumps(op) + "\n")

    report = run_crdt_convergence_audit(input_file, output_report)
    assert report["total_operations_audited"] == 5
    assert report["tombstones_detected"] == 1
    assert report["is_mathematically_converged"] is True
    assert report["documents_reconstructed"][0]["reconstructed_text"] == "CRDT"
    assert os.path.exists(output_report)
