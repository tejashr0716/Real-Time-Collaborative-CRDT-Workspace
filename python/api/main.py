import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from python.database.connection import init_db_pool, close_db_pool, get_db_pool
from python.services.cpp_bridge import SyncCanvasEngine
from python.services.redis_service import (
    init_redis, close_redis, update_presence, get_room_presence, publish_crdt_delta
)
from python.services.rabbitmq_service import init_rabbitmq, close_rabbitmq, publish_snapshot_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("synccanvas.api")

engine: Optional[SyncCanvasEngine] = None

def get_engine() -> SyncCanvasEngine:
    global engine
    if engine is None:
        engine = SyncCanvasEngine()
    return engine

class RoomConnectionManager:
    def __init__(self):
        self.rooms: Dict[str, List[WebSocket]] = {}

    async def connect(self, doc_id: str, websocket: WebSocket):
        await websocket.accept()
        if doc_id not in self.rooms:
            self.rooms[doc_id] = []
        self.rooms[doc_id].append(websocket)
        logger.info(f"Client connected to room {doc_id}. Total: {len(self.rooms[doc_id])}")

    def disconnect(self, doc_id: str, websocket: WebSocket):
        if doc_id in self.rooms and websocket in self.rooms[doc_id]:
            self.rooms[doc_id].remove(websocket)
            if not self.rooms[doc_id]:
                del self.rooms[doc_id]
            logger.info(f"Client disconnected from room {doc_id}.")

    async def broadcast_to_room(self, doc_id: str, message: Dict[str, Any]):
        if doc_id in self.rooms:
            for connection in list(self.rooms[doc_id]):
                try:
                    await connection.send_json(message)
                except Exception:
                    self.disconnect(doc_id, connection)

room_manager = RoomConnectionManager()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing SyncCanvas Services...")
    await init_db_pool()
    await init_redis()
    await init_rabbitmq()
    get_engine()
    logger.info("SyncCanvas CRDT Engine and distributed services ready.")
    yield
    logger.info("Shutting down SyncCanvas Services...")
    await close_rabbitmq()
    await close_redis()
    await close_db_pool()

app = FastAPI(
    title="SyncCanvas Real-Time Collaborative CRDT Workspace",
    version="1.0.0",
    description="Decentralized Conflict-Free Replicated Data Type (CRDT) Document Synchronization Grid",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class DocumentCreatePayload(BaseModel):
    workspace_id: str = "WS_ENG_CORP"
    title: str
    initial_text: Optional[str] = ""

class CRDTOperationItem(BaseModel):
    client_id: str
    lamport_ts: int
    op_type: str = "INSERT" # INSERT, DELETE
    char_val: str = " "
    pos_fractional: float

class BatchOperationsPayload(BaseModel):
    operations: List[CRDTOperationItem]

class PresencePayload(BaseModel):
    client_id: str
    cursor_pos: int
    color_hex: str = "#38bdf8"

@app.get("/health")
async def health_check():
    return {
        "status": "HEALTHY",
        "service": "SyncCanvas CRDT Gateway",
        "engine": "C++ CRDT Sequence Core Loaded",
        "active_rooms": len(room_manager.rooms)
    }

@app.get("/api/v1/documents")
async def list_documents(workspace_id: str = "WS_ENG_CORP"):
    db = await get_db_pool()
    async with db.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM documents WHERE workspace_id = $1 ORDER BY updated_at DESC;", workspace_id)
        return [
            {
                "doc_id": r["doc_id"],
                "workspace_id": r["workspace_id"],
                "title": r["title"],
                "version": r["version"],
                "current_text": r["current_text"],
                "created_at": r["created_at"].isoformat(),
                "updated_at": r["updated_at"].isoformat()
            }
            for r in rows
        ]

@app.post("/api/v1/documents", status_code=status.HTTP_201_CREATED)
async def create_document(payload: DocumentCreatePayload):
    db = await get_db_pool()
    doc_id = f"DOC_{abs(hash(payload.title)) % 1000000:06d}"
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO documents (doc_id, workspace_id, title, current_text, version, created_at, updated_at)
            VALUES ($1, $2, $3, $4, 1, NOW(), NOW())
            RETURNING doc_id, created_at;
            """,
            doc_id, payload.workspace_id, payload.title, payload.initial_text or ""
        )
        return {"doc_id": row["doc_id"], "title": payload.title, "workspace_id": payload.workspace_id}

@app.get("/api/v1/documents/{doc_id}")
async def get_document(doc_id: str):
    db = await get_db_pool()
    eng = get_engine()
    async with db.acquire() as conn:
        doc = await conn.fetchrow("SELECT * FROM documents WHERE doc_id = $1;", doc_id)
        if not doc:
            raise HTTPException(status_code=404, detail=f"Document '{doc_id}' not found.")

        ops = await conn.fetch("SELECT * FROM crdt_operations WHERE doc_id = $1 ORDER BY pos_fractional ASC, lamport_ts ASC;", doc_id)
        
        char_list = [
            {
                "lamport_ts": r["lamport_ts"],
                "pos_fractional": r["pos_fractional"],
                "char_val": r["char_val"],
                "is_deleted": r["is_deleted"],
                "client_id": r["client_id"]
            }
            for r in ops
        ]

        merged = eng.merge_characters(char_list)

        return {
            "doc_id": doc["doc_id"],
            "title": doc["title"],
            "version": doc["version"],
            "compiled_text": merged["compiled_text"],
            "total_characters": merged["length"],
            "tombstones": merged["total_tombstones"],
            "updated_at": doc["updated_at"].isoformat()
        }

@app.post("/api/v1/documents/{doc_id}/operations")
async def apply_operations(doc_id: str, payload: BatchOperationsPayload):
    if not payload.operations:
        return {"merged_length": 0, "status": "NOOP"}

    db = await get_db_pool()
    eng = get_engine()
    async with db.acquire() as conn:
        doc = await conn.fetchrow("SELECT * FROM documents WHERE doc_id = $1;", doc_id)
        if not doc:
            raise HTTPException(status_code=404, detail=f"Document '{doc_id}' not found.")

        # Insert operations
        records_to_insert = [
            (
                doc_id, op.client_id, op.lamport_ts, op.op_type,
                op.char_val[0] if op.char_val else " ", op.pos_fractional,
                True if op.op_type == "DELETE" else False
            )
            for op in payload.operations
        ]

        await conn.executemany(
            """
            INSERT INTO crdt_operations (doc_id, client_id, lamport_ts, op_type, char_val, pos_fractional, is_deleted)
            VALUES ($1, $2, $3, $4, $5, $6, $7);
            """,
            records_to_insert
        )

        # Merge all characters using C++ engine
        all_ops = await conn.fetch("SELECT * FROM crdt_operations WHERE doc_id = $1 ORDER BY pos_fractional ASC, lamport_ts ASC;", doc_id)
        char_list = [
            {
                "lamport_ts": r["lamport_ts"],
                "pos_fractional": r["pos_fractional"],
                "char_val": r["char_val"],
                "is_deleted": r["is_deleted"],
                "client_id": r["client_id"]
            }
            for r in all_ops
        ]
        merged = eng.merge_characters(char_list)

        # Update document compiled text
        new_version = doc["version"] + 1
        await conn.execute(
            "UPDATE documents SET current_text = $1, version = $2, updated_at = NOW() WHERE doc_id = $3;",
            merged["compiled_text"], new_version, doc_id
        )

        event = {
            "type": "CRDT_DELTA_COMMITTED",
            "doc_id": doc_id,
            "version": new_version,
            "compiled_text": merged["compiled_text"],
            "operations_applied": len(payload.operations),
            "total_chars": merged["length"]
        }

        # Redis PubSub & WebSocket broadcast
        await publish_crdt_delta(doc_id, event)
        asyncio.create_task(room_manager.broadcast_to_room(doc_id, event))

        return event

@app.post("/api/v1/documents/{doc_id}/presence")
async def report_presence(doc_id: str, payload: PresencePayload):
    await update_presence(doc_id, payload.client_id, payload.cursor_pos, payload.color_hex)
    event = {
        "type": "PRESENCE_UPDATE",
        "doc_id": doc_id,
        "client_id": payload.client_id,
        "cursor_pos": payload.cursor_pos,
        "color_hex": payload.color_hex
    }
    await publish_crdt_delta(doc_id, event)
    asyncio.create_task(room_manager.broadcast_to_room(doc_id, event))
    return {"status": "UPDATED"}

@app.get("/api/v1/documents/{doc_id}/collaborators")
async def get_collaborators(doc_id: str):
    members = await get_room_presence(doc_id)
    return {"doc_id": doc_id, "active_collaborators": members}

@app.websocket("/ws/doc/{doc_id}")
async def websocket_document(websocket: WebSocket, doc_id: str):
    await room_manager.connect(doc_id, websocket)
    try:
        await websocket.send_json({
            "type": "CONNECTED",
            "doc_id": doc_id,
            "message": f"Connected to document room {doc_id}"
        })
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_json({"type": "PONG"})
    except WebSocketDisconnect:
        room_manager.disconnect(doc_id, websocket)
    except Exception:
        room_manager.disconnect(doc_id, websocket)
