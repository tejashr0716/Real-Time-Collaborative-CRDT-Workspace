import json
import os
import redis.asyncio as redis
from typing import Optional, Dict, Any, List

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "6"))

redis_client: Optional[redis.Redis] = None

async def init_redis() -> redis.Redis:
    global redis_client
    if redis_client is not None:
        try:
            await redis_client.aclose()
        except Exception:
            pass
        redis_client = None

    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True
    )
    return redis_client

async def get_redis() -> redis.Redis:
    global redis_client
    if redis_client is None:
        await init_redis()
    return redis_client

async def close_redis():
    global redis_client
    if redis_client is not None:
        try:
            await redis_client.aclose()
        except Exception:
            pass
        redis_client = None

async def update_presence(doc_id: str, client_id: str, cursor_pos: int, color: str, ttl_seconds: int = 30):
    client = await get_redis()
    key = f"synccanvas:presence:{doc_id}"
    presence_data = {
        "client_id": client_id,
        "cursor_pos": cursor_pos,
        "color": color
    }
    await client.hset(key, client_id, json.dumps(presence_data))
    await client.expire(key, ttl_seconds)

async def get_room_presence(doc_id: str) -> List[Dict[str, Any]]:
    client = await get_redis()
    key = f"synccanvas:presence:{doc_id}"
    raw_members = await client.hgetall(key)
    return [json.loads(val) for val in raw_members.values()]

async def publish_crdt_delta(doc_id: str, delta: Dict[str, Any]):
    client = await get_redis()
    await client.publish(f"synccanvas:events:{doc_id}", json.dumps(delta))
