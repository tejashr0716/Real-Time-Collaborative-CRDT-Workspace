import asyncio
import random
import time
import statistics
from httpx import AsyncClient, ASGITransport
from python.api.main import app
from python.database.connection import close_db_pool
from python.services.redis_service import close_redis

TOTAL_OPS = 1200
CONCURRENCY = 15
DOC_ID = "DOC_DISTRIBUTED_SPECS"

async def worker(client: AsyncClient, queue: asyncio.Queue, latencies: list):
    while True:
        try:
            payload = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        t0 = time.perf_counter()
        resp = await client.post(f"/api/v1/documents/{DOC_ID}/operations", json=payload)
        t1 = time.perf_counter()
        if resp.status_code == 200:
            latencies.append((t1 - t0) * 1000.0)

async def run_stress_test():
    print("=================================================================")
    print("  SyncCanvas Collaborative CRDT Keystroke Ingest Stress Test     ")
    print(f"  Target: {TOTAL_OPS} operations | Concurrency: {CONCURRENCY} workers")
    print("=================================================================\n")

    queue = asyncio.Queue()
    for i in range(TOTAL_OPS):
        queue.put_nowait({
            "operations": [
                {
                    "client_id": f"PEER_{random.randint(1, 10)}",
                    "lamport_ts": i + 10,
                    "op_type": "INSERT" if random.random() > 0.05 else "DELETE",
                    "char_val": chr(ord('a') + (i % 26)),
                    "pos_fractional": round(0.5 + (i * 0.001), 6)
                }
            ]
        })

    latencies = []
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        start_time = time.perf_counter()
        tasks = [asyncio.create_task(worker(client, queue, latencies)) for _ in range(CONCURRENCY)]
        await asyncio.gather(*tasks)
        total_time = time.perf_counter() - start_time

    await close_db_pool()
    await close_redis()

    latencies.sort()
    throughput = len(latencies) / total_time
    p50 = statistics.median(latencies)
    p95 = latencies[int(len(latencies) * 0.95)]
    p99 = latencies[int(len(latencies) * 0.99)]

    print(f">> Completed:            {len(latencies)} / {TOTAL_OPS} operations")
    print(f">> Total Wall Time:      {total_time:.2f} seconds")
    print(f">> Sustained Throughput: {throughput:,.1f} keystroke-merges/sec")
    print(f">> Latency p50:          {p50:.2f} ms")
    print(f">> Latency p95:          {p95:.2f} ms")
    print(f">> Latency p99:          {p99:.2f} ms")
    print("=================================================================")

if __name__ == "__main__":
    asyncio.run(run_stress_test())
