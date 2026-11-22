"""API 级自测：整批事务、大小盘混排、两批并发改排、排程中查询余量。"""
from __future__ import annotations

import asyncio

import httpx
import pytest
from fastapi.testclient import TestClient

from app.inventory import DEFAULT_INVENTORY_PATH, load_inventory_file
from app.main import app
from app.store import store


@pytest.fixture(autouse=True)
def _fresh_store():
    # 每个用例前重新载入清单（含清空预留），保证相互隔离
    load_inventory_file(DEFAULT_INVENTORY_PATH)
    yield
    store.reset_reservations()


def _client() -> TestClient:
    return TestClient(app)


async def _aclient():
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    )


def _pod(pid, cpu, pvc):
    return {"id": pid, "cpu": cpu, "pvc_capacity": pvc}


def test_docs_and_health():
    with _client() as c:
        assert c.get("/health").status_code == 200
        r = c.get("/openapi.json")
        assert r.status_code == 200
        assert "/api/schedule" in r.json()["paths"]


def test_schedule_full_batch_returns_node_and_pv():
    with _client() as c:
        body = {"pods": [_pod("w1", 2, 150), _pod("w2", 1, 10), _pod("w3", 1, 10)]}
        r = c.post("/api/schedule", json=body)
        assert r.status_code == 200, r.text
        data = r.json()
        assert data["scheduled"] is True
        assert len(data["placements"]) == 3
        by_id = {p["pod_id"]: p for p in data["placements"]}
        # 150Gi 的 Pod 必须拿大盘
        assert by_id["w1"]["pv"] == "pv-n1-large"
        # 每个 Pod 都有节点和 PV，且 PV 与节点同节点
        rem = c.get("/api/remaining").json()
        pv_node = {p["name"]: p["node"] for p in rem["pvs"]}
        for p in data["placements"]:
            assert pv_node[p["pv"]] == p["node"]
        # 三块盘均已绑定，CPU 已预留
        bound = [p for p in rem["pvs"] if not p["available"]]
        assert len(bound) == 3
        n1 = next(n for n in rem["nodes"] if n["name"] == "node-1")
        assert n1["cpu_reserved"] == pytest.approx(4.0)
        assert n1["cpu_available"] == pytest.approx(4.0)


def test_unschedulable_batch_reserves_nothing():
    with _client() as c:
        before = c.get("/api/remaining").json()
        # 只有两块 200G 大盘，却要三个 150G PVC -> 无解
        body = {
            "pods": [
                _pod("a", 1, 150),
                _pod("b", 1, 150),
                _pod("c", 1, 150),
            ]
        }
        r = c.post("/api/schedule", json=body)
        assert r.status_code == 409
        assert r.json()["detail"]["scheduled"] is False
        after = c.get("/api/remaining").json()
        # 排不下：一块盘、一个 CPU 都不许占
        assert all(p["available"] for p in after["pvs"])
        assert all(n["cpu_reserved"] == 0 for n in after["nodes"])
        assert after["reservation_count"] == 0
        assert before["pvs"] == after["pvs"]


def test_batch_size_limit_and_validation():
    with _client() as c:
        r = c.post("/api/schedule", json={"pods": [_pod(str(i), 1, 1) for i in range(9)]})
        assert r.status_code == 422
        r = c.post("/api/schedule", json={"pods": [{"id": "x", "cpu": 0, "pvc_capacity": 1}]})
        assert r.status_code == 422


def test_mixed_large_small_disk_packing_via_api():
    with _client() as c:
        body = {
            "pods": [
                _pod("small-1", 1, 10),
                _pod("big", 3, 180),
                _pod("small-2", 1, 10),
            ]
        }
        r = c.post("/api/schedule", json=body)
        assert r.status_code == 200, r.text
        by_id = {p["pod_id"]: p for p in r.json()["placements"]}
        assert by_id["big"]["pv"] in {"pv-n1-large", "pv-n2-large"}
        assert all(
            "large" not in by_id[s]["pv"]
            for s in ("small-1", "small-2")
        )


def test_concurrent_batches_second_finds_alternative_placement():
    """关键并发场景：先预留的批次占了 n1，后到的批次靠别的排法也成功。"""

    async def main():
        async with await _aclient() as client:
            batch_a = {"pods": [_pod("A1", 4, 20), _pod("A2", 4, 20)]}
            batch_b = {"pods": [_pod("B1", 4, 20), _pod("B2", 4, 20)]}
            ra, rb = await asyncio.gather(
                client.post("/api/schedule", json=batch_a),
                client.post("/api/schedule", json=batch_b),
            )
            assert ra.status_code == 200, ra.text
            assert rb.status_code == 200, rb.text

            pa = {p["pod_id"]: p for p in ra.json()["placements"]}
            pb = {p["pod_id"]: p for p in rb.json()["placements"]}

            # 两批的 PV 完全不相交
            disks_a = {p["pv"] for p in pa.values()}
            disks_b = {p["pv"] for p in pb.values()}
            assert disks_a.isdisjoint(disks_b)

            rem = (await client.get("/api/remaining")).json()
            # 4 个 Pod 共预留 16 核；各节点均未超用
            assert sum(n["cpu_reserved"] for n in rem["nodes"]) == 16
            for n in rem["nodes"]:
                assert n["cpu_reserved"] <= n["cpu_total"] + 1e-9
            assert rem["reservation_count"] == 2
            assert rem["reserved_pod_count"] == 4
            # 被占用的 4 块盘各自只绑定一个 Pod
            used = [p for p in rem["pvs"] if not p["available"]]
            assert len(used) == 4
            assert len({p["name"] for p in used}) == 4

    asyncio.run(main())


def test_concurrent_conflict_one_wins_one_rejects_without_side_effect():
    """两批抢同一块唯一大盘：一个成功，另一个 409 且不留残留。"""

    async def main():
        async with await _aclient() as client:
            # 热替换成只有一块大盘的清单，制造两个并发批次抢同一块盘
            inv = {
                "nodes": [{"name": "n1", "cpu_total": 8}],
                "pvs": [{"name": "only-large", "node": "n1", "capacity": 200}],
            }
            assert (await client.post("/api/inventory", json=inv)).status_code == 200
            payload = {"pods": [_pod("x", 1, 180)]}
            r1, r2 = await asyncio.gather(
                client.post("/api/schedule", json=payload),
                client.post("/api/schedule", json=payload),
            )
            codes = sorted(r.status_code for r in (r1, r2))
            assert codes == [200, 409]

            rem = (await client.get("/api/remaining")).json()
            large_used = [p for p in rem["pvs"] if not p["available"]]
            assert len(large_used) == 1  # 唯一一块盘只被赢家占用
            assert rem["reservation_count"] == 1  # 输家 409，不留任何残留
            # 输家响应里应带失败原因
            loser = r1 if r1.status_code == 409 else r2
            assert loser.json()["detail"]["scheduled"] is False
            assert loser.json()["detail"]["reason"]

    asyncio.run(main())


def test_remaining_queryable_while_scheduling():
    """排程进行中并发查询余量：查询始终返回已生效数据，不被阻塞。"""

    async def main():
        async with await _aclient() as client:
            # 先制造一批已生效预留
            r0 = await client.post(
                "/api/schedule", json={"pods": [_pod("seed", 2, 10)]}
            )
            assert r0.status_code == 200

            async def hammer_remaining():
                codes = []
                for _ in range(20):
                    r = await client.get("/api/remaining")
                    codes.append(r.status_code)
                    await asyncio.sleep(0)
                return codes

            big = {"pods": [_pod(f"c{i}", 1, 10) for i in range(6)]}
            codes_task = asyncio.create_task(hammer_remaining())
            sched_task = asyncio.create_task(client.post("/api/schedule", json=big))
            codes = await codes_task
            rs = await sched_task
            return codes, rs

    codes, rs = asyncio.run(main())
    assert codes and all(code == 200 for code in codes)
    # 6 个小盘需求 + 已占 1 块；空闲盘只剩 5 块 -> 该批无解（409，不占资源）
    assert rs.status_code == 409


def test_inventory_replace_and_invalid_reference():
    with _client() as c:
        bad = {
            "nodes": [{"name": "z1", "cpu_total": 4}],
            "pvs": [{"name": "d1", "node": "ghost", "capacity": 100}],
        }
        assert c.post("/api/inventory", json=bad).status_code == 422

        good = {
            "nodes": [{"name": "z1", "cpu_total": 4}],
            "pvs": [{"name": "d1", "node": "z1", "capacity": 100}],
        }
        r = c.post("/api/inventory", json=good)
        assert r.status_code == 200
        rem = c.get("/api/remaining").json()
        assert len(rem["nodes"]) == 1 and len(rem["pvs"]) == 1

        ok = c.post("/api/schedule", json={"pods": [_pod("p", 4, 100)]})
        assert ok.status_code == 200
        over = c.post("/api/schedule", json={"pods": [_pod("q", 0.1, 1)]})
        assert over.status_code == 409  # 唯一的盘已被占用


def test_release_returns_resources():
    with _client() as c:
        r = c.post("/api/schedule", json={"pods": [_pod("p", 2, 150)]})
        bid = r.json()["batch_id"]
        assert c.post(f"/api/reservations/release/{bid}").status_code == 200
        rem = c.get("/api/remaining").json()
        assert all(p["available"] for p in rem["pvs"])
        assert all(n["cpu_reserved"] == 0 for n in rem["nodes"])
        assert c.post(f"/api/reservations/release/{bid}").status_code == 404
