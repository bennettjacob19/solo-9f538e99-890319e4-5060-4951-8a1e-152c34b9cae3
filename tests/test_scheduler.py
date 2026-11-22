"""调度算法纯函数测试：容量、节点亲和、CPU 超用、大小盘混排、整批事务。"""
from __future__ import annotations

from app.scheduler import Snapshot, diagnose, schedule


def make_snap(cpu: dict, pvs: list[dict]) -> Snapshot:
    return Snapshot(cpu_available=cpu, pv_records=pvs)


def pv(name, node, cap, available=True):
    return {"name": name, "node": node, "capacity": cap, "available": available}


def pod(pid, c, g):
    return {"id": pid, "cpu": c, "pvc_capacity": g}


def names(placements):
    return {p["pod_id"]: (p["node"], p["pv"]) for p in placements}


def test_simple_placement_and_affinity():
    snap = make_snap(
        {"n1": 8.0, "n2": 8.0},
        [pv("pv1", "n1", 100), pv("pv2", "n2", 100)],
    )
    out = schedule([pod("a", 2, 50), pod("b", 2, 50)], snap)
    assert out is not None and len(out) == 2
    got = names(out)
    # PV 所在节点必须等于落点节点
    for p in out:
        assert next(x for x in snap.pvs if x["name"] == p["pv"])["node"] == p["node"]
    assert {v[1] for v in got.values()} == {"pv1", "pv2"}  # 一块盘一个 Pod


def test_pv_capacity_must_be_enough():
    snap = make_snap({"n1": 8.0}, [pv("small", "n1", 20)])
    assert schedule([pod("a", 1, 50)], snap) is None
    reason = diagnose([pod("a", 1, 50)], snap)
    assert "PV" in reason


def test_node_cpu_not_overcommitted():
    # 单节点 4 核，两个 Pod 各要 3 核 -> 无解，且不修改快照
    snap = make_snap(
        {"n1": 4.0},
        [pv("p1", "n1", 100), pv("p2", "n1", 100)],
    )
    assert schedule([pod("a", 3, 10), pod("b", 3, 10)], snap) is None
    assert snap.cpu_available == {"n1": 4.0}
    assert all(p["available"] for p in snap.pvs)


def test_one_pv_per_pod_even_with_cpu_headroom():
    # CPU 够两块盘的量，但只有一块盘 -> 无解
    snap = make_snap({"n1": 16.0}, [pv("only", "n1", 100)])
    assert schedule([pod("a", 1, 10), pod("b", 1, 10)], snap) is None


def test_mixed_large_small_disk_packing():
    """大小盘混排：小 PVC 不能抢大盘导致大 PVC 无盘可用。"""
    snap = make_snap(
        {"n1": 16.0},
        [
            pv("small-1", "n1", 20),
            pv("small-2", "n1", 20),
            pv("large-1", "n1", 200),
        ],
    )
    pods = [
        pod("big-a", 2, 150),   # 只能用 large-1
        pod("small-a", 1, 10),  # 应落到 small-*
        pod("small-b", 1, 10),
    ]
    out = schedule(pods, snap)
    assert out is not None
    got = names(out)
    assert got["big-a"][1] == "large-1"
    assert {got["small-a"][1], got["small-b"][1]} == {"small-1", "small-2"}


def test_mixed_packing_with_partial_bad_guess_order():
    """大 PVC 排在请求列表后面时，MRV 仍应先给它留大盘。"""
    snap = make_snap(
        {"n1": 16.0},
        [pv("small-1", "n1", 20), pv("large-1", "n1", 200)],
    )
    pods = [pod("small-a", 1, 10), pod("big-a", 2, 150)]
    out = schedule(pods, snap)
    assert out is not None
    got = names(out)
    assert got["big-a"][1] == "large-1"
    assert got["small-a"][1] == "small-1"


def test_affinity_forces_distribution_across_nodes():
    """盘在哪个节点，Pod 就必须去哪个节点；CPU 随之按节点核算。"""
    snap = make_snap(
        {"n1": 2.0, "n2": 8.0},
        [pv("p1", "n1", 100), pv("p2", "n2", 100), pv("p3", "n2", 100)],
    )
    # n1 只有 2 核：放两个 Pod 各 2 核不可行（n1 只有一块盘也放不了两个）
    out = schedule([pod("a", 2, 10), pod("b", 2, 10)], snap)
    assert out is not None
    got = names(out)
    nodes = {v[0] for v in got.values()}
    assert nodes == {"n1", "n2"}

    # n1 只剩 1 核：需要 2 核的 Pod 无法落在 n1，而 n2 的盘只能给 n2 的 Pod，
    # 构造 n2 也放不下的情形 -> 无解
    snap_tight = make_snap(
        {"n1": 1.0, "n2": 8.0},
        [pv("p1", "n1", 100), pv("p2", "n2", 100)],
    )
    assert schedule([pod("a", 2, 10), pod("b", 8, 10)], snap_tight) is None


def test_eight_pod_batch_limit_and_full_pack():
    snap = make_snap(
        {"n1": 32.0},
        [pv(f"p{i}", "n1", 100) for i in range(8)],
    )
    pods = [pod(f"a{i}", 1, 10) for i in range(8)]
    out = schedule(pods, snap)
    assert out is not None and len(out) == 8
    assert len({p["pv"] for p in out}) == 8


def test_already_bound_pvs_are_skipped():
    snap = make_snap(
        {"n1": 8.0},
        [pv("used", "n1", 100, available=False), pv("free", "n1", 100)],
    )
    out = schedule([pod("a", 1, 10)], snap)
    assert out is not None and out[0]["pv"] == "free"


def test_diagnose_fragmentation():
    # 每块盘单看都够，但节点 CPU 碎片化导致整批无解
    snap = make_snap(
        {"n1": 1.0, "n2": 1.0},
        [pv("p1", "n1", 100), pv("p2", "n2", 100)],
    )
    pods = [pod("a", 2, 10), pod("b", 2, 10)]
    assert schedule(pods, snap) is None
    assert "CPU" in diagnose(pods, snap)
