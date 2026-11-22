"""整批调度的回溯搜索（纯函数，不碰全局状态，便于并发与测试）。

约束：
1. 每个 Pod 必须拿到 *一块* 未分配的本地 PV，且盘容量 >= PVC 申请容量；
2. 本地 PV 有节点亲和：Pod 只能落在该 PV 所在节点；
3. 节点已预留 CPU + 本批已分配 CPU 之和不得超过节点总量（不超用）；
4. 一块盘在一次解中最多给一个 Pod；
5. 整批是一个事务：全部 Pod 都能放下才有解，否则返回 None（调用方不占资源）。

搜索策略：MRV（候选 (节点,PV) 最少的 Pod 先排），并优先尝试剩余容量
更贴合的盘（best-fit），让大盘留给真正需要大盘的 Pod，大小盘混排时
更容易一次找到解。
"""
from __future__ import annotations

from copy import deepcopy
from typing import Dict, List, Optional, Tuple

EPS = 1e-9


class Snapshot:
    """已生效预留状态的不可变快照（由 store 在锁内构造，传值给搜索）。"""

    def __init__(
        self,
        cpu_available: Dict[str, float],
        pv_records: List[dict],
    ) -> None:
        self.cpu_available = dict(cpu_available)
        # 只保留搜索需要的字段，且做深拷贝，搜索中的写入不影响存储层
        self.pvs = [
            {
                "name": p["name"],
                "node": p["node"],
                "capacity": float(p["capacity"]),
                "available": bool(p["available"]),
            }
            for p in pv_records
        ]


def _candidates_for(pod: dict, snap: Snapshot, cpu_used: Dict[str, float]) -> List[Tuple[str, str]]:
    """返回该 Pod 当前可行的 (node, pv_name) 列表，best-fit（容量贴合）优先。"""
    out: List[Tuple[float, str, str]] = []
    for pv in snap.pvs:
        if not pv["available"]:
            continue
        if pv["capacity"] + EPS < pod["pvc_capacity"]:
            continue
        node = pv["node"]
        if node not in snap.cpu_available:
            continue
        if snap.cpu_available[node] - cpu_used.get(node, 0.0) + EPS < pod["cpu"]:
            continue
        # 容量浪费越少越优先；同容量时节点剩余 CPU 多的优先，分散压力
        waste = pv["capacity"] - pod["pvc_capacity"]
        out.append((waste, node, pv["name"]))
    out.sort(key=lambda t: (t[0], snap.cpu_available[t[1]] - cpu_used.get(t[1], 0.0)))
    return [(node, pv_name) for _, node, pv_name in out]


def schedule(
    pods: List[dict],
    snap: Snapshot,
) -> Optional[List[dict]]:
    """整批搜索。成功返回 placements（顺序与 pods 一致），失败返回 None。"""
    if not pods:
        return []

    pv_by_name = {p["name"]: p for p in snap.pvs}
    # 快速失败：节点名 / 容量明显不够的情况提前给出 None
    for pod in pods:
        if not any(
            p["available"]
            and p["capacity"] + EPS >= pod["pvc_capacity"]
            and p["node"] in snap.cpu_available
            and snap.cpu_available[p["node"]] + EPS >= pod["cpu"]
            for p in snap.pvs
        ):
            return None

    n = len(pods)
    assignment: List[Optional[dict]] = [None] * n
    cpu_used: Dict[str, float] = {}
    used_pv: set = set()

    def backtrack(done: int) -> bool:
        if done == n:
            return True

        # MRV：在未分配 Pod 中挑当前候选最少的
        best_i = -1
        best_cands: List[Tuple[str, str]] = []
        best_count = None
        for i, pod in enumerate(pods):
            if assignment[i] is not None:
                continue
            cands = [
                c
                for c in _candidates_for(pod, snap, cpu_used)
                if c[1] not in used_pv
            ]
            if not cands:
                return False
            if best_count is None or len(cands) < best_count:
                best_i, best_cands, best_count = i, cands, len(cands)
                if best_count == 1:
                    break

        pod = pods[best_i]
        for node, pv_name in best_cands:
            pv = pv_by_name[pv_name]
            # 再次校验（MRV 评估与试排之间状态可能因本分支前序赋值变化）
            if pv_name in used_pv or not pv["available"]:
                continue
            if snap.cpu_available[node] - cpu_used.get(node, 0.0) + EPS < pod["cpu"]:
                continue
            # 试分配
            assignment[best_i] = {
                "pod_id": pod["id"],
                "node": node,
                "pv": pv_name,
                "cpu": pod["cpu"],
                "pvc_capacity": pod["pvc_capacity"],
            }
            used_pv.add(pv_name)
            cpu_used[node] = cpu_used.get(node, 0.0) + pod["cpu"]

            if backtrack(done + 1):
                return True

            # 回滚
            cpu_used[node] -= pod["cpu"]
            if abs(cpu_used[node]) < EPS:
                cpu_used.pop(node, None)
            used_pv.discard(pv_name)
            assignment[best_i] = None
        return False

    if backtrack(0):
        return deepcopy(assignment)
    return None


def diagnose(pods: List[dict], snap: Snapshot) -> str:
    """搜索失败时给出人类可读的首要原因（尽力而为，不保证完备）。"""
    free_pvs = [p for p in snap.pvs if p["available"]]
    if len(free_pvs) < len(pods):
        return (
            f"空闲本地 PV 数量不足：需要 {len(pods)} 块，仅剩 {len(free_pvs)} 块"
        )
    for pod in pods:
        fit = [
            p
            for p in free_pvs
            if p["capacity"] + EPS >= pod["pvc_capacity"]
            and p["node"] in snap.cpu_available
        ]
        if not fit:
            return (
                f"Pod {pod['id']} 申请 {pod['pvc_capacity']} GiB，"
                "没有容量足够且节点存在的空闲本地 PV"
            )
        cpu_ok = [p for p in fit if snap.cpu_available[p["node"]] + EPS >= pod["cpu"]]
        if not cpu_ok:
            return (
                f"Pod {pod['id']} 需要 {pod['cpu']} 核，"
                "容量满足的盘所在节点剩余 CPU 均不足"
            )
    total_cpu_need = sum(p["cpu"] for p in pods)
    total_cpu_free = sum(snap.cpu_available.values())
    if total_cpu_free + EPS < total_cpu_need:
        return (
            f"集群剩余 CPU 总量不足：本批需要 {total_cpu_need} 核，"
            f"全集群仅剩 {total_cpu_free:.4g} 核"
        )
    return "单看每项资源都有余量，但受本地盘节点亲和约束，不存在让整批同时满足的组合"
