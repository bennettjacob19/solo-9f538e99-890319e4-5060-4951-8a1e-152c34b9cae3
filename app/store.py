"""线程安全的集群库存与预留存储（进程内存态）。

并发模型（对应"两批并发提交"的要求）：
  1. 调度搜索是 CPU 密集的纯函数，在锁 *外* 基于快照进行，
     因此排程期间 GET /api/remaining 仍可立即读到已生效余量；
  2. 提交时在锁内做版本号 CAS 校验并一次性预留全部 CPU 和盘，
     两个并发批次只会有一个批次先提交成功；后提交的批次拿新版本
     重试搜索——只要还有别的排法，它同样会成功；
  3. 搜索失败的批次不写入任何状态，即"排不下就不占资源"。
"""
from __future__ import annotations

import threading
import uuid
from copy import deepcopy
from typing import Dict, List, Optional, Tuple

from .scheduler import Snapshot, diagnose, schedule


class InventoryError(ValueError):
    """清单本身不合法（重名、PV 引用了不存在的节点等）。"""


class ConcurrentCommitError(Exception):
    """快照过期：取快照后有别的批次先预留了，需要重试。"""


class Store:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._version = 0
        self._nodes: Dict[str, dict] = {}
        self._pvs: Dict[str, dict] = {}
        self._reservations: Dict[str, dict] = {}

    # ------------------------------------------------------------------ #
    # 清单管理
    # ------------------------------------------------------------------ #
    def load_inventory(self, nodes: List[dict], pvs: List[dict]) -> None:
        """整体替换节点空闲 CPU 与本地 PV 清单，并清空所有预留。"""
        norm_nodes: Dict[str, dict] = {}
        for n in nodes:
            name = n["name"]
            if name in norm_nodes:
                raise InventoryError(f"节点名称重复: {name}")
            norm_nodes[name] = {
                "name": name,
                "cpu_total": float(n["cpu_total"]),
                "cpu_reserved": 0.0,
            }

        norm_pvs: Dict[str, dict] = {}
        for p in pvs:
            name = p["name"]
            if name in norm_pvs:
                raise InventoryError(f"PV 名称重复: {name}")
            if p["node"] not in norm_nodes:
                raise InventoryError(
                    f"PV {name} 标注的节点 {p['node']} 不在节点清单中"
                )
            norm_pvs[name] = {
                "name": name,
                "node": p["node"],
                "capacity": float(p["capacity"]),
                "available": True,
                "bound_pod": None,
                "bound_batch": None,
            }

        with self._lock:
            self._nodes = norm_nodes
            self._pvs = norm_pvs
            self._reservations = {}
            self._version += 1

    # ------------------------------------------------------------------ #
    # 快照与余量查询（排程期间也可调用）
    # ------------------------------------------------------------------ #
    def snapshot(self) -> Tuple[Snapshot, int]:
        with self._lock:
            return Snapshot(
                cpu_available={
                    name: n["cpu_total"] - n["cpu_reserved"]
                    for name, n in self._nodes.items()
                },
                pv_records=list(self._pvs.values()),
            ), self._version

    def remaining(self) -> dict:
        with self._lock:
            return {
                "version": self._version,
                "nodes": [
                    {
                        "name": n["name"],
                        "cpu_total": n["cpu_total"],
                        "cpu_reserved": n["cpu_reserved"],
                        "cpu_available": n["cpu_total"] - n["cpu_reserved"],
                    }
                    for n in self._nodes.values()
                ],
                "pvs": [deepcopy(p) for p in self._pvs.values()],
                "reserved_pod_count": sum(
                    len(r["placements"]) for r in self._reservations.values()
                ),
                "reservation_count": len(self._reservations),
            }

    def list_reservations(self) -> List[dict]:
        with self._lock:
            return list(self._reservations.values())

    # ------------------------------------------------------------------ #
    # 调度事务
    # ------------------------------------------------------------------ #
    def schedule_batch(
        self, pods: List[dict], max_retries: int = 5
    ) -> Tuple[Optional[dict], Optional[str], Optional[dict]]:
        """整批调度 + 一次性预留。

        返回 (成功结果, None, None) 或 (None, reason, detail)。
        成功结果含 batch_id 与每个 Pod 的 node/pv。
        """
        last_reason = "未知原因"
        for _ in range(max_retries):
            snap, version = self.snapshot()
            placements = schedule(pods, snap)
            if placements is None:
                # 当前已生效余量下无解。再确认期间没有并发提交：
                # 若版本已变，说明看到的是中间态，重试一次更稳妥。
                with self._lock:
                    if self._version != version:
                        continue
                return None, diagnose(pods, snap), {
                    "version": version,
                    "pod_count": len(pods),
                }
            try:
                batch_id = self._commit(pods, placements, version)
            except ConcurrentCommitError:
                continue  # 别的批次先占了资源，基于新余量重新找排法
            result = {
                "scheduled": True,
                "batch_id": batch_id,
                "placements": placements,
            }
            return result, None, None

        return None, "并发冲突过多，多次重试后仍未能预留资源", {
            "retries": max_retries,
        }

    def _commit(
        self, pods: List[dict], placements: List[dict], expected_version: int
    ) -> str:
        """锁内 CAS：版本未变才一次性扣减 CPU、绑定 PV。"""
        with self._lock:
            if self._version != expected_version:
                raise ConcurrentCommitError("inventory version changed")

            # 防御性二次校验（快照与提交之间本锁保证无其它写者，这里做完整性兜底）
            used_pv = set()
            cpu_delta: Dict[str, float] = {}
            for item in placements:
                pv_name = item["pv"]
                node = item["node"]
                if pv_name not in self._pvs or node not in self._nodes:
                    raise ConcurrentCommitError("inventory changed: missing node/pv")
                pv = self._pvs[pv_name]
                if not pv["available"] or pv_name in used_pv:
                    raise ConcurrentCommitError(f"pv {pv_name} no longer available")
                if pv["node"] != node:
                    raise ConcurrentCommitError(f"pv {pv_name} node affinity broken")
                used_pv.add(pv_name)
                cpu_delta[node] = cpu_delta.get(node, 0.0) + item["cpu"]

            for node, delta in cpu_delta.items():
                avail = (
                    self._nodes[node]["cpu_total"] - self._nodes[node]["cpu_reserved"]
                )
                if avail + 1e-9 < delta:
                    raise ConcurrentCommitError(f"node {node} cpu overcommit")

            # 全部校验通过，一次性生效
            batch_id = f"batch-{uuid.uuid4().hex[:12]}"
            for item in placements:
                pv = self._pvs[item["pv"]]
                pv["available"] = False
                pv["bound_pod"] = item["pod_id"]
                pv["bound_batch"] = batch_id
                self._nodes[item["node"]]["cpu_reserved"] += item["cpu"]

            self._reservations[batch_id] = {
                "batch_id": batch_id,
                "placements": deepcopy(placements),
            }
            self._version += 1
            return batch_id

    # ------------------------------------------------------------------ #
    # 释放 / 重置（演练管理接口与测试用）
    # ------------------------------------------------------------------ #
    def release_batch(self, batch_id: str) -> bool:
        with self._lock:
            res = self._reservations.pop(batch_id, None)
            if res is None:
                return False
            for item in res["placements"]:
                pv = self._pvs.get(item["pv"])
                if pv is not None and pv.get("bound_batch") == batch_id:
                    pv["available"] = True
                    pv["bound_pod"] = None
                    pv["bound_batch"] = None
                node = self._nodes.get(item["node"])
                if node is not None:
                    node["cpu_reserved"] = max(
                        0.0, node["cpu_reserved"] - item["cpu"]
                    )
            self._version += 1
            return True

    def reset_reservations(self) -> None:
        with self._lock:
            for pv in self._pvs.values():
                pv["available"] = True
                pv["bound_pod"] = None
                pv["bound_batch"] = None
            for n in self._nodes.values():
                n["cpu_reserved"] = 0.0
            self._reservations.clear()
            self._version += 1


# 单例存储
store = Store()
