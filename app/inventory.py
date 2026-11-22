"""从 JSON 文件载入节点空闲 CPU 与本地 PV 清单（可替换）。"""
from __future__ import annotations

import json
import os
from typing import Tuple

from .models import Inventory
from .store import InventoryError, store

DEFAULT_INVENTORY_PATH = os.environ.get(
    "SCHED_INVENTORY", os.path.join(os.path.dirname(__file__), "..", "data", "inventory.json")
)


def load_inventory_file(path: str) -> Tuple[int, int]:
    """读取并校验 JSON 清单，替换到全局 store，返回 (节点数, PV 数)。"""
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    inv = Inventory.model_validate(raw)
    store.load_inventory(
        nodes=[n.model_dump() for n in inv.nodes],
        pvs=[p.model_dump() for p in inv.pvs],
    )
    return len(inv.nodes), len(inv.pvs)


def load_default_inventory() -> str:
    path = os.path.abspath(DEFAULT_INVENTORY_PATH)
    n_nodes, n_pvs = load_inventory_file(path)
    return f"已从 {path} 载入清单：{n_nodes} 个节点，{n_pvs} 块本地 PV"
