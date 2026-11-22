"""FastAPI 入口：K8s 本地 PV + 节点 CPU 整批调度演练。

核心接口（/docs 可交互）：
  POST /api/schedule         提交一批 1..8 个 Pod，整批一起调度并一次性预留
  GET  /api/remaining        查询已生效余量（排程期间也可查）
  POST /api/inventory        整体替换节点空闲 CPU 与本地 PV 清单
  GET  /api/inventory        查看当前清单
  GET  /api/reservations     查看所有已生效批次预留
  POST /api/reservations/release/{batch_id}  释放某批预留
  POST /api/reset            清空全部预留（回到清单初始余量）
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse

from .inventory import load_default_inventory
from .models import (
    BatchRequest,
    BatchScheduledResponse,
    Inventory,
    MAX_PODS_PER_BATCH,
    PodRequest,
    RemainingResponse,
    ReservationView,
    UnschedulableResponse,
)
from .store import InventoryError, store


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动即载入可替换清单（路径由 SCHED_INVENTORY 环境变量指定）
    msg = load_default_inventory()
    app.state.boot_message = msg
    yield


app = FastAPI(
    title="Kubernetes 本地 PV 整批调度演练",
    version="1.0.0",
    description=(
        "模拟 MPI 作业的一批 Pod **同时启动**的调度过程："
        "每个 Pod 给出 CPU 需求和一个 PVC 的容量，调度器必须为整批 Pod "
        "同时找到满足约束的节点与本地 PV，否则整批不占任何资源。\n\n"
        "约束：\n"
        "- 本地 PV 有节点亲和，只能给同节点的 Pod 使用；\n"
        "- 盘容量必须 ≥ PVC 申请，且一块盘只分给一个 Pod；\n"
        "- 节点 CPU 不超用；\n"
        f"- 每批至多 {MAX_PODS_PER_BATCH} 个 Pod；\n"
        "- 整批要么全部预留成功，要么完全不预留（事务语义）。"
    ),
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs")


@app.get("/health", tags=["运维"])
def health():
    return {"status": "ok", "boot": getattr(app.state, "boot_message", "")}


# --------------------------------------------------------------------------- #
# 清单
# --------------------------------------------------------------------------- #
@app.get("/api/inventory", tags=["清单"], summary="查看当前节点与本地 PV 清单")
def get_inventory():
    rem = store.remaining()
    return {
        "nodes": rem["nodes"],
        "pvs": rem["pvs"],
    }


@app.post("/api/inventory", tags=["清单"], summary="替换节点空闲 CPU 与本地 PV 清单")
def replace_inventory(inv: Inventory):
    """整体替换清单（盘必须注明所在节点和容量），并清空已有预留。"""
    try:
        store.load_inventory(
            nodes=[n.model_dump() for n in inv.nodes],
            pvs=[p.model_dump() for p in inv.pvs],
        )
    except InventoryError as e:
        raise HTTPException(status_code=422, detail=str(e))
    return {"loaded": True, "nodes": len(inv.nodes), "pvs": len(inv.pvs)}


# --------------------------------------------------------------------------- #
# 余量查询（排程期间可随时调用，读已生效数据）
# --------------------------------------------------------------------------- #
@app.get(
    "/api/remaining",
    tags=["调度"],
    summary="查询已生效的节点 CPU 余量与 PV 占用情况",
    response_model=RemainingResponse,
)
def get_remaining():
    return store.remaining()


# --------------------------------------------------------------------------- #
# 整批调度
# --------------------------------------------------------------------------- #
def _normalize_pods(req: BatchRequest) -> list[dict]:
    pods = []
    seen: set[str] = set()
    for idx, p in enumerate(req.pods, start=1):
        pid = p.id or f"pod-{idx}"
        if pid in seen:
            raise HTTPException(
                status_code=422, detail=f"批次内 Pod 标识重复: {pid}"
            )
        seen.add(pid)
        pods.append(
            {
                "id": pid,
                "cpu": float(p.cpu),
                "pvc_capacity": float(p.pvc_capacity),
            }
        )
    return pods


@app.post(
    "/api/schedule",
    tags=["调度"],
    summary="提交一批 Pod（至多 8 个）整批调度并一次性预留",
    response_model=Optional[BatchScheduledResponse],
    responses={
        409: {"model": UnschedulableResponse, "description": "整批排不下，未占用任何资源"},
        422: {"description": "请求参数不合法"},
    },
)
async def schedule_batch(req: BatchRequest):
    pods = _normalize_pods(req)
    # 搜索是 CPU 密集型纯函数，放到线程池执行，避免阻塞事件循环；
    # 这样其它请求（如 GET /api/remaining）在排程期间仍可被处理。
    result, reason, detail = await asyncio.to_thread(store.schedule_batch, pods)
    if result is None:
        raise HTTPException(
            status_code=409,
            detail={"scheduled": False, "reason": reason, "detail": detail},
        )
    return result


# --------------------------------------------------------------------------- #
# 预留管理
# --------------------------------------------------------------------------- #
@app.get("/api/reservations", tags=["预留"], summary="查看全部已生效批次")
def list_reservations():
    return {"reservations": store.list_reservations()}


@app.post(
    "/api/reservations/release/{batch_id}",
    tags=["预留"],
    summary="释放指定批次，归还其 CPU 与本地 PV",
)
def release_reservation(batch_id: str):
    ok = store.release_batch(batch_id)
    if not ok:
        raise HTTPException(status_code=404, detail=f"批次不存在: {batch_id}")
    return {"released": batch_id}


@app.post("/api/reset", tags=["预留"], summary="清空全部预留，恢复清单初始余量")
def reset_reservations():
    store.reset_reservations()
    return {"reset": True}
