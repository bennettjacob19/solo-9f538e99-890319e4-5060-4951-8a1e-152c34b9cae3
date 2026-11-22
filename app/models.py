"""Pydantic 请求/响应模型。

字段命名刻意贴近 K8s 语义：节点 Node、本地 PV、PVC 容量、CPU 需求。
"""
from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field, field_validator

MAX_PODS_PER_BATCH = 8
EPS = 1e-9


class NodeSpec(BaseModel):
    name: str = Field(..., description="节点名称，例如 node-1")
    cpu_total: float = Field(..., gt=0, description="节点可分配 CPU 总量（核数）")

    @field_validator("cpu_total")
    @classmethod
    def _finite(cls, v: float) -> float:
        if v != v or v in (float("inf"), float("-inf")):
            raise ValueError("cpu_total 必须是有限正数")
        return float(v)


class PVSpec(BaseModel):
    name: str = Field(..., description="本地 PV 名称，例如 pv-node1-a")
    node: str = Field(..., description="PV 所在节点；本地盘只能被该节点上的 Pod 使用")
    capacity: float = Field(..., gt=0, description="PV 容量（GiB），必须不小于 PVC 申请容量")

    @field_validator("capacity")
    @classmethod
    def _finite(cls, v: float) -> float:
        if v != v or v in (float("inf"), float("-inf")):
            raise ValueError("capacity 必须是有限正数")
        return float(v)


class Inventory(BaseModel):
    nodes: List[NodeSpec] = Field(default_factory=list)
    pvs: List[PVSpec] = Field(default_factory=list)


class PodRequest(BaseModel):
    id: Optional[str] = Field(
        default=None,
        description="Pod 标识；留空时按提交顺序自动生成 pod-1 ... pod-n",
    )
    cpu: float = Field(..., gt=0, description="Pod 的 CPU 需求（核数）")
    pvc_capacity: float = Field(..., gt=0, description="该 Pod 唯一 PVC 的申请容量（GiB）")

    @field_validator("cpu", "pvc_capacity")
    @classmethod
    def _finite(cls, v: float) -> float:
        if v != v or v in (float("inf"), float("-inf")):
            raise ValueError("cpu / pvc_capacity 必须是有限正数")
        return float(v)


class BatchRequest(BaseModel):
    pods: List[PodRequest] = Field(
        ...,
        min_length=1,
        max_length=MAX_PODS_PER_BATCH,
        description=f"一批 MPI Pod，数量 1..{MAX_PODS_PER_BATCH}，整批一起调度、一起预留",
    )


class PodPlacement(BaseModel):
    pod_id: str
    node: str = Field(..., description="分配到的节点")
    pv: str = Field(..., description="分配到的本地 PV（与节点同节点）")
    cpu: float
    pvc_capacity: float


class BatchScheduledResponse(BaseModel):
    scheduled: bool = True
    batch_id: str
    placements: List[PodPlacement]
    """每个 Pod 落在哪个节点、占用哪块盘"""


class UnschedulableResponse(BaseModel):
    scheduled: bool = False
    reason: str
    detail: Optional[dict] = None


class NodeRemaining(BaseModel):
    name: str
    cpu_total: float
    cpu_available: float
    cpu_reserved: float


class PVRemaining(BaseModel):
    name: str
    node: str
    capacity: float
    available: bool = Field(..., description="true=未分配；false=已绑定给某个 Pod")
    bound_pod: Optional[str] = None
    bound_batch: Optional[str] = None


class RemainingResponse(BaseModel):
    """排程期间也可随时查询的已生效余量。"""

    nodes: List[NodeRemaining]
    pvs: List[PVRemaining]
    reserved_pod_count: int
    reservation_count: int


class ReservationView(BaseModel):
    batch_id: str
    placements: List[PodPlacement]
