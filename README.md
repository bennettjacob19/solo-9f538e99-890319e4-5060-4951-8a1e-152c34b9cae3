# Kubernetes 本地 PV 整批调度演练（FastAPI）

模拟 MPI 作业 **一批 Pod 同时启动** 的调度过程。每个 Pod 提交 CPU 需求和一个
PVC 的容量申请，调度器必须为整批 Pod 同时找到合法的节点与本地 PV；找不到则
整批失败、不占任何资源。

## 调度约束

1. **本地 PV 节点亲和**：每块盘注明所在节点，只能被该节点上的 Pod 使用；
2. **容量约束**：PV 容量必须 ≥ PVC 申请容量，一块盘只分给一个 Pod；
3. **CPU 不超用**：节点已预留 CPU + 本批分配量 ≤ 节点总量；
4. **整批事务**：一批 1～8 个 Pod 要么全部一次性预留成功，要么完全不预留；
5. **并发安全**：两批并发提交时，先提交的批次先占资源；后到的批次基于最新
   余量重新搜索，只要还存在别的合法排法就同样成功；
6. **排程期间可查询**：调度搜索在锁外基于快照进行，`GET /api/remaining`
   始终返回已生效余量，不会被长时间搜索阻塞。

## 目录结构

```
app/
  main.py        FastAPI 路由（/docs 交互文档）
  models.py      Pydantic 请求/响应模型与校验
  scheduler.py   纯函数回溯搜索（MRV + best-fit），不触碰全局状态
  store.py       线程安全存储：快照 + 版本号 CAS 提交 + 重试
  inventory.py   清单文件加载（路径由环境变量 SCHED_INVENTORY 指定）
data/inventory.json   可替换清单：节点空闲 CPU 与本地 PV（节点+容量）
tests/           20 个自测（算法、API、大小盘混排、并发改排）
scripts/smoke.py 对真实 HTTP 服务的端到端冒烟脚本
Dockerfile / docker-compose.yml / requirements.txt
```

## docker compose 启动

```bash
docker compose up --build
# 打开 http://localhost:8000/docs （根路径 / 会自动跳转）
```

清单通过卷挂载到容器内 `./data/inventory.json`。改文件后重启容器生效；
也可以不重启，直接调用 `POST /api/inventory` 热替换（会清空已有预留）。

## 本地直接运行（无 Docker 时）

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/uvicorn app.main:app --reload --port 8000
```

## 清单格式（data/inventory.json）

```json
{
  "nodes": [
    { "name": "node-1", "cpu_total": 8 }
  ],
  "pvs": [
    { "name": "pv-n1-large", "node": "node-1", "capacity": 200 }
  ]
}
```

- `nodes[].cpu_total`：节点可分配 CPU（核数）；
- `pvs[].node`：盘所在节点（必须引用已存在的节点，否则 422）；
- `pvs[].capacity`：盘容量（GiB）。

## 主要接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/schedule` | 提交一批 1～8 个 Pod；成功 200 返回每个 Pod 的节点和 PV，排不下 409 且不占资源 |
| GET  | `/api/remaining` | 查询已生效的节点 CPU 余量与每块 PV 的占用/绑定情况 |
| GET  | `/api/inventory` | 查看当前清单 |
| POST | `/api/inventory` | 整体替换清单（校验重名、PV 节点引用），清空预留 |
| GET  | `/api/reservations` | 查看全部已生效批次 |
| POST | `/api/reservations/release/{batch_id}` | 释放某批，归还 CPU 与盘 |
| POST | `/api/reset` | 清空全部预留，恢复清单初始余量 |

### /docs 提交示例

```json
{
  "pods": [
    { "id": "mpi-0", "cpu": 4, "pvc_capacity": 20 },
    { "id": "mpi-1", "cpu": 4, "pvc_capacity": 200 }
  ]
}
```

成功响应（每个 Pod 的节点和 PV）：

```json
{
  "scheduled": true,
  "batch_id": "batch-ab12...",
  "placements": [
    { "pod_id": "mpi-0", "node": "node-1", "pv": "pv-n1-small-a", "cpu": 4.0, "pvc_capacity": 20.0 },
    { "pod_id": "mpi-1", "node": "node-1", "pv": "pv-n1-large",     "cpu": 4.0, "pvc_capacity": 200.0 }
  ]
}
```

## 并发实现要点

- `store.snapshot()` 在锁内复制当前余量与版本号；
- 回溯搜索（`scheduler.schedule`）是纯函数，在 `asyncio.to_thread`
  线程中、锁外运行，不阻塞事件循环和余量查询；
- `store._commit()` 在锁内做版本号 CAS：版本未变才一次性扣减 CPU、
  绑定 PV 并递增版本；版本已变则抛出冲突，上层基于新快照重试（最多 5 次）；
- 搜索失败时不写入任何状态，保证"排不下就不占资源"。

## 自测

```bash
.venv/bin/python -m pytest tests/ -v          # 20 个用例
.venv/bin/python -I scripts/smoke.py          # 需先启动 uvicorn
```

覆盖：容量不足、CPU 超用、一 Pod 一盘、PV 节点亲和、8 Pod 上限、
大小盘混排（小 PVC 不抢大盘）、两批并发改排、并发抢同一资源时一胜一败且
败者无残留、排程中并发查询余量、清单替换校验、批次释放。
