"""端到端冒烟：对运行中的 uvicorn 发真实 HTTP 请求。"""
import concurrent.futures as cf
import json
import urllib.request

BASE = "http://127.0.0.1:8000"


def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


# 0. 干净起点
call("POST", "/api/reset")

# 1. 大小盘混排：1 个 150G 大 PVC + 2 个 10G 小 PVC
st, r = call("POST", "/api/schedule", {"pods": [
    {"id": "big", "cpu": 2, "pvc_capacity": 150},
    {"id": "sm1", "cpu": 1, "pvc_capacity": 10},
    {"id": "sm2", "cpu": 1, "pvc_capacity": 10},
]})
print("[1] mixed batch:", st)
for p in r["placements"]:
    print("   ", p["pod_id"], "->", p["node"], "/", p["pv"])
assert st == 200
by = {p["pod_id"]: p for p in r["placements"]}
assert by["big"]["pv"] == "pv-n1-large"
assert "large" not in by["sm1"]["pv"] and "large" not in by["sm2"]["pv"]

# 2. 排不下的批次：要 3 块 >=150G 的盘，只剩 1 块大盘
st_before, before = call("GET", "/api/remaining")
st, r = call("POST", "/api/schedule", {"pods": [
    {"id": "x1", "cpu": 1, "pvc_capacity": 150},
    {"id": "x2", "cpu": 1, "pvc_capacity": 150},
    {"id": "x3", "cpu": 1, "pvc_capacity": 150},
]})
print("[2] unschedulable:", st, "->", r["detail"]["reason"])
assert st == 409
_, after = call("GET", "/api/remaining")
assert before == after, "失败批次不得改变任何余量"
print("    余量前后一致，未占用任何资源 OK")

# 3. 两批并发提交（真实线程并发）
call("POST", "/api/reset")
batch = {"pods": [
    {"id": "A1", "cpu": 4, "pvc_capacity": 20},
    {"id": "A2", "cpu": 4, "pvc_capacity": 20},
]}
batch2 = {"pods": [
    {"id": "B1", "cpu": 4, "pvc_capacity": 20},
    {"id": "B2", "cpu": 4, "pvc_capacity": 20},
]}
with cf.ThreadPoolExecutor(max_workers=2) as ex:
    futs = [
        ex.submit(call, "POST", "/api/schedule", batch),
        ex.submit(call, "POST", "/api/schedule", batch2),
    ]
    results = [f.result() for f in futs]
print("[3] concurrent batches:", [s for s, _ in results])
assert all(s == 200 for s, _ in results)
disks = [p["pv"] for _, body in results for p in body["placements"]]
assert len(disks) == len(set(disks)), "两批不得共用盘"
print("    两批 PV 互不相交:", sorted(disks))

# 4. 排程期间查询余量：与一个大调度请求并发，查询始终 200
def slow_schedule():
    return call("POST", "/api/schedule", {"pods": [
        {"id": f"c{i}", "cpu": 1, "pvc_capacity": 10} for i in range(8)
    ]})

with cf.ThreadPoolExecutor(max_workers=2) as ex:
    f_sched = ex.submit(slow_schedule)
    codes = []
    for _ in range(10):
        s, _ = call("GET", "/api/remaining")
        codes.append(s)
    sched_code, sched_body = f_sched.result()
print("[4] concurrent reads during scheduling:", set(codes),
      "| schedule result:", sched_code)
assert set(codes) == {200}

# 5. 最终余量核对：CPU 不超用、盘绑定唯一
_, rem = call("GET", "/api/remaining")
for n in rem["nodes"]:
    assert n["cpu_reserved"] <= n["cpu_total"] + 1e-9, n
used = [p for p in rem["pvs"] if not p["available"]]
assert len(used) == len({p["name"] for p in used})
print("[5] nodes:")
for n in rem["nodes"]:
    print("   ", n["name"], "reserved", n["cpu_reserved"], "/", n["cpu_total"])
print("    bound PVs:", [(p["name"], p["bound_pod"]) for p in used])
print("ALL SMOKE CHECKS PASSED")
