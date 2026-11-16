"""断点续传上传 API：创建、按偏移追加、重传/跳写/异文拒绝、重启恢复、
摘要核验、过期拒绝、重复完成幂等，以及与分区/快照/回收的共存。"""
import hashlib
import threading

import httpx

from conftest import make_csv, upload


def _create(client, partition_id: str, content: bytes):
    return client.post(
        "/resumable/uploads",
        json={
            "partition_id": partition_id,
            "total_bytes": len(content),
            "sha256": hashlib.sha256(content).hexdigest(),
        },
    )


def _create_raw(client, partition_id: str, total_bytes: int, sha: str):
    return client.post(
        "/resumable/uploads",
        json={
            "partition_id": partition_id,
            "total_bytes": total_bytes,
            "sha256": sha,
        },
    )


def _append(client, upload_id: str, offset: int, chunk: bytes):
    return client.put(
        f"/resumable/uploads/{upload_id}/content",
        params={"offset": offset},
        content=chunk,
        headers={"Content-Type": "application/octet-stream"},
    )


def _chunks(content: bytes, size: int):
    return [content[i:i + size] for i in range(0, len(content), size)]


def _upload_in_chunks(client, upload_id: str, content: bytes, size: int):
    offset = 0
    responses = []
    for chunk in _chunks(content, size):
        r = _append(client, upload_id, offset, chunk)
        assert r.status_code == 200, r.text
        offset += len(chunk)
        responses.append(r)
    return responses


def test_create_returns_upload_id_and_zero_offset(client):
    csv1 = make_csv([("S1", 0.1), ("S2", 0.2)])
    r = _create(client, "2026-11-15", csv1)
    assert r.status_code == 201, r.text
    body = r.json()
    assert len(body["upload_id"]) == 32
    assert body["confirmed_bytes"] == 0
    assert body["total_bytes"] == len(csv1)
    assert body["status"] == "open"
    assert body["remaining_bytes"] == len(csv1)


def test_chunked_upload_then_complete_publishes_partition_and_rows(client):
    csv1 = make_csv([(f"S{i}", i / 10.0) for i in range(20)])
    upload_id = _create(client, "day", csv1).json()["upload_id"]

    _upload_in_chunks(client, upload_id, csv1, 7)  # 故意制造非整分块

    progress = client.get(f"/resumable/uploads/{upload_id}").json()
    assert progress["confirmed_bytes"] == len(csv1)
    assert progress["remaining_bytes"] == 0
    assert progress["status"] == "open"  # 到齐但未完成前仍是 open

    # 未完成文件不进入分区
    assert client.get("/partitions/day").status_code == 404

    r = client.post(f"/resumable/uploads/{upload_id}/complete")
    assert r.status_code == 200, r.text
    result = r.json()
    assert result["sha256"] == hashlib.sha256(csv1).hexdigest()
    assert result["row_count"] == 20
    assert result["replaced"] is False

    # 分区与行索引一次性可见
    assert client.get("/partitions/day/download").content == csv1
    part = client.get("/partitions/day").json()
    assert part["row_count"] == 20 and part["current_blob"] == result["sha256"]

    # 完成后状态与暂存文件清理
    progress = client.get(f"/resumable/uploads/{upload_id}").json()
    assert progress["status"] == "completed"
    assert progress["result"]["sha256"] == result["sha256"]


def test_same_byte_retransmit_is_idempotent_and_does_not_duplicate(client, data_dir):
    csv1 = make_csv([("S1", 0.1), ("S2", 0.2), ("S3", 0.3)])
    upload_id = _create(client, "day", csv1).json()["upload_id"]

    first = csv1[:10]
    assert _append(client, upload_id, 0, first).json()["confirmed_bytes"] == 10

    # 已确认区间整段同字节重传：进度不变、不重复追加
    r = _append(client, upload_id, 0, first)
    assert r.status_code == 200
    assert r.json()["confirmed_bytes"] == 10

    # 部分重叠重传 + 新字节：重叠区比对一致，仅新字节推进
    overlap = csv1[5:15]  # [5,10) 已确认重传，[10,15) 为新字节
    r = _append(client, upload_id, 5, overlap)
    assert r.status_code == 200
    assert r.json()["confirmed_bytes"] == 15

    # 继续传完并完成，最终字节与原文逐字节一致
    r = _append(client, upload_id, 15, csv1[15:])
    assert r.status_code == 200 and r.json()["confirmed_bytes"] == len(csv1)
    assert client.post(f"/resumable/uploads/{upload_id}/complete").status_code == 200
    assert client.get("/partitions/day/download").content == csv1

    parts = list((data_dir / "uploads").glob("*.part"))
    assert parts == []  # 完成后暂存文件被清理


def test_skip_write_is_rejected_without_progress_change(client):
    csv1 = make_csv([("S1", 0.1), ("S2", 0.2)])
    upload_id = _create(client, "day", csv1).json()["upload_id"]
    _append(client, upload_id, 0, csv1[:10])

    r = _append(client, upload_id, 25, csv1[25:30])  # 越过已确认偏移 10
    assert r.status_code == 409
    progress = client.get(f"/resumable/uploads/{upload_id}").json()
    assert progress["confirmed_bytes"] == 10  # 进度未变

    # 正确偏移仍可继续
    assert _append(client, upload_id, 10, csv1[10:]).status_code == 200


def test_conflicting_retransmit_is_rejected_without_progress_change(client):
    csv1 = make_csv([("S1", 0.1), ("S2", 0.2)])
    upload_id = _create(client, "day", csv1).json()["upload_id"]
    _append(client, upload_id, 0, csv1[:12])

    evil = bytearray(csv1[:12])
    evil[3] = ord("X")  # 与已确认字节不同
    r = _append(client, upload_id, 0, bytes(evil))
    assert r.status_code == 409
    progress = client.get(f"/resumable/uploads/{upload_id}").json()
    assert progress["confirmed_bytes"] == 12

    # 同字节重传仍然被接受，且内容未被污染
    assert _append(client, upload_id, 0, csv1[:12]).status_code == 200
    assert _append(client, upload_id, 12, csv1[12:]).status_code == 200
    assert client.post(f"/resumable/uploads/{upload_id}/complete").status_code == 200
    assert client.get("/partitions/day/download").content == csv1


def test_complete_before_all_bytes_arrives_is_rejected(client):
    csv1 = make_csv([("S1", 0.1)])
    upload_id = _create(client, "day", csv1).json()["upload_id"]
    _append(client, upload_id, 0, csv1[:5])
    r = client.post(f"/resumable/uploads/{upload_id}/complete")
    assert r.status_code == 409
    assert client.get("/partitions/day").status_code == 404  # 原分区（不存在）未受影响


def test_checksum_mismatch_rejects_and_keeps_original_partition(client):
    good = make_csv([("S1", 0.1)])
    upload(client, "day", good)
    other = make_csv([("S1", 0.9)])
    # 声明的是 good 的摘要，实际到齐的是 other 的字节
    upload_id = _create_raw(
        client, "day", len(other), hashlib.sha256(good).hexdigest()
    ).json()["upload_id"]
    _upload_in_chunks(client, upload_id, other, 5)
    r = client.post(f"/resumable/uploads/{upload_id}/complete")
    assert r.status_code == 409
    # 原分区纹丝不动，上传仍可重新处理（状态仍 open）
    assert client.get("/partitions/day/download").content == good
    progress = client.get(f"/resumable/uploads/{upload_id}").json()
    assert progress["status"] == "open"


def test_invalid_csv_rejects_and_keeps_original_partition(client):
    good = make_csv([("S1", 0.1)])
    upload(client, "day", good)
    bad = b"station,value\nS1,0.5\n"  # 缺湿度列
    upload_id = _create(client, "day", bad).json()["upload_id"]
    _upload_in_chunks(client, upload_id, bad, 4)
    r = client.post(f"/resumable/uploads/{upload_id}/complete")
    assert r.status_code == 422
    assert client.get("/partitions/day/download").content == good
    assert client.get(f"/resumable/uploads/{upload_id}").json()["status"] == "open"


def test_restart_keeps_confirmed_bytes_and_drops_unconfirmed_tail(client, data_dir, svc):
    csv1 = make_csv([(f"S{i:04d}", i / 7.0) for i in range(50)])
    upload_id = _create(client, "day", csv1).json()["upload_id"]
    _upload_in_chunks(client, upload_id, csv1[:40], 10)  # 确认 40 字节

    # 模拟崩溃：绕过 API 直接往暂存文件写“未确认尾部”
    from app import storage

    part = storage.partial_path(upload_id, svc.cfg)
    with open(part, "ab") as fh:
        fh.write(b"UNCONFIRMED-TAIL-BYTES")

    # 重启：新服务实例对同一数据目录做恢复
    from app.config import Settings
    from app.service import MoistureService

    restarted = MoistureService(Settings(data_dir=data_dir))
    restarted.startup()

    progress = restarted.get_resumable_upload(upload_id)
    assert progress["confirmed_bytes"] == 40  # 未确认尾部不计入进度
    assert part.read_bytes() == csv1[:40]     # 尾部已被截断

    # 接着传：从确认偏移继续即可完成
    r = _append(client, upload_id, 40, csv1[40:])
    assert r.status_code == 200 and r.json()["confirmed_bytes"] == len(csv1)
    assert client.post(f"/resumable/uploads/{upload_id}/complete").status_code == 200
    assert client.get("/partitions/day/download").content == csv1


def test_restart_then_repeated_complete_returns_original_result(client, data_dir):
    csv1 = make_csv([("S1", 0.1), ("S2", 0.2)])
    upload_id = _create(client, "day", csv1).json()["upload_id"]
    _upload_in_chunks(client, upload_id, csv1, 8)
    first = client.post(f"/resumable/uploads/{upload_id}/complete").json()

    from app.config import Settings
    from app.service import MoistureService

    restarted = MoistureService(Settings(data_dir=data_dir))
    restarted.startup()

    # 重启后重复完成：返回缓存的原结果
    again = client.post(f"/resumable/uploads/{upload_id}/complete")
    assert again.status_code == 200
    assert again.json() == first
    # 追加字节被拒：已完成的上传不可再写
    assert _append(client, upload_id, 0, b"x").status_code == 409


def test_stale_upload_rejected_after_partition_replaced_even_with_same_bytes(client):
    csv1 = make_csv([("S1", 0.1)])
    csv2 = make_csv([("S1", 0.2)])

    # 1) 为 day 创建续传 A，但先不完成
    upload_a = _create(client, "day", csv1).json()["upload_id"]
    _upload_in_chunks(client, upload_a, csv1, 5)

    # 2) 该日期被普通上传成功替换
    upload(client, "day", csv2)

    # 3) A 完成被拒（分区已被成功替换过）
    r = client.post(f"/resumable/uploads/{upload_a}/complete")
    assert r.status_code == 409
    assert client.get("/partitions/day/download").content == csv2

    # 4) 即使把内容“改回”与 A 完全相同，A 依然被拒
    upload(client, "day", csv1)
    r = client.post(f"/resumable/uploads/{upload_a}/complete")
    assert r.status_code == 409

    # 5) 替换之后新建的续传可以正常完成
    upload_b = _create(client, "day", csv2).json()["upload_id"]
    _upload_in_chunks(client, upload_b, csv2, 5)
    assert client.post(f"/resumable/uploads/{upload_b}/complete").status_code == 200
    assert client.get("/partitions/day/download").content == csv2


def test_completed_upload_never_overwrites_later_upload(client):
    csv1 = make_csv([("S1", 0.1)])
    csv2 = make_csv([("S1", 0.2)])
    # A 先完成，发布 csv1
    upload_a = _create(client, "day", csv1).json()["upload_id"]
    _upload_in_chunks(client, upload_a, csv1, 5)
    client.post(f"/resumable/uploads/{upload_a}/complete")
    # B 之后完成，发布 csv2
    upload_b = _create(client, "day", csv2).json()["upload_id"]
    _upload_in_chunks(client, upload_b, csv2, 5)
    client.post(f"/resumable/uploads/{upload_b}/complete")
    assert client.get("/partitions/day/download").content == csv2

    # 对 A 重复完成只返回原结果，当前分区仍是 csv2
    r = client.post(f"/resumable/uploads/{upload_a}/complete")
    assert r.status_code == 200
    assert r.json()["sha256"] == hashlib.sha256(csv1).hexdigest()
    assert client.get("/partitions/day/download").content == csv2


def test_other_partitions_snapshots_and_gc_unaffected(client):
    csv_a = make_csv([("A", 0.1)])
    csv_b = make_csv([("B", 0.2)])
    # 一个未完成的续传占用 day
    pending = _create(client, "day", csv_a).json()["upload_id"]
    _append(client, pending, 0, csv_a[:4])

    # 其他日期的普通上传、快照、回收照常工作
    upload(client, "other", csv_b)
    snap_id = client.post("/snapshots").json()["snapshot_id"]
    assert client.get(
        "/partitions/other/download", params={"snapshot_id": snap_id}
    ).content == csv_b
    assert client.post(f"/snapshots/{snap_id}/release").status_code == 200
    gc = client.post("/gc").json()
    assert gc["queue_remaining"] == 0

    # 未完成续传不出现在任何分区/快照中
    assert client.get("/partitions/day").status_code == 404
    listed = client.get("/resumable/uploads", params={"status": "open"}).json()
    assert [u["upload_id"] for u in listed["uploads"]] == [pending]


def test_size_limit_enforced_on_create_and_append(client):
    from app.config import Settings
    from app.service import MoistureService
    import app.main as main

    # 用很小的全局上限替换服务，验证续传沿用上传大小限制
    limited = MoistureService(
        Settings(data_dir=main.service.cfg.data_dir, max_upload_bytes=32)
    )
    main.service = limited
    csv1 = make_csv([(f"S{i}", i / 3.0) for i in range(10)])  # 明显大于 32 字节

    # 创建时声明超过全局上限 -> 413
    r = client.post(
        "/resumable/uploads",
        json={
            "partition_id": "day",
            "total_bytes": len(csv1),
            "sha256": hashlib.sha256(csv1).hexdigest(),
        },
    )
    assert r.status_code == 413

    # 声明更大的总字节数（创建时受 32 上限拦截）；为单独验证追加期的
    # 全局上限，用一个上限内的声明、分块越过 32 字节 -> 413 且进度不变
    upload_id = client.post(
        "/resumable/uploads",
        json={
            "partition_id": "day2",
            "total_bytes": 32,
            "sha256": hashlib.sha256(csv1[:32]).hexdigest(),
        },
    ).json()["upload_id"]
    assert _append(client, upload_id, 0, csv1[:20]).status_code == 200
    r = _append(client, upload_id, 20, csv1[20:40])  # 累计将达 40 > 32 上限
    assert r.status_code == 413
    assert client.get(f"/resumable/uploads/{upload_id}").json()["confirmed_bytes"] == 20


def test_appending_beyond_declared_total_is_rejected(client):
    csv1 = make_csv([("S1", 0.1), ("S2", 0.2)])
    upload_id = _create(client, "day", csv1).json()["upload_id"]
    # 声明总长 len(csv1)，尝试写入更多字节
    r = _append(client, upload_id, 0, csv1 + b"extra-bytes")
    assert r.status_code == 409
    assert client.get(f"/resumable/uploads/{upload_id}").json()["confirmed_bytes"] == 0


def test_unknown_upload_and_bad_requests(client):
    assert client.get("/resumable/uploads/nope").status_code == 404
    assert _append(client, "nope", 0, b"x").status_code == 404
    assert client.post("/resumable/uploads/nope/complete").status_code == 404

    r = client.post(
        "/resumable/uploads",
        json={"partition_id": "day", "total_bytes": 0, "sha256": "a" * 64},
    )
    assert r.status_code == 422
    r = client.post(
        "/resumable/uploads",
        json={"partition_id": "day", "total_bytes": 10, "sha256": "xyz"},
    )
    assert r.status_code == 422


def test_resumable_blob_joins_content_addressed_store_and_gc(client, data_dir):
    csv1 = make_csv([("S1", 0.1), ("S2", 0.2)])
    upload_id = _create(client, "day", csv1).json()["upload_id"]
    _upload_in_chunks(client, upload_id, csv1, 6)
    result = client.post(f"/resumable/uploads/{upload_id}/complete").json()

    # 发布后进入内容寻址数据卷，可被普通分区/快照引用与回收
    from app import storage
    import app.main as main

    blob_file = storage.blob_path(result["sha256"], main.service.cfg)
    assert blob_file.exists() and blob_file.read_bytes() == csv1

    # 替换分区且无快照引用 -> 旧 blob 可回收
    upload(client, "day", make_csv([("S1", 0.9)]))
    gc = client.post("/gc").json()
    assert result["sha256"] in gc["deleted"]


def test_concurrent_appends_serialized_and_content_intact(live_server):
    """真实 uvicorn 下多线程并发追加：单上传串行锁保证字节有序、无重复。"""
    http, _svc, _base = live_server
    csv1 = make_csv([(f"S{i:06d}", round(i / 13.0, 5)) for i in range(800)])
    digest = hashlib.sha256(csv1).hexdigest()
    uid = http.post(
        "/resumable/uploads",
        json={
            "partition_id": "day",
            "total_bytes": len(csv1),
            "sha256": digest,
        },
    ).json()["upload_id"]

    # 把文件切成小块，用固定数量工作线程并发乱序发送，每块带正确偏移
    size = 53
    pieces = [(off, csv1[off:off + size])
              for off in range(0, len(csv1), size)]
    errors = []
    n_workers = 8
    next_piece = 0
    state_lock = threading.Lock()
    start = threading.Barrier(n_workers)

    def worker():
        nonlocal next_piece
        start.wait()  # 所有工作线程同时开抢，制造真实交错
        while True:
            with state_lock:
                if next_piece >= len(pieces):
                    return
                seq = next_piece
                next_piece += 1
            off, chunk = pieces[seq]
            try:
                r = http.put(
                    f"/resumable/uploads/{uid}/content",
                    params={"offset": off},
                    content=chunk,
                    headers={"Content-Type": "application/octet-stream"},
                )
                # 乱序到达时，越过确认偏移的块会被 409 拒绝（跳写），这是预期；
                # 客户端按确认偏移重试即可，最终必须能拼出原文件。
                if r.status_code not in (200, 409):
                    with state_lock:
                        errors.append(f"unexpected {r.status_code}: {r.text}")
            except httpx.TransportError as exc:  # pragma: no cover
                with state_lock:
                    errors.append(repr(exc))

    threads = [threading.Thread(target=worker) for _ in range(n_workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
        assert not t.is_alive()
    assert errors == []

    # 按确认偏移重传所有未成功的块，直到到齐
    progress = http.get(f"/resumable/uploads/{uid}").json()
    confirmed = progress["confirmed_bytes"]
    while confirmed < len(csv1):
        chunk = csv1[confirmed:confirmed + size]
        r = http.put(
            f"/resumable/uploads/{uid}/content",
            params={"offset": confirmed},
            content=chunk,
            headers={"Content-Type": "application/octet-stream"},
        )
        assert r.status_code == 200, r.text
        confirmed = r.json()["confirmed_bytes"]

    r = http.post(f"/resumable/uploads/{uid}/complete")
    assert r.status_code == 200, r.text
    assert r.json()["sha256"] == digest
    assert http.get("/partitions/day/download").content == csv1


def test_missing_partial_file_resets_to_zero_and_allows_restart(client, svc):
    """暂存文件被外部删除：进度回退 0，客户端可从头重传并完成。"""
    from app import storage

    csv1 = make_csv([("S1", 0.1), ("S2", 0.2)])
    uid = _create(client, "day", csv1).json()["upload_id"]
    _append(client, uid, 0, csv1[:15])
    assert client.get(f"/resumable/uploads/{uid}").json()["confirmed_bytes"] == 15

    storage.discard_partial(uid, svc.cfg)

    # 下一次追加得到明确的 409，且进度已回退到 0
    r = _append(client, uid, 0, csv1[:10])
    assert r.status_code == 409
    assert client.get(f"/resumable/uploads/{uid}").json()["confirmed_bytes"] == 0

    # 从头重传并完成
    _upload_in_chunks(client, uid, csv1, 8)
    assert client.post(f"/resumable/uploads/{uid}/complete").status_code == 200
    assert client.get("/partitions/day/download").content == csv1


def test_overlap_retransmit_reaching_exact_total_is_accepted(client):
    """重叠重传块带新字节恰好到总长：只有新字节计数，不得误判超总长。"""
    csv1 = make_csv([("S1", 0.1), ("S2", 0.2), ("S3", 0.3), ("S4", 0.4)])
    uid = _create(client, "day", csv1).json()["upload_id"]
    cut = 12
    assert _append(client, uid, 0, csv1[:cut]).json()["confirmed_bytes"] == cut
    # 从 offset 8 重传：[8,12) 为已确认重叠，其余为新字节，最终恰好到总长
    r = _append(client, uid, 8, csv1[8:])
    assert r.status_code == 200, r.text
    assert r.json()["confirmed_bytes"] == len(csv1)
    assert client.post(f"/resumable/uploads/{uid}/complete").status_code == 200
    assert client.get("/partitions/day/download").content == csv1


def test_restart_sweeps_orphan_partial_files(client, data_dir, svc):
    """重启时：open 上传的暂存保留；已完成/无登记的孤儿 .part 被清扫。"""
    from app import storage
    from app.config import Settings
    from app.service import MoistureService

    csv1 = make_csv([("S1", 0.1)])
    # open 上传（保留）
    uid_open = _create(client, "day1", csv1).json()["upload_id"]
    _append(client, uid_open, 0, csv1[:6])
    open_part = storage.partial_path(uid_open, svc.cfg)
    assert open_part.exists()

    # 已完成上传，模拟其暂存文件清理失败而残留
    uid_done = _create(client, "day2", csv1).json()["upload_id"]
    _upload_in_chunks(client, uid_done, csv1, 5)
    client.post(f"/resumable/uploads/{uid_done}/complete")
    done_part = storage.partial_path(uid_done, svc.cfg)
    assert not done_part.exists()
    done_part.write_bytes(b"leftover-after-complete")

    # 完全无登记的孤儿文件
    ghost = storage.partial_path("deadbeef" * 4, svc.cfg)
    ghost.parent.mkdir(parents=True, exist_ok=True)
    ghost.write_bytes(b"no-db-row")

    MoistureService(Settings(data_dir=data_dir)).startup()

    assert open_part.exists()  # open 的保留且进度不变
    assert svc.get_resumable_upload(uid_open)["confirmed_bytes"] == 6
    assert not done_part.exists()  # 已完成残留被清扫
    assert not ghost.exists()      # 无登记孤儿被清扫
