"""FastAPI 入口：分区上传/下载、快照固定/释放、回收触发与状态查看。"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, File, HTTPException, Path as PathParam, Query, Request, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from . import storage
from .config import settings
from .service import (
    ChecksumMismatch,
    CsvValidationError,
    MissingBlobFile,
    MoistureService,
    NotFound,
    ServiceError,
    SnapshotReleased,
    UploadConflict,
    UploadNotFound,
)
from .storage import UploadTooLarge


class ResumableCreateRequest(BaseModel):
    """创建续传上传的请求体。"""

    partition_id: str = Field(
        ..., description="目标日期分区标识，如 2026-11-15"
    )
    total_bytes: int = Field(
        ..., gt=0, description="整份 CSV 的总字节数（受 MAX_UPLOAD_BYTES 限制）"
    )
    sha256: str = Field(
        ..., min_length=64, max_length=64, description="整份字节的 SHA-256（十六进制）"
    )

service = MoistureService(settings)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # 建表 + 崩溃恢复：清理临时文件、对账数据卷、继续未完成的回收
    service.startup()
    yield


app = FastAPI(
    title="土壤湿度分区内容寻址存储 API",
    version="1.1.0",
    description=(
        "按日期分区上传 CSV（站点、湿度），整份替换当前分区；"
        "相同字节按 SHA-256 共用数据卷文件；支持快照固定/释放与安全回收；"
        "并提供断点续传上传：按偏移追加、断线/重启后从已确认偏移续传，"
        "到齐后核验摘要并一次性发布。"
    ),
    lifespan=lifespan,
)


@app.exception_handler(ServiceError)
def _service_error_handler(_request, exc: ServiceError):
    status = 400
    if isinstance(exc, NotFound):
        status = 404
    elif isinstance(exc, UploadNotFound):
        status = 404
    elif isinstance(exc, SnapshotReleased):
        status = 409
    elif isinstance(exc, (UploadConflict, ChecksumMismatch)):
        status = 409
    elif isinstance(exc, MissingBlobFile):
        status = 500
    return JSONResponse(status_code=status, content={"error": str(exc)})


@app.exception_handler(UploadTooLarge)
def _too_large_handler(_request, exc: UploadTooLarge):
    return JSONResponse(status_code=413, content={"error": str(exc)})


@app.get("/health")
def health():
    return {"status": "ok", "data_dir": str(service.cfg.data_dir)}


# --------------------------------------------------------------------------- #
# 分区
# --------------------------------------------------------------------------- #
@app.post("/partitions/{partition_id}", status_code=200)
async def upload_partition(
    partition_id: str = PathParam(..., description="日期分区标识，如 2026-11-15"),
    file: UploadFile = File(..., description="CSV 文件，含站点、湿度两列"),
):
    """上传 CSV，整份替换该日期分区的当前版本。"""
    try:
        result = service.upload_partition(partition_id, file.file)
    except CsvValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except UploadTooLarge as exc:
        raise HTTPException(status_code=413, detail=str(exc))
    return result


@app.get("/partitions")
def list_partitions():
    return {"partitions": service.list_partitions()}


@app.get("/partitions/{partition_id}")
def get_partition(partition_id: str):
    try:
        return service.get_partition(partition_id)
    except NotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@app.get("/partitions/{partition_id}/download")
def download_partition(
    partition_id: str,
    snapshot_id: Optional[int] = Query(
        default=None, description="快照号；提供时读取该快照固定的版本，释放后拒绝读取"
    ),
):
    """下载分区 CSV。默认读当前分区；带 snapshot_id 读快照固定版本。

    下载开始即取得租约，流式传输期间不持元数据锁，慢下载不阻塞
    上传/释放/其他文件回收，且本文件在下载结束前不会被回收。
    """
    try:
        sha, path, cleanup = service.open_partition_download(partition_id, snapshot_id)
    except NotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except SnapshotReleased as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except MissingBlobFile as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    stream = service.stream_blob(path, cleanup)
    filename = f"{partition_id}.csv"
    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "X-Blob-SHA256": sha,
    }
    if snapshot_id is not None:
        headers["X-Snapshot-ID"] = str(snapshot_id)
    return StreamingResponse(stream, media_type="text/csv", headers=headers)


# --------------------------------------------------------------------------- #
# 断点续传上传
# --------------------------------------------------------------------------- #
@app.post("/resumable/uploads", status_code=201)
def create_resumable_upload(payload: ResumableCreateRequest):
    """创建续传上传：指定日期、总字节数和整份 SHA-256，返回上传号与已确认偏移。"""
    try:
        return service.create_resumable_upload(
            payload.partition_id, payload.total_bytes, payload.sha256
        )
    except UploadTooLarge as exc:
        raise HTTPException(status_code=413, detail=str(exc))


@app.put("/resumable/uploads/{upload_id}/content")
async def append_resumable_content(
    upload_id: str,
    request: Request,
    offset: int = Query(..., ge=0, description="本块首字节在整份文件中的偏移"),
):
    """按偏移追加原始字节（application/octet-stream 请求体）。

    - 已确认区间的同字节重传幂等，不重复追加；
    - 跳写（offset 越过已确认偏移）或异文重传返回 409 且不改进度；
    - 超过创建时声明的总字节数返回 409，超过全局大小限制返回 413。
    """
    body = await _read_request_body(request)
    try:
        return service.append_resumable_upload(upload_id, offset, body)
    except UploadNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except UploadConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except UploadTooLarge as exc:
        raise HTTPException(status_code=413, detail=str(exc))


@app.get("/resumable/uploads")
def list_resumable_uploads(
    status: Optional[str] = Query(default=None, description="按状态过滤：open/completed"),
):
    """列出续传上传及其确认偏移（可选状态过滤）。"""
    return {"uploads": service.list_resumable_uploads(status)}


@app.get("/resumable/uploads/{upload_id}")
def get_resumable_upload(upload_id: str):
    """查进度：已确认偏移、剩余字节、状态；完成后附带首次完成结果。"""
    try:
        return service.get_resumable_upload(upload_id)
    except UploadNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@app.post("/resumable/uploads/{upload_id}/complete")
def complete_resumable_upload(upload_id: str):
    """到齐后核验 SHA-256 与 CSV 规则并一次性发布分区、行索引和完成结果。

    失败不改原分区；同一上传号重复完成（含重启后）幂等返回原结果，
    不会覆盖后来已经成功的上传。
    """
    try:
        return service.complete_resumable_upload(upload_id)
    except UploadNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except UploadConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except ChecksumMismatch as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except CsvValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except MissingBlobFile as exc:
        raise HTTPException(status_code=500, detail=str(exc))


async def _read_request_body(request: Request) -> bytes:
    """读取 octet-stream 请求体；空体也允许（用于零字节探测/幂等重传）。"""
    chunks = []
    async for chunk in request.stream():
        chunks.append(chunk)
    return b"".join(chunks)


# --------------------------------------------------------------------------- #
# 快照
# --------------------------------------------------------------------------- #
@app.post("/snapshots", status_code=201)
def create_snapshot():
    """固定当前全部已提交分区，返回快照号（作业用它读取不变视图）。"""
    return service.create_snapshot()


@app.get("/snapshots")
def list_snapshots():
    return {"snapshots": service.list_snapshots()}


@app.post("/snapshots/{snapshot_id}/release")
def release_snapshot(snapshot_id: int):
    """释放快照；释放后拒绝新读取，独占的 blob 进入回收流程。"""
    try:
        return service.release_snapshot(snapshot_id)
    except NotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except SnapshotReleased as exc:
        raise HTTPException(status_code=409, detail=str(exc))


# --------------------------------------------------------------------------- #
# 回收
# --------------------------------------------------------------------------- #
@app.post("/gc")
def run_gc():
    """立即执行一轮回收（服务启动时也会自动执行）。"""
    return service.run_gc()


@app.get("/gc/status")
def gc_status():
    return {
        "queue_remaining": service.gc_queue_size(),
        "active_downloads": service.active_reader_count(),
        "blob_files_on_disk": len(storage.list_blob_files(service.cfg)),
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=os.environ.get("HOST", "0.0.0.0"),
        port=int(os.environ.get("PORT", "8000")),
        workers=1,
    )
