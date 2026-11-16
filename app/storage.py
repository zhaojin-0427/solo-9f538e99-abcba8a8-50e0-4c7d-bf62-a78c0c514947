"""数据卷文件操作：内容寻址、原子落盘、安全读取与删除。"""
from __future__ import annotations

import hashlib
import os
import tempfile
import uuid
from pathlib import Path
from typing import BinaryIO, Tuple

from .config import Settings, settings as default_settings


class UploadTooLarge(Exception):
    """上传字节数超过配置上限。"""


class PartialUploadError(Exception):
    """续传分片写入错误基类。"""


class PartialOffsetError(PartialUploadError):
    """追加偏移越过已确认偏移（跳写），拒绝且不改进度。"""


class PartialConflictError(PartialUploadError):
    """已确认区间收到不同字节（异文重传），拒绝且不改进度。"""


class PartialOverrunError(PartialUploadError):
    """追加结果超过创建时声明的总字节数。"""


def blob_path(sha256: str, cfg: Settings = default_settings) -> Path:
    # 两级目录分片，避免单目录文件过多
    return cfg.blobs_dir / sha256[:2] / sha256[2:4] / sha256


def stream_to_tmp(
    source: BinaryIO, cfg: Settings = default_settings, chunk_size: int = 1024 * 1024
) -> Tuple[str, int, Path]:
    """流式读取上传内容到临时文件，同时计算 SHA-256。

    成功返回 (sha256, size, tmp_path)；超限抛出 :class:`UploadTooLarge`，
    临时文件由调用方在失败时清理（这里也会尽力清理）。
    """
    digest = hashlib.sha256()
    total = 0
    fd, tmp_name = tempfile.mkstemp(prefix="upload-", dir=cfg.tmp_dir)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as out:
            while True:
                chunk = source.read(chunk_size)
                if not chunk:
                    break
                total += len(chunk)
                if total > cfg.max_upload_bytes:
                    raise UploadTooLarge(f"upload exceeds {cfg.max_upload_bytes} bytes")
                digest.update(chunk)
                out.write(chunk)
            out.flush()
            os.fsync(out.fileno())
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
    return digest.hexdigest(), total, tmp_path


def publish_blob(tmp_path: Path, sha256: str, cfg: Settings = default_settings) -> Path:
    """把临时文件原子地发布为数据卷文件。

    同内容（同 SHA-256）共用同一文件：目标已存在时直接丢弃临时文件。
    os.replace 是同文件系统内的原子操作，任何时刻读者要么看不到该文件，
    要么看到完整文件。
    """
    target = blob_path(sha256, cfg)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        tmp_path.unlink(missing_ok=True)
        return target
    # 先在同目录放一个唯一名，再 replace，避免并发发布时互相干扰
    staged = target.parent / f".staging-{uuid.uuid4().hex}"
    os.replace(tmp_path, staged)
    try:
        os.replace(staged, target)
    except FileExistsError:
        staged.unlink(missing_ok=True)
    return target


def discard_tmp(tmp_path: Path) -> None:
    tmp_path.unlink(missing_ok=True)


def open_blob(sha256: str, cfg: Settings = default_settings) -> BinaryIO:
    """以只读方式打开数据卷文件（调用方负责关闭）。"""
    return open(blob_path(sha256, cfg), "rb")


def remove_blob_file(sha256: str, cfg: Settings = default_settings) -> bool:
    """删除数据卷文件，返回是否真的执行了删除。

    仅在 gc 临界区（引用 + 租约重新校验通过后）调用。文件不存在视为已删除，
    保证“删除失败/中断后重试”可以幂等推进。
    """
    path = blob_path(sha256, cfg)
    try:
        path.unlink()
    except FileNotFoundError:
        return False
    # 清理可能留下的空分片目录（非空时静默跳过）
    for parent in (path.parent, path.parent.parent):
        try:
            parent.rmdir()
        except OSError:
            pass
    return True


def list_blob_files(cfg: Settings = default_settings):
    """枚举数据卷上现存的 blob 文件（用于重启后的对账）。"""
    if not cfg.blobs_dir.exists():
        return []
    return [p.name for p in cfg.blobs_dir.glob("*/*/*") if p.is_file() and not p.name.startswith(".")]


# --------------------------------------------------------------------------- #
# 断点续传暂存文件
# --------------------------------------------------------------------------- #
def partial_path(upload_id: str, cfg: Settings = default_settings) -> Path:
    """续传暂存文件路径。位于独立 uploads/ 目录，重启不清理。"""
    return cfg.uploads_dir / f"upload-{upload_id}.part"


def create_partial(upload_id: str, cfg: Settings = default_settings) -> Path:
    """创建空的续传暂存文件（创建上传时调用）。"""
    cfg.uploads_dir.mkdir(parents=True, exist_ok=True)
    path = partial_path(upload_id, cfg)
    # 新建上传不应撞上既有文件；存在则原样保留以免覆盖，交由上层报错
    with open(path, "xb"):
        pass
    return path


def append_partial(
    upload_id: str,
    offset: int,
    chunk: bytes,
    confirmed: int,
    total_bytes: int,
    max_upload_bytes: int,
    cfg: Settings = default_settings,
    chunk_size: int = 1024 * 1024,
) -> int:
    """按偏移把 chunk 追加到暂存文件，返回新的已确认字节数。

    规则（与数据库确认偏移 ``confirmed`` 对照）：

    - ``offset == confirmed``：新字节，校验总长后追加，进度推进；
    - ``offset < confirmed``：已确认区间重传。重叠部分必须逐字节相同
      （同字节重传幂等、不重复追加）；若内容不同则拒绝，进度不变；
      重叠区之后的新字节正常追加；
    - ``offset > confirmed``：跳写，拒绝，进度不变；
    - 任何写入都不得超过 ``total_bytes``，亦不得超过全局上传上限。

    整个函数在调用方持有的“单上传串行锁”内执行，无需自己加锁。
    """
    if offset < 0:
        raise PartialOffsetError("offset 不能为负数")
    if offset > confirmed:
        raise PartialOffsetError(
            f"offset {offset} 越过已确认偏移 {confirmed}，不允许跳写"
        )
    if not chunk:
        # 空请求：幂等无操作，进度不变
        return confirmed
    # 重叠部分是已确认字节的重传、不会再次落盘；只有其之后的新字节增长文件，
    # 因此两条上限都按“追加后的最终文件长度”判定。
    overlap = min(confirmed - offset, len(chunk))
    new_len = confirmed + (len(chunk) - overlap)
    if new_len > max_upload_bytes:
        # 全局上传上限是最外层约束，先于“声明总字节”检查
        raise UploadTooLarge(f"upload exceeds {max_upload_bytes} bytes")
    if new_len > total_bytes:
        raise PartialOverrunError(
            f"追加后将达到 {new_len} 字节，"
            f"超过创建时声明的总字节数 {total_bytes}"
        )

    path = partial_path(upload_id, cfg)
    if overlap > 0:
        with open(path, "rb") as verify:
            verify.seek(offset)
            remaining = overlap
            view = memoryview(chunk)
            pos = 0
            while remaining > 0:
                n = min(chunk_size, remaining)
                existing = verify.read(n)
                if existing != bytes(view[pos:pos + n]):
                    raise PartialConflictError(
                        f"已确认区间 [{offset}, {offset + overlap}) 的字节与重传内容不一致"
                    )
                pos += n
                remaining -= n

    new_bytes = chunk[overlap:]
    with open(path, "ab") as out:
        if new_bytes:
            out.write(new_bytes)
        out.flush()
        os.fsync(out.fileno())
    return new_len


def truncate_partial(upload_id: str, confirmed: int, cfg: Settings = default_settings) -> None:
    """把暂存文件截断到已确认偏移：丢弃重启前未确认的尾部。"""
    path = partial_path(upload_id, cfg)
    with open(path, "r+b") as fh:
        fh.truncate(confirmed)
        fh.flush()
        os.fsync(fh.fileno())


def discard_partial(upload_id: str, cfg: Settings = default_settings) -> None:
    """删除续传暂存文件（完成发布后调用），不存在视为已删除。"""
    partial_path(upload_id, cfg).unlink(missing_ok=True)
