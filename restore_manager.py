"""Validated, atomic and immediately usable SQLite backup restoration."""

from __future__ import annotations

import hashlib
import os
import shutil
import sqlite3
import stat
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path

import storage


MAX_RESTORE_FILE_BYTES = 512 * 1024 * 1024
_REQUIRED_TABLES = {"meta", "users", "accounts", "transactions"}
_RESTORE_LOCK = threading.Lock()


class RestoreValidationError(ValueError):
    """Raised when an uploaded file is not a compatible application backup."""


def _sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _readonly_connection(path: str) -> sqlite3.Connection:
    uri = Path(path).resolve().as_uri() + "?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True, timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA trusted_schema=OFF")
    return conn


def _meta_value(conn: sqlite3.Connection, key: str) -> str:
    row = conn.execute("SELECT value FROM meta WHERE key=?", (str(key),)).fetchone()
    return str(row[0]) if row else ""


def inspect_database_backup(
    path: str, *, max_size_bytes: int = MAX_RESTORE_FILE_BYTES
) -> dict:
    """Read-only validation and preview of an uploaded SQLite backup."""
    candidate = os.path.abspath(str(path or ""))
    if not candidate or not os.path.isfile(candidate):
        raise RestoreValidationError("فایل بکاپ پیدا نشد")
    size_bytes = int(os.path.getsize(candidate))
    if size_bytes <= 0:
        raise RestoreValidationError("فایل بکاپ خالی است")
    if size_bytes > max(int(max_size_bytes), 1):
        raise RestoreValidationError("حجم فایل بکاپ بیشتر از حد مجاز است")
    with open(candidate, "rb") as handle:
        if handle.read(16) != b"SQLite format 3\x00":
            raise RestoreValidationError("فایل ارسال‌شده یک دیتابیس SQLite معتبر نیست")

    conn = None
    try:
        conn = _readonly_connection(candidate)
        quick_rows = [str(row[0]) for row in conn.execute("PRAGMA quick_check")]
        if quick_rows != ["ok"]:
            raise RestoreValidationError("بررسی سلامت دیتابیس ناموفق بود")

        tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if "meta" not in tables:
            raise RestoreValidationError(
                "این فایل بکاپ متعلق به Account Sales Bot نیست یا ناقص است"
            )
        raw_schema_version = _meta_value(conn, "schema_version")
        try:
            schema_version = int(raw_schema_version)
        except (TypeError, ValueError) as exc:
            raise RestoreValidationError("نسخه دیتابیس داخل فایل قابل تشخیص نیست") from exc
        if schema_version <= 0:
            raise RestoreValidationError("نسخه دیتابیس داخل فایل معتبر نیست")
        if schema_version > int(storage.SCHEMA_VERSION):
            raise RestoreValidationError(
                "نسخه دیتابیس این بکاپ از نسخه فعلی ربات جدیدتر است؛ "
                "ابتدا ربات را آپدیت کنید"
            )

        missing = sorted(_REQUIRED_TABLES - tables)
        if missing:
            raise RestoreValidationError(
                "این فایل بکاپ متعلق به Account Sales Bot نیست یا ناقص است"
            )

        counts = {
            table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in ("users", "accounts", "transactions")
        }
        created_at = _meta_value(conn, "backup_created_at")
        backup_app_version = _meta_value(conn, "backup_app_version")
        backup_kind = _meta_value(conn, "backup_kind")
    except RestoreValidationError:
        raise
    except sqlite3.DatabaseError as exc:
        raise RestoreValidationError("ساختار فایل SQLite خراب یا ناسازگار است") from exc
    finally:
        if conn is not None:
            conn.close()

    return {
        "path": candidate,
        "filename": os.path.basename(candidate),
        "size_bytes": size_bytes,
        "sha256": _sha256(candidate),
        "schema_version": schema_version,
        "current_schema_version": int(storage.SCHEMA_VERSION),
        "backup_created_at": created_at,
        "backup_app_version": backup_app_version,
        "backup_kind": backup_kind,
        "counts": counts,
        "quick_check": "ok",
    }


def _remove_sidecars(db_file: str):
    for suffix in ("-wal", "-shm"):
        try:
            os.unlink(db_file + suffix)
        except FileNotFoundError:
            pass


def _copy_fsynced(source: str, destination: str, *, mode: int = 0o600):
    with open(source, "rb") as src, open(destination, "wb") as dst:
        shutil.copyfileobj(src, dst, length=1024 * 1024)
        dst.flush()
        os.fsync(dst.fileno())
    os.chmod(destination, int(mode))


def _make_safety_backup(db_file: str, backup_dir: str) -> str:
    os.makedirs(backup_dir, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S-%f")
    destination = os.path.join(backup_dir, f"pre-restore-{stamp}.sqlite3")
    src = sqlite3.connect(db_file, timeout=10.0)
    dst = sqlite3.connect(destination)
    try:
        src.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        src.backup(dst)
        storage._annotate_database_backup(
            dst,
            created_at=storage.now_iso(),
            backup_kind="pre_restore_safety",
        )
    finally:
        dst.close()
        src.close()
    os.chmod(destination, 0o600)
    return destination


def _trim_safety_backups(backup_dir: str, keep: int = 5):
    files = sorted(
        Path(backup_dir).glob("pre-restore-*.sqlite3"),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    for old in files[max(int(keep), 1):]:
        try:
            old.unlink()
        except OSError:
            pass


def _replace_database_file(staged_path: str, db_file: str):
    _remove_sidecars(db_file)
    os.replace(staged_path, db_file)
    try:
        directory_fd = os.open(os.path.dirname(db_file) or ".", os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def restore_database_backup(path: str, *, expected_sha256: str = "") -> dict:
    """Atomically install a validated backup and migrate it in this process."""
    preview = inspect_database_backup(path)
    expected = str(expected_sha256 or "").strip().lower()
    if expected and preview["sha256"].lower() != expected:
        raise RestoreValidationError("فایل بکاپ پس از بررسی اولیه تغییر کرده است")

    db_file = os.path.abspath(storage.DB_FILE)
    data_dir = os.path.dirname(db_file)
    backup_dir = os.path.abspath(storage.BACKUP_DIR)
    os.makedirs(data_dir, exist_ok=True)
    old_mode = 0o600
    if os.path.isfile(db_file):
        old_mode = stat.S_IMODE(os.stat(db_file).st_mode) or 0o600

    with _RESTORE_LOCK, storage._BACKUP_LOCK:
        fd, staged_path = tempfile.mkstemp(
            prefix=".database-restore-", suffix=".sqlite3", dir=data_dir
        )
        os.close(fd)
        safety_path = ""
        replaced = False
        try:
            _copy_fsynced(path, staged_path, mode=old_mode)
            staged_preview = inspect_database_backup(staged_path)
            if staged_preview["sha256"] != preview["sha256"]:
                raise RestoreValidationError("کپی آماده‌شده بکاپ با فایل اصلی یکسان نیست")

            if os.path.isfile(db_file):
                safety_path = _make_safety_backup(db_file, backup_dir)
            _replace_database_file(staged_path, db_file)
            replaced = True
            storage.initialize_storage()
        except Exception:
            if replaced and safety_path and os.path.isfile(safety_path):
                rollback_path = staged_path + ".rollback"
                try:
                    _copy_fsynced(safety_path, rollback_path, mode=old_mode)
                    _replace_database_file(rollback_path, db_file)
                    storage.initialize_storage()
                finally:
                    try:
                        os.unlink(rollback_path)
                    except FileNotFoundError:
                        pass
            raise
        finally:
            try:
                os.unlink(staged_path)
            except FileNotFoundError:
                pass

        _trim_safety_backups(backup_dir)
        return {
            **preview,
            "restored_at": storage.now_iso(),
            "restored_schema_version": int(storage.SCHEMA_VERSION),
            "safety_backup_path": safety_path,
        }


def rollback_to_safety_backup(path: str) -> dict:
    """Reinstall the pre-restore snapshot if runtime cache reload fails."""
    preview = inspect_database_backup(path)
    db_file = os.path.abspath(storage.DB_FILE)
    data_dir = os.path.dirname(db_file)
    mode = stat.S_IMODE(os.stat(db_file).st_mode) if os.path.isfile(db_file) else 0o600
    with _RESTORE_LOCK, storage._BACKUP_LOCK:
        fd, staged_path = tempfile.mkstemp(
            prefix=".database-rollback-", suffix=".sqlite3", dir=data_dir
        )
        os.close(fd)
        try:
            _copy_fsynced(path, staged_path, mode=mode or 0o600)
            _replace_database_file(staged_path, db_file)
            storage.initialize_storage()
        finally:
            try:
                os.unlink(staged_path)
            except FileNotFoundError:
                pass
    return preview
