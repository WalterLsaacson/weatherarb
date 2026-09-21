"""Small atomic JSON/JSONL persistence helpers for the standalone POC."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Callable, Iterable, Optional


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def load_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return default


def write_json_atomic(path: Path, payload: Any, *, indent: Optional[int] = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=".tmp-", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=indent, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, str(path))
    finally:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass


def append_jsonl(path: Path, rows: Iterable[Any]) -> int:
    values = list(rows)
    if not values:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = Path(str(path) + ".lock")
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        try:
            with path.open("a", encoding="utf-8") as handle:
                for row in values:
                    handle.write(canonical_json(row) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
    return len(values)


def append_jsonl_dedup(
    path: Path,
    rows: Iterable[Any],
    *,
    key_fn: Callable[[Any], str],
    known_keys: Optional[set[str]] = None,
    key_cache: Optional[dict[str, set[str]]] = None,
) -> int:
    incoming = list(rows)
    if not incoming:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = Path(str(path) + ".lock")
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        try:
            if known_keys is not None:
                existing = known_keys
            else:
                existing = set()
                if path.is_file():
                    try:
                        with path.open("r", encoding="utf-8") as handle:
                            for line in handle:
                                if not line.strip():
                                    continue
                                try:
                                    existing.add(key_fn(json.loads(line)))
                                except (ValueError, TypeError, KeyError):
                                    continue
                    except OSError:
                        pass
                if key_cache is not None:
                    key_cache[str(path)] = existing
            unique: list[Any] = []
            for row in incoming:
                key = key_fn(row)
                if key in existing:
                    continue
                existing.add(key)
                unique.append(row)
            if unique:
                with path.open("a", encoding="utf-8") as handle:
                    for row in unique:
                        handle.write(canonical_json(row) + "\n")
                    handle.flush()
                    os.fsync(handle.fileno())
            return len(unique)
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


def read_jsonl(path: Path, *, limit: Optional[int] = None) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except ValueError:
                    continue
                if isinstance(row, dict):
                    rows.append(row)
                if limit is not None and len(rows) >= limit:
                    break
    except OSError:
        return rows
    return rows
