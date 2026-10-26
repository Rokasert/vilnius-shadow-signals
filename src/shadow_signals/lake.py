import gzip
import hashlib
import json
import posixpath
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

import fsspec
from pydantic import BaseModel, Field

MANIFEST_NAME = "_manifest.json"


def utc_now() -> datetime:
    return datetime.now(UTC)


def new_run_id(now: datetime | None = None) -> str:
    return (now or utc_now()).strftime("%Y%m%dT%H%M%SZ")


def schema_fingerprint(fields: Iterable[str]) -> str:
    return hashlib.sha256("\n".join(sorted(fields)).encode()).hexdigest()[:16]


class FileEntry(BaseModel):
    path: str
    bytes: int
    sha256: str
    records: int | None = None
    compression: str | None = None


class Manifest(BaseModel):
    source: str
    dataset: str
    snapshot_month: str
    run_id: str
    extractor_version: str
    started_at: datetime
    finished_at: datetime | None = None
    request: dict[str, Any] = Field(default_factory=dict)
    http: dict[str, Any] = Field(default_factory=dict)
    format: dict[str, Any] = Field(default_factory=dict)
    files: list[FileEntry] = Field(default_factory=list)
    records_total: int | None = None
    columns: list[str] = Field(default_factory=list)
    schema_fingerprint: str | None = None
    source_as_of: str | None = None
    warnings: list[str] = Field(default_factory=list)

    @property
    def key(self) -> str:
        return f"{self.source}/{self.dataset}"


class BronzeRun:
    def __init__(self, lake: "Lake", manifest: Manifest) -> None:
        self.lake = lake
        self.manifest = manifest
        self.dir = lake.bronze_dir(
            manifest.source, manifest.dataset, manifest.snapshot_month, manifest.run_id
        )

    def path(self, name: str) -> str:
        return posixpath.join(self.dir, name)

    def write_part(self, name: str, data: bytes, records: int | None = None) -> FileEntry:
        entry = self.write_stream(name, [data])
        entry.records = records
        return entry

    def write_stream(
        self, name: str, chunks: Iterable[bytes], *, compress: bool = False
    ) -> FileEntry:
        """Write chunks as they arrive. Size and checksum describe the content as received,
        before any compression, so they can be compared with the source."""
        path = self.path(name)
        self.lake.fs.makedirs(posixpath.dirname(path), exist_ok=True)
        digest = hashlib.sha256()
        size = 0
        with self.lake.fs.open(path, "wb") as raw:
            sink = gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) if compress else raw
            for chunk in chunks:
                digest.update(chunk)
                size += len(chunk)
                sink.write(chunk)
            if compress:
                sink.close()
        entry = FileEntry(
            path=name,
            bytes=size,
            sha256=digest.hexdigest(),
            compression="gzip" if compress else None,
        )
        self.manifest.files.append(entry)
        return entry

    def warn(self, message: str) -> None:
        self.manifest.warnings.append(message)


class Lake:
    def __init__(self, uri: str) -> None:
        self.fs, self.root = fsspec.core.url_to_fs(uri)

    @property
    def is_local(self) -> bool:
        return "file" in self.fs.protocol or "local" in self.fs.protocol

    def path(self, *parts: str) -> str:
        return posixpath.join(self.root, *parts)

    def uri(self, path: str) -> str:
        """Address another engine (DuckDB) can open: a plain path locally, a URL otherwise."""
        return path if self.is_local else self.fs.unstrip_protocol(path)

    def bronze_dir(self, source: str, dataset: str, snapshot_month: str, run_id: str) -> str:
        return self.path(
            "bronze", source, dataset, f"snapshot_month={snapshot_month}", f"run_id={run_id}"
        )

    def write_bytes(self, path: str, data: bytes) -> None:
        self.fs.makedirs(posixpath.dirname(path), exist_ok=True)
        with self.fs.open(path, "wb") as f:
            f.write(data)

    def read_bytes(self, path: str) -> bytes:
        return self.fs.cat_file(path)

    def write_json(self, path: str, obj: Any) -> None:
        self.write_bytes(path, json.dumps(obj, indent=2, default=str).encode())

    @contextmanager
    def bronze_run(
        self, source: str, dataset: str, snapshot_month: str, extractor_version: str
    ) -> Iterator[BronzeRun]:
        """Files land as they are written; the manifest is written last, so a run without
        a manifest is incomplete and ignored downstream."""
        manifest = Manifest(
            source=source,
            dataset=dataset,
            snapshot_month=snapshot_month,
            run_id=new_run_id(),
            extractor_version=extractor_version,
            started_at=utc_now(),
        )
        run = BronzeRun(self, manifest)
        yield run
        manifest.finished_at = utc_now()
        counts = [f.records for f in manifest.files]
        manifest.records_total = None if None in counts else sum(counts)
        self.write_bytes(run.path(MANIFEST_NAME), manifest.model_dump_json(indent=2).encode())

    def run_dir(self, manifest: Manifest) -> str:
        return self.bronze_dir(
            manifest.source, manifest.dataset, manifest.snapshot_month, manifest.run_id
        )

    def manifests(
        self,
        source: str | None = None,
        dataset: str | None = None,
        snapshot_month: str | None = None,
    ) -> list[Manifest]:
        pattern = self.path(
            "bronze",
            source or "*",
            dataset or "*",
            f"snapshot_month={snapshot_month or '*'}",
            "run_id=*",
            MANIFEST_NAME,
        )
        found = [Manifest.model_validate_json(self.fs.cat_file(p)) for p in self.fs.glob(pattern)]
        return sorted(found, key=lambda m: (m.source, m.dataset, m.snapshot_month, m.run_id))

    def latest_manifest(self, source: str, dataset: str, snapshot_month: str) -> Manifest | None:
        found = self.manifests(source, dataset, snapshot_month)
        return found[-1] if found else None

    def latest_manifests(self) -> list[Manifest]:
        """The newest complete run of every dataset, whatever its snapshot month."""
        latest: dict[str, Manifest] = {}
        for manifest in self.manifests():
            current = latest.get(manifest.key)
            if current is None or (manifest.snapshot_month, manifest.run_id) > (
                current.snapshot_month,
                current.run_id,
            ):
                latest[manifest.key] = manifest
        return [latest[key] for key in sorted(latest)]

    def copy_run_from(self, other: "Lake", manifest: Manifest) -> bool:
        """Copy a complete run from another lake; returns False when it is already here."""
        target_dir = self.run_dir(manifest)
        if self.fs.exists(posixpath.join(target_dir, MANIFEST_NAME)):
            return False
        source_dir = other.run_dir(manifest)
        for entry in manifest.files:
            self.write_bytes(
                posixpath.join(target_dir, entry.path),
                other.read_bytes(posixpath.join(source_dir, entry.path)),
            )
        self.write_bytes(
            posixpath.join(target_dir, MANIFEST_NAME),
            other.read_bytes(posixpath.join(source_dir, MANIFEST_NAME)),
        )
        return True
