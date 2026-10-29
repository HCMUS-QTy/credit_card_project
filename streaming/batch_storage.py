"""Ghi metadata nguyên tử và ràng buộc vùng output với checkpoint."""

import uuid
from dataclasses import dataclass

from scripts.build_curated import GROUP_STATUSES


@dataclass(frozen=True)
class BatchPaths:
    """Tập trung quy tắc đường dẫn, tránh lặp chuỗi trong bộ xử lý batch."""

    base_path: str
    batch_id: int

    @property
    def processed_root(self) -> str:
        return f"{self.base_path}/processed_v2"

    @property
    def batch_name(self) -> str:
        return f"batch_id={self.batch_id:010d}"

    def metadata_file(self, filename: str) -> str:
        return f"{self.processed_root}/_batches/{self.batch_name}/{filename}"

    def group_directory(self, group: str) -> str:
        return f"{self.processed_root}/{group}/{self.batch_name}"

    def new_staging_directory(self) -> str:
        return f"{self.processed_root}/_staging/{self.batch_name}_{uuid.uuid4().hex}"


def publish_json(store, path, record):
    """Atomically publish immutable metadata after its temporary file is closed."""
    parent_directory = path.rsplit("/", 1)[0]
    temporary_path = path + "." + uuid.uuid4().hex + ".tmp"
    store.mkdir(parent_directory)
    try:
        store.write_json(temporary_path, record)
        store.rename(temporary_path, path)
    finally:
        store.remove_own_temporary_path(temporary_path)


def bind_lineage(store, base, checkpoint):
    """Kiểm tra checkpoint cũ trước khi cho phép ghi tiếp vào cùng output."""
    binding_path = f"{base}/processed_v2/_stream.json"
    identity_path = f"{checkpoint}/credit_card_stream.json"
    checkpoint_path = store.qualified(checkpoint)

    if store.exists(binding_path):
        saved_binding = store.read_json(binding_path)
        if not store.exists(identity_path):
            raise RuntimeError("Checkpoint khong khop processed_v2; khong ghi de batch cu.")
        saved_identity = store.read_json(identity_path)
        same_identity = saved_binding == saved_identity
        same_path = saved_binding.get("checkpoint_path") == checkpoint_path
        if not same_identity or not same_path:
            raise RuntimeError("Checkpoint khong khop processed_v2; khong ghi de batch cu.")
        return saved_binding["lineage_id"]

    if store.exists(identity_path):
        identity = store.read_json(identity_path)
        if identity.get("checkpoint_path") != checkpoint_path:
            raise RuntimeError("Checkpoint da doi duong dan")
    else:
        existing_outputs = any(
            store.children(f"{base}/processed_v2/{group}")
            for group in GROUP_STATUSES
        )
        has_checkpoint = store.exists(f"{checkpoint}/metadata")
        if existing_outputs and not has_checkpoint:
            raise RuntimeError("Co du lieu cu nhung thieu checkpoint; can dung vung du lieu moi.")
        identity = {
            "format_version": 1,
            "lineage_id": uuid.uuid4().hex,
            "checkpoint_path": checkpoint_path,
        }
        publish_json(store, identity_path, identity)

    publish_json(store, binding_path, identity)
    return identity["lineage_id"]
