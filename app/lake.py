"""Storage path boundary; GCS can be added without changing job call sites."""
from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class LakePaths:
    base: str

    @classmethod
    def from_env(cls) -> "LakePaths":
        base = os.getenv("LAKE_URI", "/opt/project/lake").rstrip("/")
        if base.startswith("gs://"):
            raise NotImplementedError("GCS adapter is optional and has not been configured")
        if not base.startswith("/") and not base.startswith("file:/"):
            raise ValueError("LAKE_URI must be an absolute local path")
        return cls(base)

    def path(self, layer: str) -> str:
        if layer not in {"raw", "curated", "dead_letter"}:
            raise ValueError("unknown lake layer")
        return f"{self.base}/{layer}"
