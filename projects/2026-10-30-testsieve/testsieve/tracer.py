from __future__ import annotations

import builtins
import contextvars
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .errors import StateError, ValidationError
from .model import normalize_path, require_test_id


ROOT_SCOPE = "__root__"


class FileAccessTracer:
    """Small single-process recorder used to demonstrate dynamic dependency observation.

    It intentionally does not claim Stripe's LD_PRELOAD/subprocess coverage. Calls to
    builtins.open and os.open inside ``root`` are attributed to the active scope.
    """

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        if not self.root.is_dir():
            raise ValidationError("trace root must be an existing directory")
        self._scope: contextvars.ContextVar[str] = contextvars.ContextVar(
            "testsieve_scope", default=ROOT_SCOPE
        )
        self._observed: dict[str, set[str]] = {}
        self._active = False
        self._original_open = builtins.open
        self._original_os_open = os.open

    def _record(self, value: Any) -> None:
        if not isinstance(value, (str, bytes, os.PathLike)):
            return
        try:
            path = Path(value).resolve()
            relative = path.relative_to(self.root).as_posix()
            normalized = normalize_path(relative)
        except (OSError, ValueError, ValidationError):
            return
        self._observed.setdefault(self._scope.get(), set()).add(normalized)

    def __enter__(self) -> "FileAccessTracer":
        if self._active:
            raise StateError("tracer is already active")
        self._active = True
        original_open = self._original_open
        original_os_open = self._original_os_open

        def traced_open(file: Any, *args: Any, **kwargs: Any):
            self._record(file)
            return original_open(file, *args, **kwargs)

        def traced_os_open(path: Any, flags: int, mode: int = 0o777, *, dir_fd=None):
            self._record(path)
            return original_os_open(path, flags, mode, dir_fd=dir_fd)

        builtins.open = traced_open
        os.open = traced_os_open
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        builtins.open = self._original_open
        os.open = self._original_os_open
        self._active = False

    @contextmanager
    def scope(self, test_id: str) -> Iterator[None]:
        if not self._active:
            raise StateError("tracer must be active before entering a scope")
        test_id = require_test_id(test_id)
        token = self._scope.set(test_id)
        try:
            yield
        finally:
            self._scope.reset(token)

    def snapshot(self) -> dict[str, Any]:
        return {
            "root_files": sorted(self._observed.get(ROOT_SCOPE, set())),
            "observations": {
                scope: sorted(paths)
                for scope, paths in sorted(self._observed.items())
                if scope != ROOT_SCOPE
            },
        }
