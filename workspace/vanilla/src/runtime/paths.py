"""Folder parameters of tasks, kept inside the workspace.

A pipeline's parameters may come from whoever starts a run, so a folder parameter
is a path relative to the workspace (``source``, ``data/tenant1/checkpoint/bronze``),
never an absolute path or one with ``..``: a run can't read or write elsewhere on
the server. ``source/`` and ``data/`` may be links to the source data and data lake
folders; paths through them are fine.
"""

from __future__ import annotations

from pathlib import Path, PurePosixPath


def workspace_path(root: Path, rel: str) -> Path:
    """``rel`` under the workspace ``root``; ValueError for an absolute path or ``..``."""
    p = PurePosixPath(str(rel).replace("\\", "/"))
    if not str(rel).strip() or p.is_absolute() or ".." in p.parts:
        raise ValueError(f"'{rel}' must be a folder inside the workspace (relative, without '..')")
    return (Path(root) / p).resolve()
