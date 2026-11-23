"""
Find lakes that CLAY has already processed.

Every CLAY run writes a folder per lake containing a ``config.json``. This
module scans one or more "output roots" for ``<root>/<lake>/config.json`` and
returns a small summary per lake, for the sidebar list.

Example
-------
>>> list_processed_lakes("tests/LIME/fixtures")
[{'name': 'Fixture_Lake', 'hylak_id': 999999, 'output_dir': '/.../Fixture_Lake'}]
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

#: Optional environment variable: extra output roots separated by os.pathsep
#: (":" on macOS/Linux), e.g. ``GLHE_OUTPUT_ROOTS=/data/glhe:/scratch/glhe``.
ENV_VAR = "GLHE_OUTPUT_ROOTS"


def default_roots() -> list[Path]:
    """
    Where CLAY puts lake folders, according to ``GLHE.CLAY.globals``.

    CLAY writes to ``LAKE_OUTPUT_FOLDER/<lake name>`` (a path relative to the
    working directory, "LakeOutputDirectory" by default) and records the last
    lake folder in ``OUTPUT_DIRECTORY``. Importing ``GLHE.CLAY`` needs GDAL; if
    that fails we fall back to the default folder name. Roots listed in the
    ``GLHE_OUTPUT_ROOTS`` environment variable are added too.
    """
    roots: list[Path] = []
    try:
        import GLHE.CLAY.globals as clay_globals  # needs GDAL via GLHE.CLAY

        dirs = clay_globals.config["DIRECTORIES"]
        roots.append(Path(dirs.get("LAKE_OUTPUT_FOLDER") or "LakeOutputDirectory"))
        if dirs.get("OUTPUT_DIRECTORY"):
            # OUTPUT_DIRECTORY is one lake's folder; its parent holds the others.
            roots.append(Path(dirs["OUTPUT_DIRECTORY"]).parent)
    except Exception as exc:  # GDAL missing, or a broken install
        logger.debug("Could not import GLHE.CLAY.globals (%s); using defaults", exc)
        roots.append(Path("LakeOutputDirectory"))
    for extra in os.environ.get(ENV_VAR, "").split(os.pathsep):
        if extra.strip():
            roots.append(Path(extra.strip()))
    return roots


def _read_entry(config_path: Path) -> dict | None:
    """Summarise one lake folder from its config.json (None if unreadable)."""
    try:
        with open(config_path) as fp:
            config = json.load(fp)
    except (OSError, ValueError) as exc:
        logger.warning("Skipping unreadable %s: %s", config_path, exc)
        return None
    folder = config_path.parent.resolve()
    hylak_id = config.get("HYLAK_ID")  # optional; real CLAY configs lack it
    try:
        hylak_id = int(hylak_id) if hylak_id is not None else None
    except (TypeError, ValueError):
        hylak_id = None
    return {
        "name": str(config.get("LAKE_NAME") or folder.name),
        "hylak_id": hylak_id,
        "output_dir": str(folder),
    }


def list_processed_lakes(root=None, extra_roots=None) -> list[dict]:
    """
    List processed lakes found under one or more output roots.

    Parameters
    ----------
    root : str | Path | None
        Folder holding one sub-folder per lake. ``None`` uses
        :func:`default_roots`. A folder that itself contains ``config.json``
        (a single lake) also works.
    extra_roots : list of str | Path, optional
        More folders to scan.

    Returns
    -------
    list of dict
        ``{"name": str, "hylak_id": int | None, "output_dir": str}`` sorted by
        name, one per lake folder; duplicates (same folder) are dropped and
        missing roots are ignored.
    """
    roots = default_roots() if root is None else [Path(root)]
    roots += [Path(r) for r in (extra_roots or [])]

    found: dict[str, dict] = {}
    for r in roots:
        if not r.is_dir():
            continue
        configs = [r / "config.json"] + sorted(r.glob("*/config.json"))
        for cfg in configs:
            if cfg.is_file():
                entry = _read_entry(cfg)
                if entry is not None:
                    found.setdefault(entry["output_dir"], entry)
    return sorted(found.values(), key=lambda e: e["name"].lower())
