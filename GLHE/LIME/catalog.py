"""
Lakes CLAY has already processed.

CLAY writes one folder per lake inside ``LAKE_OUTPUT_FOLDER`` (set in
GLHE/CLAY/globals.py, "LakeOutputDirectory" by default), each holding a
config.json.
"""

import json
from pathlib import Path

import GLHE.CLAY.globals as clay_globals


def list_processed_lakes(root=None) -> list[dict]:
    """``[{"name", "hylak_id", "output_dir"}, ...]`` for every lake folder in
    ``root`` (default: CLAY's output folder), sorted by name."""
    root = Path(root or clay_globals.config["DIRECTORIES"]["LAKE_OUTPUT_FOLDER"])
    lakes = []
    for config_path in root.glob("*/config.json"):
        try:
            config = json.loads(config_path.read_text())
        except (OSError, ValueError):
            continue  # a half-written or broken folder
        lakes.append(
            {
                "name": config.get("LAKE_NAME", config_path.parent.name),
                "hylak_id": config.get("HYLAK_ID"),
                "output_dir": str(config_path.parent.resolve()),
            }
        )
    return sorted(lakes, key=lambda lake: lake["name"].lower())
