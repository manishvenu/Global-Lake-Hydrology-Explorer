"""
Example script: run CLAY then LIME on a small lake.

Swap HYLAK_ID at the top to try different lakes.
Find lake IDs here:
  https://glhe-fe.projects.earthengine.app/view/globallakehydrologyexplorer

Sizing guide (HydroLAKES numbers largest → smallest):
  ~67    Great Salt Lake      ~4700 km²
  ~798   Mono Lake            ~180 km²
  ~5000  small regional lake  ~20-50 km²   ← good starting range

ERA5-Land has a ~9 km grid, so the lake needs to be at least that wide
for any grid cell to land on it. If you get a "mask is empty" error,
the lake is too small — try a lower ID number.

NWM only covers the contiguous US. For non-US lakes, NWM will be skipped
gracefully and the other products (ERA5-Land, CRU TS) will still run.

First run downloads data; subsequent runs use cached pickles.
"""

import json
import os
from pathlib import Path

import GLHE.CLAY.CLAY_driver
import GLHE.LIME.LIME_sample_dashboard

# ── pick your lake ──────────────────────────────────────────────────────────
HYLAK_ID = 5000
# ────────────────────────────────────────────────────────────────────────────


def run_clay(hylak_id: int) -> str:
    """Run the full CLAY pipeline and return the output directory path."""
    print(f"\n{'='*60}")
    print(f"  Running CLAY for HYLAK_ID = {hylak_id}")
    print(f"{'='*60}\n")

    clay = GLHE.CLAY.CLAY_driver.CLAY_driver()
    output_dir = clay.main(hylak_id)

    print(f"\nCLAY finished. Outputs in: {output_dir}")
    _summarise_outputs(output_dir)
    return output_dir


def run_lime(output_dir: str) -> None:
    """Launch the LIME Panel dashboard for the given CLAY output directory."""
    print(f"\n{'='*60}")
    print(f"  Launching LIME dashboard")
    print(f"  Reading from: {output_dir}")
    print(f"{'='*60}\n")
    GLHE.LIME.LIME_sample_dashboard.LIME_driver(output_dir)


def _summarise_outputs(output_dir: str) -> None:
    """Print a quick summary of what CLAY wrote."""
    config_path = Path(output_dir) / "config.json"
    if not config_path.exists():
        print("  (config.json not found — CLAY may have errored)")
        return

    with open(config_path) as f:
        config = json.load(f)

    lake_name = config.get("LAKE_NAME", "unknown")
    print(f"  Lake: {lake_name}")
    print("  Output files:")
    for key, val in config.items():
        if key not in ("LAKE_NAME", "CLAY_OUTPUT_FOLDER_LOCATION"):
            exists = "✓" if val and os.path.exists(val) else "✗"
            print(f"    [{exists}] {key}: {os.path.basename(str(val))}")


if __name__ == "__main__":
    output_dir = run_clay(HYLAK_ID)

    # Uncomment to launch the interactive dashboard after CLAY finishes:
    # run_lime(output_dir)
