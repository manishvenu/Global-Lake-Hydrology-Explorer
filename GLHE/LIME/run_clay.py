"""
Tiny command-line wrapper that runs CLAY for one lake.

LIME starts this in a *separate process* (see ``GLHE/LIME/jobs.py``) because
CLAY keeps its settings in module-level globals; a fresh process per run keeps
runs from leaking state into each other or into the web app.

Usage::

    python -m GLHE.LIME.run_clay 67

The very last line printed on success is::

    GLHE_OUTPUT_DIR=/absolute/path/to/LakeOutputDirectory/Great_Salt_Lake

which is how ``ClayJob`` learns where the results went.
"""

from __future__ import annotations

import argparse
import os
import sys

OUTPUT_MARKER = "GLHE_OUTPUT_DIR="


def main(argv: list[str] | None = None) -> int:
    """Parse the HYLAK_ID, run CLAY, print the output folder. Returns an exit code."""
    parser = argparse.ArgumentParser(description="Run GLHE CLAY for one lake.")
    parser.add_argument("hylak_id", type=int, help="HydroLAKES ID, e.g. 67")
    args = parser.parse_args(argv)

    # Imported here (not at the top) so `--help` works even without GDAL installed.
    from GLHE.CLAY.CLAY_driver import CLAY_driver

    print(f"Starting CLAY for HYLAK_ID {args.hylak_id}", flush=True)
    output_dir = CLAY_driver().main(args.hylak_id)
    print(f"{OUTPUT_MARKER}{os.path.abspath(output_dir)}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
