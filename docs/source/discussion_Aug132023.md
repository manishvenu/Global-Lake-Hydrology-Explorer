# Discussion: Aug 13, 2023

## Status & Updates

- NWM with Zarr files was successful; still need CHRTOUT files for unrecognized lakes.
- At the point of taking a first crack at UI — event bus probably needed first.
- NWM is much faster now.
- Rebuilt data products to prefer saved copies by default; `debug` or `run_cleanly` booleans force a rebuild.

## Planned

1. Event Bus / Observer Pattern — to manage state without forcing too much information to be returned from functions.
2. UI — start looking at designs.
