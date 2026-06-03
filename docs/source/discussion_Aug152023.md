# Discussion: Aug 15, 2023

## Status & Updates

- Cleaned up NWM.
- Added PubSub for README information; more uses expected later.
- Named the subpackages:
  - **LIME** — Lake Interface for Monitoring and Exploration
  - **CLAY** — Centralized Lake Data Acquisition and Yield

## Planned

1. UI — needs to be completely separate from the data layer (driven directly from `LakeOutputDirectory`).
   Main plots should take only a generic CSV; validation grids only NetCDF or similar.
   Point products (like NWM) should be straightforward.
