# Discussion: July 2, 2023

## Status

1. NWM is implemented (excluding lakes that aren't being recognized).
2. Changed data access to class-based design.
3. Dropbox migration complete — used as fallback when AWS S3 is unavailable.

## Concerns

1. NWM: haven't confirmed whether lake inflow and outflow equates to stream inflow/outflow.
   It does not include precipitation or evaporation.

## Potential Updates

1. NWM optimization is all over the place — currently takes about 50 minutes per run.
