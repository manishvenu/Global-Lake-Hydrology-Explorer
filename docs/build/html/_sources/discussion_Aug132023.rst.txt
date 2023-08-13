.. _Aug132023:

Discussion: Aug 13, 2023
=========================


Status & Updates
----------------
 - The NWM with the Zarr files was successful, still need to figure out the CHRTOUT files for unrecognized lakes
 - We're at the point of taking first crack at UI, unfortunately it looks like Event Bus might be a useful thing to have before
 - NWM is much faster
 - Rebuilt data products to prefer saved copies of files by default, can set a debug bool or a run_cleanly bool to rebuild

Planned Products
-----------------

1. Event Bus/Observer Pattern -> It'll help manage everything instead of forcing too much information being reutrned from functions
2. UI -> Start Looking at Designs.