# Deploying GLHE on the server (gram)

The app runs in a **rootless Podman container**, started by a **systemd user service**, and is reachable **only on the tailnet** through `tailscale serve`.

| What | URL |
|---|---|
| GLHE app | https://gram-wsl.tailbc2832.ts.net:8443 |
| App usage (sessions, CPU, memory, logs; private mode only) | https://gram-wsl.tailbc2832.ts.net:8443/admin |
| Machine + containers (Cockpit) | https://gram-wsl.tailbc2832.ts.net:9090 |

```
browser ──tailnet──► tailscale serve :8443 ──► 127.0.0.1:5006 ──► container "glhe" (panel serve)
                                                                     ├─ /data           ← ~/glhe-data (outputs, temp)
                                                                     ├─ .../LocalData   ← ~/glhe-data/LocalData (HydroLAKES, etc.)
                                                                     └─ CDSAPI_RC       ← ~/.cdsapirc (read-only)
```

## First-time setup

One-time root steps (already done on gram): `loginctl enable-linger manishrv`, the `dbus-user-session` package, and `tailscale set --operator=manishrv`.

The server runs from its own clean clone, `~/glhe-deploy`, so a work-in-progress checkout (`~/glhe`) never affects the live app.

```bash
git clone https://github.com/manishvenu/Global-Lake-Hydrology-Explorer.git ~/glhe-deploy
cd ~/glhe-deploy
deploy/run.sh                                        # build image, start service (first build: ~10 min)
tailscale serve --bg --https=8443 http://127.0.0.1:5006
```

API keys other than Copernicus go in `~/.config/glhe/secrets.env` (one `KEY=value` per line, `chmod 600`), for example `DAHITI_API_KEY=...` for DAHITI water levels. `run.sh` passes the file to the container if it exists; products whose key is missing are skipped.

Data lives in `~/glhe-data` (override with `GLHE_DATA`). CLAY downloads what is missing into `~/glhe-data/LocalData` on its first run.

## Updating

```bash
cd ~/glhe-deploy && git pull && deploy/run.sh
```

`deploy/run.sh --no-build` restarts without rebuilding. Settings are environment variables at the top of `run.sh` (`GLHE_PORT`, `GLHE_MEMORY`, `GLHE_ORIGIN`, ...).

## Day to day

```bash
systemctl --user status container-glhe        # running?
journalctl --user -u container-glhe -f        # live logs (app + CLAY runs)
podman stats glhe                             # CPU / memory of the container
systemctl --user restart container-glhe       # restart
systemctl --user stop container-glhe          # stop (until next boot or start)
du -sh ~/glhe-data/*                          # disk used by outputs and data
```

Rolling back: `git checkout <older commit> && deploy/run.sh`.

## Public mode (internet, via Tailscale Funnel)

Public mode adds guards for strangers (see `GLHE/LIME/public.py`): each run stops after 45 min; lakes over 10,000 km² (or of unknown size) and lakes already processed cannot be run; each visitor gets one run per 10 min; only the visitor who started a run can cancel it; a "not validated" banner is shown; and the Panel `/admin` page is **off**.

```bash
mkdir -p ~/.config/glhe && echo GLHE_PUBLIC=1 > ~/.config/glhe/deploy.env   # remembered by run.sh
deploy/run.sh --no-build
tailscale funnel --bg --https=8443 http://127.0.0.1:5006     # needs Funnel enabled in the tailnet policy
```

Limits are set in the same `deploy.env` (`GLHE_MAX_RUN_MINUTES`, `GLHE_MAX_LAKE_AREA_KM2`, `GLHE_COOLDOWN_MINUTES`). To go private again: `tailscale funnel --https=8443 off`, then `tailscale serve --bg --https=8443 http://127.0.0.1:5006`, set `GLHE_PUBLIC=0` and `deploy/run.sh --no-build`. Big lakes for the public site: run them while private (or from `~/glhe-deploy` with `podman exec -w /data glhe /opt/conda/bin/python -m GLHE.LIME.run_clay <id>`); they then open for everyone from the cache.

**Never funnel Cockpit (`:9090`).**

## Custom domain (Cloudflare Tunnel)

https://glhe.venumuddula.com reaches the same app through a Cloudflare Tunnel:
`cloudflared` on gram connects *out* to Cloudflare and Cloudflare sends visitors
down that connection to `http://127.0.0.1:5006` (no open ports, no public IP).
The Tailscale Funnel address keeps working alongside it.

- Program: `~/.local/bin/cloudflared`; settings: `~/.cloudflared/config.yml`;
  the tunnel's secret: `~/.cloudflared/<id>.json` (never commit it).
- Service: `systemctl --user status cloudflared-glhe` (restart / stop the same way;
  logs: `journalctl --user -u cloudflared-glhe -f`).
- The app must allow the address: `GLHE_ORIGIN` in run.sh lists every public
  host, comma-separated.
- Set up with: `cloudflared tunnel login`, `cloudflared tunnel create glhe`,
  `cloudflared tunnel route dns glhe glhe.venumuddula.com`, then the config
  file and the user service above.

## Lake tiles (every lake on the globe)

The globe draws all 1.4 million HydroLAKES lakes from one vector-tile file,
`HydroLAKES_polys_v10.pmtiles` (~0.9 GB, layer `lakes`). Build it (about 30
minutes) with `GLHE/CLAY/data_access/aws_zarr_templates/hydrolakes_to_pmtiles.py`.

Where the browser gets it:

- **From this server (current setup):** if `~/glhe-tiles/HydroLAKES_polys_v10.pmtiles`
  exists and `GLHE_LAKE_TILES_URL` is not set, `run.sh` mounts just that file
  read-only and the app serves it at `/tiles/HydroLAKES_polys_v10.pmtiles`.
- **From S3 (better: no load on this server):**
  1. Upload it next to the parquet file:
     `aws s3 cp ~/glhe-tiles/HydroLAKES_polys_v10.pmtiles s3://glhe/HydroLAKES/`
     (or through the S3 website).
  2. Let the site read it: S3 console → `glhe` → Permissions → CORS, paste
     ```json
     [{"AllowedOrigins":["https://glhe.venumuddula.com","https://gram-wsl.tailbc2832.ts.net:8443"],"AllowedMethods":["GET","HEAD"],"AllowedHeaders":["Range","If-Match"],"ExposeHeaders":["ETag","Content-Length","Content-Range"],"MaxAgeSeconds":3000}]
     ```
     (add `"http://localhost:5006"` to `AllowedOrigins` for local development).
  3. Switch the app to it:
     ```bash
     echo GLHE_LAKE_TILES_URL=https://glhe.s3.us-east-1.amazonaws.com/HydroLAKES/HydroLAKES_polys_v10.pmtiles >> ~/.config/glhe/deploy.env
     cd ~/glhe-deploy && deploy/run.sh --no-build
     ```

Without tiles (or with `GLHE_LAKE_TILES_URL=` empty) the globe works as before:
clicks look the lake up in HydroLAKES instead.

## Monitoring

- **Cockpit** (`:9090`, log in as `manishrv`): CPU, memory, disk and network for the whole machine; *Services* shows `container-glhe`; *Podman containers* lists containers.
- **Panel admin** (`:8443/admin`): who is using the app right now, per-session CPU/memory, and app logs.
- **Tailscale admin console**: which devices are connected.

## Security

- Rootless: the container runs as your own unprivileged user (`--userns=keep-id`), with every Linux capability dropped and `no-new-privileges`.
- Read-only root file system; only `~/glhe-data` and `/tmp` are writable. The container cannot see the rest of your home directory.
- Memory capped (`GLHE_MEMORY`, default 5g) and at most 512 processes.
- The port is bound to `127.0.0.1`; the only way in is `tailscale serve` (tailnet only). **Funnel is off**; turning it on makes the app public.
- The ERA5 key is mounted read-only and other keys come from `secrets.env`; none are baked into the image.
