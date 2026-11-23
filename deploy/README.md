# Deploying GLHE on the server (gram)

The app runs in a **rootless Podman container**, started by a **systemd user service**, and is reachable **only on the tailnet** through `tailscale serve`.

| What | URL |
|---|---|
| GLHE app | https://gram-wsl.tailbc2832.ts.net:8443 |
| App usage (sessions, CPU, memory, logs) | https://gram-wsl.tailbc2832.ts.net:8443/admin |
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

## Monitoring

- **Cockpit** (`:9090`, log in as `manishrv`): CPU, memory, disk and network for the whole machine; *Services* shows `container-glhe`; *Podman containers* lists containers.
- **Panel admin** (`:8443/admin`): who is using the app right now, per-session CPU/memory, and app logs.
- **Tailscale admin console**: which devices are connected.

## Security

- Rootless: the container runs as your own unprivileged user (`--userns=keep-id`), with every Linux capability dropped and `no-new-privileges`.
- Read-only root file system; only `~/glhe-data` and `/tmp` are writable. The container cannot see the rest of your home directory.
- Memory capped (`GLHE_MEMORY`, default 5g) and at most 512 processes.
- The port is bound to `127.0.0.1`; the only way in is `tailscale serve` (tailnet only). **Funnel is off**; turning it on makes the app public.
- The ERA5 key is mounted read-only, never baked into the image.
