#!/usr/bin/env bash
# Build the GLHE image and (re)start it as a rootless Podman container,
# managed by a systemd *user* service so it starts at boot and restarts on crash.
#
#   deploy/run.sh            # build + (re)start
#   deploy/run.sh --no-build # just restart with the current image
#
# Settings (override with environment variables):
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
DATA="${GLHE_DATA:-$HOME/glhe-data}"            # LakeOutputDirectory/, .temp/, LocalData/
CDSAPIRC="${GLHE_CDSAPIRC:-$HOME/.cdsapirc}"     # ERA5 (Copernicus) API key, mounted read-only
SECRETS="${GLHE_SECRETS:-$HOME/.config/glhe/secrets.env}"  # optional KEY=value lines, e.g. DAHITI_API_KEY
PORT="${GLHE_PORT:-5006}"                        # bound to 127.0.0.1 only
ORIGIN="${GLHE_ORIGIN:-gram-wsl.tailbc2832.ts.net:8443}"  # the public-facing host:port
MEMORY="${GLHE_MEMORY:-5g}"
IMAGE=localhost/glhe:latest
NAME=glhe

export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"

mkdir -p "$DATA/LakeOutputDirectory" "$DATA/.temp/logging" "$DATA/LocalData"

if [[ "${1:-}" != "--no-build" ]]; then
  echo "==> Building $IMAGE (first build takes several minutes)"
  # docker format: the micromamba base image needs SHELL, which OCI format ignores
  podman build --format docker -t "$IMAGE" -f "$REPO/deploy/Containerfile" "$REPO"
fi

secret_args=()
if [[ -f "$CDSAPIRC" ]]; then
  secret_args=(-v "$CDSAPIRC:/run/secrets/cdsapirc:ro,Z" -e CDSAPI_RC=/run/secrets/cdsapirc)
else
  echo "warning: $CDSAPIRC not found; ERA5-Land downloads will fail" >&2
fi
if [[ -f "$SECRETS" ]]; then
  secret_args+=(--env-file "$SECRETS")
fi

echo "==> Replacing container $NAME"
systemctl --user stop "container-$NAME.service" 2>/dev/null || true
podman rm -f "$NAME" >/dev/null 2>&1 || true

# Security: run as your own (unprivileged) uid, read-only root, no capabilities,
# no privilege escalation, memory/process caps, port on localhost only.
podman create --name "$NAME" \
  --userns=keep-id --user "$(id -u):$(id -g)" \
  --read-only --tmpfs /tmp:rw,size=2g \
  --cap-drop=ALL --security-opt=no-new-privileges \
  --memory "$MEMORY" --pids-limit 512 \
  -p "127.0.0.1:$PORT:5006" \
  -v "$DATA:/data:rw,Z" \
  -v "$DATA/LocalData:/app/GLHE/CLAY/LocalData:rw,Z" \
  "${secret_args[@]}" \
  -e GLHE_ORIGIN="$ORIGIN" \
  "$IMAGE" >/dev/null

echo "==> Installing systemd user service container-$NAME.service"
mkdir -p "$HOME/.config/systemd/user"
(cd "$HOME/.config/systemd/user" && podman generate systemd --new --name "$NAME" --files >/dev/null)
podman rm -f "$NAME" >/dev/null  # the service recreates it from the unit (--new)
systemctl --user daemon-reload
systemctl --user enable --now "container-$NAME.service"

echo "==> Checking CLAY and LIME import inside the container"
for _ in $(seq 1 15); do podman container exists "$NAME" && break; sleep 1; done
podman exec "$NAME" /opt/conda/bin/python -c "import GLHE.CLAY, GLHE.LIME.app" \
  || { echo "Import check failed: see the error above" >&2; exit 1; }

echo "==> Waiting for the app"
for _ in $(seq 1 60); do
  if curl -fsS -o /dev/null "http://127.0.0.1:$PORT/app"; then
    echo "GLHE is up on http://127.0.0.1:$PORT (tailnet: https://$ORIGIN)"
    exit 0
  fi
  sleep 2
done
echo "App did not answer in 2 minutes; check: journalctl --user -u container-$NAME -n 100" >&2
exit 1
