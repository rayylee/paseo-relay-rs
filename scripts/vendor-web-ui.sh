#!/bin/sh
# Refresh the vendored Paseo web UI from the sibling `../paseo` checkout.
#
# Requires the web app to be exported first. Either run it from a checkout
# that already has `packages/app/dist`, or pass the export directory as the
# first argument:
#
#   scripts/vendor-web-ui.sh
#   scripts/vendor-web-ui.sh /path/to/paseo/packages/app/dist

set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
DIST=${1:-"$ROOT/../paseo/packages/app/dist"}

if [ ! -f "$DIST/index.html" ]; then
    echo "web UI export not found at $DIST" >&2
    echo "build it with: (cd ../paseo && npm run build:web --workspace=@getpaseo/app)" >&2
    exit 1
fi

rm -rf "$ROOT/web-ui"
mkdir -p "$ROOT/web-ui"
cp -R "$DIST/." "$ROOT/web-ui/"
echo "vendored $DIST into $ROOT/web-ui"
