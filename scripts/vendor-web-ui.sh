#!/bin/sh
# Refresh the vendored Paseo web UI from the `vendor/paseo` submodule (tag v0.8.0).
#
# Requires the web app to be exported first. Either run it from a checkout
# that already has `packages/app/dist`, or pass the export directory as the
# first argument:
#
#   scripts/vendor-web-ui.sh
#   scripts/vendor-web-ui.sh /path/to/paseo/packages/app/dist
#
# Building the export needs that checkout's node_modules, so a prebuilt
# sibling `../paseo` checkout is accepted as a fallback source.

set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
DIST=${1:-}

if [ -z "$DIST" ]; then
    for candidate in "$ROOT/vendor/paseo/packages/app/dist" "$ROOT/../paseo/packages/app/dist"; do
        if [ -f "$candidate/index.html" ]; then
            DIST=$candidate
            break
        fi
    done
    DIST=${DIST:-"$ROOT/vendor/paseo/packages/app/dist"}
fi

if [ ! -f "$DIST/index.html" ]; then
    echo "error: web UI export not found at $DIST" >&2
    echo "build it with: (cd vendor/paseo && npm run build:web --workspace=@getpaseo/app)" >&2
    exit 1
fi

rm -rf "$ROOT/web-ui"
mkdir -p "$ROOT/web-ui"
cp -R "$DIST/." "$ROOT/web-ui/"
echo "vendored $DIST into $ROOT/web-ui"
