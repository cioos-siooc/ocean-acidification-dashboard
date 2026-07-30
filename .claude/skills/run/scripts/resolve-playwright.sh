#!/usr/bin/env bash
# Makes a working `playwright` Node module (with its cached Chromium binary)
# resolvable, without a permanent global install. `chromium-cli` is NOT
# available in this dev environment — don't `which chromium-cli` each time,
# just source this script.
#
# Usage:
#   source .claude/skills/run/scripts/resolve-playwright.sh
#   node your-driver-script.js     # `require('playwright')` now resolves
set -uo pipefail

resolve_playwright_node_path() {
  if node -e "require.resolve('playwright')" >/dev/null 2>&1; then
    return 0
  fi

  local pw_pkg
  pw_pkg=$(find "$HOME/.npm/_npx" -maxdepth 3 -type d -name playwright -path '*/node_modules/playwright' 2>/dev/null | head -1)
  if [ -n "$pw_pkg" ]; then
    export NODE_PATH
    NODE_PATH="$(dirname "$pw_pkg")"
    return 0
  fi

  # Nothing cached under ~/.npm/_npx — try fetching it (needs network).
  npx --yes playwright install chromium >/dev/null 2>&1
  pw_pkg=$(find "$HOME/.npm/_npx" -maxdepth 3 -type d -name playwright -path '*/node_modules/playwright' 2>/dev/null | head -1)
  if [ -n "$pw_pkg" ]; then
    export NODE_PATH
    NODE_PATH="$(dirname "$pw_pkg")"
    return 0
  fi

  echo "resolve-playwright.sh: playwright not available and could not be fetched (no network?)" >&2
  return 1
}

resolve_playwright_node_path
