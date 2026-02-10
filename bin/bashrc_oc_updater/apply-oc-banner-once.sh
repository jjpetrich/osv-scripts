#!/usr/bin/env bash
set -euo pipefail

BASHRC="${1:-$HOME/.bashrc}"

# Expect patches to be in the same directory as this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PATCH_EXISTING="$SCRIPT_DIR/oc-banner-once-existing.patch"
PATCH_ADD="$SCRIPT_DIR/oc-banner-once-add.patch"

if [[ ! -f "$BASHRC" ]]; then
  echo "ERROR: No such file: $BASHRC" >&2
  exit 1
fi

if ! command -v patch >/dev/null 2>&1; then
  echo "ERROR: 'patch' command not found. Install it (e.g., 'sudo apt-get install patch')." >&2
  exit 1
fi

backup="$BASHRC.bak.$(date +%Y%m%d-%H%M%S)"
cp -a "$BASHRC" "$backup"

echo "Backup created: $backup"

# Apply the right patch based on whether the OpenShift section already exists
if grep -q "OpenShift Banner & Prompt" "$BASHRC"; then
  echo "Detected an existing OpenShift prompt section. Applying: $PATCH_EXISTING"
  patch --forward -p1 -d "$(dirname "$BASHRC")" < "$PATCH_EXISTING"
else
  echo "No OpenShift prompt section detected. Applying: $PATCH_ADD"
  patch --forward -p1 -d "$(dirname "$BASHRC")" < "$PATCH_ADD"
fi

echo "Done."
echo "Reload your shell with: source \"$BASHRC\""
