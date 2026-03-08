#!/usr/bin/env bash
# Install the netrun-ui VS Code extension via symlink.
#
# Usage:
#   ./install.sh            # install (build + symlink)
#   ./install.sh --uninstall  # remove the symlink

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
EXT_NAME="netrun-ui-vscode"
EXTENSIONS_DIR="$HOME/.vscode/extensions"
LINK_PATH="$EXTENSIONS_DIR/$EXT_NAME"

if [[ "${1:-}" == "--uninstall" ]]; then
    if [[ -L "$LINK_PATH" ]]; then
        rm "$LINK_PATH"
        echo "Removed symlink $LINK_PATH"
        echo "Restart VS Code to complete uninstallation."
    else
        echo "No symlink found at $LINK_PATH"
    fi
    exit 0
fi

# Install npm dependencies
echo "Installing dependencies..."
cd "$SCRIPT_DIR"
npm install

# Build extension and webview
echo "Building extension..."
npm run build

# Create symlink
mkdir -p "$EXTENSIONS_DIR"
if [[ -L "$LINK_PATH" ]]; then
    echo "Updating existing symlink..."
    rm "$LINK_PATH"
elif [[ -e "$LINK_PATH" ]]; then
    echo "Error: $LINK_PATH exists and is not a symlink. Remove it manually."
    exit 1
fi

ln -s "$SCRIPT_DIR" "$LINK_PATH"
echo "Symlinked $LINK_PATH -> $SCRIPT_DIR"
echo ""
echo "Installation complete. Restart VS Code to activate the extension."
echo "To rebuild after changes: cd $SCRIPT_DIR && npm run build"
echo "To uninstall: $0 --uninstall"
