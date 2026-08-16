#!/usr/bin/env bash
set -Eeuo pipefail

INSTALLER="/opt/account-sales-bot/install.sh"

if [[ ! -f "$INSTALLER" ]]; then
  printf 'Installer not found: %s\n' "$INSTALLER" >&2
  exit 1
fi

if [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
  exec bash "$INSTALLER"
fi

exec sudo bash "$INSTALLER"
