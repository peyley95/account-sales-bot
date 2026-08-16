#!/usr/bin/env bash
set -Eeuo pipefail

SERVICE_NAME="account-sales-bot"
SERVICE_USER="accountbot"
DEFAULT_REPOSITORY_URL="https://github.com/peyley95/account-sales-bot.git"
REPOSITORY_URL="${ACCOUNT_SALES_BOT_REPOSITORY_URL:-$DEFAULT_REPOSITORY_URL}"
REPOSITORY_REF="${ACCOUNT_SALES_BOT_REPOSITORY_REF:-main}"
INSTALL_DIR="${ACCOUNT_SALES_BOT_INSTALL_DIR:-/opt/account-sales-bot}"
CONFIG_DIR="${ACCOUNT_SALES_BOT_CONFIG_DIR:-/etc/account-sales-bot}"
ENV_FILE="${ACCOUNT_SALES_BOT_ENV_FILE:-$CONFIG_DIR/account-sales-bot.env}"
DATA_DIR="${ACCOUNT_SALES_BOT_DATA_DIR:-/var/lib/account-sales-bot}"
VENV_DIR="$INSTALL_DIR/.venv"
SERVICE_FILE="/etc/systemd/system/account-sales-bot.service"

log() {
  printf '\n==> %s\n' "$*"
}

die() {
  printf '\nERROR: %s\n' "$*" >&2
  exit 1
}

require_root() {
  if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
    die "Run this installer with sudo or as root."
  fi
}

require_supported_os() {
  [[ -r /etc/os-release ]] || die "/etc/os-release was not found."
  # shellcheck disable=SC1091
  source /etc/os-release
  [[ "${ID:-}" == "ubuntu" ]] \
    || die "This installer supports Ubuntu only (detected: ${ID:-unknown})."
  local major="${VERSION_ID%%.*}"
  [[ "$major" =~ ^[0-9]+$ ]] && ((major >= 22)) \
    || die "Ubuntu 22.04 or newer is required (detected: ${VERSION_ID:-unknown})."
  command -v systemctl >/dev/null 2>&1 \
    || die "systemd is not available on this system."
}

install_dependencies() {
  log "Installing Ubuntu and Python dependencies"
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y \
    ca-certificates \
    curl \
    git \
    python3 \
    python3-pip \
    python3-venv

  python3 - <<'PY'
import sys
if sys.version_info < (3, 10):
    raise SystemExit("Account Sales Bot requires Python 3.10 or newer")
PY
}

env_value() {
  local key="$1"
  sed -n "s/^${key}=//p" "$ENV_FILE" | tail -n 1
}

validate_token() {
  local value="$1"
  [[ "$value" =~ ^[0-9]+:[A-Za-z0-9_-]{20,}$ ]] \
    || die "BOT_TOKEN format is invalid."
}

validate_admin_id() {
  local value="$1"
  [[ "$value" =~ ^[1-9][0-9]{0,18}$ ]] \
    || die "ADMIN_IDS must be a positive numeric Telegram ID."
  if ((${#value} == 19)) && [[ "$value" > "9223372036854775807" ]]; then
    die "ADMIN_IDS is outside the supported numeric range."
  fi
}

verify_telegram_token() {
  local value="$1"
  log "Verifying BOT_TOKEN with Telegram"
  if ! printf 'silent\nshow-error\nfail\nmax-time = 15\nurl = "https://api.telegram.org/bot%s/getMe"\n' "$value" \
      | curl --config - >/dev/null; then
    die "Telegram did not accept the token, or api.telegram.org is unreachable."
  fi
}

ensure_service_user() {
  if ! id "$SERVICE_USER" >/dev/null 2>&1; then
    useradd \
      --system \
      --home-dir "$DATA_DIR" \
      --shell /usr/sbin/nologin \
      "$SERVICE_USER"
  fi
  install -d -m 0755 "$CONFIG_DIR"
  install -d -m 0750 -o "$SERVICE_USER" -g "$SERVICE_USER" "$DATA_DIR"
  install -d -m 0750 -o "$SERVICE_USER" -g "$SERVICE_USER" "$DATA_DIR/backups"
}

create_or_load_config() {
  if [[ -f "$ENV_FILE" ]]; then
    BOT_TOKEN_VALUE="$(env_value BOT_TOKEN)"
    ADMIN_ID_VALUE="$(env_value ADMIN_IDS)"
    validate_token "$BOT_TOKEN_VALUE"
    validate_admin_id "$ADMIN_ID_VALUE"
    chmod 0600 "$ENV_FILE"
    log "Existing configuration preserved: $ENV_FILE"
    return
  fi

  [[ -r /dev/tty ]] || die "Initial installation requires an interactive terminal."

  BOT_TOKEN_VALUE="${ACCOUNT_SALES_BOT_TOKEN:-}"
  ADMIN_ID_VALUE="${ACCOUNT_SALES_BOT_ADMIN_ID:-}"

  if [[ -z "$BOT_TOKEN_VALUE" ]]; then
    read -r -p "Enter BOT_TOKEN: " BOT_TOKEN_VALUE </dev/tty
  fi
  if [[ -z "$ADMIN_ID_VALUE" ]]; then
    read -r -p "Enter numeric Telegram ID for the root admin: " ADMIN_ID_VALUE </dev/tty
  fi

  validate_token "$BOT_TOKEN_VALUE"
  validate_admin_id "$ADMIN_ID_VALUE"

  umask 077
  {
    printf 'BOT_TOKEN=%s\n' "$BOT_TOKEN_VALUE"
    printf 'ADMIN_IDS=%s\n' "$ADMIN_ID_VALUE"
    printf 'DATA_DIR=%s\n' "$DATA_DIR"
  } >"$ENV_FILE"
  chmod 0600 "$ENV_FILE"
  log "Configuration file created: $ENV_FILE"
}

sync_source() {
  install -d -m 0755 "$(dirname "$INSTALL_DIR")"
  if [[ -d "$INSTALL_DIR/.git" ]]; then
    log "Downloading the latest source from GitHub"
    if [[ -n "$(git -C "$INSTALL_DIR" status --porcelain)" ]]; then
      die "Local changes exist in $INSTALL_DIR. Commit or remove them before updating."
    fi
    git -C "$INSTALL_DIR" fetch --prune origin
    git -C "$INSTALL_DIR" checkout -q "$REPOSITORY_REF"
    git -C "$INSTALL_DIR" pull --ff-only origin "$REPOSITORY_REF"
    return
  fi

  if [[ -e "$INSTALL_DIR" ]] \
      && [[ -n "$(find "$INSTALL_DIR" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]]; then
    die "$INSTALL_DIR is not empty and is not a valid Git repository."
  fi
  rmdir "$INSTALL_DIR" 2>/dev/null || true
  log "Cloning source from GitHub"
  git clone --depth 1 --branch "$REPOSITORY_REF" "$REPOSITORY_URL" "$INSTALL_DIR"
}

install_python_runtime() {
  local version
  version="$(tr -d '\r\n ' <"$INSTALL_DIR/VERSION")"
  [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] \
    || die "Invalid VERSION value: $version"

  log "Preparing Python environment for version $version"
  if [[ ! -x "$VENV_DIR/bin/python" ]]; then
    python3 -m venv "$VENV_DIR"
  fi
  "$VENV_DIR/bin/python" -m pip install --disable-pip-version-check --upgrade pip
  "$VENV_DIR/bin/python" -m pip install --disable-pip-version-check -r "$INSTALL_DIR/requirements.txt"

  log "Running compile checks"
  "$VENV_DIR/bin/python" -m compileall -q "$INSTALL_DIR"
  "$VENV_DIR/bin/python" -m py_compile \
    "$INSTALL_DIR/bot.py" \
    "$INSTALL_DIR/config.py" \
    "$INSTALL_DIR/plans.py" \
    "$INSTALL_DIR/storage.py" \
    "$INSTALL_DIR/runtime.py" \
    "$INSTALL_DIR/app_settings.py" \
    "$INSTALL_DIR/services/mikrotik.py" \
    "$INSTALL_DIR/services/xui.py" \
    "$INSTALL_DIR/services/zarinpal.py"

  chown -R root:root "$INSTALL_DIR"
  chmod -R a+rX "$INSTALL_DIR"
  chown -R "$SERVICE_USER:$SERVICE_USER" "$DATA_DIR"
}

install_systemd_service() {
  local unit_tmp
  unit_tmp="$(mktemp)"
  cat >"$unit_tmp" <<EOF
[Unit]
Description=Account Sales Bot
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=$SERVICE_USER
Group=$SERVICE_USER
WorkingDirectory=$INSTALL_DIR
EnvironmentFile=$ENV_FILE
Environment=PYTHONUNBUFFERED=1
Environment=PYTHONDONTWRITEBYTECODE=1
ExecStart=$VENV_DIR/bin/python -u $INSTALL_DIR/bot.py
Restart=always
RestartSec=3
TimeoutStopSec=30
UMask=0027
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true

[Install]
WantedBy=multi-user.target
EOF
  install -m 0644 "$unit_tmp" "$SERVICE_FILE"
  rm -f "$unit_tmp"

  log "Starting the systemd service"
  systemctl daemon-reload
  systemctl enable "$SERVICE_NAME" >/dev/null
  systemctl restart "$SERVICE_NAME"
  sleep 3
  if ! systemctl is-active --quiet "$SERVICE_NAME"; then
    journalctl -u "$SERVICE_NAME" -n 100 --no-pager >&2 || true
    die "The service failed to start."
  fi
}

print_summary() {
  local version
  version="$(tr -d '\r\n ' <"$INSTALL_DIR/VERSION")"
  printf '\nInstallation completed successfully.\n'
  printf 'Version: %s\n' "$version"
  printf 'Source:  %s\n' "$INSTALL_DIR"
  printf 'Config:  %s\n' "$ENV_FILE"
  printf 'Data:    %s\n' "$DATA_DIR"
  printf 'Status:  systemctl status %s\n' "$SERVICE_NAME"
  printf 'Logs:    journalctl -u %s -f\n' "$SERVICE_NAME"
  printf '\nOpen the bot in Telegram, send /start, and complete the service settings in the admin panel.\n'
}

main() {
  require_root
  require_supported_os
  install_dependencies
  ensure_service_user
  create_or_load_config
  verify_telegram_token "$BOT_TOKEN_VALUE"
  sync_source
  install_python_runtime
  install_systemd_service
  print_summary
}

main "$@"
