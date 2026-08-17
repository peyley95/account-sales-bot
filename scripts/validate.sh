#!/usr/bin/env bash
set -Eeuo pipefail

cd "$(dirname "$0")/.."

bash -n install.sh update.sh scripts/validate.sh

python3 -m compileall -q .
python3 -m py_compile \
  bot.py config.py plans.py storage.py runtime.py app_settings.py account_notifications.py \
  services/mikrotik.py services/xui.py services/zarinpal.py

for test_file in \
  test_resilience.py \
  test_settings.py \
  test_sales_controls.py \
  test_card_transfer.py \
  test_resellers.py \
  test_notifications.py \
  test_reseller_trials.py \
  test_feature_toggles.py \
  test_expiry_notifications.py \
  test_public_release.py
do
  python3 -m unittest discover -s tests -p "$test_file"
done
