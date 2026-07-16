#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python main.py factory-only --config config.yaml
