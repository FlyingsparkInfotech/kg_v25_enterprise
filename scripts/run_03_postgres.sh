#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python main.py load-postgres --config config.yaml
