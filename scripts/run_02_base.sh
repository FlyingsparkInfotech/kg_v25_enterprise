#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python main.py load-base --config config.yaml
