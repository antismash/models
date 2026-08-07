#!/bin/bash
set -euo pipefail

rm -rf dist
python -m build
twine upload dist/*
