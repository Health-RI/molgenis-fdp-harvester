#!/bin/sh

set -eu

python -m pip install --upgrade pip
pip install dependency-groups
pip-install-dependency-groups lint