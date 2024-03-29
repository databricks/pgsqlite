#!/bin/bash
set -exu -o pipefail
set +e
rm -rf dist/*
rm -rf build/*
rm -rf pgsqlite.egg-info
set -e
pip install build bump-my-version twine
pyproject-build
python -m twine check dist/*
python -m twine upload --repository-url https://upload.pypi.org/legacy/ dist/*
