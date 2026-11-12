#!/usr/bin/env bash
# Build the sdist and wheel into dist/, check their metadata, and install the wheel
# (with the `api` extra) into a fresh virtualenv outside the checkout, where nothing
# can be imported from the source tree. The release workflow runs exactly this.
set -euo pipefail

cd "$(dirname "$0")/.."
rm -rf dist
uv build --out-dir dist
uvx --from 'twine==7.0.0' twine check --strict dist/*

version=$(python3 -c 'import tomllib; print(tomllib.load(open("pyproject.toml", "rb"))["project"]["version"])')
wheel=$(ls dist/*.whl)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

uv venv --quiet --python 3.12 "$work/venv"
uv pip install --quiet --python "$work/venv/bin/python" "${wheel}[api]"
cd "$work"
installed=$("$work/venv/bin/tributary" version)
test "$installed" = "$version" || { echo "installed $installed, expected $version"; exit 1; }
"$work/venv/bin/tributary" --help > /dev/null
"$work/venv/bin/python" - <<'PY'
import importlib.resources
import tributary.agent.guard as guard
from tributary.api.app import create_app

assert importlib.resources.files("tributary").joinpath("py.typed").is_file()
assert importlib.resources.files("tributary").joinpath("sql/raw.sql").is_file()
assert guard.__file__ and "site-packages" in guard.__file__, guard.__file__
create_app(lambda: None, ["k"])
PY
echo "dist OK: tributary-platform $version ($(basename "$wheel"))"
