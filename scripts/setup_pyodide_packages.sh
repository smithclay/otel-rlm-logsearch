#!/usr/bin/env bash
# Download Pyodide packages (pandas, pyarrow, etc.) into the local
# Deno/Pyodide cache so they're available inside the RLM sandbox.
#
# DSPy's RLM runs Python in a Pyodide/WASM sandbox via Deno. The npm pyodide
# package ships only the core runtime — no wheels. loadPackagesFromImports()
# can load from the local cache, but the wheels must be present on disk.
#
# Run once after `uv sync` / fresh Deno cache.

set -euo pipefail

PYODIDE_VERSION="0.29.3"
CDN_BASE="https://cdn.jsdelivr.net/pyodide/v${PYODIDE_VERSION}/full"

# Find the Deno cache directory for pyodide
CACHE_DIR="$HOME/Library/Caches/deno/npm/registry.npmjs.org/pyodide/${PYODIDE_VERSION}"
if [ ! -d "$CACHE_DIR" ]; then
    # Linux / other OS
    CACHE_DIR="$HOME/.cache/deno/npm/registry.npmjs.org/pyodide/${PYODIDE_VERSION}"
fi

if [ ! -d "$CACHE_DIR" ]; then
    echo "Error: Pyodide cache not found."
    echo "Run 'uv run python -c \"from dspy.primitives.python_interpreter import PythonInterpreter; PythonInterpreter().shutdown()\"' first to populate it."
    exit 1
fi

echo "Pyodide cache: $CACHE_DIR"
echo "Pyodide version: $PYODIDE_VERSION"
echo ""

# Also download the lock file so loadPackagesFromImports can resolve deps
if [ ! -f "$CACHE_DIR/pyodide-lock.json" ] || [ ! -s "$CACHE_DIR/pyodide-lock.json" ]; then
    echo "  [dl] pyodide-lock.json"
    curl -sL "$CDN_BASE/pyodide-lock.json" -o "$CACHE_DIR/pyodide-lock.json"
else
    echo "  [ok] pyodide-lock.json"
fi

# Packages needed for pandas + pyarrow + their transitive deps
PACKAGES=(
    "micropip-0.11.0-py3-none-any.whl"
    "numpy-2.2.5-cp313-cp313-pyodide_2025_0_wasm32.whl"
    "pandas-2.3.3-cp313-cp313-pyodide_2025_0_wasm32.whl"
    "pyarrow-22.0.0-cp313-cp313-pyodide_2025_0_wasm32.whl"
    "pyodide_unix_timezones-1.0.0-py3-none-any.whl"
    "python_dateutil-2.9.0.post0-py2.py3-none-any.whl"
    "pytz-2025.2-py2.py3-none-any.whl"
    "six-1.17.0-py2.py3-none-any.whl"
)

for pkg in "${PACKAGES[@]}"; do
    if [ -f "$CACHE_DIR/$pkg" ]; then
        echo "  [ok] $pkg"
    else
        echo "  [dl] $pkg"
        curl -sL "$CDN_BASE/$pkg" -o "$CACHE_DIR/$pkg"
    fi
done

echo ""
echo "Done. Verify with:"
echo "  uv run python -c \"from dspy.primitives.python_interpreter import PythonInterpreter; i=PythonInterpreter(); print(i.execute('import pandas; print(pandas.__version__)')); i.shutdown()\""
