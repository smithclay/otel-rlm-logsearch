"""Auto-download Pyodide wheels for the RLM sandbox."""

from __future__ import annotations

import platform
import sys
import urllib.request
from pathlib import Path

PYODIDE_VERSION = "0.29.3"
CDN_BASE = f"https://cdn.jsdelivr.net/pyodide/v{PYODIDE_VERSION}/full"

PACKAGES = [
    "micropip-0.11.0-py3-none-any.whl",
    "numpy-2.2.5-cp313-cp313-pyodide_2025_0_wasm32.whl",
    "pandas-2.3.3-cp313-cp313-pyodide_2025_0_wasm32.whl",
    "pyarrow-22.0.0-cp313-cp313-pyodide_2025_0_wasm32.whl",
    "pyodide_unix_timezones-1.0.0-py3-none-any.whl",
    "python_dateutil-2.9.0.post0-py2.py3-none-any.whl",
    "pytz-2025.2-py2.py3-none-any.whl",
    "six-1.17.0-py2.py3-none-any.whl",
]


def _find_cache_dir() -> Path | None:
    """Locate the Deno cache directory for pyodide."""
    home = Path.home()
    if platform.system() == "Darwin":
        candidate = (
            home
            / "Library/Caches/deno/npm/registry.npmjs.org/pyodide"
            / PYODIDE_VERSION
        )
        if candidate.is_dir():
            return candidate
    candidate = home / ".cache/deno/npm/registry.npmjs.org/pyodide" / PYODIDE_VERSION
    if candidate.is_dir():
        return candidate
    return None


def ensure_pyodide_packages() -> None:
    """Download Pyodide wheels if missing. Idempotent and silent when cached."""
    cache_dir = _find_cache_dir()
    if cache_dir is None:
        print(
            "Pyodide cache not found — initializing Deno runtime...",
            file=sys.stderr,
        )
        from dspy.primitives.python_interpreter import PythonInterpreter

        interp = PythonInterpreter()
        interp.shutdown()
        cache_dir = _find_cache_dir()
        if cache_dir is None:
            print("Warning: could not locate Pyodide cache after init", file=sys.stderr)
            return

    files_to_download = ["pyodide-lock.json"] + PACKAGES
    for name in files_to_download:
        dest = cache_dir / name
        if dest.exists() and dest.stat().st_size > 0:
            continue
        print(f"  Downloading {name}...", file=sys.stderr)
        urllib.request.urlretrieve(f"{CDN_BASE}/{name}", dest)
