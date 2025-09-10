import importlib.util
import os
import sys
import tempfile
from pathlib import Path

import pytest

import src.conf_path as conf_path_module


def test_env_var_valid_directory(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        custom_conf = Path(tmp) / "myconf"
        custom_conf.mkdir()
        monkeypatch.setenv("CONF_DIR", str(custom_conf))
        conf_dir, invalid = conf_path_module.resolve_conf_dir()
        assert conf_dir == str(custom_conf)
        assert invalid is None


def test_env_var_invalid_directory_falls_back_parent(monkeypatch):
    missing_path = "/nonexistent/path/xyz_does_not_exist"
    monkeypatch.setenv("CONF_DIR", missing_path)
    conf_dir, invalid = conf_path_module.resolve_conf_dir()
    # Should fall back to repository conf directory
    assert conf_dir.endswith(os.path.join("EventGate", "conf"))
    assert invalid == os.path.abspath(missing_path)


def _load_isolated_conf_path(structure_builder):
    """Utility to create an isolated module instance of conf_path with custom directory layout.

    structure_builder receives base temp directory and returns path to module directory containing conf_path.py
    and the code to write (copied from original).
    """
    import inspect

    code = Path(conf_path_module.__file__).read_text(encoding="utf-8")
    tmp = tempfile.TemporaryDirectory()
    base = Path(tmp.name)
    module_dir = structure_builder(base, code)
    module_file = module_dir / "conf_path.py"
    assert module_file.exists()
    spec = importlib.util.spec_from_file_location(f"conf_path_isolated_{id(module_dir)}", module_file)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod  # ensure import works for dependencies
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    # Attach tempdir for cleanup reference
    mod._tmp = tmp  # type: ignore[attr-defined]
    return mod


def test_current_dir_conf_used_when_parent_missing():
    def build(base: Path, code: str):
        module_dir = base / "pkg"
        module_dir.mkdir(parents=True)
        # Create conf inside module directory (current_dir/conf)
        (module_dir / "conf").mkdir()
        # Intentionally DO NOT create parent conf directory (base/conf)
        (module_dir / "conf_path.py").write_text(code, encoding="utf-8")
        return module_dir

    mod = _load_isolated_conf_path(build)
    try:
        conf_dir, invalid = mod.resolve_conf_dir()
        assert conf_dir.endswith("pkg/conf")  # current directory conf chosen
        assert invalid is None
    finally:
        mod._tmp.cleanup()  # type: ignore[attr-defined]


def test_fallback_parent_conf_even_if_missing():
    def build(base: Path, code: str):
        module_dir = base / "pkg2"
        module_dir.mkdir(parents=True)
        # No conf anywhere
        (module_dir / "conf_path.py").write_text(code, encoding="utf-8")
        return module_dir

    mod = _load_isolated_conf_path(build)
    try:
        conf_dir, invalid = mod.resolve_conf_dir()
        # Parent conf path returned even though it does not exist
        expected_parent_conf = (Path(mod.__file__).parent.parent / "conf").resolve()
        assert Path(conf_dir).resolve() == expected_parent_conf
        assert invalid is None
    finally:
        mod._tmp.cleanup()  # type: ignore[attr-defined]


def test_invalid_env_uses_current_conf_when_parent_missing(monkeypatch):
    """Invalid CONF_DIR provided, parent/conf missing, current_dir/conf present -> choose current."""

    def build(base: Path, code: str):
        module_dir = base / "pkg_invalid_current"
        module_dir.mkdir(parents=True)
        (module_dir / "conf").mkdir()
        (module_dir / "conf_path.py").write_text(code, encoding="utf-8")
        return module_dir

    mod = _load_isolated_conf_path(build)
    try:
        bad_path = "/definitely/not/there/abc123"
        monkeypatch.setenv("CONF_DIR", bad_path)
        conf_dir, invalid = mod.resolve_conf_dir()
        assert conf_dir.endswith("pkg_invalid_current/conf")
        assert invalid == os.path.abspath(bad_path)
    finally:
        mod._tmp.cleanup()  # type: ignore[attr-defined]


def test_invalid_env_all_missing_fallback_parent(monkeypatch):
    """Invalid CONF_DIR provided, no parent/conf or current/conf -> fallback parent path returned."""

    def build(base: Path, code: str):
        module_dir = base / "pkg_invalid_all"
        module_dir.mkdir(parents=True)
        (module_dir / "conf_path.py").write_text(code, encoding="utf-8")
        return module_dir

    mod = _load_isolated_conf_path(build)
    try:
        bad_path = "/also/not/there/xyz987"
        monkeypatch.setenv("CONF_DIR", bad_path)
        conf_dir, invalid = mod.resolve_conf_dir()
        expected_parent_conf = (Path(mod.__file__).parent.parent / "conf").resolve()
        assert Path(conf_dir).resolve() == expected_parent_conf
        assert invalid == os.path.abspath(bad_path)
    finally:
        mod._tmp.cleanup()  # type: ignore[attr-defined]


def test_module_level_constants_env_valid(monkeypatch):
    """Ensure module-level CONF_DIR/INVALID_CONF_ENV reflect a valid env path at import time."""
    with tempfile.TemporaryDirectory() as tmp:
        valid_conf = Path(tmp) / "conf"
        valid_conf.mkdir()
        monkeypatch.setenv("CONF_DIR", str(valid_conf))
        # Force fresh import under unique name
        spec = importlib.util.spec_from_file_location("conf_path_valid_mod", conf_path_module.__file__)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)  # type: ignore[attr-defined]
        assert mod.CONF_DIR == str(valid_conf)  # type: ignore[attr-defined]
        assert mod.INVALID_CONF_ENV is None  # type: ignore[attr-defined]


def test_module_level_constants_env_invalid(monkeypatch):
    """Ensure module-level INVALID_CONF_ENV is populated when invalid path provided at import."""
    bad_path = "/no/such/dir/abcXYZ123"
    monkeypatch.setenv("CONF_DIR", bad_path)
    spec = importlib.util.spec_from_file_location("conf_path_invalid_mod", conf_path_module.__file__)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    # Module constant should fall back to repository conf directory
    assert mod.CONF_DIR.endswith(os.path.join("EventGate", "conf"))  # type: ignore[attr-defined]
    assert mod.INVALID_CONF_ENV == os.path.abspath(bad_path)  # type: ignore[attr-defined]
