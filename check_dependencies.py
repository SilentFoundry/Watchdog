from __future__ import annotations

import argparse
import importlib
import importlib.util
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent
REQUIREMENTS_PATH = ROOT / "requirements.txt"

MIN_PYTHON = (3, 11)


@dataclass(frozen=True)
class Dependency:
    package: str
    import_name: str
    purpose: str
    required: bool = True


RUNTIME_DEPENDENCIES = [
    Dependency("requests>=2.31.0", "requests", "HTTP requests and SEC/news downloads"),
    Dependency("beautifulsoup4>=4.12.3", "bs4", "HTML cleanup and text extraction"),
    Dependency("win11toast>=0.35", "win11toast", "Windows 11 toast notifications"),
    Dependency("win10toast>=0.9", "win10toast", "Windows 10 toast fallback"),
]

BUILD_DEPENDENCIES = [
    Dependency("pyinstaller>=6.0", "PyInstaller", "EXE building", required=False),
]


def header(text: str) -> None:
    print(f"\n=== {text} ===")


def version_ok() -> bool:
    return sys.version_info >= MIN_PYTHON


def has_module(import_name: str) -> bool:
    return importlib.util.find_spec(import_name) is not None


def install_packages(packages: list[str]) -> int:
    if not packages:
        return 0
    cmd = [sys.executable, "-m", "pip", "install", *packages]
    print("Installing:", " ".join(packages))
    return subprocess.call(cmd)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check or install Watchdog dependencies.")
    parser.add_argument("--install", action="store_true", help="Install missing runtime packages.")
    parser.add_argument(
        "--include-build",
        action="store_true",
        help="Also check or install build tooling such as PyInstaller.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runtime = list(RUNTIME_DEPENDENCIES)
    build = list(BUILD_DEPENDENCIES) if args.include_build else []
    everything = runtime + build

    header("Python")
    print(f"Executable : {sys.executable}")
    print(f"Version    : {sys.version.split()[0]}")
    print(f"Project    : {ROOT}")
    if REQUIREMENTS_PATH.exists():
        print(f"Requirements file : {REQUIREMENTS_PATH}")
    else:
        print("Requirements file : missing")

    python_ok = version_ok()
    if python_ok:
        print(f"Status     : OK (>= {MIN_PYTHON[0]}.{MIN_PYTHON[1]})")
    else:
        print(f"Status     : FAIL (need >= {MIN_PYTHON[0]}.{MIN_PYTHON[1]})")

    header("Tkinter")
    try:
        import tkinter  # noqa: F401

        print("Status     : OK")
        tkinter_ok = True
    except Exception as exc:
        print(f"Status     : FAIL ({exc})")
        tkinter_ok = False

    header("Packages")
    missing_runtime: list[str] = []
    missing_build: list[str] = []

    for dep in everything:
        present = has_module(dep.import_name)
        label = "OK" if present else "MISSING"
        print(f"{dep.import_name:<15} {label:<8} {dep.package:<24} {dep.purpose}")
        if present:
            continue
        if dep in runtime:
            missing_runtime.append(dep.package)
        else:
            missing_build.append(dep.package)

    if args.install:
        if missing_runtime:
            header("Installing runtime packages")
            code = install_packages(missing_runtime)
            if code != 0:
                print("Runtime package installation failed.")
                return code
        if args.include_build and missing_build:
            header("Installing build packages")
            code = install_packages(missing_build)
            if code != 0:
                print("Build package installation failed.")
                return code

        header("Re-check")
        refreshed_missing = [dep.import_name for dep in everything if not has_module(dep.import_name)]
        if refreshed_missing:
            print("Still missing:", ", ".join(refreshed_missing))
            return 1
        print("All requested dependencies are installed.")
        return 0

    problems: list[str] = []
    if not python_ok:
        problems.append("python")
    if not tkinter_ok:
        problems.append("tkinter")
    if missing_runtime:
        problems.append("runtime packages")
    if args.include_build and missing_build:
        problems.append("build packages")

    if problems:
        print("\nMissing or invalid:", ", ".join(problems))
        print("Run: python check_dependencies.py --install" + (" --include-build" if args.include_build else ""))
        return 1

    print("\nEverything needed is in place.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
