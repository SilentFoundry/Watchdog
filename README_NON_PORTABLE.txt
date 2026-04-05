Watchdog - clean non-portable source build
==========================================

This package is the non-portable version.

Storage layout
--------------
The app writes its working files under:

  %APPDATA%\Watchdog

That includes:
- config.json
- state.json
- health.json
- watchdog.log
- watchdog.sqlite3
- notification_history.jsonl
- cache\
- reports\
- snapshots\
- summaries\

Main files
----------
- watchdog.py              core engine and watcher logic
- watchdog_gui.py          desktop GUI entry point
- check_dependencies.py    verifies and optionally installs dependencies
- build_watchdog_exe.ps1   builds the Windows EXE with PyInstaller
- build_watchdog_exe.bat   simple wrapper for the PowerShell build script
- requirements.txt         runtime packages

Run from source
---------------
1. Open a terminal in this folder.
2. Check dependencies:

     python check_dependencies.py

   Or install missing packages:

     python check_dependencies.py --install

3. Launch the GUI:

     python watchdog_gui.py

4. Run the background watcher directly:

     python watchdog_gui.py --watcher

Build the EXE
-------------
Use either of these:

  build_watchdog_exe.bat

or

  powershell -ExecutionPolicy Bypass -File .\build_watchdog_exe.ps1

Expected output:

  dist\WatchdogTerminal\WatchdogTerminal.exe

Notes
-----
- This is not the portable build.
- The EXE still stores its config, cache, logs, database, and reports in %APPDATA%\Watchdog.
- The GUI can install a startup task for the watcher.
