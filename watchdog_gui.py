from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time
import traceback
import webbrowser
from datetime import datetime
from pathlib import Path
from tkinter import BOTH, END, LEFT, RIGHT, VERTICAL, W, X, Y, messagebox, ttk
import tkinter as tk

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import watchdog as wd

BG = "#0f1115"
PANEL = "#151922"
PANEL_2 = "#11161d"
TEXT = "#d7dde8"
MUTED = "#8a94a6"
ACCENT = "#2f81f7"
ACCENT_2 = "#1f6feb"
GRID = "#263041"
SELECT = "#20304a"
FONT_UI = ("Segoe UI", 10)
FONT_CODE = ("Consolas", 10)


class TextHandler:
    def __init__(self, callback):
        self.callback = callback

    def write(self, message: str) -> None:
        if message:
            self.callback(message)

    def flush(self) -> None:
        return None


class SortableTree(ttk.Treeview):
    def __init__(self, master, columns: list[str], **kwargs):
        super().__init__(master, columns=columns, show="headings", **kwargs)
        self._sort_desc: dict[str, bool] = {}
        for col in columns:
            self.heading(col, text=col, command=lambda c=col: self.sort_by(c))

    def sort_by(self, col: str) -> None:
        rows = [(self.set(k, col), k) for k in self.get_children("")]
        desc = self._sort_desc.get(col, False)
        rows.sort(key=lambda x: self._sort_key(x[0]), reverse=desc)
        for idx, (_, item_id) in enumerate(rows):
            self.move(item_id, "", idx)
        self._sort_desc[col] = not desc

    @staticmethod
    def _sort_key(value: str):
        value = (value or "").strip()
        if not value:
            return (4, "")
        for parser in (
            lambda v: datetime.fromisoformat(v.replace("Z", "+00:00")),
            lambda v: datetime.strptime(v, "%Y-%m-%d"),
            lambda v: datetime.strptime(v, "%a, %d %b %Y %H:%M:%S %Z"),
        ):
            try:
                return (0, parser(value))
            except Exception:
                pass
        try:
            return (1, float(value.replace(",", "")))
        except Exception:
            pass
        return (3, value.lower())


class WatchdogGUI(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Watchdog // Terminal")
        self.geometry("1550x980")
        self.minsize(1260, 780)
        self.configure(bg=BG)

        self.watch_process: subprocess.Popen | None = None
        self.external_pid: int | None = None
        self.run_thread: threading.Thread | None = None
        self.tail_position = 0
        self.last_log_mtime = 0.0
        self.last_diag_refresh = 0.0
        self.diag_refresh_interval_idle = 20.0
        self.diag_refresh_interval_visible = 5.0

        self.tickers_var = tk.StringVar()
        self.user_agent_var = tk.StringVar()
        self.poll_var = tk.StringVar()
        self.status_var = tk.StringVar(value="Idle")
        self.status_detail_var = tk.StringVar(value="")
        self.autoscroll_var = tk.BooleanVar(value=True)

        self.overview_search_var = tk.StringVar()
        self.filing_ticker_var = tk.StringVar(value="All")
        self.filing_form_var = tk.StringVar(value="All")
        self.filing_search_var = tk.StringVar()
        self.contract_ticker_var = tk.StringVar(value="All")
        self.contract_search_var = tk.StringVar()
        self.news_ticker_var = tk.StringVar(value="All")
        self.news_severity_var = tk.StringVar(value="All")
        self.news_search_var = tk.StringVar()
        self.notification_kind_var = tk.StringVar(value="All")
        self.notification_ticker_var = tk.StringVar(value="All")
        self.notification_search_var = tk.StringVar()

        self.overview_records: dict[str, dict] = {}
        self.filing_records: dict[str, dict] = {}
        self.contract_records: dict[str, dict] = {}
        self.news_records: dict[str, dict] = {}
        self.notification_records: dict[str, dict] = {}

        self._build_style()
        self._build_ui()
        self._wire_traces()
        self.load_config_into_form()
        self.refresh_all()
        self.after(1200, self.poll_external_state)
        self.protocol("WM_DELETE_WINDOW", self.on_close)

    def _build_style(self) -> None:
        style = ttk.Style(self)
        style.theme_use("clam")
        self.option_add("*Font", FONT_UI)
        self.option_add("*Text.Background", PANEL)
        self.option_add("*Text.Foreground", TEXT)
        self.option_add("*Text.insertBackground", TEXT)
        style.configure("TFrame", background=BG)
        style.configure("Panel.TFrame", background=PANEL)
        style.configure("Panel2.TFrame", background=PANEL_2)
        style.configure("TLabel", background=BG, foreground=TEXT)
        style.configure("Muted.TLabel", background=BG, foreground=MUTED)
        style.configure("Title.TLabel", background=BG, foreground=TEXT, font=("Segoe UI Semibold", 14))
        style.configure("Status.TLabel", background=BG, foreground=MUTED)
        style.configure("TCheckbutton", background=BG, foreground=MUTED)
        style.map("TCheckbutton", foreground=[("active", TEXT), ("selected", TEXT)])
        style.configure(
            "TButton",
            background=PANEL,
            foreground=TEXT,
            bordercolor=GRID,
            lightcolor=PANEL,
            darkcolor=PANEL,
            padding=(10, 7),
        )
        style.map("TButton", background=[("active", PANEL_2), ("pressed", SELECT)])
        style.configure(
            "Accent.TButton",
            background=ACCENT,
            foreground="#ffffff",
            bordercolor=ACCENT,
            lightcolor=ACCENT,
            darkcolor=ACCENT,
            padding=(10, 7),
        )
        style.map("Accent.TButton", background=[("active", ACCENT_2), ("pressed", ACCENT_2)])
        style.configure("TEntry", fieldbackground=PANEL, foreground=TEXT, insertcolor=TEXT, bordercolor=GRID)
        style.configure("TCombobox", fieldbackground=PANEL, foreground=TEXT, bordercolor=GRID, arrowsize=14)
        style.map("TCombobox", fieldbackground=[("readonly", PANEL)], foreground=[("readonly", TEXT)])
        style.configure("TNotebook", background=BG, borderwidth=0)
        style.configure("TNotebook.Tab", background=PANEL_2, foreground=MUTED, padding=(14, 8), borderwidth=1)
        style.map("TNotebook.Tab", background=[("selected", PANEL), ("active", PANEL)], foreground=[("selected", TEXT), ("active", TEXT)])
        style.configure(
            "Treeview",
            background=PANEL,
            fieldbackground=PANEL,
            foreground=TEXT,
            rowheight=28,
            bordercolor=GRID,
            lightcolor=PANEL,
            darkcolor=PANEL,
        )
        style.configure(
            "Treeview.Heading",
            background=PANEL_2,
            foreground=TEXT,
            bordercolor=GRID,
            padding=(8, 7),
            font=("Segoe UI Semibold", 10),
        )
        style.map("Treeview", background=[("selected", SELECT)], foreground=[("selected", TEXT)])
        style.configure("Vertical.TScrollbar", background=PANEL, troughcolor=BG, arrowcolor=MUTED, bordercolor=GRID)

    def _build_ui(self) -> None:
        root = ttk.Frame(self, padding=14)
        root.pack(fill=BOTH, expand=True)

        header = ttk.Frame(root)
        header.pack(fill=X, pady=(0, 10))
        ttk.Label(header, text="Watchdog", style="Title.TLabel").pack(side=LEFT)
        ttk.Label(header, text="terminal v2", style="Muted.TLabel").pack(side=LEFT, padx=(10, 0), pady=(3, 0))

        form = ttk.Frame(root, style="Panel.TFrame", padding=12)
        form.pack(fill=X, pady=(0, 10))
        ttk.Label(form, text="Ticker(s)").grid(row=0, column=0, sticky=W, padx=(0, 8), pady=(0, 8))
        ttk.Entry(form, textvariable=self.tickers_var, width=42).grid(row=0, column=1, sticky="ew", padx=(0, 18), pady=(0, 8))
        ttk.Label(form, text="SEC User-Agent").grid(row=0, column=2, sticky=W, padx=(0, 8), pady=(0, 8))
        ttk.Entry(form, textvariable=self.user_agent_var, width=58).grid(row=0, column=3, sticky="ew", padx=(0, 18), pady=(0, 8))
        ttk.Label(form, text="Poll minutes").grid(row=0, column=4, sticky=W, padx=(0, 8), pady=(0, 8))
        ttk.Entry(form, textvariable=self.poll_var, width=10).grid(row=0, column=5, sticky=W, pady=(0, 8))
        form.columnconfigure(1, weight=1)
        form.columnconfigure(3, weight=2)

        toolbar = ttk.Frame(root)
        toolbar.pack(fill=X, pady=(0, 10))
        ttk.Button(toolbar, text="Save Config", command=self.save_config).pack(side=LEFT, padx=(0, 8))
        ttk.Button(toolbar, text="Run Once", style="Accent.TButton", command=self.run_once_clicked).pack(side=LEFT, padx=(0, 8))
        ttk.Button(toolbar, text="Start / Attach Watcher", command=self.start_background).pack(side=LEFT, padx=(0, 8))
        ttk.Button(toolbar, text="Stop Watcher", command=self.stop_background).pack(side=LEFT, padx=(0, 8))
        ttk.Button(toolbar, text="Install Startup Task", command=self.install_task).pack(side=LEFT, padx=(0, 8))
        ttk.Button(toolbar, text="Refresh", command=self.refresh_all).pack(side=LEFT, padx=(0, 8))
        ttk.Button(toolbar, text="Open App Folder", command=lambda: self.open_path(wd.explorer_target_dir())).pack(side=LEFT, padx=(0, 8))
        ttk.Button(toolbar, text="Open DB", command=lambda: self.open_path(wd.DB_PATH)).pack(side=LEFT, padx=(0, 8))
        ttk.Button(toolbar, text="Open Log", command=lambda: self.open_path(wd.LOG_PATH)).pack(side=LEFT, padx=(0, 8))
        ttk.Checkbutton(toolbar, text="Autoscroll log", variable=self.autoscroll_var).pack(side=RIGHT)

        status = ttk.Frame(root)
        status.pack(fill=X, pady=(0, 10))
        ttk.Label(status, text="Status:", style="Status.TLabel").pack(side=LEFT)
        ttk.Label(status, textvariable=self.status_var).pack(side=LEFT, padx=(6, 10))
        ttk.Label(status, textvariable=self.status_detail_var, style="Muted.TLabel").pack(side=LEFT)

        self.notebook = ttk.Notebook(root)
        self.notebook.pack(fill=BOTH, expand=True)

        self.overview_tab = ttk.Frame(self.notebook, style="Panel.TFrame", padding=10)
        self.notebook.add(self.overview_tab, text="Overview")
        ov_filters = ttk.Frame(self.overview_tab)
        ov_filters.pack(fill=X, pady=(0, 8))
        ttk.Label(ov_filters, text="Search", style="Muted.TLabel").pack(side=LEFT)
        ttk.Entry(ov_filters, textvariable=self.overview_search_var, width=42).pack(side=LEFT, padx=(6, 10))
        ov_pane = ttk.Panedwindow(self.overview_tab, orient="horizontal")
        ov_pane.pack(fill=BOTH, expand=True)
        ov_left = ttk.Frame(ov_pane, style="Panel.TFrame")
        ov_right = ttk.Frame(ov_pane, style="Panel.TFrame")
        ov_pane.add(ov_left, weight=3)
        ov_pane.add(ov_right, weight=2)
        self.overview_tree = self._make_tree(
            ov_left,
            ["Ticker", "Company", "Generated", "Pipeline", "Investments", "Contracts", "News"],
            widths={"Ticker": 90, "Company": 320, "Generated": 190, "Pipeline": 90, "Investments": 100, "Contracts": 90, "News": 70},
        )
        self.overview_tree.bind("<<TreeviewSelect>>", lambda _e: self.on_overview_select())
        self.overview_detail = self._make_text_panel(ov_right)

        self.filings_tab = ttk.Frame(self.notebook, style="Panel.TFrame", padding=10)
        self.notebook.add(self.filings_tab, text="Filings")
        filing_filters = ttk.Frame(self.filings_tab)
        filing_filters.pack(fill=X, pady=(0, 8))
        ttk.Label(filing_filters, text="Ticker", style="Muted.TLabel").pack(side=LEFT)
        self.filing_ticker_combo = ttk.Combobox(filing_filters, textvariable=self.filing_ticker_var, width=12, state="readonly")
        self.filing_ticker_combo.pack(side=LEFT, padx=(6, 10))
        ttk.Label(filing_filters, text="Form", style="Muted.TLabel").pack(side=LEFT)
        self.filing_form_combo = ttk.Combobox(filing_filters, textvariable=self.filing_form_var, width=12, state="readonly")
        self.filing_form_combo.pack(side=LEFT, padx=(6, 10))
        ttk.Label(filing_filters, text="Search", style="Muted.TLabel").pack(side=LEFT)
        ttk.Entry(filing_filters, textvariable=self.filing_search_var, width=42).pack(side=LEFT, padx=(6, 10))
        self.filings_tree, self.filings_detail = self._make_table_with_detail(
            self.filings_tab,
            ["Ticker", "Form", "Filing Date", "Description", "URL", "Local Path"],
            widths={"Ticker": 90, "Form": 90, "Filing Date": 120, "Description": 260, "URL": 290, "Local Path": 340},
        )
        self.filings_tree.bind("<<TreeviewSelect>>", lambda _e: self.on_filing_select())
        self.filings_tree.bind("<Double-1>", lambda _e: self.open_selected_filing_url())
        filing_btns = ttk.Frame(self.filings_tab)
        filing_btns.pack(fill=X, pady=(8, 0))
        ttk.Button(filing_btns, text="Open SEC Link", command=self.open_selected_filing_url).pack(side=LEFT)
        ttk.Button(filing_btns, text="Open Local File", command=self.open_selected_filing_local).pack(side=LEFT, padx=(8, 0))
        ttk.Button(filing_btns, text="Open Reports Folder", command=self.open_selected_reports).pack(side=LEFT, padx=(8, 0))

        self.contracts_tab = ttk.Frame(self.notebook, style="Panel.TFrame", padding=10)
        self.notebook.add(self.contracts_tab, text="Contracts")
        contract_filters = ttk.Frame(self.contracts_tab)
        contract_filters.pack(fill=X, pady=(0, 8))
        ttk.Label(contract_filters, text="Ticker", style="Muted.TLabel").pack(side=LEFT)
        self.contract_ticker_combo = ttk.Combobox(contract_filters, textvariable=self.contract_ticker_var, width=12, state="readonly")
        self.contract_ticker_combo.pack(side=LEFT, padx=(6, 10))
        ttk.Label(contract_filters, text="Search", style="Muted.TLabel").pack(side=LEFT)
        ttk.Entry(contract_filters, textvariable=self.contract_search_var, width=52).pack(side=LEFT, padx=(6, 10))
        self.contracts_tree, self.contracts_detail = self._make_table_with_detail(
            self.contracts_tab,
            ["Ticker", "Filing Date", "Form", "Title", "URL"],
            widths={"Ticker": 90, "Filing Date": 120, "Form": 90, "Title": 520, "URL": 400},
        )
        self.contracts_tree.bind("<<TreeviewSelect>>", lambda _e: self.on_contract_select())
        self.contracts_tree.bind("<Double-1>", lambda _e: self.open_selected_contract())
        contract_btns = ttk.Frame(self.contracts_tab)
        contract_btns.pack(fill=X, pady=(8, 0))
        ttk.Button(contract_btns, text="Open Exhibit Link", command=self.open_selected_contract).pack(side=LEFT)

        self.news_tab = ttk.Frame(self.notebook, style="Panel.TFrame", padding=10)
        self.notebook.add(self.news_tab, text="News")
        news_filters = ttk.Frame(self.news_tab)
        news_filters.pack(fill=X, pady=(0, 8))
        ttk.Label(news_filters, text="Ticker", style="Muted.TLabel").pack(side=LEFT)
        self.news_ticker_combo = ttk.Combobox(news_filters, textvariable=self.news_ticker_var, width=12, state="readonly")
        self.news_ticker_combo.pack(side=LEFT, padx=(6, 10))
        ttk.Label(news_filters, text="Severity", style="Muted.TLabel").pack(side=LEFT)
        self.news_severity_combo = ttk.Combobox(news_filters, textvariable=self.news_severity_var, width=12, state="readonly")
        self.news_severity_combo.pack(side=LEFT, padx=(6, 10))
        ttk.Label(news_filters, text="Search", style="Muted.TLabel").pack(side=LEFT)
        ttk.Entry(news_filters, textvariable=self.news_search_var, width=52).pack(side=LEFT, padx=(6, 10))
        self.news_tree, self.news_detail = self._make_table_with_detail(
            self.news_tab,
            ["Ticker", "Severity", "Score", "Published", "Source", "Title", "Link"],
            widths={"Ticker": 90, "Severity": 90, "Score": 70, "Published": 190, "Source": 150, "Title": 520, "Link": 300},
        )
        self.news_tree.bind("<<TreeviewSelect>>", lambda _e: self.on_news_select())
        self.news_tree.bind("<Double-1>", lambda _e: self.open_selected_news())
        self.news_tree.tag_configure("critical", background="#3b1f24")
        self.news_tree.tag_configure("high", background="#38290d")
        self.news_tree.tag_configure("medium", background="#1a2636")
        news_btns = ttk.Frame(self.news_tab)
        news_btns.pack(fill=X, pady=(8, 0))
        ttk.Button(news_btns, text="Open Article", command=self.open_selected_news).pack(side=LEFT)

        self.notifications_tab = ttk.Frame(self.notebook, style="Panel.TFrame", padding=10)
        self.notebook.add(self.notifications_tab, text="Notification History")
        notif_filters = ttk.Frame(self.notifications_tab)
        notif_filters.pack(fill=X, pady=(0, 8))
        ttk.Label(notif_filters, text="Kind", style="Muted.TLabel").pack(side=LEFT)
        self.notification_kind_combo = ttk.Combobox(notif_filters, textvariable=self.notification_kind_var, width=12, state="readonly")
        self.notification_kind_combo.pack(side=LEFT, padx=(6, 10))
        ttk.Label(notif_filters, text="Ticker", style="Muted.TLabel").pack(side=LEFT)
        self.notification_ticker_combo = ttk.Combobox(notif_filters, textvariable=self.notification_ticker_var, width=12, state="readonly")
        self.notification_ticker_combo.pack(side=LEFT, padx=(6, 10))
        ttk.Label(notif_filters, text="Search", style="Muted.TLabel").pack(side=LEFT)
        ttk.Entry(notif_filters, textvariable=self.notification_search_var, width=52).pack(side=LEFT, padx=(6, 10))
        self.notifications_tree, self.notifications_detail = self._make_table_with_detail(
            self.notifications_tab,
            ["When", "Type", "Ticker", "Title", "Link"],
            widths={"When": 190, "Type": 90, "Ticker": 90, "Title": 760, "Link": 270},
        )
        self.notifications_tree.bind("<<TreeviewSelect>>", lambda _e: self.on_notification_select())
        self.notifications_tree.bind("<Double-1>", lambda _e: self.open_selected_notification())
        notif_btns = ttk.Frame(self.notifications_tab)
        notif_btns.pack(fill=X, pady=(8, 0))
        ttk.Button(notif_btns, text="Open Selected", command=self.open_selected_notification).pack(side=LEFT)
        ttk.Button(notif_btns, text="Open History File", command=lambda: self.open_path(wd.NOTIFICATION_LOG_PATH)).pack(side=LEFT, padx=(8, 0))

        self.diagnostics_tab = ttk.Frame(self.notebook, style="Panel.TFrame", padding=10)
        self.notebook.add(self.diagnostics_tab, text="Diagnostics")
        diag_btns = ttk.Frame(self.diagnostics_tab)
        diag_btns.pack(fill=X, pady=(0, 8))
        ttk.Button(diag_btns, text="Refresh Diagnostics", command=self.refresh_diagnostics).pack(side=LEFT)
        ttk.Button(diag_btns, text="Query Task", command=self.refresh_diagnostics).pack(side=LEFT, padx=(8, 0))
        ttk.Button(diag_btns, text="Stop Known Watcher", command=self.stop_background).pack(side=LEFT, padx=(8, 0))
        diag_pane = ttk.Panedwindow(self.diagnostics_tab, orient="horizontal")
        diag_pane.pack(fill=BOTH, expand=True)
        diag_left = ttk.Frame(diag_pane, style="Panel.TFrame")
        diag_right = ttk.Frame(diag_pane, style="Panel.TFrame")
        diag_pane.add(diag_left, weight=3)
        diag_pane.add(diag_right, weight=2)
        ttk.Label(diag_left, text="Health / runtime", style="Muted.TLabel").pack(anchor=W, pady=(0, 6))
        self.health_text = self._make_text_panel(diag_left)
        ttk.Label(diag_right, text="Task / paths / counts", style="Muted.TLabel").pack(anchor=W, pady=(0, 6))
        self.task_text = self._make_text_panel(diag_right)

        self.log_tab = ttk.Frame(self.notebook, style="Panel.TFrame", padding=10)
        self.notebook.add(self.log_tab, text="Log")
        ttk.Label(self.log_tab, text="Activity log", style="Muted.TLabel").pack(anchor=W, pady=(0, 6))
        log_wrap = ttk.Frame(self.log_tab, style="Panel.TFrame")
        log_wrap.pack(fill=BOTH, expand=True)
        self.log_text = tk.Text(log_wrap, wrap="word", bg=PANEL, fg=TEXT, insertbackground=TEXT, relief="flat", font=FONT_CODE)
        self.log_text.pack(side=LEFT, fill=BOTH, expand=True)
        log_scroll = ttk.Scrollbar(log_wrap, orient=VERTICAL, command=self.log_text.yview)
        log_scroll.pack(side=RIGHT, fill=Y)
        self.log_text.configure(yscrollcommand=log_scroll.set)
        log_btns = ttk.Frame(self.log_tab)
        log_btns.pack(fill=X, pady=(8, 0))
        ttk.Button(log_btns, text="Clear Log View", command=lambda: self.log_text.delete("1.0", END)).pack(side=LEFT)
        ttk.Button(log_btns, text="Open Raw Log", command=lambda: self.open_path(wd.LOG_PATH)).pack(side=LEFT, padx=(8, 0))

    def _wire_traces(self) -> None:
        for var, callback in [
            (self.overview_search_var, self.refresh_overview),
            (self.filing_ticker_var, self.refresh_filings),
            (self.filing_form_var, self.refresh_filings),
            (self.filing_search_var, self.refresh_filings),
            (self.contract_ticker_var, self.refresh_contracts),
            (self.contract_search_var, self.refresh_contracts),
            (self.news_ticker_var, self.refresh_news),
            (self.news_severity_var, self.refresh_news),
            (self.news_search_var, self.refresh_news),
            (self.notification_kind_var, self.refresh_notifications),
            (self.notification_ticker_var, self.refresh_notifications),
            (self.notification_search_var, self.refresh_notifications),
        ]:
            var.trace_add("write", lambda *_args, cb=callback: cb())

    def _make_tree(self, parent, columns: list[str], widths: dict[str, int] | None = None) -> SortableTree:
        frame = ttk.Frame(parent, style="Panel.TFrame")
        frame.pack(fill=BOTH, expand=True)
        tree = SortableTree(frame, columns=columns, selectmode="browse")
        tree.pack(side=LEFT, fill=BOTH, expand=True)
        scroll = ttk.Scrollbar(frame, orient=VERTICAL, command=tree.yview)
        scroll.pack(side=RIGHT, fill=Y)
        tree.configure(yscrollcommand=scroll.set)
        widths = widths or {}
        for col in columns:
            tree.column(col, width=widths.get(col, 140), anchor=W, stretch=True)
        return tree

    def _make_text_panel(self, parent) -> tk.Text:
        text = tk.Text(parent, wrap="word", bg=PANEL, fg=TEXT, insertbackground=TEXT, relief="flat", font=FONT_CODE)
        text.pack(fill=BOTH, expand=True)
        return text

    def _make_table_with_detail(self, parent, columns: list[str], widths: dict[str, int] | None = None):
        pane = ttk.Panedwindow(parent, orient="vertical")
        pane.pack(fill=BOTH, expand=True)
        top = ttk.Frame(pane, style="Panel.TFrame")
        bottom = ttk.Frame(pane, style="Panel.TFrame")
        pane.add(top, weight=3)
        pane.add(bottom, weight=2)
        tree = self._make_tree(top, columns, widths)
        detail = self._make_text_panel(bottom)
        return tree, detail

    def clear_tree(self, tree: ttk.Treeview) -> None:
        for item in tree.get_children(""):
            tree.delete(item)

    def _selected_record(self, tree: ttk.Treeview, store: dict[str, dict]) -> dict | None:
        selected = tree.selection()
        if not selected:
            return None
        return store.get(selected[0])

    def append_log(self, text: str) -> None:
        def _append() -> None:
            self.log_text.insert(END, text)
            if self.autoscroll_var.get():
                self.log_text.see(END)
        self.after(0, _append)

    def open_path(self, path: Path) -> None:
        try:
            if os.name == "nt":
                os.startfile(str(path))  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(path)])
            else:
                subprocess.Popen(["xdg-open", str(path)])
        except Exception as exc:
            messagebox.showerror("Open failed", str(exc))

    def open_url(self, url: str) -> None:
        if not url:
            return
        try:
            webbrowser.open(url)
        except Exception as exc:
            messagebox.showerror("Open failed", str(exc))

    def load_config_into_form(self) -> None:
        wd.ensure_dirs()
        config = wd.merged_config()
        self.tickers_var.set(", ".join(config.get("tickers", [])))
        self.user_agent_var.set(config.get("user_agent", wd.DEFAULT_CONFIG["user_agent"]))
        self.poll_var.set(str(config.get("poll_minutes", wd.DEFAULT_CONFIG["poll_minutes"])))

    def current_config(self) -> dict:
        cfg = wd.merged_config()
        cfg["tickers"] = [wd.normalize_ticker(t) for t in self.tickers_var.get().split(",") if wd.normalize_ticker(t)]
        cfg["user_agent"] = self.user_agent_var.get().strip() or wd.DEFAULT_CONFIG["user_agent"]
        poll_text = self.poll_var.get().strip()
        if not poll_text.isdigit() or int(poll_text) <= 0:
            raise ValueError("Poll minutes must be a whole number greater than 0.")
        cfg["poll_minutes"] = int(poll_text)
        if not cfg["tickers"]:
            raise ValueError("Enter at least one ticker.")
        return cfg

    def save_config(self) -> bool:
        try:
            cfg = self.current_config()
            wd.ensure_dirs()
            wd.save_json(wd.CONFIG_PATH, cfg)
            self.status_var.set(f"Config saved // {wd.CONFIG_PATH}")
            self.append_log(f"Saved config to {wd.CONFIG_PATH}\n")
            self.refresh_all()
            return True
        except Exception as exc:
            messagebox.showerror("Config error", str(exc))
            self.status_var.set("Config error")
            return False

    def watcher_command(self) -> tuple[list[str], int]:
        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0
        if getattr(sys, "frozen", False):
            return [str(Path(sys.executable)), "--watcher"], creationflags
        python_exe = Path(sys.executable)
        pythonw_exe = python_exe.with_name("pythonw.exe")
        runner = str(pythonw_exe if pythonw_exe.exists() else python_exe)
        script = str((HERE / "watchdog_gui.py").resolve())
        return [runner, script, "--watcher"], creationflags

    def known_watcher_pid(self) -> int | None:
        health = wd.load_health()
        pid = health.get("pid")
        if isinstance(pid, int) and wd.is_process_running(pid):
            self.external_pid = pid
            return pid
        self.external_pid = None
        return None

    def run_once_clicked(self) -> None:
        if self.run_thread and self.run_thread.is_alive():
            messagebox.showinfo("Already running", "A scan is already in progress.")
            return
        if not self.save_config():
            return

        def worker() -> None:
            cfg = self.current_config()
            self.after(0, lambda: self.status_var.set("Running one scan"))
            original_stdout, original_stderr = sys.stdout, sys.stderr
            handler = TextHandler(self.append_log)
            try:
                sys.stdout = handler
                sys.stderr = handler
                wd.run_once(cfg)
                self.append_log("\nRun Once completed.\n")
                self.after(0, lambda: self.status_var.set("Run Once completed"))
                self.after(0, self.refresh_all)
            except Exception:
                self.append_log(traceback.format_exc() + "\n")
                self.after(0, lambda: self.status_var.set("Run Once failed"))
            finally:
                sys.stdout = original_stdout
                sys.stderr = original_stderr

        self.run_thread = threading.Thread(target=worker, daemon=True)
        self.run_thread.start()

    def start_background(self) -> None:
        pid = self.known_watcher_pid()
        if pid:
            self.status_var.set(f"Attached to running watcher // PID {pid}")
            self.status_detail_var.set("Watcher already running")
            self.append_log(f"Attached to existing watcher PID {pid}\n")
            return
        if not self.save_config():
            return
        try:
            cmd, creationflags = self.watcher_command()
            self.watch_process = subprocess.Popen(cmd, cwd=str(wd.RUNTIME_ROOT), creationflags=creationflags)
            self.external_pid = self.watch_process.pid
            self.status_var.set(f"Background watcher running // PID {self.watch_process.pid}")
            self.status_detail_var.set("GUI launched watcher")
            self.append_log(f"Started background watcher PID {self.watch_process.pid}\n")
        except Exception as exc:
            self.status_var.set("Start failed")
            self.append_log(f"Start failed: {exc}\n")
            messagebox.showerror("Start failed", str(exc))

    def stop_background(self) -> None:
        pid = self.known_watcher_pid() or (self.watch_process.pid if self.watch_process and self.watch_process.poll() is None else None)
        if not pid:
            self.status_var.set("No running watcher found")
            return
        ok = wd.stop_running_watcher(pid)
        if ok:
            self.status_var.set("Watcher stopped")
            self.status_detail_var.set(f"Stopped PID {pid}")
            self.append_log(f"Stopped watcher PID {pid}\n")
        else:
            self.status_var.set("Stop failed")
            self.append_log(f"Stop failed for PID {pid}\n")
        self.watch_process = None
        self.external_pid = None
        self.refresh_diagnostics()

    def install_task(self) -> None:
        if not self.save_config():
            return
        try:
            if getattr(sys, "frozen", False):
                command = f'"{Path(sys.executable)}" --watcher'
            else:
                python_exe = Path(sys.executable)
                pythonw_exe = python_exe.with_name("pythonw.exe")
                runner = pythonw_exe if pythonw_exe.exists() else python_exe
                command = f'"{runner}" "{(HERE / "watchdog_gui.py").resolve()}" --watcher'
            cfg = self.current_config()
            wd.install_startup_task(command=command, delay_minutes=int(cfg.get("task_start_delay_minutes", 1)))
            self.append_log("Installed Windows startup task.\n")
            self.status_var.set("Startup task installed")
            messagebox.showinfo("Installed", "Startup task installed. The watcher will launch at logon.")
            self.refresh_diagnostics()
        except Exception as exc:
            self.status_var.set("Task install failed")
            messagebox.showerror("Task install failed", str(exc))

    def refresh_all(self) -> None:
        self.refresh_overview()
        self.refresh_filings()
        self.refresh_contracts()
        self.refresh_news()
        self.refresh_notifications()
        self.refresh_diagnostics()

    def configured_tickers(self) -> list[str]:
        tickers = [wd.normalize_ticker(t) for t in self.tickers_var.get().split(",") if wd.normalize_ticker(t)]
        if tickers:
            return tickers
        config = wd.merged_config()
        return [wd.normalize_ticker(t) for t in config.get("tickers", []) if wd.normalize_ticker(t)]

    def read_snapshot(self, ticker: str) -> dict:
        return wd.load_json(wd.company_dir(ticker) / "latest_snapshot.json", {})

    def refresh_filter_values(self) -> None:
        tickers = ["All", *self.configured_tickers()]
        forms = {"All"}
        for ticker in self.configured_tickers():
            snap = self.read_snapshot(ticker)
            for filing in snap.get("recent_filings", []):
                forms.add(filing.get("form", ""))
        form_values = [x for x in ["All", *sorted(f for f in forms if f and f != "All")]]
        self.filing_ticker_combo["values"] = tickers
        self.contract_ticker_combo["values"] = tickers
        self.news_ticker_combo["values"] = tickers
        self.notification_ticker_combo["values"] = tickers
        self.filing_form_combo["values"] = form_values
        self.news_severity_combo["values"] = ["All", "critical", "high", "medium", "low"]
        self.notification_kind_combo["values"] = ["All", "news", "filing"]
        for var in [self.filing_ticker_var, self.contract_ticker_var, self.news_ticker_var, self.notification_ticker_var]:
            if var.get() not in tickers:
                var.set("All")
        if self.filing_form_var.get() not in form_values:
            self.filing_form_var.set("All")
        if self.news_severity_var.get() not in ["All", "critical", "high", "medium", "low"]:
            self.news_severity_var.set("All")
        if self.notification_kind_var.get() not in ["All", "news", "filing"]:
            self.notification_kind_var.set("All")

    def refresh_overview(self) -> None:
        self.refresh_filter_values()
        self.overview_records.clear()
        self.clear_tree(self.overview_tree)
        needle = self.overview_search_var.get().strip().lower()
        for ticker in self.configured_tickers():
            snap = self.read_snapshot(ticker)
            summary = wd.latest_summary_text(ticker)
            hay = " ".join([
                ticker,
                snap.get("company", ""),
                summary,
                " ".join(snap.get("pipeline", {}).get("terms", [])),
                " ".join(snap.get("investments", {}).get("entities", [])),
                " ".join(snap.get("contracts", {}).get("counterparties", [])),
            ]).lower()
            if needle and needle not in hay:
                continue
            news_count = len([e for e in wd.list_events(kind="news", ticker=ticker, limit=200)])
            row = {
                "ticker": ticker,
                "company": snap.get("company", ""),
                "generated": snap.get("generated_at_utc", ""),
                "pipeline": len(snap.get("pipeline", {}).get("snippets", [])),
                "investments": len(snap.get("investments", {}).get("snippets", [])),
                "contracts": len(snap.get("contract_exhibits", [])),
                "news": news_count,
                "snapshot": snap,
                "summary": summary,
            }
            item_id = self.overview_tree.insert(
                "",
                END,
                values=(row["ticker"], row["company"], row["generated"], row["pipeline"], row["investments"], row["contracts"], row["news"]),
            )
            self.overview_records[item_id] = row
        first = self.overview_tree.get_children("")
        if first:
            self.overview_tree.selection_set(first[0])
            self.on_overview_select()
        else:
            self.overview_detail.delete("1.0", END)
            self.overview_detail.insert(END, "No snapshots yet. Run Once or start the watcher.\n")

    def lookup_local_report(self, ticker: str, accession: str) -> str:
        target_dir = wd.reports_dir_for(ticker)
        accession_nodash = accession.replace("-", "")
        for path in target_dir.glob(f"*{accession_nodash}*"):
            return str(path)
        return ""

    def refresh_filings(self) -> None:
        self.refresh_filter_values()
        self.filing_records.clear()
        self.clear_tree(self.filings_tree)
        ticker_filter = self.filing_ticker_var.get()
        form_filter = self.filing_form_var.get()
        needle = self.filing_search_var.get().strip().lower()
        for ticker in self.configured_tickers():
            if ticker_filter != "All" and ticker != ticker_filter:
                continue
            snap = self.read_snapshot(ticker)
            for filing in snap.get("recent_filings", []):
                row = {
                    "ticker": ticker,
                    "form": filing.get("form", ""),
                    "filing_date": filing.get("filing_date", ""),
                    "description": filing.get("description", "") or filing.get("primary_document", ""),
                    "url": filing.get("url", ""),
                    "local_path": self.lookup_local_report(ticker, filing.get("accession", "")),
                    "payload": filing,
                }
                if form_filter != "All" and row["form"] != form_filter:
                    continue
                hay = " ".join([row["ticker"], row["form"], row["filing_date"], row["description"], row["url"]]).lower()
                if needle and needle not in hay:
                    continue
                item_id = self.filings_tree.insert("", END, values=(row["ticker"], row["form"], row["filing_date"], row["description"], row["url"], row["local_path"]))
                self.filing_records[item_id] = row
        first = self.filings_tree.get_children("")
        if first:
            self.filings_tree.selection_set(first[0])
            self.on_filing_select()
        else:
            self.filings_detail.delete("1.0", END)
            self.filings_detail.insert(END, "No filings cached yet.\n")

    def refresh_contracts(self) -> None:
        self.refresh_filter_values()
        self.contract_records.clear()
        self.clear_tree(self.contracts_tree)
        ticker_filter = self.contract_ticker_var.get()
        needle = self.contract_search_var.get().strip().lower()
        for ticker in self.configured_tickers():
            if ticker_filter != "All" and ticker != ticker_filter:
                continue
            snap = self.read_snapshot(ticker)
            for ex in snap.get("contract_exhibits", []):
                row = {**ex, "ticker": ticker}
                hay = " ".join([ticker, ex.get("filing_date", ""), ex.get("form", ""), ex.get("title", ""), ex.get("url", "")]).lower()
                if needle and needle not in hay:
                    continue
                item_id = self.contracts_tree.insert("", END, values=(ticker, ex.get("filing_date", ""), ex.get("form", ""), ex.get("title", ""), ex.get("url", "")))
                self.contract_records[item_id] = row
        first = self.contracts_tree.get_children("")
        if first:
            self.contracts_tree.selection_set(first[0])
            self.on_contract_select()
        else:
            self.contracts_detail.delete("1.0", END)
            self.contracts_detail.insert(END, "No contract exhibits extracted yet.\n")

    def read_news_entries(self, ticker: str) -> list[dict]:
        path = wd.company_dir(ticker) / "news_log.jsonl"
        rows: list[dict] = []
        if not path.exists():
            return rows
        with path.open("r", encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except Exception:
                    continue
        return rows

    def refresh_news(self) -> None:
        self.refresh_filter_values()
        self.news_records.clear()
        self.clear_tree(self.news_tree)
        rows: list[dict] = []
        for ticker in self.configured_tickers():
            for item in self.read_news_entries(ticker):
                rows.append({**item, "ticker": ticker})
        rows.sort(key=lambda r: (SortableTree._sort_key(r.get("published_utc", "")), r.get("score", 0)), reverse=True)
        ticker_filter = self.news_ticker_var.get()
        sev_filter = self.news_severity_var.get()
        needle = self.news_search_var.get().strip().lower()
        for row in rows:
            if ticker_filter != "All" and row.get("ticker", "") != ticker_filter:
                continue
            payload = row.get("reasons", [])
            hay = " ".join([
                row.get("ticker", ""),
                row.get("severity", ""),
                row.get("source", ""),
                row.get("title", ""),
                row.get("description", ""),
                row.get("link", ""),
                " ".join(payload) if isinstance(payload, list) else str(payload),
            ]).lower()
            if sev_filter != "All" and row.get("severity", "") != sev_filter:
                continue
            if needle and needle not in hay:
                continue
            item_id = self.news_tree.insert(
                "",
                END,
                values=(row.get("ticker", ""), row.get("severity", ""), row.get("score", ""), row.get("published_utc", ""), row.get("source", ""), row.get("title", ""), row.get("link", "")),
                tags=(row.get("severity", ""),),
            )
            self.news_records[item_id] = row
        first = self.news_tree.get_children("")
        if first:
            self.news_tree.selection_set(first[0])
            self.on_news_select()
        else:
            self.news_detail.delete("1.0", END)
            self.news_detail.insert(END, "No news captured yet.\n")

    def read_notification_history(self) -> list[dict]:
        if not wd.NOTIFICATION_LOG_PATH.exists():
            return []
        rows: list[dict] = []
        for line in wd.NOTIFICATION_LOG_PATH.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
        rows.reverse()
        return rows

    def refresh_notifications(self) -> None:
        self.refresh_filter_values()
        self.notification_records.clear()
        self.clear_tree(self.notifications_tree)
        kind_filter = self.notification_kind_var.get()
        ticker_filter = self.notification_ticker_var.get()
        needle = self.notification_search_var.get().strip().lower()
        for row in self.read_notification_history():
            if kind_filter != "All" and row.get("kind", "") != kind_filter:
                continue
            if ticker_filter != "All" and row.get("ticker", "") != ticker_filter:
                continue
            link = row.get("extra", {}).get("link") or row.get("extra", {}).get("url") or ""
            hay = " ".join([row.get("kind", ""), row.get("ticker", ""), row.get("title", ""), row.get("body", ""), link]).lower()
            if needle and needle not in hay:
                continue
            item_id = self.notifications_tree.insert("", END, values=(row.get("timestamp_utc", ""), row.get("kind", ""), row.get("ticker", ""), row.get("title", ""), link))
            self.notification_records[item_id] = row
        first = self.notifications_tree.get_children("")
        if first:
            self.notifications_tree.selection_set(first[0])
            self.on_notification_select()
        else:
            self.notifications_detail.delete("1.0", END)
            self.notifications_detail.insert(END, "No notifications recorded yet.\n")

    def refresh_diagnostics(self) -> None:
        health = wd.load_health()
        task = wd.query_startup_task()
        event_counts = {kind: len(wd.list_events(kind=kind, limit=5000)) for kind in ["news", "filing"]}
        health["watcher_running"] = wd.is_process_running(health.get("pid"))
        gui_watcher_pid = self.watch_process.pid if self.watch_process and self.watch_process.poll() is None else None
        self.health_text.delete("1.0", END)
        self.health_text.insert(END, json.dumps(health, indent=2, ensure_ascii=False))
        self.task_text.delete("1.0", END)
        paths = wd.runtime_paths_summary()
        paths.update(
            {
                "open_in_explorer_target": str(wd.explorer_target_dir()),
                "watcher_pid_from_health": health.get("pid"),
                "watcher_pid_gui_subprocess": gui_watcher_pid,
            }
        )
        self.task_text.insert(
            END,
            json.dumps(
                {
                    "runtime": paths,
                    "task": task,
                    "paths_detail": {
                        "writable_root": str(wd.APP_DIR),
                        "config": str(wd.CONFIG_PATH),
                        "health": str(wd.HEALTH_PATH),
                        "db": str(wd.DB_PATH),
                        "log": str(wd.LOG_PATH),
                        "notification_history": str(wd.NOTIFICATION_LOG_PATH),
                        "snapshots": str(wd.SNAPSHOT_DIR),
                        "reports": str(wd.REPORTS_DIR),
                        "cache": str(wd.CACHE_DIR),
                        "summaries": str(wd.SUMMARY_DIR),
                    },
                    "counts": event_counts,
                    "latest_tickers": self.configured_tickers(),
                },
                indent=2,
                ensure_ascii=False,
            ),
        )

    def on_overview_select(self) -> None:
        row = self._selected_record(self.overview_tree, self.overview_records)
        self.overview_detail.delete("1.0", END)
        if not row:
            return
        snap = row.get("snapshot", {})
        lines = [
            f"Ticker: {row.get('ticker', '')}",
            f"Company: {row.get('company', '')}",
            f"Generated: {row.get('generated', '')}",
            "",
            "Pipeline terms:",
        ]
        for term in snap.get("pipeline", {}).get("terms", [])[:10]:
            lines.append(f" - {term}")
        lines.append("")
        lines.append("Investment entities:")
        for term in snap.get("investments", {}).get("entities", [])[:10]:
            lines.append(f" - {term}")
        lines.append("")
        lines.append("Contract counterparties:")
        for term in snap.get("contracts", {}).get("counterparties", [])[:10]:
            lines.append(f" - {term}")
        summary = row.get("summary", "")
        if summary:
            lines.append("")
            lines.append("Latest summary:")
            lines.append(summary)
        self.overview_detail.insert(END, "\n".join(lines))

    def on_filing_select(self) -> None:
        row = self._selected_record(self.filings_tree, self.filing_records)
        self.filings_detail.delete("1.0", END)
        if not row:
            return
        lines = [
            f"Ticker: {row.get('ticker', '')}",
            f"Form: {row.get('form', '')}",
            f"Date: {row.get('filing_date', '')}",
            f"Description: {row.get('description', '')}",
            f"URL: {row.get('url', '')}",
            f"Local path: {row.get('local_path', '')}",
            "",
            json.dumps(row.get("payload", {}), indent=2, ensure_ascii=False),
        ]
        self.filings_detail.insert(END, "\n".join(lines))

    def on_contract_select(self) -> None:
        row = self._selected_record(self.contracts_tree, self.contract_records)
        self.contracts_detail.delete("1.0", END)
        if not row:
            return
        self.contracts_detail.insert(END, json.dumps(row, indent=2, ensure_ascii=False))

    def on_news_select(self) -> None:
        row = self._selected_record(self.news_tree, self.news_records)
        self.news_detail.delete("1.0", END)
        if not row:
            return
        self.news_detail.insert(END, json.dumps(row, indent=2, ensure_ascii=False))

    def on_notification_select(self) -> None:
        row = self._selected_record(self.notifications_tree, self.notification_records)
        self.notifications_detail.delete("1.0", END)
        if not row:
            return
        self.notifications_detail.insert(END, json.dumps(row, indent=2, ensure_ascii=False))

    def open_selected_filing_url(self) -> None:
        row = self._selected_record(self.filings_tree, self.filing_records)
        if row:
            self.open_url(row.get("url", ""))

    def open_selected_filing_local(self) -> None:
        row = self._selected_record(self.filings_tree, self.filing_records)
        if row and row.get("local_path"):
            self.open_path(Path(row["local_path"]))

    def open_selected_reports(self) -> None:
        row = self._selected_record(self.filings_tree, self.filing_records)
        if row:
            self.open_path(wd.reports_dir_for(row.get("ticker", "")))

    def open_selected_contract(self) -> None:
        row = self._selected_record(self.contracts_tree, self.contract_records)
        if row:
            self.open_url(row.get("url", ""))

    def open_selected_news(self) -> None:
        row = self._selected_record(self.news_tree, self.news_records)
        if row:
            self.open_url(row.get("link", ""))

    def open_selected_notification(self) -> None:
        row = self._selected_record(self.notifications_tree, self.notification_records)
        if row:
            link = row.get("extra", {}).get("link") or row.get("extra", {}).get("url") or ""
            self.open_url(link)

    def poll_external_state(self) -> None:
        try:
            if wd.LOG_PATH.exists():
                mtime = wd.LOG_PATH.stat().st_mtime
                if mtime != self.last_log_mtime:
                    self.last_log_mtime = mtime
                    with wd.LOG_PATH.open("r", encoding="utf-8", errors="ignore") as fh:
                        fh.seek(self.tail_position)
                        chunk = fh.read()
                        self.tail_position = fh.tell()
                    if chunk:
                        self.append_log(chunk)
            health = wd.load_health()
            pid = health.get("pid")
            running = wd.is_process_running(pid) if isinstance(pid, int) else False
            if running:
                self.external_pid = pid
                self.status_var.set(f"{health.get('status', 'running')} // PID {pid}")
                self.status_detail_var.set(health.get("status_detail", ""))
            else:
                if self.watch_process and self.watch_process.poll() is None:
                    self.status_var.set(f"running // PID {self.watch_process.pid}")
                    self.status_detail_var.set("GUI-launched watcher")
                else:
                    if health.get("status") == "error":
                        self.status_var.set("error")
                        self.status_detail_var.set(health.get("last_error", ""))
                    elif health.get("last_success_utc"):
                        self.status_var.set("idle")
                        self.status_detail_var.set(f"Last success {health.get('last_success_utc', '')}")
            now = time.time()
            diag_visible = self.notebook.select() == str(self.diagnostics_tab)
            interval = self.diag_refresh_interval_visible if diag_visible else self.diag_refresh_interval_idle
            if (now - self.last_diag_refresh) >= interval:
                self.refresh_diagnostics()
                self.last_diag_refresh = now
        except Exception:
            pass
        finally:
            self.after(1200, self.poll_external_state)

    def on_close(self) -> None:
        self.destroy()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Watchdog GUI")
    p.add_argument("--watcher", action="store_true", help="Run the background watcher instead of the GUI.")
    return p.parse_args()


def main() -> None:
    wd.ensure_dirs()
    args = parse_args()
    if args.watcher:
        cfg = wd.configure(force=False)
        wd.main_loop(cfg)
        return
    app = WatchdogGUI()
    app.mainloop()


if __name__ == "__main__":
    main()
