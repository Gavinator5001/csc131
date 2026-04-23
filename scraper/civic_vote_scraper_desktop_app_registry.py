#!/usr/bin/env python3
import csv
import os
import queue
import subprocess
import sys
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk


APP_TITLE = "Civic Vote Scraper"



class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("1220x800")
        self.minsize(1000, 700)

        self.log_queue = queue.Queue()
        self.proc = None

        self.url_var = tk.StringVar(value="")
        self.jurisdiction_var = tk.StringVar(value="")
        self.body_filter_var = tk.StringVar(value="Board of Supervisors")
        self.headless_var = tk.BooleanVar(value=True)
        self.page_limit_var = tk.StringVar(value="0")
        self.meeting_limit_var = tk.StringVar(value="200")
        self.min_confidence_var = tk.StringVar(value="0.75")

        self.form700_xlsx_var = tk.StringVar(value="")
        self.form700_registry_var = tk.StringVar(value="form700_registry.json")
        self.form700_csv_out_var = tk.StringVar(value="form700_entities.csv")
        self.form700_json_out_var = tk.StringVar(value="form700_entities.json")
        self.form700_matches_out_var = tk.StringVar(value="form700_matches.csv")
        self.out_votes_var = tk.StringVar(value="votes.csv")
        self.minutes_cache_dir_var = tk.StringVar(value="minutes_cache")
        self.minutes_text_index_var = tk.StringVar(value="minutes_text_index.json")
        self.project_dir_var = tk.StringVar(value=Path(os.getcwd()))
        self.output_var = tk.StringVar(value="")

        self._build_ui()
        self.after(150, self._drain_log_queue)

    def _build_ui(self):
        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        header = ttk.Frame(self, padding=12)
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)

        ttk.Label(header, text=APP_TITLE, font=("Segoe UI", 18, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Label(
            header,
            text="Local desktop app for Playwright discovery, minutes-first vote extraction for all politicians, and Form 700 registry matching.",
        ).grid(row=1, column=0, sticky="w", pady=(4, 0))

        main = ttk.PanedWindow(self, orient=tk.HORIZONTAL)
        main.grid(row=1, column=0, sticky="nsew", padx=12, pady=(0, 12))

        left = ttk.Frame(main, padding=8)
        right = ttk.Frame(main, padding=8)
        main.add(left, weight=3)
        main.add(right, weight=2)

        left.columnconfigure(0, weight=1)
        right.columnconfigure(0, weight=1)
        right.rowconfigure(1, weight=1)

        self._build_settings(left)
        self._build_outputs(left)
        self._build_actions(left)
        self._build_preview(right)

    def _build_settings(self, parent):
        frm = ttk.LabelFrame(parent, text="Scraper settings", padding=10)
        frm.grid(row=0, column=0, sticky="ew")
        for i in range(4):
            frm.columnconfigure(i, weight=1)

        self._labeled_entry(frm, "Project directory", self.project_dir_var, 0, 0, colspan=3)
        ttk.Button(frm, text="Browse", command=self._pick_project_dir).grid(row=1, column=3, sticky="ew", padx=(8, 0))

        self._labeled_entry(frm, "Calendar URL", self.url_var, 2, 0, colspan=2)
        self._labeled_entry(frm, "Jurisdiction", self.jurisdiction_var, 2, 2, colspan=2)

        self._labeled_entry(frm, "Form 700 registry", self.form700_registry_var, 4, 0)
        self._labeled_entry(frm, "Body filter", self.body_filter_var, 4, 1)
        self._labeled_entry(frm, "Page limit", self.page_limit_var, 4, 2)
        self._labeled_entry(frm, "Meeting limit", self.meeting_limit_var, 4, 3)

        self._labeled_entry(frm, "Min confidence", self.min_confidence_var, 6, 0)
        ttk.Checkbutton(frm, text="Run headless", variable=self.headless_var).grid(
            row=7, column=1, sticky="w", padx=6, pady=(0, 0)
        )

        self._labeled_entry(frm, "Form 700 workbook (.xlsx)", self.form700_xlsx_var, 8, 0, colspan=3)
        ttk.Button(frm, text="Browse", command=self._pick_form700).grid(row=9, column=3, sticky="ew", padx=(8, 0))



        self._labeled_entry(frm, "Output folder", self.output_var, 12, 0, colspan=3)
        ttk.Button(frm, text="Browse", command=self._pick_output_folder).grid(row=13, column=3, sticky="ew", padx=(8, 0))

    def _build_outputs(self, parent):
        frm = ttk.LabelFrame(parent, text="Output files", padding=10)
        frm.grid(row=1, column=0, sticky="ew", pady=(10, 0))
        for i in range(2):
            frm.columnconfigure(i, weight=1)

        self._labeled_entry(frm, "Votes CSV", self.out_votes_var, 0, 0)
        self._labeled_entry(frm, "Form 700 entities CSV", self.form700_csv_out_var, 0, 1)
        self._labeled_entry(frm, "Form 700 entities JSON", self.form700_json_out_var, 2, 0)
        self._labeled_entry(frm, "Form 700 matches CSV", self.form700_matches_out_var, 2, 1)
        self._labeled_entry(frm, "Minutes cache folder", self.minutes_cache_dir_var, 4, 0)
        self._labeled_entry(frm, "Minutes text index", self.minutes_text_index_var, 4, 1)

    def _build_actions(self, parent):
        frm = ttk.LabelFrame(parent, text="Actions", padding=10)
        frm.grid(row=2, column=0, sticky="nsew", pady=(10, 0))
        frm.columnconfigure(0, weight=1)
        frm.columnconfigure(1, weight=1)
        frm.rowconfigure(3, weight=1)

        ttk.Button(frm, text="Build command", command=self._refresh_command).grid(row=0, column=0, sticky="ew", padx=(0, 6))
        ttk.Button(frm, text="Run scraper", command=self._run_command).grid(row=0, column=1, sticky="ew", padx=(6, 0))
        ttk.Button(frm, text="Stop", command=self._stop_command).grid(row=1, column=0, sticky="ew", padx=(0, 6), pady=(8, 0))
        ttk.Button(frm, text="Open output folder", command=self._open_output_folder).grid(row=1, column=1, sticky="ew", padx=(6, 0), pady=(8, 0))

        ttk.Label(frm, text="Command preview").grid(row=2, column=0, columnspan=2, sticky="w", pady=(12, 4))
        self.command_text = tk.Text(frm, height=9, wrap="word")
        self.command_text.grid(row=3, column=0, columnspan=2, sticky="nsew")
        self._refresh_command()

    def _build_preview(self, parent):
        cmd_frm = ttk.LabelFrame(parent, text="Run log", padding=10)
        cmd_frm.grid(row=0, column=0, sticky="nsew")
        cmd_frm.columnconfigure(0, weight=1)
        cmd_frm.rowconfigure(0, weight=1)

        self.log_text = tk.Text(cmd_frm, height=18, wrap="word")
        self.log_text.grid(row=0, column=0, sticky="nsew")
        log_scroll = ttk.Scrollbar(cmd_frm, orient="vertical", command=self.log_text.yview)
        log_scroll.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=log_scroll.set)

        results = ttk.LabelFrame(parent, text="Output preview", padding=10)
        results.grid(row=1, column=0, sticky="nsew", pady=(10, 0))
        results.columnconfigure(0, weight=1)
        results.rowconfigure(1, weight=1)

        top = ttk.Frame(results)
        top.grid(row=0, column=0, sticky="ew")
        top.columnconfigure(0, weight=1)
        ttk.Label(top, text="Preview file").grid(row=0, column=0, sticky="w")
        self.preview_choice = ttk.Combobox(
            top,
            values=["votes.csv", "form700_entities.csv", "form700_matches.csv", "minutes_text_index.json", "form700_registry.json"],
            state="readonly",
        )
        self.preview_choice.set("votes.csv")
        self.preview_choice.grid(row=0, column=1, sticky="e")
        ttk.Button(top, text="Load preview", command=self._load_preview).grid(row=0, column=2, padx=(8, 0))

        self.preview_text = tk.Text(results, wrap="none")
        self.preview_text.grid(row=1, column=0, sticky="nsew", pady=(8, 0))
        preview_scroll = ttk.Scrollbar(results, orient="vertical", command=self.preview_text.yview)
        preview_scroll.grid(row=1, column=1, sticky="ns", pady=(8, 0))
        self.preview_text.configure(yscrollcommand=preview_scroll.set)

    def _labeled_entry(self, parent, label, var, row, col, colspan=1):
        ttk.Label(parent, text=label).grid(row=row, column=col, columnspan=colspan, sticky="w", pady=(0 if row == 0 else 8, 2))
        entry = ttk.Entry(parent, textvariable=var)
        entry.grid(row=row + 1, column=col, columnspan=colspan, sticky="ew", padx=(0, 8) if colspan == 1 else 0)

    def _pick_project_dir(self):
        path = filedialog.askdirectory(title="Select scraper project directory")
        if path:
            self.project_dir_var.set(path)
            self._refresh_command()

    def _pick_output_folder(self):
        path = filedialog.askdirectory(title="Select output folder")
        if path:
            self.output_var.set(path)
            self._refresh_command()

    def _pick_form700(self):
        path = filedialog.askopenfilename(
            title="Select Form 700 workbook",
            filetypes=[("Excel workbook", "*.xlsx"), ("All files", "*.*")],
        )
        if path:
            self.form700_xlsx_var.set(path)
            self._refresh_command()

    def _get_command(self):
        output_dir = self.output_var.get().strip()

        def get_output_path(filename):
            if not filename:
                return ""
            if output_dir:
                return str(Path(output_dir) / filename)
            return filename

        parts = [
            sys.executable,
            "-m",
            "civic_vote_scraper.cli",
            "--url", self.url_var.get().strip(),
            "--jurisdiction", self.jurisdiction_var.get().strip(),
            "--out", get_output_path(self.out_votes_var.get().strip()),
            "--minutes-cache-dir", get_output_path(self.minutes_cache_dir_var.get().strip()),
            "--minutes-text-index", get_output_path(self.minutes_text_index_var.get().strip()),
            "--form700-csv-out", get_output_path(self.form700_csv_out_var.get().strip()),
            "--form700-json-out", get_output_path(self.form700_json_out_var.get().strip()),
            "--form700-matches-out", get_output_path(self.form700_matches_out_var.get().strip()),
            "--min-confidence", self.min_confidence_var.get().strip(),
        ]

        if self.body_filter_var.get().strip():
            parts += ["--body-filter", self.body_filter_var.get().strip()]
        if self.headless_var.get():
            parts += ["--headless"]
        page_limit = self.page_limit_var.get().strip()
        if page_limit and page_limit != "0":
            parts += ["--page-limit", page_limit]
        meeting_limit = self.meeting_limit_var.get().strip()
        if meeting_limit and meeting_limit != "0":
            parts += ["--meeting-limit", meeting_limit]
        if self.form700_xlsx_var.get().strip():
            parts += ["--form700-xlsx", self.form700_xlsx_var.get().strip()]
        if self.form700_registry_var.get().strip():
            parts += ["--form700-registry", get_output_path(self.form700_registry_var.get().strip())]

        return parts

    def _refresh_command(self):
        cmd = self._get_command()
        pretty = " ".join(f'"{x}"' if " " in x else x for x in cmd)
        self.command_text.delete("1.0", tk.END)
        self.command_text.insert("1.0", pretty)

    def _append_log(self, text):
        self.log_text.insert(tk.END, text)
        self.log_text.see(tk.END)

    def _run_command(self):
        if self.proc is not None:
            messagebox.showinfo(APP_TITLE, "A process is already running.")
            return

        project_dir = self.project_dir_var.get().strip()
        if not project_dir:
            messagebox.showerror(APP_TITLE, "Select the scraper project directory first.")
            return
        if not Path(project_dir).exists():
            messagebox.showerror(APP_TITLE, "Project directory does not exist.")
            return

        self._refresh_command()
        cmd = self._get_command()
        self._append_log("[start] Running scraper\n")

        def worker():
            try:
                self.proc = subprocess.Popen(
                    cmd,
                    cwd=project_dir,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                )
                assert self.proc.stdout is not None
                for line in self.proc.stdout:
                    self.log_queue.put(line)
                code = self.proc.wait()
                self.log_queue.put(f"\n[exit] Process finished with code {code}\n")
            except Exception as e:
                self.log_queue.put(f"\n[error] {e}\n")
            finally:
                self.proc = None

        threading.Thread(target=worker, daemon=True).start()

    def _stop_command(self):
        if self.proc is None:
            return
        try:
            self.proc.terminate()
            self._append_log("[stop] Termination requested\n")
        except Exception as e:
            self._append_log(f"[error] Could not stop process: {e}\n")

    def _drain_log_queue(self):
        try:
            while True:
                line = self.log_queue.get_nowait()
                self._append_log(line)
        except queue.Empty:
            pass
        self.after(150, self._drain_log_queue)

    def _open_output_folder(self):
        target_dir = self.output_var.get().strip() or self.project_dir_var.get().strip() or os.getcwd()
        path = Path(target_dir)
        if not path.exists():
            return
        if sys.platform.startswith("win"):
            os.startfile(path)  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            subprocess.Popen(["open", str(path)])
        else:
            subprocess.Popen(["xdg-open", str(path)])

    def _load_preview(self):
        project_dir = self.project_dir_var.get().strip() or os.getcwd()
        output_dir = self.output_var.get().strip() or project_dir
        name = self.preview_choice.get().strip()
        path = Path(output_dir) / name
        if not path.exists():
            messagebox.showinfo(APP_TITLE, f"File not found:\n{path}")
            return

        self.preview_text.delete("1.0", tk.END)
        try:
            if path.suffix.lower() == ".json":
                self.preview_text.insert("1.0", path.read_text(encoding="utf-8", errors="ignore")[:20000])
                return

            with path.open("r", encoding="utf-8", newline="") as f:
                reader = csv.reader(f)
                for i, row in enumerate(reader):
                    self.preview_text.insert(tk.END, ",".join(row) + "\n")
                    if i >= 50:
                        break
        except Exception:
            self.preview_text.insert("1.0", path.read_text(encoding="utf-8", errors="ignore")[:20000])


if __name__ == "__main__":
    app = App()
    app.mainloop()
