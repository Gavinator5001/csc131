import tkinter as tk
from tkinter import filedialog, messagebox, ttk

import pandas as pd

from DatabaseScript import (
    DEFAULT_DB_HOST,
    DEFAULT_DB_NAME,
    DEFAULT_DB_PASSWORD,
    DEFAULT_DB_PORT,
    DEFAULT_DB_SCHEMA,
    DEFAULT_DB_USER,
    DEFAULT_FILE_PATH,
    consolidate_by_email,
    drop_excluded_columns,
    format_column_titles,
    load_path_to_dataframes,
    save_dataframe_to_postgres,
)


class DataApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("COI Interface")
        self.root.geometry("2000x900")

        self.processed_dataframes = {}
        self.current_names = []
        self.preview_row_limit = 250
        self.current_preview_dataframe = pd.DataFrame()

        self.path_var = tk.StringVar(value=DEFAULT_FILE_PATH)
        self.db_host_var = tk.StringVar(value=DEFAULT_DB_HOST)
        self.db_port_var = tk.StringVar(value=str(DEFAULT_DB_PORT))
        self.db_name_var = tk.StringVar(value=DEFAULT_DB_NAME)
        self.db_user_var = tk.StringVar(value=DEFAULT_DB_USER)
        self.db_password_var = tk.StringVar(value=DEFAULT_DB_PASSWORD)
        self.db_schema_var = tk.StringVar(value=DEFAULT_DB_SCHEMA)
        self.if_exists_var = tk.StringVar(value="replace")
        self.view_mode_var = tk.StringVar(value="All Columns")
        self.search_var = tk.StringVar()

        self._build_layout()

    def _build_layout(self) -> None:
        controls = ttk.Frame(self.root, padding=12)
        controls.pack(fill="x")

        ttk.Button(controls, text="Choose File", command=self.choose_file).grid(row=1, column=0, padx=4)
        ttk.Button(controls, text="Choose Folder", command=self.choose_folder).grid(row=1, column=1, padx=4)
        ttk.Button(controls, text="Load", command=self.load_data).grid(row=1, column=2, padx=4)
        ttk.Button(controls, text="Scrape County Websites", command=self.scrape_placeholder).grid(
            row=1, column=6, padx=(12, 0)
        )
        ttk.Label(controls, text="View").grid(row=0, column=3, sticky="w", padx=(12, 0))
        view_selector = ttk.Combobox(
            controls,
            textvariable=self.view_mode_var,
            values=("All Columns", "Names + Financial Interests"),
            state="readonly",
            width=26,
        )
        view_selector.grid(row=1, column=3, padx=(12, 0), sticky="ew")
        view_selector.bind("<<ComboboxSelected>>", self.show_selected_preview)
        ttk.Label(controls, text="Search Last Name or Email").grid(
            row=0, column=4, sticky="w", padx=(12, 0)
        )
        ttk.Entry(controls, textvariable=self.search_var, width=28).grid(
            row=1, column=4, padx=(12, 0), sticky="ew"
        )
        ttk.Button(controls, text="Search Loaded Data", command=self.search_loaded_data).grid(
            row=1, column=5, padx=4
        )

        db_frame = ttk.LabelFrame(self.root, text="Database", padding=12)
        db_frame.pack(fill="x", padx=12, pady=(0, 12))

        ttk.Label(db_frame, text="Host").grid(row=0, column=0, sticky="w")
        ttk.Entry(db_frame, textvariable=self.db_host_var, width=18).grid(row=1, column=0, padx=4, sticky="ew")
        ttk.Label(db_frame, text="Port").grid(row=0, column=1, sticky="w")
        ttk.Entry(db_frame, textvariable=self.db_port_var, width=10).grid(row=1, column=1, padx=4, sticky="ew")
        ttk.Label(db_frame, text="Database").grid(row=0, column=2, sticky="w")
        ttk.Entry(db_frame, textvariable=self.db_name_var, width=18).grid(row=1, column=2, padx=4, sticky="ew")
        ttk.Label(db_frame, text="User").grid(row=0, column=3, sticky="w")
        ttk.Entry(db_frame, textvariable=self.db_user_var, width=18).grid(row=1, column=3, padx=4, sticky="ew")
        ttk.Label(db_frame, text="Password").grid(row=0, column=4, sticky="w")
        ttk.Entry(db_frame, textvariable=self.db_password_var, show="*", width=18).grid(
            row=1, column=4, padx=4, sticky="ew"
        )
        ttk.Label(db_frame, text="Schema").grid(row=0, column=5, sticky="w")
        ttk.Entry(db_frame, textvariable=self.db_schema_var, width=18).grid(row=1, column=5, padx=4, sticky="ew")
        ttk.Label(db_frame, text="If Exists").grid(row=0, column=6, sticky="w")
        ttk.Combobox(
            db_frame,
            textvariable=self.if_exists_var,
            values=("replace", "append", "fail"),
            state="readonly",
            width=10,
        ).grid(row=1, column=6, padx=4, sticky="ew")
        ttk.Button(db_frame, text="Save Selected", command=self.save_selected).grid(
            row=1, column=7, padx=(12, 0)
        )
        ttk.Button(db_frame, text="Save All", command=self.save_all).grid(row=1, column=8, padx=4)

        main = ttk.PanedWindow(self.root, orient="horizontal")
        main.pack(fill="both", expand=True, padx=12, pady=(0, 12))
        self.main_pane = main

        left = ttk.Frame(main, padding=8)
        right = ttk.Frame(main, padding=8)
        main.add(left, weight=1)
        main.add(right, weight=4)

        ttk.Label(left, text="Loaded DataFrames").pack(anchor="w")
        self.file_list = tk.Listbox(left, exportselection=False)
        self.file_list.pack(fill="both", expand=True, pady=(8, 0))
        self.file_list.bind("<<ListboxSelect>>", self.show_selected_preview)

        ttk.Label(right, text="Preview").pack(anchor="w")
        self.preview_summary = ttk.Label(right, text="", anchor="w")
        self.preview_summary.pack(fill="x", pady=(8, 4))

        table_frame = ttk.Frame(right)
        table_frame.pack(fill="both", expand=True)

        self.preview_table = ttk.Treeview(table_frame, show="headings")
        self.preview_table.grid(row=0, column=0, sticky="nsew")
        self.preview_table.bind("<<TreeviewSelect>>", self.show_selected_row_details)

        y_scroll = ttk.Scrollbar(table_frame, orient="vertical", command=self.preview_table.yview)
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll = ttk.Scrollbar(table_frame, orient="horizontal", command=self.preview_table.xview)
        x_scroll.grid(row=1, column=0, sticky="ew")

        self.preview_table.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(0, weight=1)

        details_frame = ttk.LabelFrame(right, text="Row Details", padding=8)
        details_frame.pack(fill="both", expand=False, pady=(12, 0))

        self.details_text = tk.Text(details_frame, wrap="word", height=10)
        self.details_text.pack(fill="both", expand=True)
        details_scroll = ttk.Scrollbar(details_frame, orient="vertical", command=self.details_text.yview)
        details_scroll.pack(side="right", fill="y")
        self.details_text.configure(yscrollcommand=details_scroll.set)

        self.root.after(50, self.configure_initial_layout)

    def choose_file(self) -> None:
        file_path = filedialog.askopenfilename(
            filetypes=[
                ("Supported files", "*.csv *.xls *.xlsx *.xlsm"),
                ("All files", "*.*"),
            ]
        )
        if file_path:
            self.path_var.set(file_path)

    def choose_folder(self) -> None:
        folder_path = filedialog.askdirectory()
        if folder_path:
            self.path_var.set(folder_path)

    def configure_initial_layout(self) -> None:
        self.root.update_idletasks()
        total_width = self.main_pane.winfo_width()
        if total_width > 300:
            self.main_pane.sashpos(0, max(240, total_width // 4))

    def load_data(self) -> None:
        try:
            raw_dataframes = load_path_to_dataframes(self.path_var.get())
            self.processed_dataframes = {}

            for name, dataframe in raw_dataframes.items():
                formatted = format_column_titles(dataframe)
                filtered = drop_excluded_columns(formatted)
                consolidated = consolidate_by_email(filtered)
                self.processed_dataframes[name] = consolidated

            self.current_names = list(self.processed_dataframes.keys())
            self.file_list.delete(0, tk.END)
            for name in self.current_names:
                self.file_list.insert(tk.END, name)

            if self.current_names:
                self.file_list.selection_clear(0, tk.END)
                self.file_list.selection_set(0)
                self.show_selected_preview()

            messagebox.showinfo("Load Complete", f"Loaded {len(self.current_names)} file(s).")
        except Exception as exc:
            messagebox.showerror("Load Failed", str(exc))

    def show_selected_preview(self, event=None) -> None:
        selection = self.file_list.curselection()
        if not selection:
            return

        name = self.current_names[selection[0]]
        dataframe = self.processed_dataframes[name]
        preview_source = self.get_preview_dataframe(dataframe)
        preview = preview_source.head(self.preview_row_limit).fillna("")
        self.current_preview_dataframe = preview.copy()

        self.preview_summary.config(
            text=(
                f"{name} | Rows: {len(dataframe)} | "
                f"Columns: {len(preview_source.columns)} | "
                f"Showing first {len(preview)} rows"
            )
        )

        columns = [str(column) for column in preview.columns]
        self.preview_table.delete(*self.preview_table.get_children())
        self.preview_table["columns"] = columns

        for column in columns:
            self.preview_table.heading(column, text=column, anchor="w")
            self.preview_table.column(column, anchor="w", width=160, minwidth=100, stretch=True)

        for row in preview.astype(str).itertuples(index=False, name=None):
            self.preview_table.insert("", tk.END, values=row)

        self.details_text.delete("1.0", tk.END)

    def search_loaded_data(self) -> None:
        query = self.search_var.get().strip().lower()
        if not query:
            messagebox.showwarning("Missing Search", "Enter a last name or email to search.")
            return

        if not self.processed_dataframes:
            messagebox.showwarning("No Data", "Load data first.")
            return

        matches = []
        for source_name, dataframe in self.processed_dataframes.items():
            last_name_column = next(
                (
                    column
                    for column in dataframe.columns
                    if str(column).strip().lower() == "last name"
                ),
                None,
            )
            email_column = next(
                (
                    column
                    for column in dataframe.columns
                    if str(column).strip().lower() in {"email", "email address", "work email address"}
                ),
                None,
            )

            if last_name_column is None and email_column is None:
                continue

            mask = None
            if last_name_column is not None:
                last_name_match = (
                    dataframe[last_name_column].fillna("").astype(str).str.strip().str.lower().str.contains(query, regex=False)
                )
                mask = last_name_match if mask is None else (mask | last_name_match)

            if email_column is not None:
                email_match = (
                    dataframe[email_column].fillna("").astype(str).str.strip().str.lower().str.contains(query, regex=False)
                )
                mask = email_match if mask is None else (mask | email_match)

            if mask is not None and mask.any():
                matched_rows = dataframe.loc[mask].copy()
                matched_rows.insert(0, "Source File", source_name)
                matches.append(matched_rows)

        if not matches:
            messagebox.showinfo("No Matches", f"No loaded rows matched '{query}'.")
            return

        results = self.get_preview_dataframe(pd.concat(matches, ignore_index=True))
        preview = results.head(self.preview_row_limit).fillna("")
        self.current_preview_dataframe = preview.copy()

        self.preview_summary.config(
            text=(
                f"Search results for '{query}' | Rows: {len(results)} | "
                f"Columns: {len(results.columns)} | Showing first {len(preview)} rows"
            )
        )

        columns = [str(column) for column in preview.columns]
        self.preview_table.delete(*self.preview_table.get_children())
        self.preview_table["columns"] = columns

        for column in columns:
            self.preview_table.heading(column, text=column, anchor="w")
            self.preview_table.column(column, anchor="w", width=160, minwidth=100, stretch=True)

        for row in preview.astype(str).itertuples(index=False, name=None):
            self.preview_table.insert("", tk.END, values=row)

        self.details_text.delete("1.0", tk.END)

    def show_selected_row_details(self, event=None) -> None:
        selection = self.preview_table.selection()
        if not selection or self.current_preview_dataframe.empty:
            return

        row_index = self.preview_table.index(selection[0])
        if row_index >= len(self.current_preview_dataframe):
            return

        row = self.current_preview_dataframe.iloc[row_index]
        detail_lines = [f"{column}: {row[column]}" for column in self.current_preview_dataframe.columns]

        self.details_text.delete("1.0", tk.END)
        self.details_text.insert(tk.END, "\n\n".join(detail_lines))

    def scrape_placeholder(self) -> None:
        messagebox.showinfo(
            "Scraper Placeholder",
            "County website scraping is not implemented yet. This button is a placeholder for the future scraper workflow.",
        )

    def get_preview_dataframe(self, dataframe):
        if self.view_mode_var.get() == "Names + Financial Interests":
            return self.select_name_interest_columns(dataframe)
        return dataframe

    def select_name_interest_columns(self, dataframe):
        exact_columns = {
            "last name",
            "first name",
            "middle name",
            "name of business entity",
            "email",
            "email address",
            "work email address",
        }

        selected_columns = [
            column
            for column in dataframe.columns
            if str(column).strip().lower() in exact_columns
        ]

        if not selected_columns:
            return dataframe

        return dataframe.loc[:, selected_columns]

    def save_selected(self) -> None:
        selection = self.file_list.curselection()
        if not selection:
            messagebox.showwarning("No Selection", "Select a DataFrame first.")
            return

        name = self.current_names[selection[0]]
        self._save_names([name])

    def save_all(self) -> None:
        if not self.current_names:
            messagebox.showwarning("No Data", "Load data first.")
            return

        self._save_names(self.current_names)

    def _save_names(self, names: list[str]) -> None:
        try:
            saved_tables = []
            for name in names:
                table_name = save_dataframe_to_postgres(
                    dataframe=self.processed_dataframes[name],
                    table_name=name,
                    host=self.db_host_var.get(),
                    port=int(self.db_port_var.get()),
                    database=self.db_name_var.get(),
                    user=self.db_user_var.get(),
                    password=self.db_password_var.get(),
                    schema=self.db_schema_var.get(),
                    if_exists=self.if_exists_var.get(),
                )
                saved_tables.append(f"{self.db_schema_var.get()}.{table_name}")

            messagebox.showinfo("Save Complete", "\n".join(saved_tables))
        except Exception as exc:
            messagebox.showerror("Save Failed", str(exc))


def main() -> None:
    root = tk.Tk()
    app = DataApp(root)
    app.load_data()
    root.mainloop()


if __name__ == "__main__":
    main()
