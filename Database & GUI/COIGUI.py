from __future__ import annotations

import contextlib
import io
import json
import re
import sys
import traceback
from pathlib import Path

import pandas as pd
from PyQt6.QtCore import QAbstractTableModel, QElapsedTimer, QModelIndex, QObject, Qt, QThread, QTimer, pyqtSignal
from PyQt6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDialog,
    QFileDialog,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QPlainTextEdit,
    QProgressBar,
    QSpinBox,
    QDoubleSpinBox,
    QStatusBar,
    QSplitter,
    QTabWidget,
    QTableView,
    QVBoxLayout,
    QWidget,
)
from rapidfuzz import fuzz

from award_parser import (
    DEFAULT_CSV_OUTPUT_NAME,
    DEFAULT_INPUT_ROOT,
    DEFAULT_OUTPUT_NAME,
    AwardParser,
    database_name_for_location,
    save_candidates_to_location_databases,
    write_csv,
    write_jsonl,
)
from council_crawler import (
    DEFAULT_DELAY_SECONDS,
    DEFAULT_MAX_AGE_YEARS,
    DEFAULT_MAX_DOCUMENTS_PER_SITE,
    DEFAULT_MAX_PAGES_PER_SITE,
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_TIMEOUT_SECONDS,
    CouncilCrawler,
    TARGETS,
)
from DatabaseScript import (
    DEFAULT_DB_HOST,
    DEFAULT_DB_NAME,
    DEFAULT_DB_PASSWORD,
    DEFAULT_DB_PORT,
    DEFAULT_DB_SCHEMA,
    DEFAULT_DB_USER,
)
from QueryDatabase import run_query


LOCATION_CONFIG = {
    "Santa Ana": {
        "site": "santa_ana",
        "keywords": ("santa_ana", "santa ana"),
    },
    "Sonoma": {
        "site": "sonoma",
        "keywords": ("sonoma",),
    },
}


class PandasTableModel(QAbstractTableModel):
    def __init__(self, dataframe: pd.DataFrame | None = None) -> None:
        super().__init__()
        self._dataframe = pd.DataFrame() if dataframe is None else dataframe.copy()

    def set_dataframe(self, dataframe: pd.DataFrame) -> None:
        self.beginResetModel()
        self._dataframe = dataframe.copy()
        self.endResetModel()

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self._dataframe.index)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self._dataframe.columns)

    def data(self, index: QModelIndex, role: int = Qt.ItemDataRole.DisplayRole):
        if not index.isValid() or role != Qt.ItemDataRole.DisplayRole:
            return None
        value = self._dataframe.iat[index.row(), index.column()]
        if pd.isna(value):
            return ""
        return str(value)

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.ItemDataRole.DisplayRole):
        if role != Qt.ItemDataRole.DisplayRole:
            return None
        if orientation == Qt.Orientation.Horizontal:
            return str(self._dataframe.columns[section])
        return str(section + 1)


class LogEmitter(QObject):
    message = pyqtSignal(str)


class QtLogStream(io.TextIOBase):
    def __init__(self, emitter: LogEmitter) -> None:
        super().__init__()
        self.emitter = emitter

    def write(self, value: str) -> int:
        text = value.rstrip()
        if text:
            self.emitter.message.emit(text)
        return len(value)

    def flush(self) -> None:
        return None


class WorkerThread(QThread):
    result_ready = pyqtSignal(object)
    error_raised = pyqtSignal(str)
    log_message = pyqtSignal(str)

    def __init__(self, target, *args, **kwargs) -> None:
        super().__init__()
        self._target = target
        self._args = args
        self._kwargs = kwargs

    def run(self) -> None:
        emitter = LogEmitter()
        emitter.message.connect(self.log_message.emit)
        log_stream = QtLogStream(emitter)
        try:
            with contextlib.redirect_stdout(log_stream), contextlib.redirect_stderr(log_stream):
                result = self._target(*self._args, **self._kwargs)
            self.result_ready.emit(result)
        except Exception:
            self.error_raised.emit(traceback.format_exc())


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("COI GUI")
        self.resize(1600, 980)

        self.active_workers: list[WorkerThread] = []
        self.task_elapsed_timer = QElapsedTimer()
        self.task_timer = QTimer(self)
        self.task_timer.setInterval(1000)
        self.task_timer.timeout.connect(self._update_task_timer_display)

        self.result_model = PandasTableModel()

        self._build_ui()
        self._reset_results()

    def _build_ui(self) -> None:
        central = QWidget()
        root_layout = QVBoxLayout(central)
        root_layout.setContentsMargins(10, 10, 10, 10)

        location_group = QGroupBox("Location")
        location_form = QFormLayout(location_group)
        self.location_combo = QComboBox()
        self.location_combo.addItems(LOCATION_CONFIG.keys())
        self.open_tools_button = QPushButton("Open Tools")
        self.open_tools_button.clicked.connect(self.open_tools_window)
        self.load_sources_button = QPushButton("Load County Sources")
        self.load_sources_button.clicked.connect(self.load_county_sources)
        self.scrape_button = QPushButton("Scrape Website")
        self.scrape_button.clicked.connect(self.scrape_selected_location)
        self.compare_button = QPushButton("Match Interests")
        self.compare_button.clicked.connect(self.compare_selected_location)
        location_form.addRow("County / City", self.location_combo)
        location_form.addRow("", self.open_tools_button)
        location_form.addRow("", self.load_sources_button)
        location_form.addRow("", self.scrape_button)
        location_form.addRow("", self.compare_button)
        root_layout.addWidget(location_group)

        self.tabs = QTabWidget()
        self.tabs.addTab(self._build_results_tab(), "Results")
        root_layout.addWidget(self.tabs)

        self.tools_window = self._build_tools_window()

        self.setCentralWidget(central)
        self.setStatusBar(QStatusBar())
        self.status_message_label = QLabel("Ready")
        self.task_timer_label = QLabel("Elapsed: 00:00")
        self.statusBar().addPermanentWidget(self.status_message_label)
        self.statusBar().addPermanentWidget(self.task_timer_label)

    def _build_tools_window(self) -> QDialog:
        window = QDialog(self)
        window.setWindowTitle("Tools")
        window.resize(1100, 700)
        layout = QVBoxLayout(window)

        database_group = QGroupBox("Database")
        database_layout = QGridLayout(database_group)
        self.db_host_input = QLineEdit(DEFAULT_DB_HOST)
        self.db_port_input = QSpinBox()
        self.db_port_input.setRange(1, 65535)
        self.db_port_input.setValue(DEFAULT_DB_PORT)
        self.db_name_input = QLineEdit(DEFAULT_DB_NAME)
        self.db_user_input = QLineEdit(DEFAULT_DB_USER)
        self.db_password_input = QLineEdit(DEFAULT_DB_PASSWORD)
        self.db_schema_input = QLineEdit(DEFAULT_DB_SCHEMA)
        self.coi_table_combo = QComboBox()
        self.coi_table_combo.setEditable(True)
        self.coi_table_combo.currentTextChanged.connect(self._update_coi_source_label)
        self.entity_column_input = QLineEdit("name_of_business_entity")
        self.fuzzy_threshold_input = QSpinBox()
        self.fuzzy_threshold_input.setRange(0, 100)
        self.fuzzy_threshold_input.setValue(90)
        database_layout.addWidget(QLabel("Host"), 0, 0)
        database_layout.addWidget(self.db_host_input, 1, 0)
        database_layout.addWidget(QLabel("Port"), 0, 1)
        database_layout.addWidget(self.db_port_input, 1, 1)
        database_layout.addWidget(QLabel("Database"), 0, 2)
        database_layout.addWidget(self.db_name_input, 1, 2)
        database_layout.addWidget(QLabel("User"), 0, 3)
        database_layout.addWidget(self.db_user_input, 1, 3)
        database_layout.addWidget(QLabel("Password"), 0, 4)
        database_layout.addWidget(self.db_password_input, 1, 4)
        database_layout.addWidget(QLabel("Schema"), 2, 0)
        database_layout.addWidget(self.db_schema_input, 3, 0)
        database_layout.addWidget(QLabel("COI Table"), 2, 1)
        database_layout.addWidget(self.coi_table_combo, 3, 1, 1, 2)
        database_layout.addWidget(QLabel("Business Column"), 2, 3)
        database_layout.addWidget(self.entity_column_input, 3, 3)
        database_layout.addWidget(QLabel("Fuzzy Threshold"), 2, 4)
        database_layout.addWidget(self.fuzzy_threshold_input, 3, 4)
        database_layout.addWidget(self.load_sources_button, 3, 5)
        layout.addWidget(database_group)

        scrape_group = QGroupBox("Scrape")
        scrape_layout = QGridLayout(scrape_group)
        self.output_root_input = QLineEdit(str(DEFAULT_OUTPUT_ROOT))
        choose_output_button = QPushButton("Choose Output Folder")
        choose_output_button.clicked.connect(self.choose_output_root)
        self.save_html_checkbox = QCheckBox("Save HTML")
        self.max_pages_input = QSpinBox()
        self.max_pages_input.setRange(1, 100000)
        self.max_pages_input.setValue(DEFAULT_MAX_PAGES_PER_SITE)
        self.max_documents_input = QSpinBox()
        self.max_documents_input.setRange(1, 100000)
        self.max_documents_input.setValue(DEFAULT_MAX_DOCUMENTS_PER_SITE)
        self.max_age_years_input = QSpinBox()
        self.max_age_years_input.setRange(1, 50)
        self.max_age_years_input.setValue(DEFAULT_MAX_AGE_YEARS)
        self.delay_seconds_input = QDoubleSpinBox()
        self.delay_seconds_input.setRange(0.0, 60.0)
        self.delay_seconds_input.setSingleStep(0.05)
        self.delay_seconds_input.setValue(DEFAULT_DELAY_SECONDS)
        self.timeout_seconds_input = QSpinBox()
        self.timeout_seconds_input.setRange(1, 600)
        self.timeout_seconds_input.setValue(DEFAULT_TIMEOUT_SECONDS)
        self.years_input = QLineEdit()
        self.years_input.setPlaceholderText("Optional, e.g. 2026 2025")

        scrape_layout.addWidget(QLabel("Output Root"), 0, 0)
        scrape_layout.addWidget(self.output_root_input, 0, 1, 1, 3)
        scrape_layout.addWidget(choose_output_button, 0, 4)
        scrape_layout.addWidget(self.save_html_checkbox, 0, 5)
        scrape_layout.addWidget(QLabel("Max Pages"), 1, 0)
        scrape_layout.addWidget(self.max_pages_input, 1, 1)
        scrape_layout.addWidget(QLabel("Max Documents"), 1, 2)
        scrape_layout.addWidget(self.max_documents_input, 1, 3)
        scrape_layout.addWidget(QLabel("Max Age Years"), 1, 4)
        scrape_layout.addWidget(self.max_age_years_input, 1, 5)
        scrape_layout.addWidget(QLabel("Delay Seconds"), 2, 0)
        scrape_layout.addWidget(self.delay_seconds_input, 2, 1)
        scrape_layout.addWidget(QLabel("Timeout Seconds"), 2, 2)
        scrape_layout.addWidget(self.timeout_seconds_input, 2, 3)
        scrape_layout.addWidget(QLabel("Years"), 2, 4)
        scrape_layout.addWidget(self.years_input, 2, 5)
        layout.addWidget(scrape_group)

        action_group = QGroupBox("Actions")
        action_layout = QGridLayout(action_group)
        self.activity_status_label = QLabel("Status: Idle")
        self.activity_progress = QProgressBar()
        self.activity_progress.setRange(0, 1)
        self.activity_progress.setValue(0)
        self.activity_progress.setTextVisible(False)

        action_layout.addWidget(self.activity_status_label, 0, 0, 1, 2)
        action_layout.addWidget(self.activity_progress, 1, 0, 1, 2)
        layout.addWidget(action_group)
        layout.addStretch(1)

        return window

    def open_tools_window(self) -> None:
        self.tools_window.show()
        self.tools_window.raise_()
        self.tools_window.activateWindow()

    def _build_results_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        source_splitter = QSplitter(Qt.Orientation.Horizontal)

        coi_group = QGroupBox("Council Member Interests Source")
        coi_layout = QVBoxLayout(coi_group)
        self.coi_source_text = QPlainTextEdit()
        self.coi_source_text.setReadOnly(True)
        coi_layout.addWidget(self.coi_source_text)

        parser_group = QGroupBox("Award Parser Company Source")
        parser_layout = QVBoxLayout(parser_group)
        self.parser_source_text = QPlainTextEdit()
        self.parser_source_text.setReadOnly(True)
        parser_layout.addWidget(self.parser_source_text)

        source_splitter.addWidget(coi_group)
        source_splitter.addWidget(parser_group)
        source_splitter.setStretchFactor(0, 1)
        source_splitter.setStretchFactor(1, 1)
        layout.addWidget(source_splitter, 1)

        summary_group = QGroupBox("Summary")
        summary_layout = QVBoxLayout(summary_group)
        self.summary_text = QPlainTextEdit()
        self.summary_text.setReadOnly(True)
        summary_layout.addWidget(self.summary_text)
        layout.addWidget(summary_group, 1)

        result_group = QGroupBox("Matches")
        result_layout = QVBoxLayout(result_group)
        self.result_summary_label = QLabel("")
        self.result_table = QTableView()
        self.result_table.setModel(self.result_model)
        self.result_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.result_table.verticalHeader().setVisible(False)
        result_layout.addWidget(self.result_summary_label)
        result_layout.addWidget(self.result_table)
        layout.addWidget(result_group, 3)

        log_group = QGroupBox("Logs")
        log_layout = QVBoxLayout(log_group)
        self.log_output = QPlainTextEdit()
        self.log_output.setReadOnly(True)
        log_layout.addWidget(self.log_output)
        layout.addWidget(log_group, 1)

        return tab

    def _reset_results(self) -> None:
        self.result_model.set_dataframe(pd.DataFrame())
        self.result_summary_label.setText("No results loaded.")
        self.summary_text.setPlainText("")
        self.coi_source_text.setPlainText("COI source not loaded.")
        self.parser_source_text.setPlainText("Award parser source not loaded.")
        self.status_message_label.setText("Ready")

    def _log(self, message: str) -> None:
        self.log_output.appendPlainText(message)
        self.statusBar().showMessage(message, 5000)
        self.status_message_label.setText(message)

    def _set_activity_status(self, message: str, running: bool) -> None:
        self.activity_status_label.setText(f"Status: {message}")
        if running:
            self.activity_progress.setRange(0, 0)
        else:
            self.activity_progress.setRange(0, 1)
            self.activity_progress.setValue(1 if message.lower() == "complete" else 0)

    def _start_task_timer(self) -> None:
        if not self.active_workers:
            self.task_elapsed_timer.start()
            self.task_timer.start()
            self._update_task_timer_display()

    def _stop_task_timer(self) -> None:
        self.task_timer.stop()
        self.task_timer_label.setText("Elapsed: 00:00")

    def _update_task_timer_display(self) -> None:
        if not self.task_elapsed_timer.isValid():
            self.task_timer_label.setText("Elapsed: 00:00")
            return
        total_seconds = self.task_elapsed_timer.elapsed() // 1000
        minutes, seconds = divmod(total_seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            self.task_timer_label.setText(f"Elapsed: {hours:02}:{minutes:02}:{seconds:02}")
        else:
            self.task_timer_label.setText(f"Elapsed: {minutes:02}:{seconds:02}")

    def _safe_identifier(self, value: str, field_name: str) -> str:
        cleaned = value.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", cleaned):
            raise ValueError(f"Invalid {field_name}: {value!r}")
        return cleaned

    def _normalize_entity_name(self, value: str) -> str:
        normalized = value.strip().lower().replace("&", " and ")
        normalized = re.sub(r"[^a-z0-9\s]+", " ", normalized)
        tokens = [token for token in normalized.split() if token]
        legal_suffixes = {
            "inc",
            "llc",
            "corp",
            "corporation",
            "co",
            "company",
            "ltd",
            "lp",
            "llp",
            "pllc",
        }
        while tokens and tokens[-1] in legal_suffixes:
            tokens.pop()
        return " ".join(tokens)

    def _find_coi_column(self, columns: list[str], candidates: tuple[str, ...]) -> str | None:
        normalized_map = {
            re.sub(r"[\s_]+", " ", str(column).strip().lower()): str(column)
            for column in columns
        }
        for candidate in candidates:
            if candidate in normalized_map:
                return normalized_map[candidate]
        return None

    def _current_location(self) -> str:
        return self.location_combo.currentText().strip()

    def _current_location_site(self) -> str:
        return LOCATION_CONFIG[self._current_location()]["site"]

    def _current_parser_database(self) -> str:
        return database_name_for_location(self._current_location())

    def _current_location_keywords(self) -> tuple[str, ...]:
        return LOCATION_CONFIG[self._current_location()]["keywords"]

    def _parser_csv_path_for_location(self, output_root: Path, location_name: str) -> Path:
        site_name = LOCATION_CONFIG[location_name]["site"]
        site_root = output_root / site_name
        site_csv = site_root / DEFAULT_CSV_OUTPUT_NAME
        if site_csv.exists():
            return site_csv
        root_csv = output_root / DEFAULT_CSV_OUTPUT_NAME
        return root_csv

    def _update_coi_source_label(self) -> None:
        schema_name = self.db_schema_input.text().strip()
        table_name = self.coi_table_combo.currentText().strip()
        if schema_name and table_name:
            self.coi_source_text.setPlainText(
                f"Location: {self._current_location()}\n"
                f"Database: {self.db_name_input.text().strip()}\n"
                f"Schema: {schema_name}\n"
                f"Table: {table_name}\n"
                f"Business Column: {self.entity_column_input.text().strip()}"
            )

    def choose_output_root(self) -> None:
        folder_path = QFileDialog.getExistingDirectory(
            self,
            "Choose Output Folder",
            self.output_root_input.text(),
        )
        if folder_path:
            self.output_root_input.setText(folder_path)

    def _parse_years_input(self) -> list[int] | None:
        raw_value = self.years_input.text().strip()
        if not raw_value:
            return None
        tokens = [token for token in raw_value.replace(",", " ").split() if token]
        try:
            return [int(token) for token in tokens]
        except ValueError as exc:
            raise ValueError("Years must be integers separated by spaces or commas.") from exc

    def load_county_sources(self) -> None:
        schema_name = self.db_schema_input.text().strip()
        if not schema_name:
            self._show_error("Missing Schema", "Enter a database schema first.")
            return

        self.load_sources_button.setEnabled(False)
        self._set_activity_status("Loading Sources", running=True)
        self._start_worker(
            self._run_load_county_sources_job,
            self._handle_load_county_sources_result,
            error_callback=self._handle_load_county_sources_error,
            location_name=self._current_location(),
            database_name=self.db_name_input.text().strip(),
            db_host=self.db_host_input.text().strip(),
            db_port=int(self.db_port_input.value()),
            db_user=self.db_user_input.text().strip(),
            db_password=self.db_password_input.text(),
            schema_name=schema_name,
            output_root=Path(self.output_root_input.text().strip()),
        )

    def _run_load_county_sources_job(
        self,
        *,
        location_name: str,
        database_name: str,
        db_host: str,
        db_port: int,
        db_user: str,
        db_password: str,
        schema_name: str,
        output_root: Path,
    ) -> dict:
        safe_schema = self._safe_identifier(schema_name, "schema")
        query = (
            "SELECT table_name "
            "FROM information_schema.tables "
            f"WHERE table_schema = '{safe_schema}' "
            "AND table_type = 'BASE TABLE' "
            "ORDER BY table_name;"
        )
        rows = run_query(
            database_name=database_name,
            query=query,
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
        )
        all_tables = [str(row.get("table_name") or "").strip() for row in rows if str(row.get("table_name") or "").strip()]
        keywords = LOCATION_CONFIG[location_name]["keywords"]
        matching_tables = [
            table_name
            for table_name in all_tables
            if any(keyword in table_name.lower().replace("_", " ") or keyword in table_name.lower() for keyword in keywords)
        ]

        parser_database = database_name_for_location(location_name)
        parser_row_count = None
        parser_error = None
        parser_csv_path = self._parser_csv_path_for_location(output_root, location_name)
        parser_csv_row_count = None
        parser_source_type = "database"
        try:
            parser_rows = run_query(
                database_name=parser_database,
                query="SELECT COUNT(*) AS row_count FROM company_links;",
                host=db_host,
                port=db_port,
                user=db_user,
                password=db_password,
            )
            parser_row_count = int(parser_rows[0]["row_count"]) if parser_rows else 0
        except Exception as exc:
            parser_error = str(exc)
            if parser_csv_path.exists():
                parser_csv = pd.read_csv(parser_csv_path)
                if "company_name" in parser_csv.columns:
                    parser_csv = parser_csv[parser_csv["company_name"].fillna("").astype(str).str.strip() != ""]
                parser_csv_row_count = len(parser_csv)
                parser_source_type = "csv"

        return {
            "location": location_name,
            "schema": safe_schema,
            "tables": matching_tables or all_tables,
            "preferred_table": matching_tables[0] if matching_tables else "",
            "parser_database": parser_database,
            "parser_row_count": parser_row_count,
            "parser_error": parser_error,
            "parser_csv_path": str(parser_csv_path),
            "parser_csv_row_count": parser_csv_row_count,
            "parser_source_type": parser_source_type,
        }

    def _handle_load_county_sources_result(self, result: dict) -> None:
        current_value = self.coi_table_combo.currentText().strip()
        self.coi_table_combo.clear()
        self.coi_table_combo.addItems(result["tables"])
        if result["preferred_table"]:
            self.coi_table_combo.setCurrentText(result["preferred_table"])
        elif current_value:
            self.coi_table_combo.setCurrentText(current_value)
        elif result["tables"]:
            self.coi_table_combo.setCurrentIndex(0)

        self._update_coi_source_label()
        parser_text = [
            f"Location: {result['location']}",
            f"Parser Database: {result['parser_database']}",
        ]
        if result["parser_source_type"] == "database":
            parser_text.append("Source Type: database")
            parser_text.append("Source Table: company_links")
            parser_text.append(f"Rows: {result['parser_row_count']}")
        elif result["parser_source_type"] == "csv":
            parser_text.append("Source Type: csv fallback")
            parser_text.append(f"CSV Path: {result['parser_csv_path']}")
            parser_text.append(f"Rows: {result['parser_csv_row_count']}")
            parser_text.append(f"Database Error: {result['parser_error']}")
        else:
            parser_text.append("Status: Not available")
            parser_text.append(f"Database Error: {result['parser_error']}")
            parser_text.append(f"CSV Path Checked: {result['parser_csv_path']}")
        self.parser_source_text.setPlainText("\n".join(parser_text))

        self.load_sources_button.setEnabled(True)
        self._set_activity_status("Complete", running=False)
        self._log(f"Loaded county sources for {result['location']}.")
        self.tabs.setCurrentIndex(1)

    def _handle_load_county_sources_error(self, traceback_text: str) -> None:
        self.load_sources_button.setEnabled(True)
        self._set_activity_status("Failed", running=False)
        self._show_error("Load County Sources Failed", traceback_text)

    def scrape_selected_location(self) -> None:
        try:
            years = self._parse_years_input()
        except ValueError as exc:
            self._show_error("Invalid Years", str(exc))
            return

        self.scrape_button.setEnabled(False)
        self._set_activity_status("Scraping", running=True)
        self._start_worker(
            self._run_scrape_job,
            self._handle_scrape_result,
            error_callback=self._handle_scrape_error,
            location_name=self._current_location(),
            output_root=Path(self.output_root_input.text().strip()),
            save_html=self.save_html_checkbox.isChecked(),
            max_pages=self.max_pages_input.value(),
            max_documents=self.max_documents_input.value(),
            max_age_years=self.max_age_years_input.value(),
            delay_seconds=self.delay_seconds_input.value(),
            timeout_seconds=self.timeout_seconds_input.value(),
            years=years,
            db_host=self.db_host_input.text().strip(),
            db_port=int(self.db_port_input.value()),
            db_admin_database=self.db_name_input.text().strip(),
            db_user=self.db_user_input.text().strip(),
            db_password=self.db_password_input.text(),
        )
        self._log(f"Started scrape for {self._current_location()}.")

    def _run_scrape_job(
        self,
        *,
        location_name: str,
        output_root: Path,
        save_html: bool,
        max_pages: int,
        max_documents: int,
        max_age_years: int,
        delay_seconds: float,
        timeout_seconds: int,
        years: list[int] | None,
        db_host: str,
        db_port: int,
        db_admin_database: str,
        db_user: str,
        db_password: str,
    ) -> dict:
        site_name = LOCATION_CONFIG[location_name]["site"]
        crawler = CouncilCrawler(
            output_root=output_root,
            save_html=save_html,
            max_pages_per_site=max_pages,
            max_documents_per_site=max_documents,
            max_age_years=max_age_years,
            delay_seconds=delay_seconds,
            timeout_seconds=timeout_seconds,
            years=years,
        )
        crawl_summary = crawler.crawl(TARGETS[site_name])

        parser = AwardParser(input_root=output_root)
        candidates, warnings = parser.parse(sites=[site_name])
        jsonl_output = output_root / DEFAULT_OUTPUT_NAME
        csv_output = output_root / DEFAULT_CSV_OUTPUT_NAME
        jsonl_output.parent.mkdir(parents=True, exist_ok=True)
        csv_output.parent.mkdir(parents=True, exist_ok=True)
        write_jsonl(jsonl_output, candidates)
        write_csv(csv_output, candidates)

        inserted_counts = save_candidates_to_location_databases(
            candidates=candidates,
            host=db_host,
            port=db_port,
            admin_database=db_admin_database,
            user=db_user,
            password=db_password,
        )

        return {
            "location": location_name,
            "site": site_name,
            "crawl_summary": crawl_summary,
            "candidate_count": len(candidates),
            "warning_count": len(warnings),
            "warnings": warnings,
            "jsonl_output": str(jsonl_output),
            "csv_output": str(csv_output),
            "parser_database": database_name_for_location(location_name),
            "inserted_counts": inserted_counts,
        }

    def _handle_scrape_result(self, result: dict) -> None:
        self.scrape_button.setEnabled(True)
        self._set_activity_status("Complete", running=False)
        self.summary_text.setPlainText(json.dumps(result, indent=2))
        self.parser_source_text.setPlainText(
            f"Location: {result['location']}\n"
            f"Parser Database: {result['parser_database']}\n"
            f"JSONL Output: {result['jsonl_output']}\n"
            f"CSV Output: {result['csv_output']}\n"
            f"Inserted Rows: {json.dumps(result['inserted_counts'])}"
        )
        self.result_model.set_dataframe(pd.DataFrame())
        self.result_summary_label.setText("Scrape complete. Ready for matching.")
        self._log(f"Scrape finished for {result['location']}.")
        self.tabs.setCurrentIndex(1)
        self.load_county_sources()

    def _handle_scrape_error(self, traceback_text: str) -> None:
        self.scrape_button.setEnabled(True)
        self._set_activity_status("Failed", running=False)
        self._show_error("Scrape Failed", traceback_text)

    def compare_selected_location(self) -> None:
        schema_name = self.db_schema_input.text().strip()
        table_name = self.coi_table_combo.currentText().strip()
        column_name = self.entity_column_input.text().strip()
        if not schema_name or not table_name or not column_name:
            self._show_error("Missing Comparison Settings", "Load the county sources and select a COI table first.")
            return

        self.compare_button.setEnabled(False)
        self._set_activity_status("Matching", running=True)
        self._start_worker(
            self._run_compare_job,
            self._handle_compare_result,
            error_callback=self._handle_compare_error,
            location_name=self._current_location(),
            database_name=self.db_name_input.text().strip(),
            db_host=self.db_host_input.text().strip(),
            db_port=int(self.db_port_input.value()),
            db_user=self.db_user_input.text().strip(),
            db_password=self.db_password_input.text(),
            schema_name=schema_name,
            table_name=table_name,
            column_name=column_name,
            fuzzy_threshold=self.fuzzy_threshold_input.value(),
            output_root=Path(self.output_root_input.text().strip()),
        )
        self._log(f"Started comparison for {self._current_location()}.")

    def _run_compare_job(
        self,
        *,
        location_name: str,
        database_name: str,
        db_host: str,
        db_port: int,
        db_user: str,
        db_password: str,
        schema_name: str,
        table_name: str,
        column_name: str,
        fuzzy_threshold: int,
        output_root: Path,
    ) -> dict:
        safe_schema = self._safe_identifier(schema_name, "schema")
        safe_table = self._safe_identifier(table_name, "table")
        safe_column = self._safe_identifier(column_name, "column")
        coi_query = (
            f'SELECT * '
            f'FROM "{safe_schema}"."{safe_table}" '
            f'WHERE "{safe_column}" IS NOT NULL AND BTRIM("{safe_column}"::text) <> \'\';'
        )
        coi_rows = run_query(
            database_name=database_name,
            query=coi_query,
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
        )
        coi_columns = list(coi_rows[0].keys()) if coi_rows else []
        full_name_column = self._find_coi_column(
            coi_columns,
            ("full name", "name", "official name", "council member name"),
        )
        first_name_column = self._find_coi_column(coi_columns, ("first name", "firstname", "first_name"))
        middle_name_column = self._find_coi_column(coi_columns, ("middle name", "middlename", "middle_name"))
        last_name_column = self._find_coi_column(coi_columns, ("last name", "lastname", "last_name"))
        email_column = self._find_coi_column(
            coi_columns,
            ("email", "email address", "work email address", "work_email_address", "email_address"),
        )
        position_column = self._find_coi_column(
            coi_columns,
            (
                "position",
                "title",
                "office",
                "elected office",
                "position/title",
                "position title",
                "position_title",
                "public position",
            ),
        )

        parser_database_name = database_name_for_location(location_name)
        parser_source_type = "database"
        parser_source_detail = f"{parser_database_name}.company_links"
        parser_csv_path = self._parser_csv_path_for_location(output_root, location_name)
        parser_csv_rows: list[dict] = []
        if parser_csv_path.exists():
            parser_csv = pd.read_csv(parser_csv_path).fillna("")
            if "company_name" in parser_csv.columns:
                parser_csv_rows = parser_csv.to_dict("records")
        try:
            parser_rows = run_query(
                database_name=parser_database_name,
                query=(
                    "SELECT company_name, source_url, COALESCE(snippet, '') AS snippet, COALESCE(source_path, '') AS source_path "
                    "FROM company_links "
                    "WHERE company_name IS NOT NULL AND BTRIM(company_name) <> '';"
                ),
                host=db_host,
                port=db_port,
                user=db_user,
                password=db_password,
            )
        except Exception as exc:
            parser_csv_path = self._parser_csv_path_for_location(output_root, location_name)
            if not parser_csv_path.exists():
                raise RuntimeError(
                    f"Parser database unavailable ({exc}) and parser CSV not found at {parser_csv_path}"
                ) from exc
            parser_csv = pd.read_csv(parser_csv_path)
            if "company_name" not in parser_csv.columns:
                raise RuntimeError(f"Parser CSV missing company_name column: {parser_csv_path}")
            parser_rows = parser_csv.fillna("").to_dict("records")
            parser_source_type = "csv"
            parser_source_detail = str(parser_csv_path)

        snippet_lookup: dict[tuple[str, str], list[str]] = {}
        source_path_lookup: dict[tuple[str, str], list[str]] = {}
        for row in parser_csv_rows:
            company_name = str(row.get("company_name") or "").strip()
            source_url = str(row.get("source_url") or "").strip()
            snippet = str(row.get("snippet") or "").strip()
            source_path = str(row.get("source_path") or "").strip()
            normalized = self._normalize_entity_name(company_name) if company_name else ""
            if not normalized:
                continue
            key = (normalized, source_url)
            if snippet:
                snippet_lookup.setdefault(key, [])
                if snippet not in snippet_lookup[key]:
                    snippet_lookup[key].append(snippet)
            if source_path:
                source_path_lookup.setdefault(key, [])
                if source_path not in source_path_lookup[key]:
                    source_path_lookup[key].append(source_path)

        parser_companies: dict[str, list[dict[str, str]]] = {}
        for row in parser_rows:
            company_name = str(row.get("company_name") or "").strip()
            if not company_name:
                continue
            normalized = self._normalize_entity_name(company_name)
            if not normalized:
                continue
            source_url = str(row.get("source_url") or "").strip()
            db_snippet = str(row.get("snippet") or "").strip()
            db_source_path = str(row.get("source_path") or "").strip()
            snippets = []
            source_paths = []
            if db_snippet:
                snippets.append(db_snippet)
            if db_source_path:
                source_paths.append(db_source_path)
            snippets.extend(snippet_lookup.get((normalized, source_url), []))
            source_paths.extend(source_path_lookup.get((normalized, source_url), []))
            unique_snippets = list(dict.fromkeys(snippets))
            unique_source_paths = list(dict.fromkeys(source_paths))
            parser_companies.setdefault(normalized, []).append(
                {
                    "company_name": company_name,
                    "source_url": source_url,
                    "snippet": " | ".join(unique_snippets),
                    "source_path": " | ".join(unique_source_paths),
                }
            )

        comparison_rows: list[dict[str, str]] = []
        exact_match_count = 0
        partial_match_count = 0
        fuzzy_match_count = 0
        unmatched_count = 0

        for row in coi_rows:
            business_entity_name = str(row.get(safe_column) or "").strip()
            if not business_entity_name:
                continue
            first_name = str(row.get(first_name_column) or "").strip() if first_name_column else ""
            middle_name = str(row.get(middle_name_column) or "").strip() if middle_name_column else ""
            last_name = str(row.get(last_name_column) or "").strip() if last_name_column else ""
            fallback_full_name = " ".join(part for part in (first_name, middle_name, last_name) if part)
            full_name = str(row.get(full_name_column) or "").strip() if full_name_column else fallback_full_name
            if not full_name:
                full_name = fallback_full_name
            email = str(row.get(email_column) or "").strip() if email_column else ""
            position = str(row.get(position_column) or "").strip() if position_column else ""
            normalized_entity = self._normalize_entity_name(business_entity_name)
            exact_matches = parser_companies.get(normalized_entity, [])
            partial_matches: list[dict[str, str]] = []
            fuzzy_matches: list[dict[str, str]] = []
            fuzzy_score = 0
            match_type = "unmatched"

            if exact_matches:
                match_type = "exact"
                exact_match_count += 1
                matches = exact_matches
            else:
                if normalized_entity:
                    for parser_name, parser_entries in parser_companies.items():
                        if normalized_entity in parser_name or parser_name in normalized_entity:
                            partial_matches.extend(parser_entries)
                if partial_matches:
                    match_type = "partial"
                    partial_match_count += 1
                    matches = partial_matches
                else:
                    best_score = -1
                    best_entries: list[dict[str, str]] = []
                    for parser_name, parser_entries in parser_companies.items():
                        score = int(fuzz.token_set_ratio(normalized_entity, parser_name))
                        if score > best_score:
                            best_score = score
                            best_entries = parser_entries
                    if best_score >= fuzzy_threshold:
                        fuzzy_matches = best_entries
                        fuzzy_score = best_score
                        match_type = "fuzzy"
                        fuzzy_match_count += 1
                        matches = fuzzy_matches
                    else:
                        unmatched_count += 1
                        matches = []

            comparison_rows.append(
                {
                    "business_entity_name": business_entity_name,
                    "council_member_name": full_name,
                    "council_member_email": email,
                    "council_member_position": position,
                    "normalized_business_entity_name": normalized_entity,
                    "match_type": match_type,
                    "match_score": str(fuzzy_score if match_type == "fuzzy" else (100 if match_type == "exact" else "")),
                    "matched_companies": "; ".join(
                        list(dict.fromkeys(match["company_name"] for match in matches if match["company_name"]))
                    ),
                    "matched_source_urls": "; ".join(
                        list(dict.fromkeys(match["source_url"] for match in matches if match["source_url"]))
                    ),
                    "matched_source_paths": " || ".join(
                        list(dict.fromkeys(match["source_path"] for match in matches if match.get("source_path")))
                    ),
                    "agenda_description": " || ".join(
                        list(dict.fromkeys(match["snippet"] for match in matches if match.get("snippet")))
                    ),
                    "matched_snippets": " || ".join(
                        list(dict.fromkeys(match["snippet"] for match in matches if match.get("snippet")))
                    ),
                }
            )

        filtered_rows = [row for row in comparison_rows if row.get("match_type") != "unmatched"]
        match_order = {"exact": 0, "fuzzy": 1, "partial": 2}
        filtered_rows.sort(
            key=lambda row: (
                match_order.get(str(row.get("match_type") or ""), 99),
                -int(str(row.get("match_score") or "0") or "0"),
                str(row.get("business_entity_name") or "").lower(),
            )
        )
        result_dataframe = pd.DataFrame(filtered_rows)
        summary = {
            "location": location_name,
            "coi_database": database_name,
            "coi_schema": safe_schema,
            "coi_table": safe_table,
            "coi_business_column": safe_column,
            "award_parser_source_type": parser_source_type,
            "award_parser_source": parser_source_detail,
            "fuzzy_threshold": fuzzy_threshold,
            "business_entities_checked": len(comparison_rows),
            "displayed_matches": len(filtered_rows),
            "exact_matches": exact_match_count,
            "partial_matches": partial_match_count,
            "fuzzy_matches": fuzzy_match_count,
            "unmatched": unmatched_count,
        }
        return {"summary": summary, "dataframe": result_dataframe}

    def _handle_compare_result(self, result: dict) -> None:
        self.compare_button.setEnabled(True)
        self._set_activity_status("Complete", running=False)
        self.summary_text.setPlainText(json.dumps(result["summary"], indent=2))
        dataframe = result["dataframe"]
        self.result_model.set_dataframe(dataframe.astype(str) if not dataframe.empty else dataframe)
        self.result_summary_label.setText(
            f"Rows: {len(dataframe)} | Columns: {len(dataframe.columns)}"
        )
        self.result_table.resizeColumnsToContents()
        self._log(f"Comparison finished for {result['summary']['location']}.")
        self.tabs.setCurrentIndex(1)

    def _handle_compare_error(self, traceback_text: str) -> None:
        self.compare_button.setEnabled(True)
        self._set_activity_status("Failed", running=False)
        self._show_error("Comparison Failed", traceback_text)

    def _start_worker(self, job, callback, error_callback=None, **kwargs) -> None:
        worker = WorkerThread(job, **kwargs)
        worker.result_ready.connect(callback)
        worker.log_message.connect(self._log)
        worker.error_raised.connect(error_callback or self._handle_worker_error)
        worker.finished.connect(lambda: self._cleanup_worker(worker))
        self._start_task_timer()
        self.active_workers.append(worker)
        worker.start()

    def _handle_worker_error(self, traceback_text: str) -> None:
        self.load_sources_button.setEnabled(True)
        self.scrape_button.setEnabled(True)
        self.compare_button.setEnabled(True)
        self._set_activity_status("Failed", running=False)
        self._show_error("Background Task Failed", traceback_text)

    def _cleanup_worker(self, worker: WorkerThread) -> None:
        if worker in self.active_workers:
            self.active_workers.remove(worker)
        if not self.active_workers:
            self._stop_task_timer()

    def _show_error(self, title: str, message: str) -> None:
        QMessageBox.critical(self, title, message)
        self._log(f"{title}: {message}")


def main() -> None:
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
