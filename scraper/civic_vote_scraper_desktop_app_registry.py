#!/usr/bin/env python3
from __future__ import annotations

import csv
import os
import sys
from pathlib import Path

try:
    from PyQt5.QtCore import QProcess, QRectF, QSize, Qt
    from PyQt5.QtGui import QColor, QPainter, QPen
    from PyQt5.QtWidgets import (
        QApplication,
        QCheckBox,
        QComboBox,
        QDialog,
        QFileDialog,
        QFrame,
        QGridLayout,
        QHBoxLayout,
        QHeaderView,
        QLabel,
        QLineEdit,
        QMainWindow,
        QMessageBox,
        QPlainTextEdit,
        QPushButton,
        QSpinBox,
        QStackedWidget,
        QTableWidget,
        QTableWidgetItem,
        QVBoxLayout,
        QWidget,
    )
except ImportError as exc:  # pragma: no cover - shown to desktop users at startup.
    raise SystemExit("PyQt5 is required. Install it with: pip install -r requirements.txt") from exc


APP_TITLE = "Civic Vote Scraper"

COLORS = {
    "app": "#fbfcfd",
    "surface": "#ffffff",
    "line": "#d9dee3",
    "line_soft": "#edf1f3",
    "text": "#13252e",
    "muted": "#5c6870",
    "teal": "#00827c",
    "teal_dark": "#006a65",
    "teal_soft": "#e8f6f4",
    "green": "#16a034",
    "orange": "#c46f00",
    "blue": "#0078d4",
    "danger": "#d92d20",
    "danger_soft": "#fff5f4",
}


class IconWidget(QWidget):
    def __init__(self, kind: str, color: str, size: int = 32, parent: QWidget | None = None):
        super().__init__(parent)
        self.kind = kind
        self.color = QColor(color)
        self._size = size
        self.setFixedSize(size, size)

    def sizeHint(self) -> QSize:
        return QSize(self._size, self._size)

    def paintEvent(self, _event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        painter.setPen(QPen(self.color, max(2, self._size // 16), Qt.SolidLine, Qt.RoundCap, Qt.RoundJoin))
        painter.setBrush(Qt.NoBrush)
        w = self.width()
        h = self.height()

        if self.kind == "building":
            painter.setBrush(self.color)
            points = [
                self._pt(w * 0.12, h * 0.35),
                self._pt(w * 0.50, h * 0.10),
                self._pt(w * 0.88, h * 0.35),
            ]
            painter.drawPolygon(*points)
            painter.drawRect(QRectF(w * 0.18, h * 0.42, w * 0.64, h * 0.08))
            for x in (0.27, 0.47, 0.67):
                painter.drawRect(QRectF(w * x, h * 0.54, w * 0.07, h * 0.26))
            painter.drawRect(QRectF(w * 0.16, h * 0.86, w * 0.68, h * 0.07))
        elif self.kind == "search":
            painter.drawEllipse(QRectF(w * 0.15, h * 0.12, w * 0.48, h * 0.48))
            painter.drawLine(self._pt(w * 0.56, h * 0.56), self._pt(w * 0.84, h * 0.84))
        elif self.kind == "file":
            painter.drawRect(QRectF(w * 0.24, h * 0.10, w * 0.50, h * 0.78))
            painter.drawLine(self._pt(w * 0.58, h * 0.10), self._pt(w * 0.74, h * 0.26))
            painter.drawLine(self._pt(w * 0.58, h * 0.10), self._pt(w * 0.58, h * 0.28))
            painter.drawLine(self._pt(w * 0.34, h * 0.48), self._pt(w * 0.64, h * 0.48))
            painter.drawLine(self._pt(w * 0.34, h * 0.60), self._pt(w * 0.64, h * 0.60))
        elif self.kind == "clipboard":
            painter.drawRect(QRectF(w * 0.23, h * 0.20, w * 0.54, h * 0.68))
            painter.drawRect(QRectF(w * 0.38, h * 0.10, w * 0.24, h * 0.16))
            painter.drawLine(self._pt(w * 0.34, h * 0.53), self._pt(w * 0.44, h * 0.64))
            painter.drawLine(self._pt(w * 0.44, h * 0.64), self._pt(w * 0.68, h * 0.38))
        elif self.kind == "database":
            painter.drawEllipse(QRectF(w * 0.22, h * 0.10, w * 0.56, h * 0.22))
            painter.drawLine(self._pt(w * 0.22, h * 0.21), self._pt(w * 0.22, h * 0.76))
            painter.drawLine(self._pt(w * 0.78, h * 0.21), self._pt(w * 0.78, h * 0.76))
            painter.drawArc(QRectF(w * 0.22, h * 0.64, w * 0.56, h * 0.22), 180 * 16, 180 * 16)
            painter.drawArc(QRectF(w * 0.22, h * 0.38, w * 0.56, h * 0.22), 180 * 16, 180 * 16)
        elif self.kind == "folder":
            painter.drawRect(QRectF(w * 0.12, h * 0.34, w * 0.76, h * 0.44))
            painter.drawLine(self._pt(w * 0.12, h * 0.34), self._pt(w * 0.35, h * 0.34))
            painter.drawLine(self._pt(w * 0.35, h * 0.34), self._pt(w * 0.43, h * 0.43))
            painter.drawLine(self._pt(w * 0.43, h * 0.43), self._pt(w * 0.88, h * 0.43))
        elif self.kind == "csv":
            painter.drawRect(QRectF(w * 0.24, h * 0.10, w * 0.50, h * 0.78))
            painter.drawText(QRectF(0, h * 0.35, w, h * 0.36), Qt.AlignCenter, "CSV")
        elif self.kind == "settings":
            painter.drawEllipse(QRectF(w * 0.30, h * 0.30, w * 0.40, h * 0.40))
            for x1, y1, x2, y2 in (
                (0.50, 0.06, 0.50, 0.20),
                (0.50, 0.80, 0.50, 0.94),
                (0.06, 0.50, 0.20, 0.50),
                (0.80, 0.50, 0.94, 0.50),
                (0.18, 0.18, 0.28, 0.28),
                (0.72, 0.72, 0.82, 0.82),
            ):
                painter.drawLine(self._pt(w * x1, h * y1), self._pt(w * x2, h * y2))

    def _pt(self, x: float, y: float):
        from PyQt5.QtCore import QPointF

        return QPointF(x, y)


class Dot(QWidget):
    def __init__(self, color: str, parent: QWidget | None = None):
        super().__init__(parent)
        self.color = QColor(color)
        self.setFixedSize(14, 14)

    def paintEvent(self, _event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        painter.setPen(Qt.NoPen)
        painter.setBrush(self.color)
        painter.drawEllipse(QRectF(2, 2, 10, 10))


class SettingsDialog(QDialog):
    def __init__(self, app: "App"):
        super().__init__(app)
        self.app = app
        self.setWindowTitle("Settings")
        self.setModal(True)
        self.setMinimumWidth(760)
        self.fields: dict[str, QWidget] = {}

        root = QVBoxLayout(self)
        root.setContentsMargins(22, 22, 22, 22)
        root.setSpacing(16)

        title = QLabel("Settings")
        title.setObjectName("DialogTitle")
        root.addWidget(title)

        grid = QGridLayout()
        grid.setHorizontalSpacing(14)
        grid.setVerticalSpacing(12)
        root.addLayout(grid)

        self._path_field(grid, "Project directory", "project_dir", 0, True)
        self._path_field(grid, "Output folder", "output_dir", 1, True)
        self._text_field(grid, "Form 700 search URL", "form700_search_url", 2, 0)
        self._text_field(grid, "Form 700 folder", "form700_folder", 2, 1)
        self._text_field(grid, "Page limit", "page_limit", 3, 0)
        self._text_field(grid, "Meeting limit", "meeting_limit", 3, 1)
        self._text_field(grid, "Min confidence", "min_confidence", 4, 0)
        self._text_field(grid, "Text index", "minutes_text_index", 4, 1)
        self._text_field(grid, "Entities CSV", "form700_csv_out", 5, 0)
        self._text_field(grid, "Entities JSON", "form700_json_out", 5, 1)
        self._text_field(grid, "Matches CSV", "form700_matches_out", 6, 0)
        self._text_field(grid, "Matches JSON", "form700_matches_json_out", 6, 1)

        checks = QHBoxLayout()
        self.headless = QCheckBox("Run headless")
        self.headless.setChecked(bool(app.values["headless"]))
        self.reparse = QCheckBox("Re-parse known minutes")
        self.reparse.setChecked(bool(app.values["reparse_existing_minutes"]))
        self.reparse_form700 = QCheckBox("Re-parse known Form 700 PDFs")
        self.reparse_form700.setChecked(bool(app.values["reparse_existing_form700s"]))
        checks.addWidget(self.headless)
        checks.addWidget(self.reparse)
        checks.addWidget(self.reparse_form700)
        checks.addStretch()
        root.addLayout(checks)

        actions = QHBoxLayout()
        actions.addStretch()
        cancel = QPushButton("Cancel")
        cancel.clicked.connect(self.reject)
        save = QPushButton("Save settings")
        save.setObjectName("PrimaryButton")
        save.clicked.connect(self._save)
        actions.addWidget(cancel)
        actions.addWidget(save)
        root.addLayout(actions)

    def _text_field(self, grid: QGridLayout, label: str, key: str, row: int, col: int):
        wrap = QVBoxLayout()
        wrap.addWidget(QLabel(label))
        edit = QLineEdit(str(self.app.values[key]))
        wrap.addWidget(edit)
        grid.addLayout(wrap, row, col)
        self.fields[key] = edit

    def _path_field(self, grid: QGridLayout, label: str, key: str, row: int, directory: bool):
        wrap = QVBoxLayout()
        wrap.addWidget(QLabel(label))
        line = QHBoxLayout()
        edit = QLineEdit(str(self.app.values[key]))
        browse = QPushButton("Browse")
        browse.clicked.connect(lambda: self._browse(edit, directory))
        line.addWidget(edit, 1)
        line.addWidget(browse)
        wrap.addLayout(line)
        grid.addLayout(wrap, row, 0, 1, 2)
        self.fields[key] = edit

    def _browse(self, edit: QLineEdit, directory: bool):
        if directory:
            path = QFileDialog.getExistingDirectory(self, "Choose folder", edit.text())
        else:
            path, _ = QFileDialog.getOpenFileName(self, "Choose file", edit.text(), "Excel files (*.xlsx);;All files (*.*)")
        if path:
            edit.setText(path)

    def _save(self):
        for key, widget in self.fields.items():
            if isinstance(widget, QLineEdit):
                self.app.set_value(key, widget.text())
        self.app.set_value("headless", self.headless.isChecked())
        self.app.set_value("reparse_existing_minutes", self.reparse.isChecked())
        self.app.set_value("reparse_existing_form700s", self.reparse_form700.isChecked())
        self.accept()


class App(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(APP_TITLE)
        self.resize(1536, 1024)
        self.setMinimumSize(1180, 760)
        self.nav_items: dict[str, QFrame] = {}
        self.pages: dict[str, QWidget] = {}

        default_project_dir = Path(__file__).resolve().parent
        self.values: dict[str, object] = {
            "url": "https://sonoma-county.legistar.com/Calendar.aspx",
            "jurisdiction": "County of Sonoma",
            "body_filter": "Board of Supervisors",
            "interval": 5,
            "minutes_db": "data/minutes.db",
            "minutes_cache": "data/cache",
            "out_votes": "votes.csv",
            "form700_search_url": "https://form700search.fppc.ca.gov/Search/SearchFilerForms.aspx",
            "form700_folder": "form700",
            "project_dir": str(default_project_dir),
            "output_dir": str(default_project_dir / "output"),
            "page_limit": "100",
            "meeting_limit": "0",
            "headless": True,
            "reparse_existing_minutes": False,
            "reparse_existing_form700s": False,
            "min_confidence": "0.75",
            "minutes_text_index": "minutes_text_index.json",
            "form700_csv_out": "form700_entities.csv",
            "form700_json_out": "form700_entities.json",
            "form700_matches_out": "form700_matches.csv",
            "form700_matches_json_out": "form700_matches.json",
        }
        self.bound_widgets: dict[str, QWidget] = {}
        self.storage_size_labels: dict[str, QLabel] = {}
        self.process: QProcess | None = None

        self._build_ui()
        self._apply_style()
        self._seed_log()
        self._seed_preview()

    def _build_ui(self):
        root = QWidget()
        self.setCentralWidget(root)
        root_layout = QHBoxLayout(root)
        root_layout.setContentsMargins(0, 0, 0, 0)
        root_layout.setSpacing(0)

        root_layout.addWidget(self._sidebar())

        shell = QWidget()
        shell_layout = QVBoxLayout(shell)
        shell_layout.setContentsMargins(0, 0, 0, 0)
        shell_layout.setSpacing(0)
        root_layout.addWidget(shell, 1)

        shell_layout.addWidget(self._top_bar())
        shell_layout.addWidget(self._content(), 1)

    def _sidebar(self) -> QWidget:
        sidebar = QFrame()
        sidebar.setObjectName("Sidebar")
        sidebar.setFixedWidth(240)
        layout = QVBoxLayout(sidebar)
        layout.setContentsMargins(16, 28, 16, 24)
        layout.setSpacing(16)

        brand = QHBoxLayout()
        brand.setSpacing(14)
        brand.addWidget(IconWidget("building", COLORS["teal"], 54), 0, Qt.AlignTop)
        label = QLabel("Civic Vote<br>Scraper")
        label.setObjectName("Brand")
        brand.addWidget(label)
        layout.addLayout(brand)
        layout.addSpacing(28)

        layout.addWidget(self._nav_item("search", "Search", "search", True))
        layout.addWidget(self._nav_item("file", "Outputs", "outputs", False))
        layout.addWidget(self._nav_item("clipboard", "Review", "review", False))
        layout.addStretch()

        version = QLabel("Version 1.0.0")
        version.setObjectName("Version")
        layout.addWidget(version)
        settings = self._nav_item("settings", "Settings", "settings", False)
        settings.mousePressEvent = lambda _event: self._show_settings()
        layout.addWidget(settings)
        return sidebar

    def _nav_item(self, icon: str, text: str, page_name: str, selected: bool) -> QFrame:
        item = QFrame()
        item.setObjectName("NavSelected" if selected else "NavItem")
        item.setCursor(Qt.PointingHandCursor)
        layout = QHBoxLayout(item)
        layout.setContentsMargins(16, 12, 16, 12)
        layout.setSpacing(14)
        layout.addWidget(IconWidget(icon, COLORS["teal"] if selected else "#555b61", 30))
        label = QLabel(text)
        label.setObjectName("NavLabel")
        layout.addWidget(label)
        layout.addStretch()
        if page_name != "settings":
            self.nav_items[page_name] = item
            item.mousePressEvent = lambda _event, name=page_name: self._set_page(name)
        return item

    def _top_bar(self) -> QWidget:
        bar = QFrame()
        bar.setObjectName("TopBar")
        bar.setFixedHeight(70)
        layout = QHBoxLayout(bar)
        layout.setContentsMargins(30, 0, 30, 0)
        layout.setSpacing(22)

        layout.addWidget(Dot(COLORS["green"]))
        self.status_label = QLabel("Live search: IDLE")
        self.status_label.setObjectName("TopStatus")
        layout.addWidget(self.status_label)
        layout.addStretch(1)
        layout.addWidget(self._divider())
        self.next_run_label = QLabel("Next run in: --:--:--")
        self.next_run_label.setObjectName("TopStatus")
        layout.addWidget(self.next_run_label)
        layout.addStretch(1)
        layout.addWidget(self._divider())
        layout.addWidget(IconWidget("database", "#555b61", 34))
        self.database_label = QLabel("Database: minutes.db")
        self.database_label.setObjectName("TopText")
        layout.addWidget(self.database_label)
        layout.addSpacing(26)
        layout.addWidget(Dot(COLORS["green"]))
        connected = QLabel("Connected")
        connected.setObjectName("TopText")
        layout.addWidget(connected)
        return bar

    def _content(self) -> QWidget:
        self.page_stack = QStackedWidget()
        self.page_stack.setObjectName("Content")
        self.pages["search"] = self._search_page()
        self.pages["outputs"] = self._outputs_page()
        self.pages["review"] = self._review_page()
        for page in self.pages.values():
            self.page_stack.addWidget(page)
        return self.page_stack

    def _search_page(self) -> QWidget:
        content = QWidget()
        content.setObjectName("Content")
        layout = QHBoxLayout(content)
        layout.setContentsMargins(16, 18, 16, 18)
        layout.setSpacing(16)

        left = QVBoxLayout()
        left.setSpacing(16)
        right = QVBoxLayout()
        right.setSpacing(16)

        left.addWidget(self._source_card())
        left.addWidget(self._live_card())
        left.addWidget(self._storage_card(), 1)
        left.addWidget(self._summary_footer())

        right.addWidget(self._log_card(), 3)
        right.addWidget(self._preview_card(), 2)

        layout.addLayout(left, 11)
        layout.addLayout(right, 10)
        return content

    def _outputs_page(self) -> QWidget:
        page = QWidget()
        page.setObjectName("Content")
        layout = QVBoxLayout(page)
        layout.setContentsMargins(16, 18, 16, 18)
        layout.setSpacing(16)

        header = QHBoxLayout()
        title = QLabel("Outputs")
        title.setObjectName("PageTitle")
        header.addWidget(title)
        header.addStretch()
        open_folder = QPushButton("Open output folder")
        open_folder.setObjectName("PlainButton")
        open_folder.clicked.connect(lambda: self._open_path(self.values["output_dir"]))
        refresh = QPushButton("Refresh sizes")
        refresh.setObjectName("PrimaryButton")
        refresh.clicked.connect(self._refresh_outputs_page)
        header.addWidget(open_folder)
        header.addWidget(refresh)
        layout.addLayout(header)

        self.outputs_table = QTableWidget()
        self.outputs_table.setObjectName("PreviewTable")
        self.outputs_table.setColumnCount(4)
        self.outputs_table.setHorizontalHeaderLabels(["Output", "Path", "Size", "Status"])
        self.outputs_table.verticalHeader().hide()
        self.outputs_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.outputs_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.outputs_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.outputs_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.outputs_table.cellDoubleClicked.connect(self._open_output_table_row)
        layout.addWidget(self.outputs_table, 1)

        hint = QLabel("Double-click a row to open the file or folder.")
        hint.setObjectName("Hint")
        layout.addWidget(hint)
        self._refresh_outputs_page()
        return page

    def _review_page(self) -> QWidget:
        page = QWidget()
        page.setObjectName("Content")
        layout = QVBoxLayout(page)
        layout.setContentsMargins(16, 18, 16, 18)
        layout.setSpacing(16)

        header = QHBoxLayout()
        title = QLabel("Review")
        title.setObjectName("PageTitle")
        header.addWidget(title)
        header.addStretch()
        refresh = QPushButton("Refresh command")
        refresh.setObjectName("PrimaryButton")
        refresh.clicked.connect(self._refresh_review_page)
        header.addWidget(refresh)
        layout.addLayout(header)

        command_card, command_body = self._card("Command preview")
        self.command_preview = QPlainTextEdit()
        self.command_preview.setObjectName("Log")
        self.command_preview.setReadOnly(True)
        command_body.addWidget(self.command_preview)
        layout.addWidget(command_card, 2)

        config_card, config_body = self._card("Current configuration")
        self.config_table = QTableWidget()
        self.config_table.setObjectName("PreviewTable")
        self.config_table.setColumnCount(2)
        self.config_table.setHorizontalHeaderLabels(["Setting", "Value"])
        self.config_table.verticalHeader().hide()
        self.config_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.config_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        config_body.addWidget(self.config_table)
        layout.addWidget(config_card, 3)

        self._refresh_review_page()
        return page

    def _source_card(self) -> QWidget:
        card, body = self._card("1. Source setup")
        grid = QGridLayout()
        grid.setHorizontalSpacing(16)
        grid.setVerticalSpacing(8)
        body.addLayout(grid)

        self._field(grid, "Calendar URL", self._line_edit("url"), 0, 0, 1, 3)
        self._field(grid, "Jurisdiction", self._combo("jurisdiction", ["San Francisco", "City of Example", "County of Sonoma"]), 0, 3)
        self._field(
            grid,
            "Body filter (optional)",
            self._combo("body_filter", ["Board of Supervisors", "City Council", "Planning Commission"]),
            0,
            4,
            1,
            2,
        )
        return card

    def _live_card(self) -> QWidget:
        card, body = self._card("2. Live search controls")
        grid = QGridLayout()
        grid.setHorizontalSpacing(16)
        grid.setVerticalSpacing(10)
        body.addLayout(grid)

        interval = QSpinBox()
        interval.setRange(1, 10080)
        interval.setValue(int(self.values["interval"]))
        interval.valueChanged.connect(lambda value: self.set_value("interval", value))
        self.bound_widgets["interval"] = interval
        self._field(grid, "Run every (minutes)", interval, 0, 0)

        run_once = QPushButton("Run once")
        run_once.setObjectName("PlainButton")
        run_once.clicked.connect(lambda: self._run_command(False))
        grid.addWidget(run_once, 1, 2)

        self.start_button = QPushButton("Start live search")
        self.start_button.setObjectName("PrimaryButton")
        self.start_button.clicked.connect(lambda: self._run_command(True))
        grid.addWidget(self.start_button, 1, 3, 1, 2)

        self.stop_button = QPushButton("Stop")
        self.stop_button.setObjectName("DangerButton")
        self.stop_button.clicked.connect(self._stop_command)
        grid.addWidget(self.stop_button, 1, 5)

        helper = QLabel("Set how often to check for new votes.")
        helper.setObjectName("Hint")
        grid.addWidget(helper, 2, 0, 1, 6)
        return card

    def _storage_card(self) -> QWidget:
        card, body = self._card("3. Storage and outputs")
        rows = QFrame()
        rows.setObjectName("StorageTable")
        rows_layout = QVBoxLayout(rows)
        rows_layout.setContentsMargins(0, 0, 0, 0)
        rows_layout.setSpacing(0)
        body.addWidget(rows)

        rows_layout.addWidget(self._storage_row("database", COLORS["teal"], "Minutes database", "SQLite", "minutes_db"))
        rows_layout.addWidget(self._storage_row("folder", COLORS["orange"], "Cache folder", "HTML responses", "minutes_cache"))
        rows_layout.addWidget(self._storage_row("csv", COLORS["green"], "Votes CSV", "Structured votes", "out_votes"))
        rows_layout.addWidget(self._storage_row("file", COLORS["blue"], "Form 700 files", "Downloaded PDFs", "form700_folder"))
        body.addStretch()
        return card

    def _storage_row(self, icon: str, color: str, title: str, subtitle: str, key: str) -> QWidget:
        row = QFrame()
        row.setObjectName("StorageRow")
        layout = QHBoxLayout(row)
        layout.setContentsMargins(14, 10, 14, 10)
        layout.setSpacing(14)
        layout.addWidget(IconWidget(icon, color, 36))

        labels = QVBoxLayout()
        name = QLabel(title)
        name.setObjectName("StorageTitle")
        sub = QLabel(subtitle)
        sub.setObjectName("StorageSub")
        labels.addWidget(name)
        labels.addWidget(sub)
        layout.addLayout(labels)

        edit = self._line_edit(key)
        layout.addWidget(edit, 1)

        size_label = QLabel(self._display_size(self._value_path(key)))
        size_label.setStyleSheet(f"color: {color}; font-weight: 700;")
        self.storage_size_labels[key] = size_label
        layout.addWidget(size_label)

        open_button = QPushButton("Open")
        open_button.setObjectName("SmallButton")
        open_button.clicked.connect(lambda: self._open_value_path(key))
        layout.addWidget(open_button)
        return row

    def _summary_footer(self) -> QWidget:
        footer = QWidget()
        layout = QHBoxLayout(footer)
        layout.setContentsMargins(2, 2, 2, 2)
        layout.setSpacing(20)
        self.last_success_label = QLabel("Last successful run: --")
        self.total_votes_label = QLabel("Total votes: --")
        self.new_votes_label = QLabel("New since last run: --")
        for label in (self.last_success_label, self.total_votes_label):
            label.setObjectName("FooterText")
            layout.addWidget(label)
            layout.addWidget(self._divider())
        new_label = QLabel("New since last run:")
        new_label.setObjectName("FooterText")
        layout.addWidget(new_label)
        self.new_votes_value = QLabel("--")
        self.new_votes_value.setObjectName("FooterAccent")
        layout.addWidget(self.new_votes_value)
        layout.addStretch()
        return footer

    def _log_card(self) -> QWidget:
        card, body = self._card("Run log")
        tools = QHBoxLayout()
        tools.addStretch()
        auto = QLabel("Auto-scroll")
        auto.setObjectName("TopText")
        tools.addWidget(auto)
        self.auto_scroll_check = QCheckBox()
        self.auto_scroll_check.setChecked(True)
        tools.addWidget(self.auto_scroll_check)
        clear = QPushButton("Clear")
        clear.setObjectName("SmallButton")
        clear.clicked.connect(lambda: self.log_text.clear())
        tools.addWidget(clear)
        body.addLayout(tools)

        self.log_text = QPlainTextEdit()
        self.log_text.setObjectName("Log")
        self.log_text.setReadOnly(True)
        body.addWidget(self.log_text, 1)
        return card

    def _preview_card(self) -> QWidget:
        card, body = self._card("Output preview (votes.csv)")
        header = QHBoxLayout()
        header.addStretch()
        self.rows_label = QLabel("Rows: --")
        self.rows_label.setObjectName("Hint")
        header.addWidget(self.rows_label)
        body.addLayout(header)

        self.preview_stack = QStackedWidget()
        self.preview_table = QTableWidget()
        self.preview_table.setObjectName("PreviewTable")
        self.preview_table.setAlternatingRowColors(False)
        self.preview_table.verticalHeader().hide()
        self.preview_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.preview_text = QPlainTextEdit()
        self.preview_text.setReadOnly(True)
        self.preview_stack.addWidget(self.preview_table)
        self.preview_stack.addWidget(self.preview_text)
        body.addWidget(self.preview_stack, 1)

        footer = QHBoxLayout()
        open_votes = QPushButton("Open votes.csv")
        open_votes.setObjectName("PlainButton")
        open_votes.clicked.connect(lambda: self._open_path(self._output_path(str(self.values["out_votes"]))))
        footer.addWidget(open_votes)
        footer.addStretch()
        refresh = QPushButton("Refresh")
        refresh.setObjectName("PlainButton")
        refresh.clicked.connect(self._load_preview)
        footer.addWidget(refresh)
        body.addLayout(footer)
        return card

    def _card(self, title: str) -> tuple[QFrame, QVBoxLayout]:
        frame = QFrame()
        frame.setObjectName("Card")
        layout = QVBoxLayout(frame)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(12)
        label = QLabel(title)
        label.setObjectName("CardTitle")
        layout.addWidget(label)
        body = QVBoxLayout()
        body.setSpacing(10)
        layout.addLayout(body, 1)
        return frame, body

    def _field(self, grid: QGridLayout, label: str, widget: QWidget, row: int, col: int, row_span: int = 1, col_span: int = 1):
        wrap = QVBoxLayout()
        text = QLabel(label)
        text.setObjectName("FieldLabel")
        wrap.addWidget(text)
        wrap.addWidget(widget)
        grid.addLayout(wrap, row, col, row_span + 1, col_span)

    def _line_edit(self, key: str) -> QLineEdit:
        edit = QLineEdit(str(self.values[key]))
        edit.textChanged.connect(lambda value, k=key: self.set_value(k, value, update_widget=False))
        self.bound_widgets[key] = edit
        return edit

    def _combo(self, key: str, values: list[str]) -> QComboBox:
        combo = QComboBox()
        combo.setEditable(True)
        combo.addItems(values)
        combo.setCurrentText(str(self.values[key]))
        combo.currentTextChanged.connect(lambda value, k=key: self.set_value(k, value, update_widget=False))
        self.bound_widgets[key] = combo
        return combo

    def _divider(self) -> QFrame:
        divider = QFrame()
        divider.setObjectName("Divider")
        divider.setFrameShape(QFrame.VLine)
        return divider

    def _set_page(self, name: str):
        if name not in self.pages:
            return
        self.page_stack.setCurrentWidget(self.pages[name])
        for page_name, item in self.nav_items.items():
            item.setObjectName("NavSelected" if page_name == name else "NavItem")
            item.style().unpolish(item)
            item.style().polish(item)
            item.update()
        if name == "outputs":
            self._refresh_outputs_page()
        elif name == "review":
            self._refresh_review_page()

    def _refresh_storage_sizes(self):
        for key, label in self.storage_size_labels.items():
            label.setText(self._display_size(self._value_path(key)))

    def _path_size_bytes(self, path: Path) -> int | None:
        if not path.exists():
            return None
        if path.is_file():
            return path.stat().st_size
        total = 0
        for child in path.rglob("*"):
            try:
                if child.is_file():
                    total += child.stat().st_size
            except OSError:
                continue
        return total

    def _display_size(self, path: Path) -> str:
        size = self._path_size_bytes(path)
        if size is None:
            return "Missing"
        units = ["B", "KB", "MB", "GB", "TB"]
        value = float(size)
        unit = units[0]
        for unit in units:
            if value < 1024 or unit == units[-1]:
                break
            value /= 1024
        if unit == "B":
            return f"{int(value)} {unit}"
        return f"{value:.1f} {unit}"

    def _output_artifacts(self) -> list[tuple[str, Path]]:
        return [
            ("Output folder", Path(str(self.values["output_dir"]))),
            ("Minutes database", self._value_path("minutes_db")),
            ("Minutes cache", self._value_path("minutes_cache")),
            ("Votes CSV", self._value_path("out_votes")),
            ("Minutes text index", self._value_path("minutes_text_index")),
            ("Form 700 entities CSV", self._value_path("form700_csv_out")),
            ("Form 700 entities JSON", self._value_path("form700_json_out")),
            ("Form 700 matches CSV", self._value_path("form700_matches_out")),
            ("Form 700 matches JSON", self._value_path("form700_matches_json_out")),
            ("Form 700 folder", self._value_path("form700_folder")),
        ]

    def _refresh_outputs_page(self):
        if not hasattr(self, "outputs_table"):
            return
        artifacts = self._output_artifacts()
        self.outputs_table.setRowCount(len(artifacts))
        for row, (name, path) in enumerate(artifacts):
            status = "Ready" if path.exists() else "Missing"
            values = [name, str(path), self._display_size(path), status]
            for col, value in enumerate(values):
                item = QTableWidgetItem(value)
                if col == 3:
                    item.setForeground(QColor(COLORS["green"] if status == "Ready" else COLORS["muted"]))
                self.outputs_table.setItem(row, col, item)

    def _open_output_table_row(self, row: int, _column: int):
        item = self.outputs_table.item(row, 1)
        if item is not None:
            self._open_path(item.text())

    def _refresh_review_page(self):
        if not hasattr(self, "command_preview"):
            return
        self.command_preview.setPlainText(self._format_command(self._get_command(False)))

        review_items = [
            ("Calendar URL", str(self.values["url"])),
            ("jurisdiction", str(self.values["jurisdiction"])),
            ("Body filter", str(self.values["body_filter"])),
            ("Form 700 search URL", str(self.values["form700_search_url"])),
            ("Run interval", f"{self.values['interval']} minutes"),
            ("Project directory", str(self.values["project_dir"])),
            ("Output folder", str(self.values["output_dir"])),
            ("Minutes database", self._output_path(str(self.values["minutes_db"]))),
            ("Minutes cache", self._output_path(str(self.values["minutes_cache"]))),
            ("Votes CSV", self._output_path(str(self.values["out_votes"]))),
            ("Form 700 folder", self._output_path(str(self.values["form700_folder"]))),
            ("Meeting limit", str(self.values["meeting_limit"])),
            ("Page limit", str(self.values["page_limit"])),
            ("Headless browser", "Yes" if self.values["headless"] else "No"),
            ("Re-parse known minutes", "Yes" if self.values["reparse_existing_minutes"] else "No"),
            ("Re-parse known Form 700 PDFs", "Yes" if self.values["reparse_existing_form700s"] else "No"),
        ]
        self.config_table.setRowCount(len(review_items))
        for row, (setting, value) in enumerate(review_items):
            self.config_table.setItem(row, 0, QTableWidgetItem(setting))
            self.config_table.setItem(row, 1, QTableWidgetItem(value))

    def _format_command(self, command: list[str]) -> str:
        return " ".join(f'"{part}"' if " " in part else part for part in command)

    def set_value(self, key: str, value: object, update_widget: bool = True):
        self.values[key] = value
        if key == "minutes_db":
            self.database_label.setText(f"Database: {Path(str(value)).name}")
        if key in self.storage_size_labels:
            self.storage_size_labels[key].setText(self._display_size(self._value_path(key)))
        if key == "output_dir":
            self._refresh_storage_sizes()
        if hasattr(self, "outputs_table"):
            self._refresh_outputs_page()
        if hasattr(self, "command_preview"):
            self._refresh_review_page()
        if not update_widget or key not in self.bound_widgets:
            return
        widget = self.bound_widgets[key]
        if isinstance(widget, QLineEdit) and widget.text() != str(value):
            widget.setText(str(value))
        elif isinstance(widget, QComboBox) and widget.currentText() != str(value):
            widget.setCurrentText(str(value))
        elif isinstance(widget, QSpinBox) and widget.value() != int(value):
            widget.setValue(int(value))

    def _show_settings(self):
        SettingsDialog(self).exec_()

    def _seed_log(self):
        self.log_text.setPlainText(
            "Live search ready\n"
            "Calendar feed configured\n"
            "Waiting for next run"
        )

    def _seed_preview(self):
        self.preview_text.setPlainText("No preview loaded. Use Refresh after a scrape has produced votes.csv.")
        self.preview_stack.setCurrentWidget(self.preview_text)

    def _get_command(self, live: bool = False) -> list[str]:
        parts = [
            sys.executable,
            "-u",
            "-m",
            "civic_vote_scraper.cli",
            "--url",
            str(self.values["url"]).strip(),
            "--jurisdiction",
            str(self.values["jurisdiction"]).strip(),
            "--out",
            self._output_path(str(self.values["out_votes"])),
            "--minutes-cache-dir",
            self._output_path(str(self.values["minutes_cache"])),
            "--minutes-text-index",
            self._output_path(str(self.values["minutes_text_index"])),
            "--minutes-db",
            self._output_path(str(self.values["minutes_db"])),
            "--form700-search-url",
            str(self.values["form700_search_url"]).strip(),
            "--form700-folder",
            self._output_path(str(self.values["form700_folder"])),
            "--form700-csv-out",
            self._output_path(str(self.values["form700_csv_out"])),
            "--form700-json-out",
            self._output_path(str(self.values["form700_json_out"])),
            "--form700-matches-out",
            self._output_path(str(self.values["form700_matches_out"])),
            "--form700-matches-json-out",
            self._output_path(str(self.values["form700_matches_json_out"])),
            "--min-confidence",
            str(self.values["min_confidence"]).strip(),
        ]

        if str(self.values["body_filter"]).strip():
            parts += ["--body-filter", str(self.values["body_filter"]).strip()]
        if bool(self.values["headless"]):
            parts += ["--headless"]
        if str(self.values["page_limit"]).strip() not in {"", "0"}:
            parts += ["--page-limit", str(self.values["page_limit"]).strip()]
        if str(self.values["meeting_limit"]).strip() not in {"", "0"}:
            parts += ["--meeting-limit", str(self.values["meeting_limit"]).strip()]
        if bool(self.values["reparse_existing_minutes"]):
            parts += ["--reparse-existing-minutes"]
        if bool(self.values["reparse_existing_form700s"]):
            parts += ["--reparse-existing-form700s"]
        if live:
            parts += ["--live", "--live-interval-minutes", str(self.values["interval"])]
        return parts

    def _output_path(self, filename: str) -> str:
        if not filename:
            return ""
        path = Path(filename)
        if path.is_absolute():
            return str(path)
        return str(Path(str(self.values["output_dir"])) / path)

    def _value_path(self, key: str) -> Path:
        value = str(self.values[key]).strip()
        if not value:
            return Path(str(self.values["output_dir"]))
        path = Path(value)
        return path if path.is_absolute() else Path(str(self.values["output_dir"])) / path

    def _validate_before_run(self, live: bool) -> bool:
        if not str(self.values["project_dir"]).strip():
            QMessageBox.critical(self, APP_TITLE, "Select the scraper project directory first.")
            return False
        if not Path(str(self.values["project_dir"])).exists():
            QMessageBox.critical(self, APP_TITLE, "Project directory does not exist.")
            return False
        if not str(self.values["url"]).strip():
            QMessageBox.critical(self, APP_TITLE, "Enter a calendar URL.")
            return False
        if live and int(self.values["interval"]) <= 0:
            QMessageBox.critical(self, APP_TITLE, "Search interval must be greater than zero.")
            return False
        return True

    def _run_command(self, live: bool):
        if self.process is not None and self.process.state() != QProcess.NotRunning:
            QMessageBox.information(self, APP_TITLE, "A search is already running.")
            return
        if not self._validate_before_run(live):
            return

        command = self._get_command(live)
        self.process = QProcess(self)
        self.process.setProgram(command[0])
        self.process.setArguments(command[1:])
        self.process.setWorkingDirectory(str(self.values["project_dir"]))
        self.process.setProcessChannelMode(QProcess.MergedChannels)
        self.process.readyReadStandardOutput.connect(self._read_process_output)
        self.process.finished.connect(self._process_finished)

        self.status_label.setText("Live search: RUNNING" if live else "Live search: RUN ONCE")
        self.next_run_label.setText(f"Next run in: {int(self.values['interval']) * 60:02d}s" if live else "Next run in: --:--:--")
        self._append_log("[start] Running live search" if live else "[start] Running scraper once")
        self.process.start()

    def _read_process_output(self):
        if self.process is None:
            return
        text = bytes(self.process.readAllStandardOutput()).decode("utf-8", errors="replace")
        for line in text.splitlines():
            self._append_log(line)
            self._update_status_from_log(line)

    def _process_finished(self, code: int, _status):
        self._append_log(f"[exit] Process finished with code {code}")
        self.status_label.setText("Live search: IDLE")
        self.next_run_label.setText("Next run in: --:--:--")
        self._refresh_storage_sizes()
        self._refresh_outputs_page()
        self._load_preview_if_available()
        self.process = None

    def _stop_command(self):
        if self.process is None or self.process.state() == QProcess.NotRunning:
            self.status_label.setText("Live search: IDLE")
            return
        self.status_label.setText("Live search: STOPPING")
        self._append_log("[stop] Termination requested")
        self.process.terminate()

    def _append_log(self, text: str):
        if not text:
            return
        self.log_text.appendPlainText(text)
        if self.auto_scroll_check.isChecked():
            self.log_text.verticalScrollBar().setValue(self.log_text.verticalScrollBar().maximum())

    def _update_status_from_log(self, line: str):
        clean = line.strip()
        if "next live search in" in clean:
            self.next_run_label.setText(clean.replace("[info] ", ""))
        elif "database totals:" in clean:
            self.total_votes_label.setText(clean.replace("[info] database totals: ", "Total: "))
        elif "vote rows" in clean and "loaded" in clean:
            self.rows_label.setText(clean.replace("[info] loaded ", "Rows: ").replace(" total vote rows from minutes database", ""))
        elif clean.startswith("[error]"):
            self.status_label.setText("Live search: ERROR")

    def _open_value_path(self, key: str):
        self._open_path(self._value_path(key))

    def _open_path(self, path: str | Path):
        path = Path(path)
        target = path if path.exists() else path.parent
        if not target.exists():
            QMessageBox.information(self, APP_TITLE, f"Path not found:\n{path}")
            return
        if sys.platform.startswith("win"):
            os.startfile(target)  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            QProcess.startDetached("open", [str(target)])
        else:
            QProcess.startDetached("xdg-open", [str(target)])

    def _load_preview(self):
        path = Path(self._output_path(str(self.values["out_votes"])))
        if not path.exists():
            QMessageBox.information(self, APP_TITLE, f"File not found:\n{path}")
            self._seed_preview()
            return
        self._load_preview_path(path)

    def _load_preview_if_available(self):
        path = Path(self._output_path(str(self.values["out_votes"])))
        if path.exists():
            self._load_preview_path(path)

    def _load_preview_path(self, path: Path):
        try:
            with path.open("r", encoding="utf-8", newline="") as f:
                reader = csv.reader(f)
                rows = []
                for i, row in enumerate(reader):
                    rows.append(row)
                    if i >= 80:
                        break
            self._show_preview_rows(rows)
            self.rows_label.setText(f"Rows: {self._count_csv_rows(path):,}")
        except Exception:
            self.preview_text.setPlainText(path.read_text(encoding="utf-8", errors="ignore")[:30000])
            self.preview_stack.setCurrentWidget(self.preview_text)

    def _count_csv_rows(self, path: Path) -> int:
        try:
            with path.open("r", encoding="utf-8", newline="") as f:
                total = sum(1 for _ in csv.reader(f))
            return max(total - 1, 0)
        except OSError:
            return 0

    def _show_preview_rows(self, rows: list[list[str]]):
        if not rows:
            self.preview_table.clear()
            self.preview_table.setRowCount(0)
            self.preview_table.setColumnCount(0)
            self.preview_stack.setCurrentWidget(self.preview_table)
            return

        headers = [header or f"Column {i + 1}" for i, header in enumerate(rows[0])]
        self.preview_table.clear()
        self.preview_table.setColumnCount(len(headers))
        self.preview_table.setHorizontalHeaderLabels(headers)
        self.preview_table.setRowCount(max(len(rows) - 1, 0))
        for row_index, row in enumerate(rows[1:]):
            for col_index in range(len(headers)):
                value = row[col_index] if col_index < len(row) else ""
                self.preview_table.setItem(row_index, col_index, QTableWidgetItem(value))
        self.preview_stack.setCurrentWidget(self.preview_table)

    def _apply_style(self):
        self.setStyleSheet(
            f"""
            QMainWindow {{
                background: {COLORS['app']};
                color: {COLORS['text']};
                font-family: "Segoe UI";
            }}
            QFrame#Sidebar {{
                background: {COLORS['surface']};
                border-right: 1px solid {COLORS['line']};
            }}
            QLabel#Brand {{
                color: {COLORS['text']};
                font-size: 24px;
                font-weight: 800;
                line-height: 1.05;
            }}
            QLabel#Version, QLabel#Hint, QLabel#FooterText {{
                color: {COLORS['muted']};
            }}
            QLabel#FooterAccent {{
                color: {COLORS['teal']};
                font-weight: 700;
            }}
            QFrame#NavItem, QFrame#NavSelected {{
                border-radius: 6px;
                background: transparent;
            }}
            QFrame#NavSelected {{
                background: {COLORS['teal_soft']};
            }}
            QLabel#NavLabel {{
                color: {COLORS['text']};
                font-size: 16px;
            }}
            QFrame#TopBar {{
                background: {COLORS['surface']};
                border-bottom: 1px solid {COLORS['line']};
            }}
            QLabel#TopStatus {{
                color: {COLORS['text']};
                font-size: 18px;
            }}
            QLabel#TopText {{
                color: {COLORS['text']};
                font-size: 14px;
            }}
            QFrame#Divider {{
                color: {COLORS['line']};
                background: {COLORS['line']};
                max-width: 1px;
            }}
            QWidget#Content {{
                background: {COLORS['app']};
            }}
            QFrame#Card {{
                background: {COLORS['surface']};
                border: 1px solid {COLORS['line']};
                border-radius: 8px;
            }}
            QLabel#CardTitle {{
                color: {COLORS['teal']};
                font-size: 20px;
                font-weight: 800;
            }}
            QLabel#FieldLabel {{
                color: {COLORS['text']};
                font-size: 14px;
            }}
            QLineEdit, QComboBox, QSpinBox {{
                background: #ffffff;
                color: {COLORS['text']};
                border: 1px solid {COLORS['line']};
                border-radius: 4px;
                padding: 9px 10px;
                min-height: 28px;
                font-size: 15px;
            }}
            QPushButton {{
                border: 1px solid {COLORS['line']};
                border-radius: 5px;
                padding: 10px 16px;
                background: #ffffff;
                color: {COLORS['text']};
                font-size: 14px;
            }}
            QPushButton#PrimaryButton {{
                background: {COLORS['teal']};
                border-color: {COLORS['teal']};
                color: white;
                font-weight: 700;
            }}
            QPushButton#PrimaryButton:hover {{
                background: {COLORS['teal_dark']};
            }}
            QPushButton#DangerButton {{
                background: {COLORS['danger_soft']};
                border-color: #ffcbc6;
                color: {COLORS['danger']};
            }}
            QPushButton#PlainButton {{
                background: #ffffff;
                min-height: 32px;
            }}
            QPushButton#SmallButton {{
                padding: 8px 12px;
                background: #ffffff;
            }}
            QFrame#StorageTable {{
                border: 1px solid {COLORS['line']};
                border-radius: 6px;
                background: {COLORS['surface']};
            }}
            QFrame#StorageRow {{
                border-bottom: 1px solid {COLORS['line_soft']};
                background: {COLORS['surface']};
            }}
            QLabel#StorageTitle {{
                color: {COLORS['text']};
                font-size: 14px;
                font-weight: 800;
            }}
            QLabel#StorageSub {{
                color: {COLORS['muted']};
                font-size: 12px;
            }}
            QPlainTextEdit#Log {{
                background: #ffffff;
                border: 1px solid {COLORS['line']};
                border-radius: 4px;
                padding: 10px;
                font-family: "Cascadia Mono", Consolas, monospace;
                font-size: 13px;
            }}
            QTableWidget#PreviewTable {{
                background: #ffffff;
                border: 1px solid {COLORS['line']};
                gridline-color: {COLORS['line_soft']};
                font-size: 13px;
            }}
            QHeaderView::section {{
                background: #f7f9fa;
                border: 1px solid {COLORS['line']};
                padding: 8px;
                font-weight: 700;
            }}
            QLabel#DialogTitle {{
                font-size: 22px;
                font-weight: 800;
                color: {COLORS['text']};
            }}
            """
        )


def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    window = App()
    window.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
