from pathlib import Path
import argparse
import re
from typing import Dict

import pandas as pd
from sqlalchemy import create_engine
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)

DEFAULT_FILE_PATH = r"C:\Users\PC\Desktop\Copy of City of Santa Ana_Sonoma_2021"
DEFAULT_DB_HOST = "localhost"
DEFAULT_DB_PORT = 5432
DEFAULT_DB_NAME = "postgres"
DEFAULT_DB_USER = "postgres"
DEFAULT_DB_PASSWORD = "123sega"
DEFAULT_DB_SCHEMA = "public"

EMAIL_COLUMN_NAMES = {
    "email",
    "email address",
    "work email address",
}
BUSINESS_ENTITY_COLUMN_NAMES = {
    "name of business entity",
}
EXCLUDED_COLUMN_NAMES = {
    "employer name",
    "filing type",
    "due date",
    "fair market value",
    "nature of investment",
    "unnamed",
}


def load_file_to_dataframe(file_path: str) -> pd.DataFrame:
    """Load a CSV file or all sheets from an Excel file into one DataFrame."""
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    suffix = path.suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(path)

    if suffix in {".xls", ".xlsx", ".xlsm"}:
        sheets = pd.read_excel(path, sheet_name=None)
        if isinstance(sheets, pd.DataFrame):
            return sheets
        return pd.concat(sheets.values(), ignore_index=True)

    raise ValueError("Unsupported file type. Use .csv, .xls, .xlsx, or .xlsm.")


def load_path_to_dataframes(file_path: str) -> Dict[str, pd.DataFrame]:
    """Load one file or a directory of supported files into separate DataFrames."""
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    if path.is_dir():
        supported_files = sorted(
            file
            for pattern in ("*.csv", "*.xls", "*.xlsx", "*.xlsm")
            for file in path.glob(pattern)
            if not file.name.startswith("~$")
        )
        if not supported_files:
            raise ValueError(f"No CSV or Excel files found in directory: {path}")

        return {
            supported_file.name: load_file_to_dataframe(str(supported_file))
            for supported_file in supported_files
        }

    return {path.name: load_file_to_dataframe(file_path)}


def normalize_column_name(column_name) -> str:
    """Normalize a column name for case-insensitive comparisons."""
    return " ".join(str(column_name).strip().lower().split())


def sanitize_sql_name(name: str) -> str:
    """Convert a file or column name into a PostgreSQL-safe identifier."""
    cleaned = re.sub(r"\W+", "_", str(name).strip().lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")

    if not cleaned:
        return "dataframe"

    if cleaned[0].isdigit():
        return f"table_{cleaned}"

    return cleaned


def format_column_titles(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Remove embedded line breaks and extra spacing from column titles."""
    formatted = dataframe.copy()
    formatted.columns = [" ".join(str(column).split()) for column in formatted.columns]
    return formatted


def drop_empty_rows(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Remove fully blank rows and rows with no meaningful name/email data."""
    cleaned = dataframe.dropna(how="all").copy()

    candidate_columns = [
        column
        for column in cleaned.columns
        if str(column).strip().lower()
        in {"first name", "middle name", "last name", "email", "email address", "work email address"}
    ]

    if not candidate_columns:
        return cleaned

    normalized = cleaned[candidate_columns].fillna("").astype(str).apply(lambda column: column.str.strip())
    has_key_data = normalized.ne("").any(axis=1)
    return cleaned.loc[has_key_data].copy()


def drop_excluded_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Drop columns that should not appear in the final DataFrame."""
    columns_to_keep = [
        column
        for column in dataframe.columns
        if not any(
            excluded_name in normalize_column_name(column)
            for excluded_name in EXCLUDED_COLUMN_NAMES
        )
    ]
    return dataframe.loc[:, columns_to_keep]


def sanitize_dataframe_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Rename columns to SQL-safe unique identifiers for PostgreSQL."""
    sanitized = dataframe.copy()
    counts = {}
    new_columns = []

    for column in sanitized.columns:
        base_name = sanitize_sql_name(column)
        count = counts.get(base_name, 0)
        counts[base_name] = count + 1
        new_columns.append(base_name if count == 0 else f"{base_name}_{count}")

    sanitized.columns = new_columns
    return sanitized


def first_non_empty_value(series: pd.Series):
    """Return the first non-empty value in a Series."""
    for value in series:
        if pd.isna(value):
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def append_unique_values(series: pd.Series) -> str | None:
    """Combine unique non-empty values into wrapped lines of two entries each."""
    values = []

    for value in series:
        if pd.isna(value):
            continue

        text = str(value).strip()
        if text:
            values.append(text)

    unique_values = list(dict.fromkeys(values))
    if not unique_values:
        return None

    wrapped_lines = []
    for index in range(0, len(unique_values), 2):
        wrapped_lines.append("; ".join(unique_values[index:index + 2]))

    return "\n".join(wrapped_lines)


def consolidate_by_email(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Merge rows with the same email into one row.

    The business entity column is appended across duplicates. Other columns
    keep the first non-empty value found for that email.
    """
    email_column = next(
        (
            column
            for column in dataframe.columns
            if str(column).strip().lower() in EMAIL_COLUMN_NAMES
        ),
        None,
    )
    business_entity_column = next(
        (
            column
            for column in dataframe.columns
            if str(column).strip().lower() in BUSINESS_ENTITY_COLUMN_NAMES
        ),
        None,
    )

    if email_column is None or business_entity_column is None:
        return dataframe
    consolidated_source = dataframe.copy()
    consolidated_source[email_column] = consolidated_source[email_column].where(
        consolidated_source[email_column].notna(), None
    )

    rows_with_email = consolidated_source[consolidated_source[email_column].notna()].copy()
    rows_without_email = consolidated_source[consolidated_source[email_column].isna()].copy()

    if rows_with_email.empty:
        return dataframe

    rows_with_email[email_column] = (
        rows_with_email[email_column].astype(str).str.strip().str.lower()
    )
    rows_with_email = rows_with_email[rows_with_email[email_column] != ""]

    aggregations = {}
    for column in rows_with_email.columns:
        if column == email_column:
            continue
        if column == business_entity_column:
            aggregations[column] = append_unique_values
        else:
            aggregations[column] = first_non_empty_value

    consolidated_rows = rows_with_email.groupby(email_column, as_index=False).agg(aggregations)

    if rows_without_email.empty:
        return consolidated_rows

    return pd.concat([consolidated_rows, rows_without_email], ignore_index=True)


def save_dataframe_to_postgres(
    dataframe: pd.DataFrame,
    table_name: str,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    schema: str,
    if_exists: str,
) -> str:
    """Save a DataFrame to PostgreSQL using SQLAlchemy."""
    sanitized = sanitize_dataframe_columns(dataframe)
    final_table_name = sanitize_sql_name(Path(table_name).stem)
    connection_url = (
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    )
    engine = create_engine(connection_url)

    try:
        sanitized.to_sql(
            name=final_table_name,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
        )
    finally:
        engine.dispose()

    return final_table_name


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read a CSV or Excel file and consolidate duplicate emails."
    )
    parser.add_argument(
        "file_path",
        nargs="?",
        default=DEFAULT_FILE_PATH,
        help=f"Path to a CSV/Excel file or a directory of supported files (default: {DEFAULT_FILE_PATH})",
    )
    parser.add_argument(
        "--save-to-postgres",
        action="store_true",
        help="Save each processed DataFrame to PostgreSQL.",
    )
    parser.add_argument("--db-host", default=DEFAULT_DB_HOST, help="PostgreSQL host")
    parser.add_argument("--db-port", type=int, default=DEFAULT_DB_PORT, help="PostgreSQL port")
    parser.add_argument("--db-name", default=DEFAULT_DB_NAME, help="PostgreSQL database")
    parser.add_argument("--db-user", default=DEFAULT_DB_USER, help="PostgreSQL user")
    parser.add_argument(
        "--db-password",
        default=DEFAULT_DB_PASSWORD,
        help="PostgreSQL password",
    )
    parser.add_argument("--db-schema", default=DEFAULT_DB_SCHEMA, help="PostgreSQL schema")
    parser.add_argument(
        "--if-exists",
        choices=["fail", "replace", "append"],
        default="replace",
        help="How to handle an existing table in PostgreSQL.",
    )
    args = parser.parse_args()

    dataframes = load_path_to_dataframes(args.file_path)

    for name, dataframe in dataframes.items():
        dataframe = format_column_titles(dataframe)
        dataframe = drop_empty_rows(dataframe)
        dataframe = drop_excluded_columns(dataframe)
        consolidated = consolidate_by_email(dataframe)

        print(f"\n{name}")
        print("DataFrame created successfully.")
        print(f"Rows: {len(consolidated)}")
        print(f"Columns: {len(consolidated.columns)}")
        print(consolidated.head().to_string(index=False))

        if args.save_to_postgres:
            table_name = save_dataframe_to_postgres(
                dataframe=consolidated,
                table_name=name,
                host=args.db_host,
                port=args.db_port,
                database=args.db_name,
                user=args.db_user,
                password=args.db_password,
                schema=args.db_schema,
                if_exists=args.if_exists,
            )
            print(
                f"Saved to PostgreSQL table: {args.db_schema}.{table_name}"
            )


if __name__ == "__main__":
    main()
