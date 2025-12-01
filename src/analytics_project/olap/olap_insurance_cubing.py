import pandas as pd
import pathlib
import sqlite3
from loguru import logger  

# --- 1. Path Configuration ---
THIS_DIR: pathlib.Path = pathlib.Path(__file__).resolve().parent
PACKAGE_DIR: pathlib.Path = THIS_DIR.parent
SRC_DIR: pathlib.Path = PACKAGE_DIR.parent
PROJECT_ROOT_DIR: pathlib.Path = SRC_DIR.parent

DATA_DIR: pathlib.Path = PROJECT_ROOT_DIR / "data"
WAREHOUSE_DIR: pathlib.Path = DATA_DIR / "dw"
# ✅ Data warehouse for your insurance project
DB_PATH: pathlib.Path = WAREHOUSE_DIR / "insurance_dw.db"
OLAP_OUTPUT_DIR: pathlib.Path = DATA_DIR / "olap_cubing_outputs"

# Create the output directory if it does not exist
OLAP_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Simple Loguru configuration
logger.remove()
logger.add(
    lambda msg: print(msg, end=""),
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
    colorize=True,
)
# --- End Configuration ---


def ingest_fact_insurance_from_dw() -> pd.DataFrame:
    """
    Load the fact table (fact_insurance_charges) from the SQLite data warehouse.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        fact_df = pd.read_sql_query("SELECT * FROM fact_insurance_charges", conn)
        conn.close()
        logger.info("fact_insurance_charges successfully loaded from SQLite data warehouse.\n")
        return fact_df
    except sqlite3.OperationalError as e:
        logger.error(f"Database connection/read error: {e}. Check the path {DB_PATH}.")
        raise
    except Exception as e:
        logger.error(f"Error loading fact_insurance_charges table: {e}")
        raise


def ingest_dim_table(table_name: str) -> pd.DataFrame:
    """
    Load a dimension table (e.g., dim_demographics, dim_region, dim_risk) from the DW.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()
        logger.info(f"{table_name} successfully loaded.\n")
        return df
    except Exception as e:
        logger.error(f"Error loading {table_name} table data: {e}")
        raise


def generate_column_names(dimensions: list, metrics: dict) -> list:
    """
    Generate explicit column names for the OLAP cube based on dimensions and metrics.
    """
    column_names = dimensions.copy()
    for column, agg_funcs in metrics.items():
        if isinstance(agg_funcs, list):
            for func in agg_funcs:
                column_names.append(f"{column}_{func}")
        else:
            column_names.append(f"{column}_{agg_funcs}")

    # Cleanup
    column_names = [col.replace("__", "_").rstrip("_") for col in column_names]

    logger.info(f"Generated column names for OLAP cube: {column_names}\n")
    return column_names


def create_olap_cube(data_df: pd.DataFrame, dimensions: list, metrics: dict) -> pd.DataFrame_
