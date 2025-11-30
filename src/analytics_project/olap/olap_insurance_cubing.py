import pandas as pd
import pathlib
import sqlite3
from loguru import logger  # Assurez-vous que loguru est installé

# --- 1. Configuration des Chemins ---
THIS_DIR: pathlib.Path = pathlib.Path(__file__).resolve().parent
PACKAGE_DIR: pathlib.Path = THIS_DIR.parent
SRC_DIR: pathlib.Path = PACKAGE_DIR.parent
PROJECT_ROOT_DIR: pathlib.Path = SRC_DIR.parent

DATA_DIR: pathlib.Path = PROJECT_ROOT_DIR / "data"
WAREHOUSE_DIR: pathlib.Path = DATA_DIR / "dw"
# ✅ Data warehouse pour ton projet insurance
DB_PATH: pathlib.Path = WAREHOUSE_DIR / "insurance_dw.db"
OLAP_OUTPUT_DIR: pathlib.Path = DATA_DIR / "olap_cubing_outputs"

# Crée le répertoire de sortie s'il n'existe pas
OLAP_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Configuration simple de loguru
logger.remove()
logger.add(
    lambda msg: print(msg, end=""),
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
    colorize=True,
)
# --- Fin Configuration ---


def ingest_fact_insurance_from_dw() -> pd.DataFrame:
    """
    Ingest fact table data (fact_insurance_charges) from SQLite data warehouse.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        fact_df = pd.read_sql_query("SELECT * FROM fact_insurance_charges", conn)
        conn.close()
        logger.info("fact_insurance_charges successfully loaded from SQLite data warehouse.\n")
        return fact_df
    except sqlite3.OperationalError as e:
        logger.error(f"Erreur de connexion/lecture de la DB : {e}. Vérifiez le chemin {DB_PATH}.")
        raise
    except Exception as e:
        logger.error(f"Erreur lors du chargement de la table fact_insurance_charges : {e}")
        raise


def ingest_dim_table(table_name: str) -> pd.DataFrame:
    """
    Ingest a dimension table (e.g., dim_demographics, dim_region, dim_risk) from the DW.
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
    Generate explicit column names for OLAP cube based on dimensions and metrics.
    """
    column_names = dimensions.copy()
    for column, agg_funcs in metrics.items():
        if isinstance(agg_funcs, list):
            for func in agg_funcs:
                column_names.append(f"{column}_{func}")
        else:
            column_names.append(f"{column}_{agg_funcs}")

    # Nettoyage
    column_names = [col.replace("__", "_").rstrip("_") for col in column_names]

    logger.info(f"Generated column names for OLAP cube: {column_names}\n")
    return column_names


def create_olap_cube(data_df: pd.DataFrame, dimensions: list, metrics: dict) -> pd.DataFrame:
    """
    Create an OLAP cube by aggregating data across multiple dimensions.

    - dimensions: liste de colonnes catégorielles (age_group, smoker, bmi_category, region, etc.)
    - metrics: dict des mesures, ex: {"charges": ["sum", "mean"], "fact_key": "count"}
    """
    if data_df.empty:
        logger.warning("Input DataFrame is empty, cannot create cube.\n")
        return pd.DataFrame()

    try:
        grouped = data_df.groupby(dimensions, dropna=True)
        cube = grouped.agg(metrics).reset_index()

        explicit_columns = generate_column_names(dimensions, metrics)
        cube.columns = explicit_columns

        logger.info(f"OLAP cube created with dimensions: {dimensions}\n")
        return cube
    except KeyError as e:
        logger.error(
            f"KeyError: One or more dimensions/metrics ({dimensions}, {list(metrics.keys())}) "
            f"not found in the DataFrame: {e}\n"
        )
        raise


def write_cube_to_csv(cube: pd.DataFrame, filename: str) -> None:
    """
    Write the OLAP cube to a CSV file.
    """
    output_path = OLAP_OUTPUT_DIR.joinpath(filename)
    cube.to_csv(output_path, index=False)
    logger.info(f"OLAP cube saved to {output_path}.\n")


def main():
    """
    Execute OLAP cubing process for P6 Goal:
    "Which patient groups generate the highest medical insurance costs?"

    On va construire un cube multi-dimensionnel basé sur :
    - age_group
    - smoker (yes/no)
    - bmi_category
    - region

    Mesures :
    - charges_sum : total charges (coût total)
    - charges_mean : average charges (coût moyen par patient)
    - fact_key_count : nombre de lignes (approx = nombre de patients)
    """
    logger.info("Starting OLAP Cubing process for P6 Goal (High-Cost Patient Groups)...\n")

    # Step 1: Ingest fact + dimensions depuis le data warehouse Insurance
    fact_df = ingest_fact_insurance_from_dw()
    demo_df = ingest_dim_table("dim_demographics")
    region_df = ingest_dim_table("dim_region")
    risk_df = ingest_dim_table("dim_risk")

    # Step 2: Join tables pour créer le Data Mart nécessaire au cube
    # fact_insurance_charges contient:
    #   demographics_key, region_key, risk_key, charges, age, bmi, children

    # Join sur demographics_key
    merged_df = fact_df.merge(
        demo_df,
        on="demographics_key",
        how="left",
        suffixes=("", "_demo"),
    )

    # Join sur region_key
    merged_df = merged_df.merge(
        region_df,
        on="region_key",
        how="left",
        suffixes=("", "_region"),
    )

    # Join sur risk_key
    final_df = merged_df.merge(
        risk_df,
        on="risk_key",
        how="left",
        suffixes=("", "_risk"),
    )

    if final_df.isnull().any().any():
        logger.warning(
            "Merged DataFrame contains NaN values (missing dimension rows or keys). "
            "This is expected if some fact rows don't match dimension tables.\n"
        )

    # Step 3: Define dimensions and metrics for the cube (adapté à ta question)
    # Dimensions P6: Age group, smoker status, BMI category, region
    dimensions = ["age_group", "smoker", "bmi_category", "region"]

    # Metrics: total et moyenne des charges, + nombre de lignes (patients)
    metrics = {
        "charges": ["sum", "mean"],
        "fact_key": "count",
    }

    # Step 4: Create the cube
    olap_cube = create_olap_cube(final_df, dimensions, metrics)

    # Step 5: Save the cube to a CSV file
    write_cube_to_csv(olap_cube, "insurance_multidimensional_olap_cube.csv")

    logger.info("OLAP Cubing process completed successfully.\n")
    logger.info(
        f"Output saved to {OLAP_OUTPUT_DIR / 'insurance_multidimensional_olap_cube.csv'}\n"
    )


if __name__ == "__main__":
    main()
