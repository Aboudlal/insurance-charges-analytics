"""
analytics_project/data_preparation/prepare_insurance_charges.py

This script reads medical insurance data from the data/raw folder,
cleans and enriches the data, and writes the prepared version
to the data/prepared folder.

Dataset: insurance.csv
Expected columns:
- age (int)
- sex (string: male/female)
- bmi (float)
- children (int)
- smoker (string: yes/no)
- region (string: southwest/southeast/northeast/northwest)
- charges (float)

Main tasks:
- Ensure clean column names and correct data types
- Remove duplicate records
- Handle missing values using simple, transparent business rules
- Remove extreme outliers in charges
- Add useful derived features for later analysis
  (age_group, bmi_category, smoker_flag)

The prepared file (insurance_prepared.csv) will be used to analyze
which patient groups generate the highest medical costs.
"""

#####################################
# Import Modules at the Top
#####################################

# Import from Python Standard Library
import pathlib
import sys

# Import from external packages (requires a virtual environment)
import pandas as pd

# Ensure project root is in sys.path for local imports (now 3 parents are needed)
sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent.parent))

# Import local modules (e.g. utils/logger.py)
from utils.logger import logger

# Optional: Use a data_scrubber module for common data cleaning tasks
from utils.data_scrubber import DataScrubber


# Constants
SCRIPTS_DATA_PREP_DIR: pathlib.Path = pathlib.Path(__file__).resolve().parent
SCRIPTS_DIR: pathlib.Path = SCRIPTS_DATA_PREP_DIR.parent
SRC_DIR: pathlib.Path = SCRIPTS_DIR.parent
PROJECT_ROOT: pathlib.Path = SRC_DIR.parent
DATA_DIR: pathlib.Path = PROJECT_ROOT / "data"
RAW_DATA_DIR: pathlib.Path = DATA_DIR / "raw"
PREPARED_DATA_DIR: pathlib.Path = DATA_DIR / "prepared"
ANALYTICS_PROJECT_DIR = SCRIPTS_DIR

# Ensure the directories exist or create them
DATA_DIR.mkdir(exist_ok=True)
RAW_DATA_DIR.mkdir(exist_ok=True)
PREPARED_DATA_DIR.mkdir(exist_ok=True)

#####################################
# Define Functions - Reusable blocks of code / instructions
#####################################


def read_raw_data(file_name: str) -> pd.DataFrame:
    """
    Read raw data from CSV located in data/raw.

    Args:
        file_name: Name of the CSV file (e.g., "insurance.csv").

    Returns:
        A pandas DataFrame with the loaded data, or an empty
        DataFrame if the file cannot be read.
    """
    file_path: pathlib.Path = RAW_DATA_DIR / file_name
    try:
        logger.info(f"READING: {file_path}")
        df = pd.read_csv(file_path)
        logger.info(f"Loaded dataframe with shape: {df.shape}")
        return df
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return pd.DataFrame()
    except Exception as e:  # noqa: BLE001
        logger.error(f"Error reading {file_path}: {e}")
        return pd.DataFrame()


def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    """
    Save prepared data to data/prepared.

    Args:
        df: Cleaned and enriched DataFrame.
        file_name: Output CSV file name (e.g., "insurance_prepared.csv").
    """
    logger.info(
        f"FUNCTION START: save_prepared_data with file_name={file_name}, dataframe shape={df.shape}"
    )
    file_path = PREPARED_DATA_DIR / file_name
    df.to_csv(file_path, index=False)
    logger.info(f"Prepared data saved to {file_path}")


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Strip leading/trailing whitespace and standardize column names.

    - Removes extra spaces.
    - Replaces spaces with underscores.
    - Lowercases column names.

    This helps avoid bugs later when referencing columns.
    """
    original_columns = df.columns.tolist()
    df.columns = (
        df.columns.str.strip().str.replace(" ", "_").str.lower()
    )  # standardize
    changed_columns = [
        f"{old} -> {new}"
        for old, new in zip(original_columns, df.columns)
        if old != new
    ]
    if changed_columns:
        logger.info(f"Cleaned column names: {', '.join(changed_columns)}")
    return df


def convert_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure correct data types for key columns.

    - age, children: Int64 (nullable integer)
    - bmi, charges: float
    - sex, smoker, region: standardized strings

    This makes later analysis more reliable and explicit.
    """
    logger.info("FUNCTION START: convert_dtypes")

    # Numeric columns
    if "age" in df.columns:
        df["age"] = pd.to_numeric(df["age"], errors="coerce").astype("Int64")
        logger.info("Converted age to Int64.")

    if "children" in df.columns:
        df["children"] = pd.to_numeric(df["children"], errors="coerce").astype("Int64")
        logger.info("Converted children to Int64.")

    if "bmi" in df.columns:
        df["bmi"] = pd.to_numeric(df["bmi"], errors="coerce")
        logger.info("Converted bmi to numeric (float).")

    if "charges" in df.columns:
        df["charges"] = pd.to_numeric(df["charges"], errors="coerce")
        logger.info("Converted charges to numeric (float).")

    # Categorical columns: clean strings
    for col in ["sex", "smoker", "region"]:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.lower()
            )
            logger.info(f"Standardized {col} as lowercase string.")

    return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate rows from the DataFrame.

    For this dataset, there's no strict natural key,
    so we let DataScrubber handle duplicate detection.
    """
    logger.info(f"FUNCTION START: remove_duplicates with dataframe shape={df.shape}")

    df_scrubber = DataScrubber(df)
    df_deduped = df_scrubber.remove_duplicate_records()

    logger.info(f"Original dataframe shape: {df.shape}")
    logger.info(f"Deduped  dataframe shape: {df_deduped.shape}")
    return df_deduped


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle missing values using simple, transparent rules:

    - Drop rows where key fields (age, bmi, smoker, region, charges) are missing.
    - Fill missing children with 0 (no children).
    - Leave sex as-is but drop if charges is missing.

    These rules are intentionally conservative and easy to explain.
    """
    logger.info(f"FUNCTION START: handle_missing_values with dataframe shape={df.shape}")

    missing_before = df.isna().sum().sum()
    logger.info(f"Total missing values before handling: {missing_before}")

    key_cols = [c for c in ["age", "bmi", "smoker", "region", "charges"] if c in df.columns]
    df = df.dropna(subset=key_cols)

    if "children" in df.columns:
        df["children"] = df["children"].fillna(0)

    # If sex is missing but charges etc. exist, we could keep it,
    # but for simplicity we drop rows with missing sex if present.
    if "sex" in df.columns:
        df = df.dropna(subset=["sex"])

    missing_after = df.isna().sum().sum()
    logger.info(f"Total missing values after handling: {missing_after}")
    logger.info(f"{len(df)} records remaining after handling missing values.")
    return df


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove extreme outliers in charges using the IQR method.

    We use a relatively wide range (±3 * IQR) to avoid removing
    too many valid high-cost patients.

    Negative values, if any, are removed automatically by the IQR filter.
    """
    logger.info(f"FUNCTION START: remove_outliers with dataframe shape={df.shape}")

    if "charges" not in df.columns:
        logger.warning("charges column not found. Skipping outlier removal.")
        return df

    initial_count = len(df)

    q1 = df["charges"].quantile(0.25)
    q3 = df["charges"].quantile(0.75)
    iqr = q3 - q1

    lower = q1 - 3 * iqr
    upper = q3 + 3 * iqr

    df = df[(df["charges"] >= lower) & (df["charges"] <= upper)]

    removed_count = initial_count - len(df)
    logger.info(f"Removed {removed_count} outlier rows based on charges.")
    logger.info(f"{len(df)} records remaining after removing outliers.")
    return df


def add_risk_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add derived features useful for BI analysis:

    - age_group: bins (e.g., 18–29, 30–39, 40–49, 50–59, 60+)
    - bmi_category: underweight, normal, overweight, obese (rough cut)
    - smoker_flag: 1 for smokers, 0 otherwise
    """
    logger.info("FUNCTION START: add_risk_features")

    # age_group
    if "age" in df.columns:
        df["age_group"] = pd.cut(
            df["age"].astype("float"),
            bins=[0, 29, 39, 49, 59, 120],
            labels=["18–29", "30–39", "40–49", "50–59", "60+"],
            right=True,
        )
        logger.info("Added age_group feature.")

    # bmi_category
    if "bmi" in df.columns:
        df["bmi_category"] = pd.cut(
            df["bmi"],
            bins=[0, 18.5, 25, 30, 100],
            labels=["underweight/normal", "overweight", "obese", "extreme"],
            right=False,
        )
        logger.info("Added bmi_category feature.")

    # smoker_flag
    if "smoker" in df.columns:
        df["smoker_flag"] = df["smoker"].map({"yes": 1, "no": 0})
        logger.info("Added smoker_flag feature.")

    return df


#####################################
# Main
#####################################


def main() -> None:
    """
    Main entry point for processing insurance charges data.

    Steps:
    1. Read raw insurance.csv from data/raw.
    2. Clean column names and convert data types.
    3. Remove duplicate records.
    4. Handle missing values.
    5. Remove extreme outliers in charges.
    6. Add derived risk-related features.
    7. Save prepared data to data/prepared/insurance_prepared.csv.
    """
    logger.info("============================================")
    logger.info("STARTING prepare_insurance_charges.py")
    logger.info("============================================")

    logger.info(f"PROJECT_ROOT      : {PROJECT_ROOT}")
    logger.info(f"src               : {SRC_DIR}")
    logger.info(f"analytics_project : {ANALYTICS_PROJECT_DIR}")
    logger.info(f"data/raw          : {RAW_DATA_DIR}")
    logger.info(f"data/prepared     : {PREPARED_DATA_DIR}")

    input_file = "insurance.csv"
    output_file = "insurance_prepared.csv"

    df = read_raw_data(input_file)
    original_shape = df.shape
    logger.info(f"Initial dataframe shape: {original_shape}")
    logger.info(
        f"Initial dataframe columns: {', '.join(df.columns.astype(str).tolist())}"
    )

    if df.empty:
        logger.error("Dataframe is empty. Exiting script.")
        return

    # Preparation pipeline
    df = clean_column_names(df)
    df = convert_dtypes(df)
    df = remove_duplicates(df)
    df = handle_missing_values(df)
    df = remove_outliers(df)
    df = add_risk_features(df)

    save_prepared_data(df, output_file)

    logger.info("============================================")
    logger.info(f"Original shape: {original_shape}")
    logger.info(f"Cleaned shape:  {df.shape}")
    logger.info("FINISHED prepare_insurance_charges.py")
    logger.info("============================================")


if __name__ == "__main__":
    main()
