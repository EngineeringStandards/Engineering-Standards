import pandas as pd


def process_csv(file_path: str) -> pd.DataFrame:
    """Reads a CSV file and returns a cleaned DataFrame."""
    df = pd.read_csv(file_path)
    # Example cleaning: drop rows with any missing values
    df_cleaned = df.dropna()
    return df_cleaned