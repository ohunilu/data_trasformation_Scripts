"""
Reusable utilities for reading CSV files and writing Parquet and Avro files.
Designed to be used by multiple projects for data format conversion.
"""
import os
import pandas as pd


def read_csv_file(
    filepath: str
) -> pd.DataFrame | None:

    if not os.path.isfile(filepath):
        print(f"Error: File not found → {filepath}")
        return None

    try:
        print(f"Reading CSV: {filepath}")
        df = pd.read_csv(filepath)
        rows = len(df)
        cols = len(df.columns)
        print(f"Successfully read {rows:,} rows and {cols} columns")
        return df
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return None


def write_parquet_file(
    df: pd.DataFrame,
    output_path: str,
    compression: str = "snappy",
    engine: str = "auto"
) -> bool:

    if df is None or df.empty:
        print("No data to write (DataFrame is empty or None)")
        return False

    try:
        print(f"Writing Parquet: {output_path}")
        df.to_parquet(
            output_path,
            compression=compression,
            engine=engine
        )
        size_mb = os.path.getsize(output_path) / 1024 / 1024
        print(f"Successfully saved → {size_mb:.1f} MB")
        return True
    except Exception as e:
        print(f"Error writing Parquet: {e}")
        return False


def write_csv_file(
    df: pd.DataFrame,
    output_path: str,
    index: bool = False
) -> bool:

    if df is None or df.empty:
        print("No data to write (DataFrame is empty or None)")
        return False

    try:
        print(f"Writing CSV: {output_path}")
        df.to_csv(
            output_path,
            index=index
        )
        size_mb = os.path.getsize(output_path) / 1024 / 1024
        print(f"Successfully saved → {size_mb:.1f} MB")
        return True
    except Exception as e:
        print(f"Error writing CSV: {e}")
        return False


def write_avro_file(
    df: pd.DataFrame,
    output_path: str
) -> bool:

    if df is None or df.empty:
        print("No data to write (DataFrame is empty or None)")
        return False

    try:
        from fastavro import writer, parse_schema
        print(f"Writing Avro: {output_path}")

        # Generate simple schema from dataframe
        schema = {
            "doc": "Auto-generated schema",
            "name": "DataRecord",
            "namespace": "example.avro",
            "type": "record",
            "fields": [
                {"name": col, "type": ["null", "string"]}
                for col in df.columns
            ]
        }

        parsed_schema = parse_schema(schema)
        records = df.astype(str).to_dict(orient="records")

        with open(output_path, "wb") as out:
            writer(out, parsed_schema, records)

        print("Successfully saved")
        return True

    except Exception as e:
        print(f"Error writing Avro: {e}")
        return False
