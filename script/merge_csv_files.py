# -*- coding: utf-8 -*-
# @Organization  : Wildfire
# @Author        : Fire Gu
# @Time          : 4/8/2025 10:50 am
# @Function      : Combine sub-datasets into one unified dataset


import os
import pandas as pd


def list_csv_files(directory):
    """List all CSV files in the given directory."""
    return [file for file in os.listdir(directory) if file.endswith('.csv')]


def get_csv_fields(file_path):
    """Get the field names and total number of fields of a CSV file."""
    try:
        df = pd.read_csv(file_path, low_memory=False)
        field_names = df.columns.tolist()
        num_fields = len(field_names)
        return field_names, num_fields
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return [], 0


def process_directory(directory):
    """Process all CSV files in the given directory and print their field names and total number of fields."""
    csv_files = list_csv_files(directory)
    for csv_file in csv_files:
        file_path = os.path.join(directory, csv_file)
        fields, num_fields = get_csv_fields(file_path)
        print(f"File: {csv_file}, Fields: {fields}, Total Fields: {num_fields}")


def main():
    directory_path = '../data_process/crash'
    process_directory(directory_path)

    csv_files = sorted(
        [file for file in os.listdir(directory_path) if file.endswith('.csv')]
    )

    all_dfs = []

    for file in csv_files:
        file_path = os.path.join(directory_path, file)
        try:
            df = pd.read_csv(file_path, low_memory=False)
            all_dfs.append(df)
        except Exception as e:
            print(f"Failed to read {file}: {e}")

    concatenated_df = pd.concat(all_dfs, ignore_index=True)
    print(concatenated_df.head())
    print(f"Total rows: {len(concatenated_df)}")

    output_file = '../data_process/opt/2013-2022_DATA_SA_Crash.csv'
    concatenated_df.to_csv(output_file, index=False)
    print(f"Saved combined CSV to {output_file}")


if __name__ == "__main__":
    main()
