import pandas as pd
import os
import argparse
import numpy as np

def split_dataset(input_file, output_folder, num_files):
    """
    Splits a dataset into multiple smaller CSV files.

    Args:
        input_file (str): Path to the original CSV dataset.
        output_folder (str): Directory where the split files will be saved.
        num_files (int): Number of files to generate.
    """
    # Read the original dataset
    df = pd.read_csv(input_file)

    # Shuffle the dataset to ensure randomness
    df = df.sample(frac=1, random_state=42).reset_index(drop=True)

    # Split the dataset into 'num_files' parts
    file_list = np.array_split(df, num_files)

    # Ensure the output directory exists
    os.makedirs(output_folder, exist_ok=True)

    # Save each part as a new CSV file
    for i, chunk in enumerate(file_list):
        output_path = os.path.join(output_folder, f"data_part_{i+1}.csv")
        chunk.to_csv(output_path, index=False)
        print(f"Saved {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split dataset into multiple CSV files.")
    parser.add_argument("--input", required=True, help="Path to input CSV file")
    parser.add_argument("--output_folder", required=True, help="Folder to save split files")
    parser.add_argument("--num_files", type=int, required=True, help="Number of files to generate")
    args = parser.parse_args()

    split_dataset(args.input, args.output_folder, args.num_files)

#python split_dataset.py --input "C:\Users\hp\dsp-g6-s1-25-tfd\data\Clean_Dataset.csv" --output_folder "C:\Users\hp\dsp-g6-s1-25-tfd\data\raw-data" --num_files 5
