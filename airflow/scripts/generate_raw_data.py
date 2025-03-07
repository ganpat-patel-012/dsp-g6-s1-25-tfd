import pandas as pd
import os

# Define file paths
input_file = "data\Clean_Dataset.csv"
raw_data_folder = "raw_data"

# Create raw-data folder if it doesn't exist
os.makedirs(raw_data_folder, exist_ok=True)

# Load entire dataset
df = pd.read_csv(input_file)

# Split dataset into chunks of 10 rows each
num_rows_per_file = 10
num_files = len(df) // num_rows_per_file + (1 if len(df) % num_rows_per_file != 0 else 0)

# Save each chunk as a separate file
for i in range(num_files):
    start_idx = i * num_rows_per_file
    end_idx = start_idx + num_rows_per_file
    split_df = df.iloc[start_idx:end_idx]
    output_file = os.path.join(raw_data_folder, f"data_part_{i+1}.csv")
    split_df.to_csv(output_file, index=False)
    print(f"Data saved to {output_file}")
