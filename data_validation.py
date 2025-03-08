import os
import pandas as pd
import shutil

# Define folder paths
raw_data_folder = "raw_data"
good_data_folder = "good_data"
bad_data_folder = "bad_data"

# Create directories
os.makedirs(raw_data_folder, exist_ok=True)
os.makedirs(good_data_folder, exist_ok=True)
os.makedirs(bad_data_folder, exist_ok=True)

# Print directory existence and current working directory
print(os.path.exists("raw_data"))
print(os.getcwd())

def check_errors(df, file):
    """
    Check if the dataset contains any introduced errors.
    Returns True if errors are found, otherwise False.
    """
    error_found = False

    # Check for blank price values in SpiceJet
    if df[(df['airline'] == 'SpiceJet') & (df['price'].isna())].shape[0] > 0:
        error_found = True
        print(f"[ERROR] {file}: Blank price values in SpiceJet.")
    
    # Check for negative days_left values
    if df[df['days_left'] < 0].shape[0] > 0:
        error_found = True
        print(f"[ERROR] {file}: Negative 'days_left' values.")
    
    # Check for same source and destination cities
    if df[df['source_city'] == df['destination_city']].shape[0] > 0:
        error_found = True
        print(f"[ERROR] {file}: Same source and destination cities.")
    
    # Check for Invalid 'duration' values (ensure it is numeric)
    try:
        temp_duration = pd.to_numeric(df['duration'], errors='coerce')  # Convert to numeric
        if temp_duration.isna().any():
            error_found = True
            print(f"[ERROR] {file}: Invalid 'duration' values (non-numeric).")
    except Exception as e:
        error_found = True
        print(f"[ERROR] {file}: Error processing 'duration' column: {e}")
    
    # Check for Premium class
    if df[df['travel_class'] == 'Premium'].shape[0] > 0:
        error_found = True
        print(f"[ERROR] {file}: Premium class found.")
    
    # Check for Air India flights with Vistara flight numbers
    df['flight'] = df['flight'].astype(str)
    if df[(df['airline'] == 'Air_India') & (df['flight'].str.startswith('UK-'))].shape[0] > 0:
        error_found = True
        print(f"[ERROR] {file}: Air India flights with Vistara flight numbers.")
    
    # Check for zero stops with duration > 20 hours
    if df[(df['stops'] == 'zero') & (temp_duration > 20)].shape[0] > 0:
        error_found = True
        print(f"[ERROR] {file}: Zero stops with duration > 20 hours.")

    return error_found

print("Starting processing files...")

# Get the list of visible .csv files in raw_data_folder, excluding hidden files, limited to first 1000 files(Just for quick process and testing)
files = [f for f in os.listdir(raw_data_folder) if f.endswith(".csv") and not f.startswith(".")][:1000]

# Process each file and track the results
bad_data_count = 0
good_data_count = 0
skipped_files = []

# Start processing each file
for file in files:
    file_path = os.path.join(raw_data_folder, file)
    
    try:
        # Attempt to read the CSV file
        df = pd.read_csv(file_path)

        # Check for errors in the data
        if check_errors(df, file):
            # Copy the file to bad_data folder if errors are found
            shutil.copy(file_path, os.path.join(bad_data_folder, file))
            bad_data_count += 1
            print(f"{file} is copied to bad_data.")
        else:
            # Copy the file to good_data folder if no errors are found
            shutil.copy(file_path, os.path.join(good_data_folder, file))
            good_data_count += 1
            print(f"{file} is copied to good_data.")

    except Exception as e:
        # Log the error and skip the file if it cannot be processed
        skipped_files.append(file)
        print(f"[ERROR] Could not process {file}: {e}")

# Final summary
print(f"\nTotal files processed: {len(files)}")
print(f"Files copied to bad_data: {bad_data_count}")
print(f"Files copied to good_data: {good_data_count}")
print(f"Files skipped due to errors: {len(skipped_files)}")
