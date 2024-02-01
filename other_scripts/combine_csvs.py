import os
import glob
import pandas as pd

# Change directory to where the CSV files are located
os.chdir("path to folder containing the csvs you want to union")

# Get a list of all the CSV files in the directory
file_extension = "*.csv"
all_csv_files = [file for file in glob.glob(file_extension)]

# Combine all the CSV files into a single dataframe
combined_df = pd.concat([pd.read_csv(file,low_memory=False) for file in all_csv_files])

combined_df_nodups=combined_df.drop_duplicates().reset_index(drop=True)

# Save the combined dataframe to a CSV file
combined_df.to_csv("combined_csv_file.csv", index=False)
# combined_df_nodups.to_csv("combined_csv_file_no_dups.csv", index=False)