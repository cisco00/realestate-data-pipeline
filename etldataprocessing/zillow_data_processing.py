import pandas as pd
import os

local_path = "/zillow_realestate_raw_data_storage"
output_df = "/home/oem/PycharmProjects/RealEstate_Data_Pipeline/spark-warehouse"

def files_names(path):
    names = ("City_zhvi_bdrmcnt",
             "County_zhvi_bdrmcnt",
             "Metro_zhvi_bdrmcnt",
             "Metro_zhvi_uc_sfrcondo",
             "Neighborhood_zhvi_bdrmcnt_5_uc_sfrcondo")

    files_name = []
    for files in os.listdir(path):
        if files.startswith(names):
            files_name.append(files)

    return files_name

def data_processing(data_paths):
    files = files_names(data_paths)
    files.sort()

    data_shape = {}
    data_frame = []

    for index, file_name in enumerate(files):
        file_path = os.path.join(data_paths, file_name)

        if os.path.isfile(file_path) and file_name.endswith('.csv'):
            print(f"Reading file {index + 1}: {file_name}")
            try:
                df = pd.read_csv(file_path)
                data_shape[file_name] = df.shape
                data_frame.append(df)
            except Exception as e:
                print(f"Error reading file {file_name}: {e}")
        else:
            print(f"Skipping non-CSV file: {file_name}")

    if len(set(data_shape.values())) == 1:
        print("\nShapes match for all files.")
        concat_data = pd.concat(data_frame, axis = 0, ignore_index=True)
        return concat_data
    else:
        print("\nShapes do not match. Files and their shapes:")
        for file_name, shape in data_shape.items():
            print(f"{file_name}: {shape}")

        common_shape = max(set(data_shape.values()), key=list(data_shape.values()).count)
        filtered_dfs = [df for df in data_frame if df.shape == common_shape]

        if filtered_dfs:
            print("\nConcatenating files with the most common shape...")
            concat_data = pd.concat(filtered_dfs, axis=0, ignore_index=True)
            return concat_data
        else:
            print("\nNo files with matching shapes.")
            return None

if __name__ == "__main__":
    df = data_processing(local_path)
    if df is not None:
        print("\nSample data from concatenated DataFrame:")
        print(df.head(4))
        df.to_csv("define_data.csv")
    else:
        print("\nNo data was concatenated.")