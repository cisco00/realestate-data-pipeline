import pandas as pd

path_directory = "/home/oem/PycharmProjects/RealEstate_Data_Pipeline/processed_data/.ipynb_checkpoints/combined_data-checkpoint.csv"
path_direct2 = "/home/oem/PycharmProjects/RealEstate_Data_Pipeline/spark_query/dataframe1.csv"


# Loadin function
def load_data(path_directory, path_direct2):
    df1 = pd.read_csv(path_directory, low_memory=False)
    df2 = pd.read_csv(path_direct2, low_memory=False)

    for cols in df1.select_dtypes(include=["object"]).columns:
        if df1[cols].apply(type).nunique() > 1:
            print(f"These col: {cols} has mixed data type in df1")

    for cols in df2.select_dtypes(include=["object"]).columns:
        if df2[cols].apply(type).nunique() > 1:
            print(f"These col: {cols} has mixed data type in df2")
    return df1, df2


# Correcting the dtype of columns
def correcting_dtypes(df1, df2):
    cols_affected = [
        "State",
        "City",
        "Metro",
        "CountyName",
        "StateName"
    ]

    for col in cols_affected:
        if col in df1.columns:
            df1[col] = df1[col].astype(str)
        if col in df2.columns:
            df2[col] = df2[col].astype(str)

    return df1, df2

# Droping abnormal columns
def clean_data(df):
    drop_cols = ['City', 'StateCodeFIPS', 'MunicipalCodeFIPS']
    df = df.drop(columns=[col for col in drop_cols if col in df.columns], errors='ignore')

    print(f"Number of duplicate rows: {df.duplicated(keep='first').sum()}")
    df.fillna(0, inplace=True)
    return df


def implementing_func(df1, df2):
    #Load data
    zillow_df, zillow_df2  = load_data(df1, df2)

    #Correct data types
    df1, df2 = correcting_dtypes(zillow_df, zillow_df2)

    #Clean data
    df_clean = clean_data(df1)
    df2_clean = clean_data(df2)

    print("data is ready to be loaded in DB")

    return df_clean, df2_clean
