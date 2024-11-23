import pandas as pd
from scripts.data_transformation_scripts import calculate_quarter_prices

path_directory = "/home/oem/PycharmProjects/RealEstate_Data_Pipeline/processed_data/.ipynb_checkpoints/combined_data-checkpoint.csv"
path_direct2 = "/home/oem/PycharmProjects/RealEstate_Data_Pipeline/spark_query/dataframe1.csv"


# Loadin function
def load_data(path_directory, path_direct2):
    df1 = pd.read_csv(path_directory, low_memory=False)
    df2 = pd.read_csv(path_direct2, low_memory=False)

    for cols in df1.columns:
        if df1[cols].apply(type).nunique() > 1:
            print(f"These col: {cols} has mixed data type in df1")

    for cols in df2.columns:
        if df2[cols].apply(type).nunique() > 1:
            print(f"These col: {cols} has mixed data type in df2")
    return df1, df2

#Loading files into python environemnts
df1, df2 = load_data(path_directory, path_direct2)


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
        if isinstance(df1[col], object):
            df1[col] = df1[col].astype(str)
        else:
            if isinstance(df2[col], object):
                df2[col] = df2[col].astype(str)

    return df1, df2

df1, df2 = correcting_dtypes(df1, df2)

# Droping abnormal columns
def clean_data(df):
    df = df.drop(columns=['City', 'StateCodeFIPS', "MunicipalCodeFIPS"], axis=1)
    print(df.duplicated(keep='first').sum())
    df.fillna(0, inplace=True)

    return df


def implementing_func(df1, df2):
    zillow_df, zillow_df2  = load_data(df1, df2)
    df1, df2 = correcting_dtypes(zillow_df, zillow_df2)

    df_clean = clean_data(df1)
    df2_clean = clean_data(df2)

    avg_price_houses = calculate_quarter_prices(df_clean, df2_clean)

df1 = clean_data(df1)
df2 = clean_data(df2)