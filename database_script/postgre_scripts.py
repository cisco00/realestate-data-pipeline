import psycopg2
import json
from spark_query.zillow_data_processing import implementing_func
from scripts.data_transformation_scripts import create_state_dfs, processed_quarters

#Loading data
path_directory = "/home/oem/PycharmProjects/RealEstate_Data_Pipeline/processed_data/.ipynb_checkpoints/combined_data-checkpoint.csv"
path_direct2 = "/home/oem/PycharmProjects/RealEstate_Data_Pipeline/spark_query/dataframe1.csv"

#Loading and processing data
df1_clean, df2_clean = implementing_func(path_directory, path_direct2)

#Calculating mean quarterly house prices
final_df = processed_quarters(df1_clean)

#Data transformation
state_df = create_state_dfs(final_df)

# Access specific state DataFrames
tx_df = state_df["TX"]
fl_df = state_df["FL"]
oh_df = state_df["OH"]

print(tx_df.columns)

with open("/home/oem/PycharmProjects/RealEstate_Data_Pipeline/secret.json") as f:
   secret =  json.load(f)

db_name = secret["DB_NAME"]
db_pass = secret["DB_PASS"]
user = secret["DB_USER"]


def connect_postgres(db_name, db_pass, user):

    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=user,
            password=db_pass,
            host="localhost",
            port=5432
        )
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise

def remove_duplicate_columns(df):
    if len(df.columns) != len(set(df.columns)):
        print(f"Removing duplicate columns: {df.columns[df.columns.duplicated()].tolist()}")
    return df.loc[:, ~df.columns.duplicated()]

def create_tables(df, table_name, db_name, db_pass, user):

    # Remove duplicate columns
    df = remove_duplicate_columns(df)

    # Connect to PostgreSQL
    conn = connect_postgres(db_name, db_pass, user)
    cur = conn.cursor()

    dtype_mapping = {
        'object': 'VARCHAR',
        'int64': 'INTEGER',
        'float64': 'REAL'
    }

    # Build column definitions for the table
    columns = []
    for col in df.columns:
        col_dtype = str(df[col].dtypes)
        pg_dtype = dtype_mapping.get(col_dtype, 'TEXT')  # Default to TEXT for unknown types
        col_quoted = f'"{col}"'  # Enclose column names in double quotes
        columns.append(f"{col_quoted} {pg_dtype}")

    columns_query = ", ".join(columns)

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        {columns_query}
    );
    """

    try:
        cur.execute(create_table_query)
        conn.commit()
        print(f"Table '{table_name}' created successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error creating table '{table_name}': {e}")
    finally:
        print("Table created successfully on postgres database.")
        cur.close()
        conn.close()


table_names = ["TexasTable", "FloridaTable", "OhioTable"]
datasets = [tx_df, fl_df, oh_df]

for dataset, table_name in zip(table_names, datasets):
        create_tables(tx_df, table_name, db_name, db_pass, user)