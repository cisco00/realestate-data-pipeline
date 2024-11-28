import psycopg2
import json
from spark_query.zillow_data_transformation import implementing_func

#Loading data
path_directory = "/home/oem/PycharmProjects/RealEstate_Data_Pipeline/processed_data/.ipynb_checkpoints/combined_data-checkpoint.csv"
path_direct2 = "/home/oem/PycharmProjects/RealEstate_Data_Pipeline/spark_query/dataframe1.csv"

df1_clean, df2_clean = implementing_func(path_directory, path_direct2)


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


def create_tables(df, table_name, db_name, db_pass, user):

    # Connect to PostgreSQL
    conn = connect_postgres(db_name, db_pass, user)
    cur = conn.cursor()

    # Define a mapping for pandas data types to PostgreSQL data types
    dtype_mapping = {
        'object': 'VARCHAR',
        'int64': 'INTEGER',
        'float64': 'REAL'
    }

    # Build column definitions for the table
    columns = []
    for col in df.columns:
        col_dtype = str(df[col].dtype)
        pg_dtype = dtype_mapping.get(col_dtype, 'TEXT')  # Default to TEXT for unknown types
        columns.append(f"{col} {pg_dtype}")

    columns_query = ", ".join(columns)

    # Construct the CREATE TABLE query
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_query}
    );
    """

    try:
        # Execute the CREATE TABLE query
        cur.execute(create_table_query)
        conn.commit()
        print(f"Table '{table_name}' created successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error creating table '{table_name}': {e}")
    finally:
        cur.close()
        conn.close()

table_name = "TexasTable"

# Create table
create_tables(df1_clean, table_name, db_name, db_pass, user)