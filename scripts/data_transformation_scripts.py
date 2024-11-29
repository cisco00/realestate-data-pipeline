import pandas as pd


def drop_col(df):
    col_drop = df.iloc[:, 8:306]

    for col in col_drop:
        if col in df.columns:
            print(f"Dropped column: {col}")
            df = df.drop(col, axis=1)
    return df


def calculate_quarter_prices(df, quarter, year_start=2000, year_end=2024):
    quarter_months = {
        "first": ["01-31", "02-28", "03-31"],
        "second": ["04-30", "05-31", "06-30"],
        "third": ["07-31", "08-31", "09-30"],
        "fourth": ["10-31", "11-30", "12-31"]
    }

    if quarter not in quarter_months:
        raise ValueError(f"Invalid quarter name: {quarter}. Must be one of: {list(quarter_months.keys())}")

    months = quarter_months[quarter]

    for year in range(year_start, year_end + 1):
        # Adjust February for leap years if it's the first quarter
        if quarter == "first" and pd.Timestamp(f"{year}-02-01").is_leap_year:
            months[1] = "02-29"  # Replace 02-28 with 02-29 for leap years
        else:
            months[1] = "02-28"  # Reset to 02-28 for non-leap years

        # Generate the column names for the quarter
        date_columns = [f"{year}-{month}" for month in months]

        # Check for missing columns
        missing_cols = [col for col in date_columns if col not in df.columns]
        if missing_cols:
            print(f"Warning: Missing columns for {year} {quarter}: {missing_cols}")
            continue

        # Calculate the average price of houses
        column_name = f"{year}_{quarter}_qtr_prices"
        df[column_name] = df[date_columns].mean(axis=1).round(2)

    return df


def create_state_dfs(df):
    # List of states
    states = [
        "TX", "CA", "NY", "FL", "IL",
        "OH", "GA", "MA", "VA", "WA",
        "PA", "NC", "CO", "MN", "IN",
        "MI", "IA", "MD", "KS", "UT", "OR"
    ]

    # Create a dictionary of DataFrames for each state
    state_dfs = {state: df[df["State"] == state] for state in states}

    return state_dfs


def processed_quarters(df1):
    first_qtrs_df = calculate_quarter_prices(df1, quarter="first")
    second_qtrs_df = calculate_quarter_prices(df1, quarter="second")
    third_qtrs_df = calculate_quarter_prices(df1, quarter="third")
    fourth_qtrs_df = calculate_quarter_prices(df1, quarter="fourth")

    comm_col = df1.iloc[:, 0:8]
    drop_cols = df1.iloc[:, 0:306].columns

    # # Drop specified columns in each DataFrame if they exist
    first_qtrs_df.drop(columns=drop_cols, inplace=True, errors="ignore")
    second_qtrs_df.drop(columns=drop_cols, inplace=True, errors="ignore")
    third_qtrs_df.drop(columns=drop_cols, inplace=True, errors="ignore")
    fourth_qtrs_df.drop(columns=drop_cols, inplace=True, errors="ignore")

    df = pd.concat([comm_col, first_qtrs_df, second_qtrs_df, third_qtrs_df, fourth_qtrs_df], axis=1)

    return df

