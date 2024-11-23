import pandas as pd

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

        df1 = df
        df1 = df1.copy()

        # Calculate the average and add the new column
        column_name = f"{year}_{quarter}_qtr_prices"
        df1[column_name] = df1[date_columns].mean(axis=1).round(2)

    return df1


def droping_columns(df):
    selected_cols = df.iloc[:, 8:309]
    df1 = selected_cols.copy()
    df1 = df1.drop(columns=df1.columns)

    return df1


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



