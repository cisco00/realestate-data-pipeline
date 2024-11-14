import ast
import pandas as pd
from scripts import zillow_scrapping_scripts as zl
import time
import os
from bs4 import BeautifulSoup

# Initialize an empty DataFrame
df = pd.DataFrame({
    'address': [],
    'bathrooms': [],
    'bedrooms': [],
    'city': [],
    'sale_type': [],
    'state': [],
    'sqft': [],
    'url': [],
    'zip': [],
    'zpid': []
})

# Define the path for local storage
local_storage = "/home/oem/PycharmProjects/RealEstate_Data_Pipeline/realestate_raw_data_solde"

if not os.path.exists(local_storage):
    os.makedirs(local_storage)
    print("Local storage directory created successfully")
else:
    print("Local storage directory already exists")

# List all files in the directory
files = [file for file in os.listdir(local_storage) if os.path.isfile(os.path.join(local_storage, file))]

if files:
    for file_name in files:
        file_path = os.path.join(local_storage, file_name)

        # Open and read each file
        with open(file_path, "r", encoding="utf-8") as f:
            file_content = f.read()

            # Attempt to parse the content
            try:
                content = ast.literal_eval(file_content)
            except (ValueError, SyntaxError) as e:
                print(f"Error parsing {file_name}: {e}")
                continue

            # Process each item in the content
            for item in content:
                soup = BeautifulSoup(item, "html.parser")
                new_obs = []

                # Extract information using custom functions
                card_info = zl.card_details(soup)
                new_obs.append(zl.str_address(card_info))
                new_obs.append(zl.bedrooms(soup))
                new_obs.append(zl.get_city(soup))
                new_obs.append(zl.get_url(soup))
                new_obs.append(zl.get_bathroom(soup))
                new_obs.append(zl.get_sale_type(soup))
                new_obs.append(zl.sqrt_ft(soup))
                new_obs.append(zl.get_url(soup))
                new_obs.append(zl.zipcode(soup))

                # Add the new observation to the DataFrame
                if len(new_obs) == len(df.columns):
                    df.loc[len(df.index)] = new_obs

# Reorder the columns
columns = ['address', 'city', 'state', 'zip', 'sqft', 'bedrooms',
           'bathrooms', 'sale_type', 'url', 'zpid']
data = df[columns]

# Create a timestamp for the filename
localtime = time.localtime()
timeString = time.strftime("%Y%m%d%H%M%S", localtime)

# Construct the file path for the CSV
csv_file_path = os.path.join(local_storage, f"parsed_sold_AZ_{timeString}.csv")

# Save the DataFrame to a CSV file
df.to_csv(csv_file_path, index=False)
print(f"Data saved successfully to {csv_file_path}")