import os
import json
import ast
import pandas as pd
from bs4 import BeautifulSoup
import time
from io import StringIO  # Import StringIO for in-memory buffer
from scripts import zillow_scrapping_scripts as zl

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

# Local storage directory
local_storage = "/home/oem/PycharmProjects/RealEstate_Data_Pipeline/realestate_raw_data_storage"

if not os.path.exists(local_storage):
    os.makedirs(local_storage)
    print("Local storage directory created successfully")
else:
    print("Local storage directory already exists")

files = [f for f in os.listdir(local_storage) if os.path.isfile(os.path.join(local_storage, f))]
if files:
    for file_name in files:
        file_path = os.path.join(local_storage, file_name)

        with open(file_path, "r", encoding="utf-8") as f:
            text = f.read().strip()  # Read and strip any whitespace
            if not text:
                print(f"Skipping empty file: {file_name}")
                continue  # Skip empty files

            try:
                content = ast.literal_eval(text)
            except (ValueError, SyntaxError) as e:
                print(f"Error parsing {file_name}: {e}")
                continue

            for item in content:
                soup = BeautifulSoup(item, "html.parser")
                new_obs = []

                card_info = zl.card_details(soup)
                new_obs.append(zl.str_address(soup))
                new_obs.append(zl.get_bathroom(card_info))
                new_obs.append(zl.bedrooms(card_info))
                new_obs.append(zl.get_city(soup))
                new_obs.append(zl.get_sale_type(soup))
                new_obs.append(zl.sqrt_ft(card_info))
                new_obs.append(zl.str_address(soup))
                new_obs.append(zl.get_url(soup))
                new_obs.append(zl.get_zipcode_list(soup))
                new_obs.append(zl.get_d(soup))

                if len(new_obs) == len(df.columns):
                    df.loc[len(df.index)] = new_obs

    print("Data processing completed and files parsed")
else:
    print("No files found in the local storage directory")

columns = ['address', 'city', 'state', 'zip', 'sqft', 'bedrooms',
           'bathrooms', 'sale_type', 'url', 'zpid']
df = df[columns]

csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)

timeString = time.strftime("%Y%m%d%H%M%S", time.localtime())
csv_file_path = os.path.join(local_storage, f"parsed_sold_AZ_{timeString}.csv")

with open(csv_file_path, "w", encoding="utf-8") as file:
    file.write(csv_buffer.getvalue())

print(f"Data saved successfully to {csv_file_path}")