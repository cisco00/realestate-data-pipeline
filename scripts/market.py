from bs4 import BeautifulSoup
from scripts import zillow_scrapping_scripts as zl
import pandas as pd
import time
import os
import ast

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

local_storage = ("/home/oem/PycharmProjects/RealEstate_Data_Pipeline/realestate_raw_data_storage")

if not os.path.exists(local_storage):
    os.makedirs(local_storage)
    print("Local storage directory created successfully")
else:
    print("Local storage directory already exists")

files = [f for f in os.listdir(local_storage) if os.path.isfile(os.path.join(local_storage, f))]
if files:
    for file_name in files:
        file_path = os.path.join(local_storage, file_name)  # Construct the file path

        with open(file_path, "r", encoding="utf-8") as f:
            text = f.read()
            try:
                content = ast.literal_eval(text)  # Parse the file content
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

# Create a timestamp for the filename
localtime = time.localtime()
timeString = time.strftime("%Y%m%d%H%M%S", localtime)

# Construct the file path for the CSV
csv_file_path = os.path.join(local_storage, f"parsed_sold_AZ_{timeString}.csv")

# Save the DataFrame to a CSV file
df.to_csv(csv_file_path, index=False)
print(f"Data saved successfully to {csv_file_path}")