from os.path import exists

from bs4 import BeautifulSoup
from scripts import zillow_scrapping_scripts as zl
import pandas as pd
from io import StringIO
import time
import os
import ast

from scripts.zillow_scrapping_scripts import bedrooms

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

# Creating path for local storage for data
local_storage = ("/home/oem/PycharmProjects/RealEstate_Data_Pipeline/realestate_raw_data_storage")

#Checking if file exist
if not os.path.exists(local_storage) == True:
    os.makedirs(local_storage)
    print("file path created successfully")
else:
    print("Fails to create local storage for data")

files = [f for f in os.listdir(local_storage) if os.path.isfile(os.path.join(local_storage, f))]
if files:
    for f in files:
        with open(local_storage + f, "r", encoding="utf-8") as f:
            text = f.read()
            try:
                files = ast.literal_eval(text)
            except (ValueError, SyntaxError) as e:
                print(f"Error parsing {files}: {e}")
                continue

            for fil in range(len(files)):
                soup = BeautifulSoup(files[fil], "html.parser")

                new_obs = []

                # Extract information using custom functions
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
localtime = time.localtime(time.time())
timeString = time.strftime("%Y%m%d%H%M%S", localtime)

files = os.path.join(local_storage, f"parsed_sold_AZ_{timeString}.csv")

#Saving Data
data = df.to_csv(local_storage, index=False)
print("Data saved successfully")



