import pandas as pd
import ast
from scripts import zillow_scrapping_scripts as zl
import os
import time
from bs4 import BeautifulSoup


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
    print("storage directory created")
else:
    print("storage directory already exists")

files = [file for file in os.listdir(local_storage) if os.path.isfile(os.path.join(local_storage, "recently_sold.csv"))]
if files:

    for file in files:
        file_path = os.path.join(local_storage, file)

        with open(file_path, mode='r', encoding="utf-8") as csv_file:
            csv_reader = csv_file.read()

            try:
                content = ast.literal_eval(csv_reader)
            except Exception as e:
                print(f"{e} error occure while trying to parse {file_path}")
                continue

            new_obs = []
            for file in content:
                soup = BeautifulSoup(file)
            new_obs.append(zl.zipcode(soup))