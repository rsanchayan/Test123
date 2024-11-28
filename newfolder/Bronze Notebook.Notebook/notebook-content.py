# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6b44c088-93ea-4bfc-841b-5373ac4eb4d9",
# META       "default_lakehouse_name": "Earthquake_Data",
# META       "default_lakehouse_workspace_id": "0edc4343-cc99-493c-86b8-9cd20247b605",
# META       "known_lakehouses": [
# META         {
# META           "id": "6b44c088-93ea-4bfc-841b-5373ac4eb4d9"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#Importing requests library to get data from API
import requests
import json

#Storing the API location to a variable
url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_date}&endtime={end_date}"

data_raw = requests.get(url)
if data_raw.status_code == 200:
    print("Data successfully fetched")
    data_json = data_raw.json()
    data_json = data_json['features']

    lakehouse_filepath = f'/lakehouse/default/Files/{start_date}_earthquake_data.json'

    with open(lakehouse_filepath, 'w') as file:
        json.dump(data_json,file,indent=2)

    print(f"Data successfully saved to {lakehouse_filepath}")

else:
    print("Error: Failed to fetch data. Status Code:", data_raw.status_code)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
