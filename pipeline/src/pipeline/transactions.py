# -*- coding: utf-8 -*-
"""
Create topup request
"""
from distutils.command.upload import upload
import pandas as pd
from espo_api_client import EspoAPI
#from redrose_api_client import RedRoseAPI, RedRoseAPIError
from rr_api import RRApi
import os
from dotenv import load_dotenv
import click
from datetime import datetime
load_dotenv(dotenv_path="../credentials/.env")

# Create a client to EspoCRM
espo_client = EspoAPI(os.getenv("ESPOURL"), os.getenv("ESPOAPIKEY"))

# Create a client to RedRose
redrose_client = RRApi(os.getenv("HOSTNAME"), os.getenv("RRAPIUSER"), os.getenv("RRAPIKEY"))

# Setting query parameters for API request
params = {
    "select": "internalId,amount,rrActivity",
    "where": [
        {
            "type": "and",
            "value": [
                {
                    "type": "equals",
                    "attribute": "status",
                    "value": "done"
                },
                {
                    "type": "or",
                    "value": [
                        {
                            "type": "today",
                            "attribute": "date"
                        },
                        {
                            "type": "past",
                            "attribute": "date"
                        }
                    ]
                }
            ]
        }
    ]
}

# Api call to espo & saving response as a dataframe
response = espo_client.request('GET', 'Payment', params)['list']
df_full = pd.DataFrame(response)

# Selecting relevant columns and split dataframe per activity ID
df = df_full[['internalId','amount','rrActivity']]
dfs = [y for x, y in df.groupby('rrActivity', as_index=True)]

# For each dataframe, create an Excel sheet and upload to RedRose
x = 0
while x < len(dfs):
    activity = dfs[x][['internalId','amount']]
    activityId = dfs[x].iloc[0]['rrActivity']
    activity.to_excel(f"IndividualTopup-{activityId}.xlsx", index=False)



    #Upload to RedRose and print output
    print(f"IndividualTopup-{activityId}.xlsx")
    filename = f"IndividualTopup-{activityId}.xlsx"
    filepath = f"C:/Users/TZiere/Git/espo2redrose/pipeline/IndividualTopup-{activityId}.xlsx"
    #activityId = 'ce2dff1e-0103-45f0-81f9-6fede2f75683' #for testing purposes
    upload_result_id = redrose_client.upload_individual_distribution_excel(filename, filepath, activityId)
    upload_result = redrose_client.get_excel_import_status(upload_result_id)
    print(datetime.now(), upload_result)

    while upload_result['status'] not in ['SUCCEEDED', 'FAILED']:
        upload_result = redrose_client.get_excel_import_status(upload_result_id)
        print(datetime.now(), upload_result)

    x += 1