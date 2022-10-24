# -*- coding: utf-8 -*-
"""
Create topup request
"""
import pandas as pd
from espo_api_client import EspoAPI
from redrose_api_client import RedRoseAPI, RedRoseAPIError
import rr_api
import os
from dotenv import load_dotenv
import click
load_dotenv(dotenv_path="../credentials/.env")

# Create a client to EspoCRM
espo_client = EspoAPI(os.getenv("ESPOURL"), os.getenv("ESPOAPIKEY"))

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
                    "value": "readyforpayment"
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
    upload_result_id = rr_api.upload_individual_distribution_excel(f"IndividualTopup-{activityId}.xlsx", 'xlsx/' + f"IndividualTopup-{activityId}.xlsx", activity['activityId'])
    upload_result = rr_api.get_excel_import_status(upload_result_id)
    print(datetime.now(), upload_result)
    x += 1