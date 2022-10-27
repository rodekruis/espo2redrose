# -*- coding: utf-8 -*-
"""
Connector between EspoCRM and RedRose
"""
import pandas as pd
from pipeline.espo_api_client import EspoAPI
from pipeline.redrose_api_client import RedRoseAPI, RedRosePaymentsAPI, RedRoseAPIError
import os
import requests
from datetime import datetime
from dotenv import load_dotenv
import click
load_dotenv(dotenv_path="../credentials/.env")


def update_redrose_id(rr_data, entity_name, entity, espo_client):
    if 'm' in rr_data.keys():
        if 'id' in rr_data['m'].keys():
            espo_client.request('PUT', f"{entity_name}/{entity['id']}",
                                {"redroseInternalID": f"{rr_data['m']['id']}"})


@click.command()
@click.option('--verbose', '-v', is_flag=True, default=False, help="Print more output.")
def main(verbose):

    # Setup APIs
    if verbose:
        print(f'setting up APIs')
        print(f'from EspoCRM: {os.getenv("ESPOURL")}')
        print(f'to RedRose: {os.getenv("RRURL")}')
    df_map = pd.read_csv('../data/esporedrosemapping.csv')
    espo_client = EspoAPI(os.getenv("ESPOURL"), os.getenv("ESPOAPIKEY"))
    redrose_client = RedRoseAPI(os.getenv("RRURL"), os.getenv("RRAPIUSER"), os.getenv("RRAPIKEY"),
                                os.getenv("RRMODULE"))
    redrose_pay_client = RedRosePaymentsAPI(host_name=os.getenv("RRURL").replace("https://", ""),
                                            user_name=os.getenv("RRAPIUSER"),
                                            password=os.getenv("RRAPIKEY"))

    # create beneficiaries
    df_map_bnf = df_map[df_map['action'] == 'Create bnf']

    for entity_name in df_map_bnf['espo.entity'].unique():

        df_map_ = df_map_bnf[df_map_bnf['espo.entity'] == entity_name]

        # get approved entities from EspoCRM
        entity_list = espo_client.request('GET', entity_name)['list']

        for entity in entity_list:

            # prepare payload for RedRose
            payload = {}
            for ix, row in df_map_.iterrows():
                if row['espo.field'] in entity.keys():
                    payload[row['redrose.field']] = str(entity[row['espo.field']])
                else:
                    print(f"ERROR: field {row['espo.field']} not found in EspoCRM !!!")

            # post data to RedRose
            if verbose:
                print(f'payload: {payload}')
            try:  # first try to create new beneficiary
                rr_data = redrose_client.request('POST', 'importBeneficiaryWithIqId', files=payload)
                update_redrose_id(rr_data, entity_name, entity, espo_client)
            except RedRoseAPIError as e:  # if beneficiary already exists, update it
                try:
                    params = {'beneficiaryIqId': payload['m.iqId']}
                    redrose_client.request('POST', 'updateBeneficiaryByIqId', params=params, files=payload)
                except RedRoseAPIError as e:
                    print('update failed!')
                    print(payload)
                    continue

    # create top-up requests
    df_map_pay = df_map[df_map['action'] == 'Create topup']

    # select approved payments which are due today or in the past
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
    payment_data = espo_client.request('GET', 'Payment', params)['list']

    # create a top-up request for each activity
    df_espo_pay = pd.DataFrame(payment_data)
    if len(df_espo_pay) > 0:
        df_espo_pay = df_espo_pay[df_map_pay['espo.field'].unique()]  # keep only relevant fields
        topup_files = {}
        for activity in df_espo_pay['rrActivity'].unique():
            df_espo_pay_activity = df_espo_pay[df_espo_pay['rrActivity'] == activity]
            topup_file = f"../data/IndividualTopup-{activity}.xlsx"
            df_espo_pay_activity.drop(columns=['rrActivity']).to_excel(topup_file, index=False)
            topup_files[activity] = topup_file

        # upload each top-up request to RedRose and print output
        for activity, topup_file in topup_files.items():
            upload_result_id = redrose_pay_client.upload_individual_distribution_excel(
                filename=os.path.basename(topup_file),
                file_path=topup_file,
                activity_id=activity)
            upload_result = redrose_pay_client.get_excel_import_status(upload_result_id)
            print(datetime.now(), upload_result)
            while upload_result['status'] not in ['SUCCEEDED', 'FAILED']:
                upload_result = redrose_pay_client.get_excel_import_status(upload_result_id)
                print(datetime.now(), upload_result)


if __name__ == "__main__":
    main()