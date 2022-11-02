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

    ####################################################################################################################

    # 1. Create beneficiaries in RedRose

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

            # mark all beneficiaries as approved
            payload['m.beneficiaryStatus'] = 'Approved'

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

    ####################################################################################################################

    # 2. Create top-up request(s) in RedRose

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

            # if top-up request succeeded update payment status to "Pending", if failed to "Failed"
            for payment in payment_data:
                if upload_result['status'] == 'SUCCEEDED':
                    espo_client.request('PUT', f"Payment/{payment['id']}", {"status": "Pending"})
                elif upload_result['status'] == 'FAILED':
                    espo_client.request('PUT', f"Payment/{payment['id']}", {"status": "Failed"})

    ####################################################################################################################

    # 3. Update payment status in EspoCRM

    # get all transactions
    transactions = redrose_client.request('GET', 'getTransactions')
    # parse date
    for t in transactions:
        t['date'] = pd.to_datetime(t['dated']).strftime("%Y-%m-%d")
    multiple_payments, missing_payments = [], []
    is_multiple_transaction, is_missing_transaction = False, False

    # for each espo payment
    espo_payments = espo_client.request('GET', 'Payment')['list']
    for espo_payment in espo_payments:
        if espo_payment["status"] == "readyforpayment" or espo_payment["status"] == "Planned":
            continue
        shelter_id = espo_payment['shelterID']
        date = espo_payment['date']
        # get transactions for same beneficiary on same date
        transactions_filtered = [t for t in transactions if t['iqId'] == shelter_id]
        transactions_filtered_days = []
        for t in transactions_filtered:
            datetime_diff = pd.to_datetime(t['date']) - pd.to_datetime(date)
            datetime_diff_days = datetime_diff.days
            if 0 < datetime_diff_days < 8:
                transactions_filtered_days.append(t)

        # if God is merciful, there is ONE transaction corresponding to ONE payment
        if len(transactions_filtered_days) == 1:
            transaction = transactions_filtered_days[0]
            if 'approved' in transaction['salesStatus'].lower():
                espo_client.request('PUT', f'Payment/{espo_payment["id"]}',
                                    {"status": "Done", "transactionID": transaction['id']})
            if 'cancelled' in transaction['salesStatus'].lower():
                espo_client.request('PUT', f'Payment/{espo_payment["id"]}',
                                    {"status": "Failed", "transactionID": transaction['id']})
        # if God is cruel, there are MULTIPLE transactions corresponding to ONE payment, or NO transactions at all
        elif len(transactions_filtered_days) > 1:
            multiple_payments += [t["id"] for t in transactions_filtered_days]
            is_multiple_transaction = True
        else:
            missing_payments += [espo_payment["id"]]
            is_missing_transaction = True

    if is_missing_transaction:
        raise ValueError(f'Failed to update payments: no transactions found for payments {missing_payments}')
    if is_multiple_transaction:
        raise ValueError(
            f'Failed to update payments: multiple transactions found for payments {multiple_payments}')


if __name__ == "__main__":
    main()
