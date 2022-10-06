# -*- coding: utf-8 -*-
"""
Get a submission with a specific ID from kobo and upload it to espocrm via the API

The script takes exactly 1 argument, which is the kObO ID of the submission
"""
import pandas as pd
from pipeline.espo_api_client import EspoAPI
from pipeline.redrose_api_client import RedRoseAPI, RedRoseAPIError
import os
import requests
from dotenv import load_dotenv
import click
load_dotenv(dotenv_path="../credentials/.env")


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
    # get all RedRose transactions
    transactions = redrose_client.request('GET', 'getTransactions')
    # get all EspoCRM payments
    payments = espo_client.request('GET', "Payment")['list']
    payments_redroseids = [p['redRoseTransactionID'] for p in payments]

    for entity_name in df_map.espoentity.unique():

        # get approved entities from EspoCRM
        entity_list = espo_client.request('GET', entity_name)['list']

        for entity in entity_list:

            # prepare payload for RedRose
            payload = {}
            for ix, row in df_map.iterrows():
                if row['espofield'] in entity.keys():
                    payload[row['rrfield']] = str(entity[row['espofield']])
                else:
                    print(f"ERROR: field {row['espofield']} not found in EspoCRM !!!")

            if 'm.name' and 'm.surname' in payload.keys():
                if payload['m.surname'] is not None:
                    payload['m.name'] = payload['m.name'] + " " + payload['m.surname']
                payload.pop('m.surname')
            payload['gh[0]'] = 'Slovak Red Cross - Shelter'  ## WARNING HARD-CODED
            payload['gh[1]'] = 'Slovak Red Cross - Shelter, Slovakia'

            if entity['status'] == 'Payments approved and send to RedRose':
                payload['m.beneficiaryStatus'] = 'Approved'
            elif entity['status'] == 'Rejected':
                payload['m.beneficiaryStatus'] = 'Rejected'
            else:
                continue

            # get related entities (payments)
            related_entities = espo_client.request('GET', f"{entity_name}/{entity['id']}/payments")['list']
            if verbose:
                print(f'related_entities: {related_entities}')
            if len(related_entities) > 0:

                # for each payment, copy the amount to redrose
                for payment in related_entities:
                    num_payment = payment['numPayment']
                    if not isinstance(num_payment, int):
                        try:
                            num_payment = int(num_payment)
                        except ValueError:
                            continue
                    payload[f'm.shelterPayment{num_payment}'] = payment['amount']

                # get active payment, i.e. earliest payment which is planned
                related_entities_dated = []
                for e in related_entities:
                    if pd.to_datetime(e['date']) is not None:
                        e['date'] = pd.to_datetime(e['date']).date()
                        related_entities_dated.append(e)
                related_entities_dated = sorted(related_entities_dated, key=lambda d: d['date'])
                planned_payments = [e for e in related_entities_dated if e['status'] == 'Planned']
                active_payment = next((e for e in planned_payments), None)

                # copy latest transaction status to active payment status
                transactions_beneficiary = [t for t in transactions if t['iqId'] == payload['m.iqId']]
                # filter out all transactions already copied to EspoCRM
                transactions_beneficiary = [t for t in transactions_beneficiary if t['id'] not in payments_redroseids]
                if len(transactions_beneficiary) > 0:
                    for e in transactions_beneficiary:
                        e['dated'] = pd.to_datetime(e['dated']).date()
                    transactions_beneficiary = sorted(transactions_beneficiary, key=lambda d: d['date'])
                    latest_transaction = transactions_beneficiary[-1]
                    if 'success' in latest_transaction['status'].lower():
                        espo_client.request('PUT', f'Payment/{active_payment["id"]}',
                                            {"status": "Done", "redRoseTransactionID": latest_transaction['id']})
                    if 'failed' in latest_transaction['status'].lower() or 'reverted' in latest_transaction['status'].lower():
                        espo_client.request('PUT', f'Payment/{active_payment["id"]}',
                                            {"status": "Failed", "redRoseTransactionID": latest_transaction['id']})

            # post data to RedRose
            if verbose:
                print(f'payload: {payload}')
            try:
                redrose_client.request('POST', 'importBeneficiaryWithIqId', files=payload)
            except RedRoseAPIError as e:
                try:
                    params = {'beneficiaryIqId': payload['m.iqId']}
                    redrose_client.request('POST', 'updateBeneficiaryByIqId', params=params, files=payload)
                except RedRoseAPIError as e:
                    print('\n')
                    print('update failed!')
                    print(payload)
                    continue


if __name__ == "__main__":
    main()