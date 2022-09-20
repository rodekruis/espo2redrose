# -*- coding: utf-8 -*-
"""
Get a submission with a specific ID from kobo and upload it to espocrm via the API

The script takes exactly 1 argument, which is the kObO ID of the submission
"""
import pandas as pd
from pipeline.espo_api_client import EspoAPI
from pipeline.redrose_api_client import RedRoseAPI, RedRoseAPIError
import os
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

    for entity_name in df_map.espoentity.unique():

        # get new entities from Espo
        params = {
            "where": [
                {
                    "type": "equals",
                    "attribute": "syncedRedRose",
                    "value": False
                }
            ]
        }
        entity_list = espo_client.request('GET', entity_name, params)['list']

        for entity in entity_list:
            # prepare payload for RedRose
            payload = {}
            for ix, row in df_map.iterrows():
                if row['espofield'] in entity.keys():
                    payload[row['rrfield']] = str(entity[row['espofield']])
                else:
                    print(f"ERROR: field {row['espofield']} not found in EspoCRM !!!")

            # START WARNING: HARD-CODED STUFF FOR SHELTER PROGRAM IN SLOVAKIA#############
            if 'm.name' and 'm.surname' in payload.keys():
                if payload['m.surname'] is not None:
                    payload['m.name'] = payload['m.name'] + " " + payload['m.surname']
                payload.pop('m.surname')
            payload['gh[0]'] = 'Slovak Red Cross - Shelter'  ## WARNING HARD-CODED
            payload['gh[1]'] = 'Slovak Red Cross - Shelter, Slovakia'
            # END WARNING ################################################################

            # post data to RedRose
            synced_redrose = False
            if verbose:
                print(f'payload: {payload}')
            try:
                redrose_client.request('POST', 'importBeneficiaryWithIqId', payload)
                synced_redrose = True
            except RedRoseAPIError as e:
                print(payload)
                print(e)
                continue

            if synced_redrose:
                espo_client.request('PUT', f'{entity_name}/{entity["id"]}', {"syncedRedRose": synced_redrose})


if __name__ == "__main__":
    main()
