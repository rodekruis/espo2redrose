# -*- coding: utf-8 -*-
"""
Connector between EspoCRM and RedRose
"""
import pandas as pd
from pipeline.espo_api_client import EspoAPI
from pipeline.redrose_api_client import RedRoseAPI, RedRosePaymentsAPI, RedRoseAPIError
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, To, Attachment, FileContent, FileName, FileType, Disposition
import base64
import os
import sys
import json
from unidecode import unidecode
from dotenv import load_dotenv
import click
from datetime import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s : %(levelname)s : %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("requests_oauthlib").setLevel(logging.WARNING)

load_dotenv(dotenv_path="../credentials/.env")
MAX_NUMBER_PAYMENTS = 500


def update_redrose_id(rr_data, entity_name, entity, espo_client):
    if 'm' in rr_data.keys():
        if 'id' in rr_data['m'].keys():
            espo_client.request('PUT', f"{entity_name}/{entity['id']}",
                                {"redroseInternalID": f"{rr_data['m']['id']}"})


def make_hyperlink(espo_url, value):
    url = f"{espo_url}/#Shelter/view/"+"{}"
    linkname = "Link to profile"
    return '=HYPERLINK("%s", "%s")' % (url.format(value), linkname)


@click.command()
@click.option('--beneficiaries', '-b', is_flag=True, default=False, help="Create beneficiaries.")
@click.option('--topup', '-t', is_flag=True, default=False, help="Create top-up request.")
@click.option('--verbose', '-v', is_flag=True, default=False, help="Print more output.")
def main(beneficiaries, topup, verbose):

    # Setup APIs
    if verbose:
        logging.info(f'setting up APIs')
        logging.info(f'from EspoCRM: {os.getenv("ESPOURL")}')
        logging.info(f'to RedRose: {os.getenv("RRURL")}')
    df_map = pd.read_csv('../data/esporedrosemapping.csv')
    espo_client = EspoAPI(os.getenv("ESPOURL"), os.getenv("ESPOAPIKEY"))
    redrose_client = RedRoseAPI(os.getenv("RRURL"), os.getenv("RRAPIUSER"), os.getenv("RRAPIKEY"),
                                os.getenv("RRMODULE"))
    redrose_pay_client = RedRosePaymentsAPI(host_name=os.getenv("RRURL").replace("https://", ""),
                                            user_name=os.getenv("RRAPIUSER"),
                                            password=os.getenv("RRAPIKEY"))

    ####################################################################################################################

    # 1. Create or update beneficiaries in RedRose
    if beneficiaries:
        if verbose:
            logging.info(f"Step 1: Create or update beneficiaries in RedRose")

        df_map_bnf = df_map[df_map['action'] == 'Create bnf']

        for entity_name in df_map_bnf['espo.entity'].unique():

            df_map_ = df_map_bnf[df_map_bnf['espo.entity'] == entity_name]

            # get approved entities from EspoCRM
            entity_list = espo_client.request('GET', entity_name)['list']
            if verbose:
                logging.info(f'updating {len(entity_list)} beneficiaries')

            for entity in entity_list:

                # prepare payload for RedRose
                payload = {}
                for ix, row in df_map_.iterrows():
                    if row['espo.field'] in entity.keys():
                        payload[row['redrose.field']] = unidecode(str(entity[row['espo.field']]))
                    else:
                        logging.error(f"ERROR: field {row['espo.field']} not found in EspoCRM !!!")

                # mark all beneficiaries as approved
                payload['m.beneficiaryStatus'] = 'Approved'

                # post data to RedRose
                if pd.isna(entity["redroseInternalID"]):  # create new beneficiary
                    if verbose:
                        logging.info(f'creating beneficiary: {payload}')
                    try:
                        rr_data = redrose_client.request('POST', 'importBeneficiaryWithIqId', files=payload)
                    except RedRoseAPIError:
                        logging.error('create beneficiary failed!')
                        continue
                    update_redrose_id(rr_data, entity_name, entity, espo_client)
                else:  # if beneficiary already exists, update it
                    if verbose:
                        logging.info(f'updating beneficiary: {payload}')
                    try:
                        params = {'beneficiaryIqId': payload['m.iqId']}
                        redrose_client.request('POST', 'updateBeneficiaryByIqId', params=params, files=payload)
                    except RedRoseAPIError:
                        logging.error('update failed!')
                        continue

    ####################################################################################################################

    # 2. Create top-up request(s) in RedRose
    if topup:
        if verbose:
            logging.info(f"Step 2: Create top-up requests in RedRose")

        df_map_pay = df_map[df_map['action'] == 'Create topup']

        # select approved payments which are due today or in the past
        params = {
            "select": "id,internalId,amount,rrActivity",
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
            if verbose:
                logging.info(f'creating top-up requests for {len(df_espo_pay)} payments')
            df_espo_pay = df_espo_pay[['id']+list(df_map_pay['espo.field'].unique())]  # keep only relevant fields
            topup_files, topup_payment_ids = {}, {}
            for activity in df_espo_pay['rrActivity'].unique():
                topup_files[activity], topup_payment_ids[activity] = [], []
                df_espo_pay_activity = df_espo_pay[df_espo_pay['rrActivity'] == activity]
                # if more than MAX_NUMBER_PAYMENTS, split and create multiple topup requests
                if len(df_espo_pay_activity) > MAX_NUMBER_PAYMENTS:
                    list_df = [df_espo_pay_activity[i:i + MAX_NUMBER_PAYMENTS].copy()
                               for i in range(0, len(df_espo_pay_activity), MAX_NUMBER_PAYMENTS)]
                    for ndf, df in enumerate(list_df):
                        topup_file = f"../data/IndividualTopup-{activity}-{ndf}.xlsx"
                        df.drop(columns=['id', 'rrActivity']).to_excel(topup_file, index=False)
                        topup_files[activity].append(topup_file)
                        topup_payment_ids[activity].append(list(df['id'].unique()))
                else:
                    topup_file = f"../data/IndividualTopup-{activity}.xlsx"
                    df_espo_pay_activity.drop(columns=['id', 'rrActivity']).to_excel(topup_file, index=False)
                    topup_files[activity].append(topup_file)
                    topup_payment_ids[activity].append(list(df_espo_pay_activity['id'].unique()))

            # upload each top-up request to RedRose and print output
            for activity, topup_file_list in topup_files.items():
                for topup_file, payment_ids in zip(topup_file_list, topup_payment_ids[activity]):
                    logging.info(f"sending {topup_file.replace('../data/', '')} with {len(payment_ids)} payments")
                    upload_result_id = redrose_pay_client.upload_individual_distribution_excel(
                        filename=os.path.basename(topup_file),
                        file_path=topup_file,
                        activity_id=activity)
                    upload_result = redrose_pay_client.get_excel_import_status(upload_result_id)
                    while upload_result['status'] not in ['SUCCEEDED', 'FAILED']:
                        upload_result = redrose_pay_client.get_excel_import_status(upload_result_id)
                    if upload_result['status'] == 'FAILED':
                        logging.error(f"Top-up request submitted, status FAILED")
                    elif verbose:
                        logging.info(f"Top-up request submitted, status {upload_result['status']}")

                    # if top-up request succeeded update corresponding payments' status
                    for payment_id in payment_ids:
                        if upload_result['status'] == 'SUCCEEDED':
                            espo_client.request('PUT', f"Payment/{payment_id}", {
                                "status": "Pending",
                                "dateTopup": datetime.today().strftime("%Y-%m-%d")
                            })
                        elif upload_result['status'] == 'FAILED':
                            espo_client.request('PUT', f"Payment/{payment_id}", {"status": "Failed"})
        else:
            logging.info("No payments in EspoCRM with status=readyforpayment")

        ################################################################################################################

        # 3. Create audit file and send it around if a top-up request was created
        if len(df_espo_pay) > 0:
            logging.info("Creating audit file")
            # Create audit file and define function to create excel hyperlinks
            writer = pd.ExcelWriter('auditfile.xlsx', engine='xlsxwriter')

            # Get due payments and write to excel
            paymentsEspo = espo_client.request('GET', "Payment")['list']
            payments = pd.json_normalize(paymentsEspo)
            payments = payments.loc[payments['status'] == "Pending"]
            logging.info(payments)
            payments = payments.reset_index()
            paymentsoverview = payments[[
                    'shelterID', 'date', 'amount', 'amountCurrency', 'status', 'numPayment', 'modifiedAt',
                    'shelterName', 'numberOfPayments'
                ]]

            # Get associated shelterIds from payments
            shelterIds = payments.shelterId.unique()

            # Get changes for beneficiaries associated to payments and write to excel
            paymentchanges = []
            paymentsto = []

            for id in shelterIds:
                stream = espo_client.request('GET', f"Shelter/{id}/stream")['list']
                dfs = pd.read_json(json.dumps(stream))
                if dfs.empty:
                    pass
                else:
                    dfs = dfs.loc[dfs['type'] == 'Update']
                    paymentchanges.append(dfs)

                to = espo_client.request('GET', f"Shelter/{id}")
                dfto = pd.json_normalize(to)
                if dfto.empty:
                    pass
                else:
                    paymentsto.append(dfto)

            if paymentchanges == []:
                paymentchanges = pd.DataFrame(['no payment info was changed by users in this batch'],
                                              columns=['Paymentchanges'])
            else:
                paymentchanges = pd.concat(paymentchanges)
                paymentchanges['Link'] = paymentchanges['parentId'].apply(
                    lambda x: make_hyperlink(os.getenv("ESPOURL"), x))
                paymentchanges = paymentchanges[['data', 'createdAt', 'createdByName', 'parentId', 'Link']]
                paymentchanges.rename(columns={'data': 'Changes'}, inplace=True)
                paymentchanges.rename(columns={'parentId': 'EspoCRM ID'}, inplace=True)

            if len(paymentsto) > 0:
                paymentsto = pd.concat(paymentsto)

                paymentsto["Payment to"] = paymentsto["rrName"] + " " + paymentsto["rrSurname"]
                paymentsto = paymentsto[[
                    'shelterID', 'Payment to', 'contactName', 'id', 'status', 'accType', 'modifiedByName',
                    'ibanpayment', 'paymentBankName', 'bicPayment', 'gh0', 'gh1', 'reasonIbanChange'
                ]]
                paymentsto.rename(columns={'id': 'EspoCRM ID'}, inplace=True)
                paymentsID = paymentsto[['EspoCRM ID', 'shelterID']]

                paymentchanges = pd.merge(paymentchanges, paymentsID, on='EspoCRM ID', how='left')
                paymentchanges = paymentchanges[['shelterID', 'Changes', 'createdAt', 'createdByName', 'Link']]

                consolidated = pd.merge(paymentsoverview, paymentsto, on='shelterID', how='left')
                consolidated = consolidated[[
                    'shelterID', 'amount', 'amountCurrency', 'status_x', 'numPayment','numberOfPayments', 'Payment to',
                    'contactName', 'status_y', 'accType', 'gh0', 'gh1', 'reasonIbanChange'
                ]]
                consolidated.rename(
                    columns={'status_x': 'Payment Status',
                             'status_y': 'Beneficiary Status',
                             'contactName': 'Beneficiary Name',
                             'accType': 'Accomodation Type'},
                    inplace=True)

                # Save audit file
                consolidated.to_excel(writer, sheet_name='Payment Overview', index=False)
                paymentchanges.to_excel(writer, sheet_name='Changes', index=False)
                writer.save()

                logging.info("Sending audit file around")
                email_from = os.getenv("AUDIT_EMAIL_FROM")
                email_to1 = os.getenv("AUDIT_EMAIL_TO_1")
                email_to2 = os.getenv("AUDIT_EMAIL_TO_2")
                email_to3 = os.getenv("AUDIT_EMAIL_TO_3")
                email_to4 = os.getenv("AUDIT_EMAIL_TO_4")
                # Send emails around
                if email_from is not None and email_to1 is not None and email_to2 is not None and email_to3 is not None and email_to4 is not None:
                    message = Mail(
                        from_email=email_from,
                        to_emails=[To(email_to1), To(email_to2), To(email_to3), To(email_to4)],
                        subject='Shelter Auditfile',
                        html_content='This is the audit file for the sheltertopup of today')

                    data = open('auditfile.xlsx', 'rb').read()
                    encoded_file = base64.b64encode(data).decode('UTF-8')

                    attachedFile = Attachment(
                        FileContent(encoded_file),
                        FileName('auditfile.xlsx'),
                        FileType('application/xlsx'),
                        Disposition('attachment')
                    )
                    message.attachment = attachedFile

                    try:
                        logging.info(f"SENDGRID_API_KEY {os.getenv('SENDGRID_API_KEY')}")
                        sg = SendGridAPIClient(os.getenv("SENDGRID_API_KEY"))
                        response = sg.send(message)
                        logging.info(response.status_code)
                        logging.info(response.body)
                        logging.info(response.headers)
                    except Exception as e:
                        logging.error(e)
            else:
                logging.warning("No payments found for audit file")

        ################################################################################################################

        # 4. Update payment status in EspoCRM

        # get all transactions
        transactions = redrose_client.request('GET', 'getTransactions')
        if verbose:
            logging.info(f"Step 3: Update payment status in EspoCRM")
            logging.info(f"Found {len(transactions)} transactions in RedRose")
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
            if not pd.isna(espo_payment['dateTopup']):
                date = espo_payment['dateTopup']
            else:
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
            logging.warning(f'No transactions found for payments {missing_payments}')
        if is_multiple_transaction:
            logging.warning(
                f'Failed to update payments: multiple transactions found for payments {multiple_payments}')


if __name__ == "__main__":
    main()
