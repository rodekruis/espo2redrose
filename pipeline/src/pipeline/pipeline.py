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
import logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
load_dotenv(dotenv_path="../credentials/.env")


def update_redrose_id(rr_data, entity_name, entity, espo_client):
    if 'm' in rr_data.keys():
        if 'id' in rr_data['m'].keys():
            espo_client.request('PUT', f"{entity_name}/{entity['id']}",
                                {"redroseInternalID": f"{rr_data['m']['id']}"})


def make_hyperlink(espo_url, value):
    url = f"{espo_url}/#Shelter/view/"+"{}"
    linkname = "Link to profile"
    return '=HYPERLINK("%s", "%s")' % (url.format(value), linkname)
    # url = f"{espo_url}/#Shelter/view/{{{value}}}"
    # linkname = "Link to profile"
    # return f'=HYPERLINK("{url}", "{linkname}")'


@click.command()
@click.option('--verbose', '-v', is_flag=True, default=False, help="Print more output.")
def main(verbose):

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
    if verbose:
        logging.info(f"Step 2: Create top-up requests in RedRose")

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
    is_topup_successful = False
    if len(df_espo_pay) > 0:
        if verbose:
            logging.info(f'creating top-up requests for {len(df_espo_pay)} payments')
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
            while upload_result['status'] not in ['SUCCEEDED', 'FAILED']:
                upload_result = redrose_pay_client.get_excel_import_status(upload_result_id)
            if upload_result['status'] == 'FAILED':
                logging.error(f"Top-up request submitted, status FAILED")
            elif verbose:
                logging.info(f"Top-up request submitted, status {upload_result['status']}")

            # if top-up request succeeded update payment status to "Pending", if failed to "Failed"
            for payment in payment_data:
                if upload_result['status'] == 'SUCCEEDED':
                    espo_client.request('PUT', f"Payment/{payment['id']}", {"status": "Pending"})
                elif upload_result['status'] == 'FAILED':
                    espo_client.request('PUT', f"Payment/{payment['id']}", {"status": "Failed"})
            is_topup_successful = upload_result['status'] == 'SUCCEEDED'
    else:
        logging.info("No payments in EspoCRM with status=readyforpayment")

    ####################################################################################################################

    # 3. Create audit file and send it around to if a top-up request was created

    if is_topup_successful:
        logging.info("Creating audit file")
        # Create audit file and define function to create excel hyperlinks
        writer = pd.ExcelWriter('auditfile.xlsx', engine='xlsxwriter')

        # Get due payments and writh to excel
        paymentsEspo = espo_client.request('GET', "Payment")['list']
        payments = pd.json_normalize(paymentsEspo)
        payments = payments.loc[payments['status'] == "Pending"]
        logging.info(payments)
        payments = payments.reset_index()
        paymentsoverview = payments[
            ['shelterID', 'date', 'amount', 'amountCurrency', 'status', 'numPayment', 'modifiedAt', 'shelterName', 'numberOfPayments']]

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
            paymentchanges['Link'] = paymentchanges['parentId'].apply(lambda x: make_hyperlink(os.getenv("ESPOURL"), x))
            paymentchanges = paymentchanges[['data', 'createdAt', 'createdByName', 'parentId', 'Link']]
            paymentchanges.rename(columns={'data': 'Changes'}, inplace=True)
            paymentchanges.rename(columns={'parentId': 'EspoCRM ID'}, inplace=True)

        if len(paymentsto) > 0:
            paymentsto = pd.concat(paymentsto)

            paymentsto["Payment to"] = paymentsto["rrName"] + " " + paymentsto["rrSurname"]
            paymentsto = paymentsto[
                ['shelterID', 'Payment to', 'contactName', 'id', 'status', 'accType', 'modifiedByName', 'ibanpayment',
                 'paymentBankName', 'bicPayment', 'gh0', 'gh1']]
            paymentsto.rename(columns={'id': 'EspoCRM ID'}, inplace=True)
            paymentsID = paymentsto[['EspoCRM ID', 'shelterID']]

            paymentchanges = pd.merge(paymentchanges, paymentsID, on='EspoCRM ID', how='left')
            paymentchanges = paymentchanges[['shelterID', 'Changes', 'createdAt', 'createdByName', 'Link']]

            consolidated = pd.merge(paymentsoverview, paymentsto, on='shelterID', how='left')
            consolidated = consolidated[
                ['shelterID', 'amount', 'amountCurrency', 'status_x', 'numPayment', 'numberOfPayments', 'Payment to', 'contactName', 'status_y',
                 'accType', 'gh0', 'gh1']]
            consolidated.rename(
                columns={'status_x': 'Payment Status', 'status_y': 'Beneficiary Status', 'contactName': 'Beneficiary Name',
                         'accType': 'Accomodation Type'}, inplace=True)

            # Save audit file
            consolidated.to_excel(writer, sheet_name='Payment Overview', index=False)
            paymentchanges.to_excel(writer, sheet_name='Changes', index=False)
            writer.save()

            logging.info("Sending audit file around")
            # Send email to claudia, monica, tijs, dante
            message = Mail(
                from_email='ukraineresponse@510.global',
                to_emails=[To('rrcvaim.sims@ifrc.org'), To('monica.shah@ifrc.org'), To('claudia.kelly@ifrc.org'), To('dante.moses@ifrc.org')],
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

    ####################################################################################################################

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
