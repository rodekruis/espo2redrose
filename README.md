# espo2redrose
Connector between EspoCRM and RedRose.

Developed in support to the [Ukraine crisis 2022](https://go.ifrc.org/emergencies/5854).

## Description

Synopsis: a [dockerized](https://www.docker.com/) [python app](https://www.python.org/) that connects EspoCRM and RedRose.

Functionality:
- Push new records (beneficiaries) from EspoCRM to RedRose
- Update existing records in RedRose if changed in EspoCRM
- Automatically create topup requests in RedRose based on Payment records with status `readyforpayment` in EspoCRM
- Check payment status after execution in RedRose and update EspoCRM payment accordingly

## WARNING
Currently only IBAN transfers through Moneygram are supported
In step `3. Update payment status in EspoCRM` of the script, `shelterID` is currendly hard-coded, needs refactoring to be generalizable. 

## Setup
Generic requirements:
- a running EspoCRM instance
- a running RedRose instance
- the mapping of fields from EspoCRM to RedRose (as .csv uder `data/`)
- all necessary credentials (as .env under `credentials/`)

### Setup your EspoCRM instance
To be able to create the beneficiaries in RedRose, as a minimum create (or use existing) datafields in EspoCRM for the following information (and include in the mapping.csv) for the entity specified in your .env under `credentials/` under `ESPOENTITY`:
- iqId, Unique identifier, specific to the beneficiary
- gh0, Administrative level 0, usually Country for Payment (Example Ukraine)
- gh1, Administrative level 1, usually State, City, Oblast (Example Lviv)
- bankName, Name of the Bank for Payment
- bankAccountNumber, Bic / Swift code
- bankAccountHolderName, The Name on the Bank Account
- iban, IBAN of the Bank Account
- taxNumber, Tax Number of the Person (country dependent, check with RedRose)
- firstName, Given Name (In Latin Characters)
- lastName, Surname (In Latin Characters)
- internalID to save the internal RedRose ID of the record, will be returned as an api response when creating the beneficiary

To be able to execute payments, create a `Payments` entity, related to the entity of the previous step, with the following fields as a minimum
- amount, currency, to store the amount for the tranches
- status, enum, options: `readyforpayment, Pending, Failed, Done`
- date, date, to save the date that the transfer should be made
- rrActivity, varchar field that should contain the Activity id from redrose that you want to use for the topup. To be found in the url if opening an activity: `https://{{yourhostname}}.redrosecps.com/activity?id={{activityID}}`
- transactionID, varchar, to save the internal transaction id from redrose to update the status
- iqId, Unique identifier from the related beneficiary entity

Create an api user via Administration >> API Users, assign the role needed, put the username and api-key in the `.env` file.


### Setup your RedRose instace
- Contact your RedRose focal point and make sure that they have enabled the following endpoints:
  - `https://{{yourhostname}}.redrosecps.com/externalapi/modules/ifrcpoland/importBeneficiaryWithIqId`
  - `https://{{yourhostname}}.redrosecps.com/externalapi/modules/ifrcpoland/updateBeneficiaryByIqId`
  - `https://{{yourhostname}}.redrosecps.com/externalapi/modules/ifrcpoland/getTransactions`
  - `https://{{yourhostname}}.redrosecps.com/api/activity/uploadIndividualDistributionExcel`
- Create a user, put the username (`RRAPIUSER`) and password (`RRAPIKEY`) in the `.env` file, disable password change policy and assign it a role with following rights:
  - BENEFICIARY_CREATE
  - BENEFICIARY_UPDATE
  - FINANCE_TOPUP_PROPOSAL
  - FINANCE_BANK_TRANSACTION_VIEW


### Setup your mapping file
The mapping file is a csv file with the following columns: `action,redrose.field,espo.entity,espo.field`
- `action`: can assume either `Create bnf` or `Create topup`
  - `Create bnf`: These fields are used to create the beneficiary in RedRose
  - `Create topup`: These fields are used when the topup is created
- `redrose.field`: the name of the field in RedRose. You can find these fieldnames when opening a downloaded export from thje RedRose platform in Excel and observing row 2
- `espo.entity`: the entity that is used to collect the information from, this should correspond to your `ESPOENTITY` in your `.env` file.
- `espo.field`: the field that holds the specific information you want to push to the `redrose.field`

## Docker Setup
1. Install [Docker](https://www.docker.com/get-started)
2. Build the docker image from the root directory
```
docker build -t rodekruis/espo2redrose .
```
3. Run the docker image in a new container and access it
```
docker run -it --entrypoint /bin/bash rodekruis/espo2redrose
```
4. Check that everything is working by running the pipeline (see [Usage](https://github.com/rodekruis/espo2redrose#usage) below)

## Deployment
This docker image could be deployed in different ways, for example with an [Azure Logic App](https://github.com/rodekruis/crm-for-humanitarians/blob/main/docs/administration/logicappgeneral.md)


## Manual Setup
TBI


## Usage
Command:
```
espo2redrose [OPTIONS]
```
Options:
  ```
  --verbose                   print more output
  --help                      show this message and exit
  ```
