# imports
import os, json, pytz, time, logging
from datetime import date, timedelta, datetime
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# set google api scopes
SCOPES = ['https://www.googleapis.com/auth/adsdatahub']

# variables
GOOGLE_DEVELOPER_KEY = 'your_google_developer_key'
DISCOVERY_URL = 'https://adsdatahub.googleapis.com/$discovery/rest?version=v1'
ADH_CUSTOMER_ID = 'your_adh_customer_id'  # If not specified, defaults to the customer that owns the query.
QUERY_ID = 'customers/[ADH_accountId]/analysisQueries/[ADH_QueryId]'
TIMEZONE = 'your_TimeZone' #'UTC'

# environment variables
# SERVICE_ACCOUNT_SECRET = os.environ.get('SERVICE_ACCOUNT_SECRET')
# GOOGLE_DEVELOPER_KEY = os.environ.get('GOOGLE_DEVELOPER_KEY')

# DISCOVERY_URL = os.environ.get('DISCOVERY_URL')
# ADH_CUSTOMER_ID = os.environ.get('ADH_CUSTOMER_ID')
# QUERY_ID = os.environ.get('QUERY_ID')
# TIMEZONE = os.environ.get('TIMEZONE')

# GCP settings
GCP_PROJECT = 'your_GCP_project'
GCP_DATASET = 'your_Dataset'
GCP_TABLE_NAME = 'your_table_name_'

def adh_wait_for_job_to_complete(ads_data_hub_service, operation):
    statusDone = False
    print('Waiting for the job to complete...')
    while statusDone is False:
        updatedOperation = ads_data_hub_service.operations().get(name=operation['name']).execute()        
        if 'done' in updatedOperation.keys() and updatedOperation['done'] == True:
            statusDone = True
        if(statusDone == False):
            time.sleep(6) # THERE IS A LIMIT OF 10 API CALL PER MINUTES!!!!!
    print('Job completed...')
    return updatedOperation


def main(request):
    credentials = Credentials.from_service_account_file('secret_key.json').with_scopes(SCOPES)
    print('QueryId: ' + QUERY_ID)
    print('ADH_CustomerID: ' + ADH_CUSTOMER_ID)
        
    ads_data_hub_service = build('AdsDataHub', 'v1', credentials=credentials, developerKey=GOOGLE_DEVELOPER_KEY, discoveryServiceUrl=DISCOVERY_URL)
        
    # set timezone for the dates
    tz = pytz.timezone(TIMEZONE)
    start_date = tz.localize(datetime.now() - timedelta(days=delta))
    end_date = tz.localize(datetime.now() - timedelta(days=delta))
    print(f'Reporting from {start_date.date()} to {end_date.date()}')
        
    # run the adh query 
    response = ads_data_hub_service.customers().analysisQueries().start(
            name=QUERY_ID,
            body={
                'spec':{
                    'adsDataCustomerId': ADH_CUSTOMER_ID,
                    'timeZone': TIMEZONE,
                    'startDate': {'year': start_date.year, 'month': start_date.month, 'day': start_date.day},
                    'endDate': {'year': end_date.year, 'month': end_date.month, 'day': end_date.day},
                },
                'destTable': f'{GCP_PROJECT}.{GCP_DATASET}.{GCP_TABLE_NAME}' + end_date.strftime('%Y%m%d')
            }
        ).execute()
    print(f'Query started, with Job ID {response["name"]}')
    
    adh_wait_for_job_to_complete(ads_data_hub_service, response)
