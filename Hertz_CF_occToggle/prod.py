import pygsheets
from google.cloud import secretmanager
from google.cloud import bigquery
from datetime import datetime 
from datetime import timedelta

import requests
import os
import urllib
import google.oauth2.id_token
import google.auth.transport.requests
import subprocess

import flask

from credentials import PROJECT_ID, SM_SECRET_NAME, SHEET_KEY_HEIRLOOM_PRICING_MODEL, SHEET_TAB_COMPLIANCE_TOGGLE, CF_GET_TOKEN_V2_URL

# ------ SECRET MANAGER ---------
class SecretManager():
    """ Construct Secret Manager client
    """
    client = secretmanager.SecretManagerServiceClient()

    def __init__(self, project_id):
        self.project_id = project_id
        self.parent = f"projects/{project_id}"

    def get_secret(self, secret_id, version_id:str = "latest"):
        """ Get secret value

            TODO:
                - Raise if secret not found
            Args:
                secret_id (str): Secret ID
                version_id (str): Secret version ID
                    default: latest
            Returns:
                secret_value (str): Secret value
        """
        # Access the secret version
        response = self.client.access_secret_version(
            request={
                "name": f"projects/{self.project_id}/secrets/{secret_id}/versions/{version_id}"
                })
        print("Accessed secret version: {}".format(response.name))
        # Return the decoded payload
        return response.payload.data.decode("UTF-8")    

# Get the Guesty API key
def get_token(url):
    """ Generate token and make request to endpoint

        Docs:
            - https://cloud.google.com/functions/docs/securing/authenticating#console
    """
    # Environment: C.Functions
    if "FUNCTION_TARGET" in os.environ:
        req = urllib.request.Request(url)
        auth_req = google.auth.transport.requests.Request()
        id_token = google.oauth2.id_token.fetch_id_token(auth_req, url)
        req.add_header("Authorization", f"Bearer {id_token}")
        response = urllib.request.urlopen(req)
        return response.read().decode("utf-8")
    # Environment: Local
    else:
        # HACK: Get the token from gcloud
        gcloud_itoken = subprocess.check_output(["gcloud","auth", "print-identity-token"], shell= True)
        token = gcloud_itoken.decode().strip()
        # Make request
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
        }
        return requests.get(url, headers=headers).text 
    
def main(request):
    # ---------------------- Data Extraction --------------------------
    sm = SecretManager(PROJECT_ID)
    sa_key_json = sm.get_secret(SM_SECRET_NAME)
    
    # Google sheet data
    gc = pygsheets.authorize(service_account_json=sa_key_json)
    sheet_pricingModel = gc.open_by_key(SHEET_KEY_HEIRLOOM_PRICING_MODEL)
    sheet_complianceToggle = sheet_pricingModel.worksheet_by_title(SHEET_TAB_COMPLIANCE_TOGGLE)
    df_complianceToggle = sheet_complianceToggle.get_as_df()
    df_complianceToggle.rename(columns={'Listing Unit Address': 'Listing_Unit_Address'}, inplace=True) # to match with BQ column name
    
    # BigQuery data
    credentials, project = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery",
        ]
    )
    bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    query = bq_client.query("SELECT * FROM stayloom.Listings.t_listing_dictionary")
    df_listing_dict = query.result().to_dataframe() 
    df_listingDict_listedTrue = df_listing_dict[['External_ID', 'Listing_Unit_Address']][df_listing_dict['Listied_on_any_channels_'] == True] # filtering the listings which are listed to get unique external id
    
    # Final dataframe
    df_joined = df_complianceToggle.merge(df_listingDict_listedTrue[['External_ID', 'Listing_Unit_Address']], on='Listing_Unit_Address')
    
    # ---------------------- Data preprocessing -------------------------
    # timezones mapping with their current time
    utc_now = datetime.utcnow() 
    mapping_timezone = {
        'AST': (utc_now - timedelta(hours=4)).strftime('%H:%M'),
        'EST': (utc_now - timedelta(hours=5)).strftime('%H:%M'),
        'EDT': (utc_now - timedelta(hours=4)).strftime('%H:%M'),
        'CST': (utc_now - timedelta(hours=6)).strftime('%H:%M'),
        'CDT': (utc_now - timedelta(hours=5)).strftime('%H:%M'),
        'MST': (utc_now - timedelta(hours=7)).strftime('%H:%M'),
        'MDT': (utc_now - timedelta(hours=6)).strftime('%H:%M'),
        'PST': (utc_now - timedelta(hours=8)).strftime('%H:%M'),
        'PDT': (utc_now - timedelta(hours=7)).strftime('%H:%M'),
        'AKST': (utc_now - timedelta(hours=9)).strftime('%H:%M'),
        'AKDT': (utc_now - timedelta(hours=8)).strftime('%H:%M'),
        'HST': (utc_now - timedelta(hours=10)).strftime('%H:%M'),
        'HAST': (utc_now - timedelta(hours=10)).strftime('%H:%M'),
        'HADT': (utc_now - timedelta(hours=9)).strftime('%H:%M'),
        'SST': (utc_now - timedelta(hours=11)).strftime('%H:%M'),
        'SDT': (utc_now - timedelta(hours=10)).strftime('%H:%M'),
        'CHST': (utc_now + timedelta(hours=10)).strftime('%H:%M')
    }
    df_joined['actual_occupancy'] = ''
    
    # function to apply logic of populating actual occupancy 
    def set_actual_occupancy(df_row):
        timezone = df_row['Timezone'] # fetching timezone of a listing
        tz_time = mapping_timezone[timezone] # mapping timezone with its current time 
        time_in = df_row['Time In']
        time_out = df_row['Time Out']
        
        # if the current time of the listing's timezone lies between time in and time out value of that listing then 
        # Number of Occupants (In) value will be actual occupancy value
        if datetime.strptime(time_in, '%H:%M') <= datetime.strptime(tz_time, '%H:%M') <= datetime.strptime(time_out, '%H:%M'):
            return df_row['Number of Occupants (In)']
        else:
            return df_row['Number of Occupants (Out)']
    df_joined['actual_occupancy'] = df_joined.apply(set_actual_occupancy, axis= 1)
    
    # ------------------------ Data loading -------------------------------------
    # generating guesty headers
    token = get_token(CF_GET_TOKEN_V2_URL)
    guesty_headers = {
        'Authorization': f'Bearer {token}',
        'content-type': 'application/json'
    }

    # function to populate actual occupancy of each listing on Guesty
    def populating_actualOcc_toGuesty(df_row):
        url_update_accomodate = f"https://open-api.guesty.com/v1/listings/{df_row['External_ID']}"
        payload = { "accommodates": str(df_row['actual_occupancy']) }
        response = requests.put(url_update_accomodate, json=payload, headers=guesty_headers)

        print(f"API responded with status code: {response.status_code}")
    df_joined.apply(populating_actualOcc_toGuesty, axis=1)
    
    return flask.Response(status=200)

if __name__ == '__main__':
    main(request=None)