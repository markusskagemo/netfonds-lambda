import json
import requests as r
import google.oauth2.credentials


def get_access_token(filename='assets/client_secret.json'):
    """Get Google API access token.
    
    # Parameters:
        FILENAME: str
            Json client secrets filename.
    # Returns:
        _: str
            Access token string from post request response.
    """
    with open(filename) as f:
        data = json.load(f)['web']
    
    body = {
        'client_id': data['client_id'],
        'client_secret': data['client_secret'],
        'refresh_token': data['refresh_token'],
        'grant_type': 'refresh_token'
    }
    res = r.post(
        'https://www.googleapis.com/oauth2/v4/token', 
        data=body
    )
    
    return res.json()['access_token']


def get_credentials(filename='assets/client_secret.json', scopes=None, access_token=None):
    """OAuth2 authenticate user, get credentials
    
    # Parameters:
        FILENAME: str
            Json client secrets filename.
        scopes: list of str
            Specified API scopes, if needed.
        access_token: str
            Generated access token.
    # Returns:
        credentials: google.oauth2.credentials.Credentials object
    """
    # Open client secret file
    with open(filename) as f:
        data = json.load(f)['web']
    
    # Credential parameters
    cred_params = {
        'refresh_token': data['refresh_token'],
        'token_uri': data['token_uri'],
        'client_id': data['client_id'],
        'client_secret': data['client_secret']
    }
    if scopes is not None:
        cred_params['scopes'] = scopes
    if not access_token is None:
        cred_params['token'] = access_token
    else:
        raise ValueError('Invalid access_token.')
        
    # Unpack parameters on oauth2
    credentials = google.oauth2.credentials.Credentials(**cred_params)
    
    return credentials


# > Maybe unecessary in netfonds-cron
def get_folder_files(drive, folder_id, by_mimeType=None):
    """Get metadata of all files in drive folder.
    Issue: Only returns the first 100 files in folder.
    
    # Parameters:
        drive: Drive v3 service instance.
        folder_id: str
            ID of folder in Google Drive.
        by_mimeType: str
            Only return metadata from files with specified mimeType.
            > Not implemented
    # Returns:
        package: dict
    """
    query = "'{}' in parents".format(folder_id)
    prepared_query = drive.files().list(q=query)
    package = prepared_query.execute()
    
    if by_mimeType is not None:
        raise NotImplementedError()
        
    return package


# > Maybe unecessary in netfonds-cron
def create_file(drive, name, folder_id, mimeType='application/vnd.google-apps.spreadsheet'):
    """Creates a file in folder specified by folder_id.
    
    # Parameters:
        drive: Drive v3 service instance.
        name: str
        folder_id: str
            ID of folder in Google Drive.
        mimeType: str
    """
    body = {
        'mimeType': mimeType,
        'name': name,
        'parents': [folder_id]
    }
    file = drive.files().create(body=body).execute()
    
    return file


# > Maybe unecessary in netfonds-cron
def populate_sheet_header(gc=None, sheet_name=None, sps=None, ohlc=False):
    """Give sheet netfonds column title.
    
    # Parameters:
        gc: Sheet API client
            If sps is not passed, pass this.
        sheet_name: str
            If sps is not passed, pass this.
        sps:  gspread.models.Spreadsheet
            If the spreadsheet is opened already, pass this.
            Reduces redundant API calls.
    """
    # Spreadsheet open and append
    if sps is None:
        sps = gc.open(sheet_name)
        
    if not ohlc:
        body = {'values': [
            ['time',
             'bid',
             'bid_depth',
             'bid_depth_total',
             'offer',
             'offer_depth',
             'offer_depth_total']
        ]}
        response = sps.values_update(
            range='Sheet1!A1:G1', 
            body=body, 
            params={'valueInputOption': 'RAW'}
        )
    else:
        body = {'values': [   
            ['time',
            'bid_open', 'bid_high', 'bid_low', 'bid_close',
            'bid_depth', 
            'bid_depth_total_open', 'bid_depth_total_high', 
            'bid_depth_total_low', 'bid_depth_total_close',
            'offer_depth', 
            'offer_depth_total_open', 'offer_depth_total_high', 
            'offer_depth_total_low', 'offer_depth_total_close',
            'spread']      
        ]}
        response = sps.values_update(
            range='Sheet1!A1:P1', 
            body=body, 
            params={'valueInputOption': 'RAW'}
        )
    
    return response
    

def sheet_append(gc, sheet_name, np_data):
    """Append numpy array data to end of sheet.
    
    # Parameters:
        gc: Sheet API client
        sheet_name: str
        np_data: np.array
    """
    # Spreadsheet open and append
    sps = gc.open(sheet_name)
    body = {'values': np_data.values.tolist()}
    response = sps.values_append(
        range='Sheet1!A1', 
        body=body, 
        params={'valueInputOption': 'RAW'}
    )
    
    return response


def last_filled_cell(worksheet, col=1):
    """Get last non-empty cell from worksheet col.
    
    # Parameters:
        worksheet: gspread.models.Worksheet
            Gspread worksheet, G-Sheets API wrapper
        col: int
            column index (columns start at 1)
    # Returns:
        val: str
            value of first non-empty cell in worksheet col
            if worksheet is empty, val = ''
    """
    str_list = list(filter(None, worksheet.col_values(1)))  # fastest
    index = len(str_list)
    if index:
        val = worksheet.cell(index, col).value
        return val
    else:
        return ''