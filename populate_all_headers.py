from kvant_google_api import *
from googleapiclient.discovery import build
import gspread
import pandas as pd
import time


# Authorization scheme
def authorize():
    token = get_access_token()
    credentials = get_credentials(access_token=token)
    
    # Init Sheets gspread instance
    credentials.access_token = credentials.token
    gc = gspread.authorize(credentials)

    # Init Drive service instance
    drive = build('drive', 'v3', credentials=credentials)
    
    return gc, drive


def main():
    # Data 04 folder ID
    data_folder = '1mTf4HKFsGpk2bWvCVtmNestZp9owKuG-' # Data 04
    data_folder = '1dOOIYGn1wq4hz6O4mnbCIOCMacIWSXt5' # Posdump
    data_folder = '1FYb_QwZzrzGOyq3Huqd-3n0pia8-4lJA' # 1MIN
    
    # Authorization scheme
    gc, drive = authorize()
    
    # Import all tickers
    ticker_list = pd.read_csv('../data/OSE_tickers.csv', sep=';')['paper']

    """# Get all file metadata from Data folder
    files = get_folder_files(drive, data_folder)['files']
    # List all filenames
    # > Get more than 100 results (NextPage or equiv)
    filenames = [file['name'] for file in files]
    """
    #filenames = ['{}_posdump'.format(ticker) for ticker in ticker_list]
    filenames = []
    
    # Request burst timer
    old_time = time.time()
    
    # Create and populate tick sheet headers
    for ticker in ticker_list:
        # Prevent Google API rate limiting
        while time.time() - old_time < 0.6: # Max 5 requests/sec
            time.sleep(0.1)
        old_time = time.time()
        
        sheet_name = '{}_minute'.format(ticker)
        # Check if sheet file sheet_name already exists
        if sheet_name not in filenames:
            create_file(drive, sheet_name, data_folder)
        else:
            print('Sheet with name "{}" already exists.'.format(sheet_name))
        
        # Open spreadsheet
        sps = gc.open(sheet_name)
        # Select worksheet
        wks = sps.sheet1
        
        # Get last cell in col 1, most likely col 1 is 'time'
        lfc = last_filled_cell(wks)
        if lfc == '':
            populate_sheet_header(sps=sps, ohlc=True)
            print(sheet_name, ': Header successfully populated', sep='')
        elif lfc == 'time':
            print(sheet_name, ': Header already populated', sep='')
        else:
            print(sheet_name, ': Non-empty sheet. Last filled cell of col1: ', lfc)

            
if __name__ == '__main__':
    main()