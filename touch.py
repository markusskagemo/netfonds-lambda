#! /usr/bin/python3
import sys
import gspread
import logging
import argparse
import pandas as pd
import requests as r
import datetime as dt
import netfonds_utils as nu
import kvant_google_api as kga
import logging
import time

from googleapiclient.discovery import build
from collections import deque

class Session(object):
    """Oauth2 session object for Google Drive and Sheets APIs.
    """
    def __init__(self):
        # Sheets API
        self.sheets = None
        # Drive API
        self.drive = None
        self.token = None
        
    def authorize(self, filename='assets/client_secret.json'):
        """Authorize Drive and Sheets APIs.
        
        # Parameters:
            filename: str
                Client secret json file destination.
        """
        self.token = kga.get_access_token(filename=filename)
        credentials = kga.get_credentials(access_token=self.token)

        # Init Sheets gspread instance
        credentials.access_token = credentials.token
        self.sheets = gspread.authorize(credentials)

        # Init Drive service instance
        self.drive = build('drive', 'v3', credentials=credentials)
        
    def valid(self, expiry_threshold=1000):
        """Check validity of session.
        
        # Parameters:
            expiry_threshold: int
                Minimum allowed duration left of session (token).
        # Returns:
            _, boolean
        """
        get_str = \
            'https://www.googleapis.com/oauth2/v1/tokeninfo?access_token=' + self.token
        token_info = r.get(get_str).json()
        
        return token_info['expires_in'] >= expiry_threshold
    
    
class AssetUpdate(object):
    """Single asset sheets append.
    
    # Parameters:
        ticker: str
            Ex. 'DNB'
        exchange: str:
            Ex. 'OSE'
        granularity: str if specified
            Resampling frequency if called, else append ticks.
            Ex. '1T', 'H', 'D', etc
            Warning: Only tested with '1T' (minute)
    """
    def __init__(self, date, session, ticker, exchange='OSE', granularity=None):
        self.drive = session.drive
        self.sheets = session.sheets
        self.ticker = ticker
        self.exchange = exchange
        self.granularity = granularity
        self.resample = granularity is not None
        self.data = None
        self.date = date
        self.nf_type = 'posdump' # Netfonds data type: currenly only supports 'posdump',
        # but should be easily expandable to f.ex. LOB data
        # with netfonds API client.
        
        if granularity is not None:
            self.dt_format = '%Y-%m-%d %H:%M:%S'
        else:
            self.dt_format = '%Y%m%dT%H%M%S'
        # For debugging
        self.error_log = []
        #
        
    def get_data(self):
        """Download, parse and resample data from Netfonds.
        
        # Returns:
            data: pd.DataFrame
        """
        if self.nf_type is not 'posdump':
            raise NotImplementedError(
                'nf_type "{}" not supported'.format(self.nf_type)
            )
            
        data = nu.get_date_depth(self.date, self.ticker, self.exchange)

        if self.resample:
            # Resampling scheme
            df = nu.ohlc_resample(data)
            df['time'] = df.index
            cols = df.columns.tolist()
            cols = cols[-1:] + cols[:-1]
            df = df[cols]
            df['time'] = df['time'].map(str)
            data = df
            
        self.data = data
    
    @staticmethod
    def datetime_check(dt0, dt1, dt_format):
        """Ad-hoc parse datetimes and check if dt0 is before dt1.
        
        # Parameters:
            dt0, dt1: str
                Time strings
        # Returns:
            _: boolean
                True is good
        """
        if dt0 == 'date':
            return True
        try:
            dt0 = nu.parse_netfonds_time(dt0, dt_format)
            dt1 = nu.parse_netfonds_time(dt1, dt_format)
            return dt0 < dt1
        except Exception as e:
            raise e
    
    def upload(self):
        """Uploads data to google drive.
        Checks that data has not already been added to the sheet.
        """
        if self.data is None:
            raise ValueError(
                'No data in object. Asset: {}'.format(self.ticker))
        if self.granularity == '1T':
            sheet_name = self.ticker + '_minute' # Should've named all minutes '1T'
        elif self.granularity is None:
            sheet_name = '{}_{}'.format(self.ticker, self.nf_type)
        else:
            # > Support all resampling freqs
            raise NotImplementedError()
            sheet_name = '{}_{}'.format(self.ticker, self.granularity)
            
        # Open first worksheet of spreadsheet
        try:
            wks = self.sheets.open(sheet_name).sheet1
        except Exception as e:
            # For debugging
            self.error_log.append(e)
            #
            if e['error']['code'] == 503:
                time.sleep(5)
                wks = self.sheets.open(sheet_name).sheet1
        
        if self.datetime_check(
                kga.last_filled_cell(wks),
                self.data.iloc[0, 0],
                self.dt_format):
            response = kga.sheet_append(self.sheets, sheet_name, self.data)
        else:
            response = None # Datetime check failed
            raise ValueError('Datetime check failed')
            
        return response
        
    
class DriveUpdate(object):
    """Update Google Drive with one or more downloaded and resampled tickers.
    
    # Parameters:
        date: str
            If this parameters if not passed, get todays data
        tickers: list
            If passed, manually define which tickers will be updated
    """
    # > Maybe implement procedurally in lambda handler
    def __init__(self, date=None, tickers=None, 
                 granularity='1T', TICKERFILE='assets/OSE_tickers.csv', 
                 cred_verify_freq=10, exchange='OSE'):        
        self.exchange = exchange
        self.granularity = granularity
        self.max_deque_size = 1 # Make this changable
        # Deque for asynchronous requests and general debugging
        self.asset_deque = deque(maxlen=self.max_deque_size)
        self.retry_list = set()
        self.succeeded_tickers = set()
        
        if tickers is None:
            self.tickers = nu.get_assets(tickerfile=TICKERFILE)
        else:
            self.tickers = tickers
            
        if date is not None:
            self.date = date
        else:
            dt_format = '%Y%m%d'
            now = dt.datetime.now()
            self.date = now.strftime(dt_format)
            
        self.session = Session()
        self.session.authorize()
        self.cred_verify_freq = cred_verify_freq
    
    def update_asset(self, ticker, exchange='OSE'):
        """Download/upload process. Function stores asset object 
        in deque of recents.
        
        # Parameters:
            ticker: str
            exchange: str
        # Returns:
            response: dict
                Google sheets API sheet update response.
        """
        params = {
            'date': self.date, 
            'session': self.session, 
            'ticker': ticker, 
            'exchange': exchange,
            'granularity': self.granularity
        }
        asset = AssetUpdate(**params)
        asset.get_data()
        response = asset.upload()
        # Push asset to asset obj deque
        self.asset_deque.append(asset)
        
        return response
        
    # > Asynchronous can be implemented here
    # > Burst limiter can be implemented here
    def run(self):
        """Main routine. Download-resample-upload process in RTF-package.
        """
        # Logging config
        logging.basicConfig(
            level=logging.INFO,
            format='[%(asctime)s] - [%(levelname)s] - %(message)s'
        )
        console = logging.StreamHandler()
        # Only display messages at or above INFO level
        console.setLevel(logging.INFO)

        for cnt, ticker in enumerate(self.tickers):
            # Verify Oauth session
            if not cnt % self.cred_verify_freq:
                if not self.session.valid(expiry_threshold=3000):
                    self.session.authorize()
                    logging.info('Session token refreshed.')
            try:
                # Asset update scheme
                response = self.update_asset(ticker, self.exchange)
                
                # Assumes that append process was a success
                self.succeeded_tickers.add(ticker)
                
                logging.info(
                    '{}: Updated cells: {}'.format(
                        ticker, response['updates']['updatedCells'])
                )
            except Exception as e:
                logging.error(e)
                # Moving to retry list.
                logging.info('Exception at ticker: {}.'.format(ticker))
                self.retry_list.add(ticker)
                
    def retry(self):
        """Retries download-resample-upload process for all items in retry_list
        """
        self.temp_tickers = self.tickers
        self.tickers = self.retry_list
        try:
            logging.info('Retrying tickers from retry list.')
        except Exception as e:
            print(e)
            pass
        self.run()
        self.tickers = self.temp_tickers
        try:
            logging.info('Retried tickers: {}'.format(self.retry_list))
        except Exception as e:
            # Logging error (retry should be called after run)
            raise e