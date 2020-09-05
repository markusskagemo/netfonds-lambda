#! /usr/bin/python3
import datetime as dt
from touch import DriveUpdate
import logging
import os

def lambda_handler(event, context):
    """Lambda handler. Context parameters defined 
    in Lambda Management Console
    
    # Expected context parameter formats:
        'date': '%Y%m%d'
            Ex. 20190130
        'tickers': 'ticker1 ticker2 ... tickerN'
            Space separated string
            If not passed, all tickers in tickerfile will be updated.
        'granularity': '1T'        
    """
    # Log configuration
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] - [%(levelname)s] - %(message)s'
    )
    
    if os.environ.get('date') is not None:
        date_str = os.environ.get('date')
    else:
        dt_format = '%Y%m%d'
        now = dt.datetime.now()
        date_str = now.strftime(dt_format)
        
    tickers = None
    if os.environ.get('tickers') is not None:
        try:
            tickers = os.environ.get('tickers').split()
        except Exception as e:
            print('Wrong ticker format, interpreting as tickers=None')
            tickers = None
            
    granularity = None
    if os.environ.get('granularity') is not None:
        granularity = os.environ.get('granularity') # 1T
            
    params = {
        'date': date_str,
        'tickers': tickers,
        'granularity': granularity
    }
    # Log DriveUpdate parameters
    logging.info('Params: {}'.format(params))
    
    du = DriveUpdate(**params)
    du.run()
    if len(du.retry_list):
        du.retry()
        
    return 'Lambda function ended.'