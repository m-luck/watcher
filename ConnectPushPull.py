# System Pkgs
import ast
import csv
import time
from typing import List, Dict

# 3rd Party
import arrow
import numpy as np
import pandas as pd
# import pyodbc
import quandl
import yaml
from argparse import ArgumentParser
from google.cloud import bigquery
from google.oauth2 import service_account

# App specific
import LocalPrinter as lp

class Connector:

    def __init__(self):
        pass

    def __call__(self):
        pass

    def connect_sql(self, sql_connection_options):
        """Connect to an SQL database.

        Args:
            self 
            sql_connection_options (str): String of SERVER, UID, PWD, OPTION
        Returns: 
            the actual connection item that can be used for future queries and commits

        """

        cnxn = pyodbc.connect('DRIVER={MySQL ODBC 5.3 ANSI Driver};'+sql_connection_options)
        server, uid, pwd, _, _ = sql_connection_options.split(';')
        server = server.split('=')[1]
        uid = uid.split('=')[1]
        pwd = pwd.split('=')[1]
        return cnxn 

    def connect_bq(self, bqid, bqkey_path):
        """Connect to an BQ database.

        Args:
            self 
            bqid: BigQuery Project ID
            bqkey_path: Path relative to running file including only the BiqQuery project key.
            
        Returns: 
            the actual connection item that can be used for future queries and commits

        """

        credentials = service_account.Credentials.from_service_account_file(bqkey_path)

        bq_client_live = bigquery.Client(credentials=credentials, project=bqid)

        return bq_client_live
        
class Puller:

    def __init__(self):
        pass

    def __call__(self):
        pass

    def join_data(self, api_key_path, df, table: str, start: str, end: str, tickers: List[str]) -> pd.DataFrame:
        """Joins data from Quandl tables to main dataframe.

        Args:
            df: main dataframe
            table: table name with slash
            start: Start date as string (YYMMDD)
            end: End date as string
        
        Returns:
            New df with joined rows
        """

        with open(api_key_path, 'r') as key:
            quandl.ApiConfig.api_key = key.read()
        hashed_rows = {}
        for tic in tickers:
            ticker = tic[0]
            print(table,'started:',ticker)
            temp_df = quandl.get(table+ticker, start_date=start, end_date=end)
            temp_df['ticker'] = ticker
            temp_df['date'] = temp_df.index
            # Save each row in a dictionary and also as a dictionary to reduce runtime when searching through SEP base once. With adequate RAM, this is reasonable.
            for i, row in temp_df.iterrows():
                hash_key = row['ticker']+str(row['date']).split(' ')[0]
                row_dict = row.to_dict()
                hashed_rows[hash_key] = row_dict
        table_columns = [table_key for table_key in hashed_rows[hash_key]]
        table_columns.sort()

        # Make new columns for data (these NaN's will be filled in).
        for col in table_columns:
            df[col] = np.nan if (col != 'ticker' and col != 'date') else df[col]
                        
        for i, row in df.iterrows(): # Now we are going through the base. O(n_of_base)
            if i == 0: print('Joining', table, 'row',i,'to base data')
            if i % 100 == 0 and i != 0: print('---------------',i,'rows...')
            query_key = row['ticker']+str(row['date']).split(' ')[0]
            if query_key in hashed_rows:
                for col in table_columns:
                    if col != 'ticker' and col != 'date': 
                        val = hashed_rows[query_key][col]
                        df.at[i, col] = val
        df.to_html(table[:-1]+'plusBase.html') # Save df to analyze.
        print("Done joining",table,"to base.")
        
        return df

    @lp.trace_fun
    def pull_quandl_data(
                        self, 
                        start: str, 
                        end: str, 
                        tickers_path: str, 
                        data_sets: List[str], 
                        api_key_path: str) -> pd.DataFrame:
        """Pull data from Quandl API tables.

        Args:
            self
            start: Start date as a YYMMDD string.
            end: End date as a string.
            tickers_path: File with [list of tickers]
        """

        # Connect to Quandl
        with open(tickers_path, 'r') as f, open(api_key_path, 'r') as key:
            quandl.ApiConfig.api_key = key.read()
            reader = csv.reader(f)
            tickers = list(reader)
        
        if 'SHARADAR/SEP' in data_sets:
            table = 'SHARADAR/SEP/'
            df = quandl.get_table(table, date = { 'gte': start, 'lte': end }, ticker=tickers, paginate=True)
            print(df.shape[0], 'rows of SEP returned by query.')
            print("Done loading SEP base.")

        if 'QOA' in data_sets:
            df = self.join_data(api_key_path, df, 'QOA/', start, end, tickers)

        if 'IFT/NSA' in data_sets:
            # df = self.join_data(api_key_path, df, 'IFT/NSA/', start, end, tickers)
            table = 'IFT/NSA'
            hashed_sentiment_rows = {}
            for tic in tickers: 
                ticker = tic[0]
                has_ticker = True
                print('Sentiment started:', ticker)
                for date in arrow.Arrow.span_range('day', arrow.get(start, 'YYYY-MM-DD'), arrow.get(end, 'YYYY-MM-DD')):
                    if has_ticker is True:
                        date_str = date[0].format('YYYY-MM-DD')
                        try:
                            temp_df = quandl.get_table(table, ticker=ticker, date=date_str).iloc[0]
                            temp_df['ticker'] = ticker
                            temp_df['date'] = temp_df.index
                            # Save each row in a dictionary and also as a dictionary to reduce runtime when searching through SEP base once. With adequate RAM, this is reasonable.
                            hash_key = temp_df['ticker']+str(temp_df['date']).split(' ')[0]
                            row_dict = temp_df.to_dict()
                            hashed_sentiment_rows[hash_key] = row_dict
                        except:
                            has_ticker = False
                            print('Sentiment data does not have ticker', ticker)
                if has_ticker is True:
                    sentiment_columns = [sentiment_key for sentiment_key in hashed_sentiment_rows[hash_key]]
                    sentiment_columns.sort()
            # Make new columns for sentiment data.
            for col in sentiment_columns:
                if col != 'ticker' and col != 'date': df[col] = np.nan
            for i, row in df.iterrows(): # Now we are going throught the base. O(n_base)
                if i == 0: print('Joining Sentiment row',i,'to data')
                if i % 100 == 0 and i != 0: print('---------------',i,'to data')
                query_key = row['ticker']+str(row['date']).split(' ')[0]
                if query_key in hashed_sentiment_rows:
                    for col in sentiment_columns:
                        if col != 'ticker' and col != 'date': 
                            val = hashed_sentiment_rows[query_key][col]
                            df.at[i, col] = val
            df.to_html('SENTplusLOAD.html') # Save df to analyze.
            print("Done joining payload to Sentiment Data.")

        return df

class Pusher:
    
    def __init__(self):
        pass

    def __call__(self):
        pass

    @lp.trace_fun
    def push_data_to_bq(self, 
                        data: pd.DataFrame, 
                        bqschema: str, 
                        bqtable: str, 
                        bqid: str, 
                        if_exists: str ='append'):
        """Pushes a table to BigQuery.

        Args:
            data: a dataframe
            bqschema: schema name
            bqtable: table name
            bqid: project id
            if_exists: protocol if table exists
        Returns:
            ---
        """

        data.to_gbq(bqschema+'.'+bqtable, bqid, if_exists=if_exists)
