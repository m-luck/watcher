import datetime

from google.cloud import bigquery
import arrow

from ConnectPushPull import Connector, Puller, Pusher
import LocalPrinter as lp

core = Connector()
puller = Puller()
pusher = Pusher()

bq_id = 'fluent-justice-215821'
bq_client = core.connect_bq(bq_id, 'git_ignore.json')

def initial_load():
    """Does an initial load from 2014 to 2016. Should only run once.

    Args:
        ---
    Returns:
        ---
    Side Effects:
        Makes a new schema in BQ.

    """
    stocks = puller.pull_quandl_data(
                        '2014-01-01',
                        '2014-01-03',
                        'tickers_short.csv',
                        ['SHARADAR/SEP', 'QOA', 'IFT/NSA'],
                        'quandl.ignore')
    pusher.push_data_to_bq(
                        stocks, 
                        'watch_tables', 
                        'daily_data', 
                        bq_id, 
                        'replace')


def procedural_load(bq_id, recent: str):
    """Loads the rest rest of the dates until today.

        Args: 
            bq_id: Project ID
            recent: Start pulling from 1 day after this date, if possible
    """
    today = arrow.utcnow().format(fmt="YYYY-MM-DD")

    def pull_push(interval, recent, today):
        recent = arrow.get(recent).shift(days=-1)
        recent_plus = arrow.get(recent).shift(days=+interval)
        while arrow.get(recent) < arrow.get(today).shift(days=-interval):
            print('Getting from',recent.format(fmt="YYYY-MM-DD"),'to',recent_plus.format(fmt="YYYY-MM-DD"))
            
            stocks = puller.pull_quandl_data(
                                recent.format(fmt="YYYY-MM-DD"),
                                recent_plus.format(fmt="YYYY-MM-DD"),
                                'tickers_short.csv',
                                ['SHARADAR/SEP', 'QOA', 'IFT/NSA'],
                                'quandl.ignore')
            pusher.push_data_to_bq(
                                stocks, 
                                'watch_tables', 
                                'daily_data', 
                                bq_id, 
                                'append')
            recent = recent_plus
            recent_plus = arrow.get(recent_plus).shift(days=+interval)

    pull_push(7, recent, today)
    print("Finished weekly ranges up until present. Now daily.")

    recent = check_most_recent_date('watch_tables','daily_data')
    recent = arrow.get(recent)
    pull_push(1, recent, today)

def check_most_recent_date(schema, table):
    """Checks what the most recent date is in the schema table.

    Args:
        schema: Schema string.
        table: Table string.
    Returns:
        The most recent date in the table.
    """

    query_job = bq_client.query("SELECT * FROM " + schema + "." + table + " ORDER BY date DESC LIMIT 1")

    results = query_job.result().to_dataframe()
    date = [row[1]['date'] for row in results.iterrows()][0]

    return date

if __name__ == "__main__":
    initial_load()
    date = check_most_recent_date('watch_tables','daily_data')
    date_str = date.strftime('%Y-%m-%d')
    procedural_load(bq_id, date_str)
