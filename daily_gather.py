from google.cloud import bigquery

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
                        '2016-01-01',
                        'tickers.csv',
                        ['SHARADAR/SEP', 'QOA', 'IFT/NSA'],
                        'quandl.ignore')
    pusher.push_data_to_bq(
                        stocks, 
                        'watch_tables', 
                        'daily_data', 
                        bq_id, 
                        'replace')


def check_most_recent_date(schema, table):
    """Checks what the most recent date is in the schema table.

    Args:
        schema: Schema string.
        table: Table string.
    Returns:
    """

    query_job = bq_client.query(
        """
        SELECT *
        FROM watch_data.
        ORDER BY date DESC
        LIMIT 2
        """
    )

    results = query_job.results()
    print(results)

if __name__ == "__main__":
    initial_load()