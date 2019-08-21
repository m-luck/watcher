FROM python:3.7-slim
COPY daily_gather.py ConnectPushPull.py git_ignore.json quandl.ignore tickers.csv tickers_short.csv LocalPrinter.py ./
RUN pip install arrow google-cloud-bigquery numpy pandas quandl pyyaml argparse
CMD python daily_gather.py
