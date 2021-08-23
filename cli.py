#!/home/grimm/Desktop/PythonEnvs/TIDAL2/bin/python

import argparse
import datetime
from dateutil.parser import parse

from lib.ingest import ingest

TODAY = "TODAY"
parser = argparse.ArgumentParser(description='Pull comment and submission data for the TIDAL subreddit for a 24 hour period proceeding the current day.')
parser.add_argument('--date', dest='date', action='store', default="TODAY",
                    help='Pull comment and submission data for the 24 hour period proceeding the start of this date.')

args = parser.parse_args()

date = None

if args.date == TODAY:
    today = datetime.date.today()
    today = datetime.datetime.combine(today, datetime.datetime.min.time())
    date = today
else:
    # Parse supports most datetime formats per
    # https://dateutil.readthedocs.io/en/stable/parser.html
    date = parse(args.date)
    date = date.replace(hour=0,
                        minute=0,
                        second=0,
                        microsecond=0)

ingest(date)
