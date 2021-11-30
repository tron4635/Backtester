# libraries you will need
import datetime as dt
import numpy as np
import pandas as pd
import tables as tb
import requests
import fxcmpy
from fxcmpy import fxcmpy_tick_data_reader as tdr
import os
import gzip
from io import StringIO
#import simpleaudio as sa
from pathlib import Path

# Construct the years, weeks and symbol lists required for the scraper.
years = [2020, ]
weeks = list(range(20))
symbols = []
for pair in tdr.get_available_symbols():
    if pair not in symbols:
        symbols.append(pair)

# Scrape time
import os
dirname = os.path.dirname(__file__)
directory = os.path.join(dirname, 'Data/')
if not os.path.exists(directory):
    os.makedirs(directory)
#directory = "../Data/"
#directory = Path(__file__)
print(directory)
symbol = "AUDNZD"
for year in years:
    for week in weeks:
        url = f"https://tickdata.fxcorporate.com/{symbol}/{year}/{week}.csv.gz"
        r = requests.get(url, stream=True)
        with open(f"{directory}{symbol}_{year}_w{week}.csv.gz", 'wb') as file:
            for chunk in r.iter_content(chunk_size=1024):
                file.write(chunk)

# Check all the files for each currency pair was downloaded (should be 104 for each)
total = 0
count = 0
for file in os.listdir(directory):
    if file[:6] == symbol:
        count+=1
total += count
print(f"{symbol} files downloaded = {count} ")
print(f"\nTotal files downloaded = {total}")
