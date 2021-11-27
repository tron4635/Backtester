# ETL script
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
from pathlib import Path

directory = "/Users/thaddausengelhardt/Documents/quant_python/Datadownload/Results/"
hdf5_file = "/Users/thaddausengelhardt/Documents/quant_python/Datadownload/Results/HDF5/FxTickData.h5"

for file in os.listdir(directory):
    if file.endswith('.gz'):
        print(f"\nExtracting: {file}")
        
        # extract gzip file and assign to Dataframe
        codec = 'utf-16'
        f = gzip.GzipFile(f'{directory}{file}')
        data = f.read()
        data_str = data.decode(codec)
        data_pd = pd.read_csv(StringIO(data_str))
        
        # pad missing zeros in microsecond field
        data_pd['DateTime'] = data_pd.DateTime.str.pad(26, side='right', fillchar='0')
        
        # assign Datetime column as index
        data_pd.set_index('DateTime', inplace=True)

        # determine datetime format and supply srftime directive
        dates_pd = data_pd.index.str[:10].unique()
        if dates_pd[0][2] == '/': # month or day first
            # if month is before day, then, in a list of unique dates, either the first two characters of the first
            # two dates is the same, or (in case of month end where the first two dates have different starting characters)
            # the first two characters of the second and third date is the same
            if (dates_pd[0][:2] == dates_pd[1][:2]) or (dates_pd[1][:2] == dates_pd[2][:2]):
                # month is first
                data_pd.index = pd.to_datetime(data_pd.index, format="%m/%d/%Y %H:%M:%S.%f")
            else:
                # day is first
                data_pd.index = pd.to_datetime(data_pd.index, format="%d/%m/%Y %H:%M:%S.%f")
        elif dates_pd[0][4] == '/': # year first
            if (dates_pd[0][5:7] == dates_pd[1][5:7]) or (dates_pd[1][5:7] == dates_pd[2][5:7]):
                # month is first
                data_pd.index = pd.to_datetime(data_pd.index, format="%Y/%m/%d %H:%M:%S.%f")
            else:
                # day is first
                data_pd.index = pd.to_datetime(data_pd.index, format="%Y/%d/%m %H:%M:%S.%f")
        else:
            print("no matching format found")
        
        print(f"\nIndex set to: {type(data_pd.index)}")

        print("\nDATA SUMMARY:")
        print(data_pd.info())
        
        # Load data into database
        store = pd.HDFStore(hdf5_file)
        symbol = file[:6]
        store.append(symbol, data_pd, format='t') 
        store.flush()
        print("\nH5 DATASTORE SUMMARY:")
        print(store.info()+"\n"+"-"*75)
        store.close()