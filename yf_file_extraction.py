#!/usr/bin/env python
# coding: utf-8

import datetime
import os
import argparse
import pandas as pd
import yfinance as yahooFinance

parser = argparse.ArgumentParser()

parser.add_argument('--year', required=True)
parser.add_argument('--file', required=True)

args = parser.parse_args()

year = int(args.year)
file = args.file

# startDate , as per our convenience we can modify
startDate = datetime.datetime(year, 1, 1)
 
# endDate , as per our convenience we can modify
endDate = datetime.datetime(year, 12, 31)

stock = yahooFinance.Ticker(file)
# pass the parameters as the taken dates for start and end
hist = stock.history(start=startDate,end=endDate)
filename = f'{file}_{year}.csv'
print(f'Writing to csv file - {filename}')
hist.to_csv(f'{file}_{year}.csv')
print(filename)

