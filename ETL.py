import os
import sys
import petl
import pymysql
import configparser
import requests
import datetime
import json
import decimal

# Get data from configuration file
config = configparser.ConfigParser()
try:
    config.read('ETL.ini')
except Exception as e:
    print('Could not read configuration file :' + str(e))
    sys.exit()

# Read settings from configuration file
startDate = config['CONFIG']['startDate']
url = config['CONFIG']['url']
destServer = config['CONFIG']['server']
destDatabase = config['CONFIG']['database']

# Request data from URL
try:
    BOCResponse = requests.get(url+startDate)
except Exception as e:
    print('Could not make request:' + str(e))
    sys.exit()

# Initializing list of lists for data storage
BOCDates = []
BOCRates = []

# Check response status and process BOC JSON object
if (BOCResponse.status_code == 200):
    BOCRaw = json.loads(BOCResponse.text)

    # Extract observataion data into column arrays
    for row in BOCRaw['observations']:
        BOCDates.append(datetime.datetime.strptime(row['d'],'%Y-%m-%d'))
        BOCRates.append(decimal.Decimal(row['FXUSDCAD']['v']))

    # Create petl table from column arrays and rename the columns
    exchangeRate = petl.fromcolumns([BOCDates,BOCRates],header=['date', 'rate'])

    print(exchangeRate)