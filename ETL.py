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
destUser = config['CONFIG']['user']
destPassword = config['CONFIG']['password']

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
    exchangeRates = petl.fromcolumns([BOCDates,BOCRates],header=['creation_date', 'rate'])

    # Load expense document
    try:
        expenses = petl.io.xlsx.fromxlsx('ExpensesSQL.xlsx' ,sheet='Sheet1')
    except Exception as e:
        print('Could not open ExpensesSQL.xlsx ' + str(e))
        sys.exit()
    
    # Join tables
    expenses = petl.outerjoin(exchangeRates,expenses,key='creation_date')

    # Fill down missing values
    expenses = petl.filldown(expenses,'rate')

    # Remove data with no expenses
    expenses = petl.select(expenses,lambda rec: rec.USD != None)

    # Add CDN column
    expenses = petl.addfield(expenses,'CAD', lambda rec: decimal.Decimal(rec.USD) * rec.rate)

    # print(expenses)
    # sys.exit()

    # Initialize database connection
    try:
        dbConnection = pymysql.connect(host=destServer, database=destDatabase, user=destUser, password=destPassword)
    except Exception as e:
        print('Could not connect to database ' + str(e))
        sys.exit()

    # Populate Expenses database table
    try:
        petl.io.todb(expenses, dbConnection, 'expenses')
        print('ETL completed successfully...')
    except Exception as e:
        print('Could not write to database: ' + str(e))

    # # Generate SQL insert queries
    # insert_queries = []
    # for row in petl.dicts(expenses):
    #     date = row['creation_date'].strftime('%Y-%m-%d')
    #     usd = str(row['USD'])
    #     rate = str(row['rate'])
    #     cad = str(row['CAD'])
    #     query = f"INSERT INTO expenses (`date`, USD, rate, CAD) VALUES ('{date}', {usd}, {rate}, {cad});"
    #     insert_queries.append(query)

    # # Save queries to a text file
    # output_file = 'insert_queries.txt'
    # try:
    #     with open(output_file, 'w') as f:
    #         for query in insert_queries:
    #             f.write(query + '\n')
    #     print('SQL insert queries saved to', output_file)
    # except Exception as e:
    #     print('Could not save the insert queries:', str(e))