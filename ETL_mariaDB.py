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
    print('Could not read configuration file: ' + str(e))
    sys.exit()

# Read settings from configuration file
startDate = config['CONFIG']['startDate']
url = config['CONFIG']['url']
destServer = config['CONFIG']['server']
destDatabase = config['CONFIG']['database']

# Request data from URL
try:
    BOCResponse = requests.get(url + startDate)
except Exception as e:
    print('Could not make request: ' + str(e))
    sys.exit()

# Initializing list of lists for data storage
BOCDates = []
BOCRates = []

# Check response status and process BOC JSON object
if BOCResponse.status_code == 200:
    BOCRaw = json.loads(BOCResponse.text)

    # Extract observation data into column arrays
    for row in BOCRaw['observations']:
        BOCDates.append(datetime.datetime.strptime(row['d'], '%Y-%m-%d'))
        BOCRates.append(decimal.Decimal(row['FXUSDCAD']['v']))

    # Create petl table from column arrays and rename the columns
    exchangeRates = petl.fromcolumns([BOCDates, BOCRates], header=['date', 'rate'])

    # Load expense document
    try:
        expenses = petl.io.xlsx.fromxlsx('Expenses.xlsx', sheet='Sheet1')
    except Exception as e:
        print('Could not open expenses.xlsx: ' + str(e))
        sys.exit()

    # Join tables
    expenses = petl.outerjoin(exchangeRates, expenses, key='date')

    # Fill down missing values
    expenses = petl.filldown(expenses, 'rate')

    # Remove data with no expenses
    expenses = petl.select(expenses, lambda rec: rec.USD is not None)

    # Add CDN column
    expenses = petl.addfield(expenses, 'CAD', lambda rec: decimal.Decimal(rec.USD) * rec.rate)

    # Convert petl table to a list of dictionaries
    expense_data = expenses.dicts()

    # Initialize database connection
    try:
        dbConnection = pymysql.connect(host=destServer, database=destDatabase)
    except Exception as e:
        print('Could not connect to database: ' + str(e))
        sys.exit()

    # Populate Expenses database table
    try:
        cursor = dbConnection.cursor()
        # Check if the 'CAD' column exists in the 'expenses' table
        try:
            cursor.execute("SELECT CAD FROM expenses LIMIT 1")
            result = cursor.fetchone()
        except Exception as e:
            result = None

        # Add 'CAD' column if it doesn't exist
        if result is None:
            try:
                cursor.execute("ALTER TABLE expenses ADD COLUMN CAD DECIMAL(18, 2)")
                print("Added 'CAD' column to the 'expenses' table.")
            except Exception as e:
                print("Could not add 'CAD' column to the 'expenses' table: " + str(e))
                sys.exit()

        insert_query = "INSERT INTO expenses (`date`, USD, rate, CAD) VALUES (%s, %s, %s, %s)"

        # Iterate over the expense data and insert each row
        for row in expense_data:
            values = (row['date'], row['USD'], row['rate'], row['CAD'])
            cursor.execute(insert_query, values)

        dbConnection.commit()
        print('ETL completed successfully...')
    except Exception as e:
        dbConnection.rollback()
        print('Could not write to database: ' + str(e))
    finally:
        cursor.close()
        dbConnection.close()
