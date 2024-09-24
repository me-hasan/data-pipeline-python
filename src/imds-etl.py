import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from urllib.parse import quote  # Importing quote
import os
from datetime import datetime  # Import datetime module

# Log the start time
start_time = datetime.now()
print(f"Script started at: {start_time}")

# Load environment variables from .env file
load_dotenv('../.env')

# Get connection details from environment variables
mysql_user = os.getenv('MYSQL_USER')
mysql_password = os.getenv('MYSQL_PASSWORD')
mysql_encoded_password = quote(mysql_password)  # Encode the password

mysql_host = os.getenv('MYSQL_HOST')
mysql_port = os.getenv('MYSQL_PORT')
mysql_database = os.getenv('MYSQL_DATABASE')

postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')
postgres_encoded_password = quote(postgres_password)
postgres_host = os.getenv('POSTGRES_HOST')
postgres_port = os.getenv('POSTGRES_PORT')
postgres_database = os.getenv('POSTGRES_DATABASE')

# Print out the connection parameters for debugging
print(f"Connecting to MySQL at {mysql_host}:{mysql_port} as {mysql_user}")

# Define MySQL and PostgreSQL connection URLs
mysql_engine = create_engine(f'mysql+pymysql://{mysql_user}:{mysql_encoded_password}@{mysql_host}:{mysql_port}/{mysql_database}')
postgresql_engine = create_engine(f'postgresql://{postgres_user}:{postgres_encoded_password}@{postgres_host}:{postgres_port}/{postgres_database}')

# Create a session for PostgreSQL
Session = sessionmaker(bind=postgresql_engine)

# Fetch data from MySQL
mysql_query = "SELECT * FROM MKISTAT"
imds_data = pd.read_sql(mysql_query, mysql_engine)

# Initialize MetaData without the bind argument
metadata = MetaData()

# Reflect the target table (imds_mk_istats) from the PostgreSQL database
imds_mk_istat_table = Table('imds_mk_istats', metadata, autoload_with=postgresql_engine)

# Prepare data for bulk insert
insert_records = []

# Construct records for insertion
for _, data in imds_data.iterrows():
    record = {
        'mkstat_instrument_code': data['MKISTAT_INSTRUMENT_CODE'],
        'mkstat_instrument_number': data['MKISTAT_INSTRUMENT_NUMBER'],
        'mkstat_quote_bases': data['MKISTAT_QUOTE_BASES'],
        'mkstat_open_price': data['MKISTAT_OPEN_PRICE'],
        'mkstat_pub_last_trade_price': data['MKISTAT_PUB_LAST_TRADED_PRICE'],
        'mkstat_spot_last_trade_price': data['MKISTAT_SPOT_LAST_TRADED_PRICE'],
        'mkstat_high_price': data['MKISTAT_HIGH_PRICE'],
        'mkstat_low_price': data['MKISTAT_LOW_PRICE'],
        'mkstat_close_price': data['MKISTAT_CLOSE_PRICE'],
        'mkstat_yday_close_price': data['MKISTAT_YDAY_CLOSE_PRICE'],
        'mkstat_total_trades': data['MKISTAT_TOTAL_TRADES'],
        'mkstat_total_volume': data['MKISTAT_TOTAL_VOLUME'],
        'mkstat_total_value': data['MKISTAT_TOTAL_VALUE'],
        'mkstat_public_total_trades': data['MKISTAT_PUBLIC_TOTAL_TRADES'],
        'mkstat_public_total_volume': data['MKISTAT_PUBLIC_TOTAL_VOLUME'],
        'mkstat_public_total_value': data['MKISTAT_PUBLIC_TOTAL_VALUE'],
        'mkstat_spot_total_trades': data['MKISTAT_SPOT_TOTAL_TRADES'],
        'mkstat_spot_total_volume': data['MKISTAT_SPOT_TOTAL_VOLUME'],
        'mkstat_spot_total_value': data['MKISTAT_SPOT_TOTAL_VALUE'],
        'mkstat_lm_date_time': data['MKISTAT_LM_DATE_TIME']
    }
    
    # Append to insert list
    insert_records.append(record)

# Using a session context manager for PostgreSQL
with Session() as session:
    for record in insert_records:
        # Check if the row already exists in PostgreSQL
        existing_record = session.query(imds_mk_istat_table).filter_by(
            mkstat_instrument_code=record['mkstat_instrument_code'],
            mkstat_lm_date_time=record['mkstat_lm_date_time']
        ).first()

        # Insert new record if it doesn't exist
        if not existing_record:
            insert_stmt = imds_mk_istat_table.insert().values(record)
            session.execute(insert_stmt)

    # Commit the changes
    session.commit()

    # Log the update completion time
    update_time = datetime.now()
    print(f"Data inserted successfully! Process completed at: {update_time}")

# Calculate the total execution time
total_duration = update_time - start_time
print(f"Total execution time: {total_duration}")
