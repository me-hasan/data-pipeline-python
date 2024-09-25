import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from urllib.parse import quote
import os
from datetime import datetime
import uuid
import logging
from logging.handlers import RotatingFileHandler

# --- Setup Logging ---
log_file = 'imds_trds.log'
logging.basicConfig(level=logging.INFO)
handler = RotatingFileHandler(log_file, maxBytes=10**6, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(handler)

# Log the start time
start_time = datetime.now()
logger.info(f"imds_trds script started at: {start_time}")

# Load environment variables from .env file
load_dotenv('../.env')

# Get connection details from environment variables
mysql_user = os.getenv('MYSQL_USER')
mysql_password = os.getenv('MYSQL_PASSWORD')
mysql_encoded_password = quote(mysql_password)
mysql_host = os.getenv('MYSQL_HOST')
mysql_port = os.getenv('MYSQL_PORT')
mysql_database = os.getenv('MYSQL_DATABASE')

postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')
postgres_encoded_password = quote(postgres_password)
postgres_host = os.getenv('POSTGRES_HOST')
postgres_port = os.getenv('POSTGRES_PORT')
postgres_database = os.getenv('POSTGRES_DATABASE')

try:
    # Define MySQL and PostgreSQL connection URLs
    mysql_engine = create_engine(f'mysql+pymysql://{mysql_user}:{mysql_encoded_password}@{mysql_host}:{mysql_port}/{mysql_database}')
    postgresql_engine = create_engine(f'postgresql://{postgres_user}:{postgres_encoded_password}@{postgres_host}:{postgres_port}/{postgres_database}')
    
    # Create a session for PostgreSQL
    Session = sessionmaker(bind=postgresql_engine)
    
    # Fetch data from MySQL
    mysql_query = "SELECT * FROM TRD"
    logger.info(f"Executing query on MySQL: {mysql_query}")
    trd_data = pd.read_sql(mysql_query, mysql_engine)
    logger.info(f"Fetched {len(trd_data)} records from MySQL")

    # Initialize MetaData without the bind argument
    metadata = MetaData()
    
    # Reflect the target table (imds_trds) from the PostgreSQL database
    imds_trds_table = Table('imds_trds', metadata, autoload_with=postgresql_engine)
    
    # Prepare data for bulk insert
    insert_records = []
    
    # Construct records for insertion
    for _, data in trd_data.iterrows():
        record = {
            'id': str(uuid.uuid4()),
            'trd_total_trades': data['TRD_TOTAL_TRADES'],
            'trd_total_volume': data['TRD_TOTAL_VOLUME'],
            'trd_total_value': data['TRD_TOTAL_VALUE'],
            'trd_lm_date_time': data['TRD_LM_DATE_TIME']
        }
        insert_records.append(record)
    
    logger.info(f"Prepared {len(insert_records)} records for insertion to PostgreSQL")
    
    # Using a session context manager for PostgreSQL
    with Session() as session:
        for record in insert_records:
            # Check if the row already exists in PostgreSQL
            existing_record = session.query(imds_trds_table).filter_by(
                trd_total_trades=record['trd_total_trades'],
                trd_lm_date_time=record['trd_lm_date_time']
            ).first()
    
            # Insert new record if it doesn't exist
            if not existing_record:
                insert_stmt = imds_trds_table.insert().values(record)
                session.execute(insert_stmt)
    
        session.commit()
        logger.info(f"Inserted {len(insert_records)} new records into PostgreSQL")
except Exception as e:
    logger.error(f"An error occurred: {str(e)}")

# Log the update completion time
update_time = datetime.now()
logger.info(f"Process completed at: {update_time}")
