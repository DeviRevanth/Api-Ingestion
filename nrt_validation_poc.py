import pandas as pd
import boto3
from sqlalchemy import create_engine
from io import StringIO
import configparser
import logging
from datetime import datetime

logger = logging.getLoggerClass()
logger.info(logging.INFO)

def put_dataframe_to_s3(df, bucket_name, file_key):
    try:
        # Initialize S3 client
        s3 = boto3.client('s3')

        # Convert DataFrame to CSV
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # Put the CSV file to S3
        s3.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue())
    except Exception as e:
        pass

# Fetch data from RDS table
def fetch_data_from_rds():
    try:
        # Read database connection parameters from config file
        config = configparser.ConfigParser()
        config.read('config.ini')
        db_host = config['database']['host']
        db_port = config['database']['port']
        db_name = config['database']['name']
        db_user = config['database']['user']
        db_password = config['database']['password']
        # Create a connection to the database
        engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
        connection = engine.connect()
        # Query to fetch data
        query = 'SELECT SRC_COL_NM,ODP_COL_NM FROM your_table_name where STREAM_ID = ""'
        df_rds = pd.read_sql(query, connection)
        # Close the connection
        connection.close()
        return df_rds
    except Exception as e:
        pass

# Read file from S3 and process it
def read_file_from_s3(bucket_name, file_key):
    try:
        # Initialize S3 client
        s3 = boto3.client('s3')

        # Get the file object from S3
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        df_s3 = pd.read_csv(obj['Body'])

        return df_s3
    except Exception as e:
        pass

def validation(metadata_df,source_df):pass


# Example usage
if __name__ == "__main__":
    logger.info("Validation Started")
    # Fetch data from RDS
    try:
        bucket_name = 'your_bucket_name'
        file_key = 'your_file_key.csv'
        df_s3 = read_file_from_s3(bucket_name, file_key) # Read and process file from S3
        print(df_s3.head())

        df_rds = fetch_data_from_rds()
        print(df_rds.head())

        validation(metadata_df=df_rds,source_df=df_s3)
    except Exception as e:
        logger.error()
