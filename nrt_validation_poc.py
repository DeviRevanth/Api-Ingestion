import pandas as pd 
import boto3
import argparse
from datetime import datetime
import json
import sys,os

def copy_matching_files(src_path,dest_path,pattern):
    s3_client = boto3.client('s3')
    bucket_name = 'odp-us-innovation-ent-raw'
    directory_prefix = "saas_fusion_s3/test_files/"
    response = s3_client.list_objects_v2(
    Bucket = bucket_name,
    Prefix = directory_prefix,
    Delimiter='/')
    for content in response.get("Contents",None):
        key = content["Key"]
        if pattern in key:
            filename = key.split("/")[-1]
            s3_client.copy_object()
##Copy Files from landing to processing path

class Validation() :
    def __init__(self,logger) -> None:
        """
        """
        self.logger = logger
        try:
            from s3_operations import S3Operations
            self.s3_session = S3Operations(logger=logger,profile_name=config["s3_profile_name"])
            logger.info("S3 Connection Successful")
        except Exception as e :
            logger.error("S3 Connection Failure")
            raise
        try:
            from database_connector import get_connection
            self.engine = get_connection(filepath = config["db_config_path"],profile= config["db_profile"])
            logger.info("RDS Connection Successful")
            self.files_metadata = self.metadata_filenames_fetcher()
            self.file_names_pattern = self.files_metadata["file_name_pattern"]
        except Exception as e:
            logger.error("RDS Connection Failure")
            raise

    def source_file_fetcher(self):
        """
        """
        try:
            for file in self.file_names_pattern:
                self.s3_session.list_files(bucket=config["s3_bucket_name"],prefix=config["s3_prefix_name"],file_name=file)

        except Exception as e:pass
    def metadata_filenames_fetcher(self):
        """
        """
        try :
            query = f"""
            select subquery.stream_id,subquery.stream_name,subquery.file_name_pattern,subquery.processing_path,subquery.rejected_path from (
            SELECT *, ROW_NUMBER() OVER (
            PARTITION BY stream_id ORDER BY saas_ingstn_stream_control.created_dt DESC, saas_ingstn_stream_control.updated_dt DESC) AS rn 
            FROM fusion_metadata.saas_ingstn_stream_control 
            WHERE enable_flg = True) 
            AS subquery where rn = 1;
            """
            required_files_metadata = pd.read_sql(query)
            return required_files_metadata
        except Exception as e:pass


    def column_names_metadata_fetcher(self):
        """
        """
        try:
            query = None
        except Exception as e:pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', nargs=1, help="JSON file to be processed", type=argparse.FileType('r'))
    arguments = parser.parse_args()
    config = json.load(arguments.infile[0])
    sys.path.append(config["utils_path"])
    from utils import setup_logger, send_email_notification
    log_filename = str(arguments.infile[0].name).split('/')[-1].replace('json', 'log')
    log_filename=str(log_filename.replace(".log",f"_{datetime.today().strftime('%Y_%m_%d_%H_%M_%S')}.log"))
    log_path = os.path.join(config["log_file"],log_filename)
    logger = setup_logger(log_path)
    try:
        logger.info("Saas Fusion NRT Validation Execution Started")
    except Exception as e:
        print(f"Error Connecting to the database:{e}")


