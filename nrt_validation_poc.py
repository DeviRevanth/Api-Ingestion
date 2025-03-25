#### Importing Required Packages ####
import pandas as pd
import argparse
from datetime import datetime
import json
import traceback
from sqlalchemy import text 
import sys,os

class Validation() :
    def __init__(self, logger) -> None:
        """
        """
        self.logger = logger
        try:
            self.adjust_logging_levels()
            from s3_operations import S3Operations
            self.s3_session :object = S3Operations(logger=logger, profile_name=config["s3_profile_name"] ,partition="N")
            self.logger.info("S3 Connection Successful")
        except Exception as e :
            self.logger.error(f"S3 Connection Failed with error -> {str(e)}", exc_info=True)
            raise
        try:
            from redshift_connector import get_connection
            self.engine :object  = get_connection(filepath=config["db_config_path"], profile=config["db_profile"] ,logger=self.logger)
            self.logger.info("Database Connection Successful")
            self.files_metadata :pd.DataFrame  = self.metadata_filenames_fetcher()
            self.logger.info("Metadata Details fetched successfully")
            self.s3_landing_prefix :str= self.files_metadata["landing_path"].unique()[0]
            self.file_names_pattern :list = self.files_metadata["file_name_pattern"].tolist()
            self.source_file_fetcher()
        except Exception as e:
            self.logger.error(f"Database Connection Failed with error -> {str(e)}", exc_info=True)
            raise

    @staticmethod
    def adjust_logging_levels() -> None:
        """
        Adjust logging levels for boto3 and related libraries
        """
        import logging
        logging.getLogger('boto3').setLevel(logging.WARNING)
        logging.getLogger('botocore').setLevel(logging.WARNING)
        logging.getLogger('s3transfer').setLevel(logging.WARNING)
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logger.info("Logger setup completed. Debug logs from boto3 are suppressed.")

    def sql_query_to_pandas_dataframe(self, engine :object, query :str) -> pd.DataFrame:
        """
        """
        try:
            self.logger.info(f"Executing sql_query_to_pandas_dataframe method to fetch data from DB using query :{query}")
            df = pd.read_sql(query,engine)
            self.logger.info("SQL Query Execution Successful")
            return df
        # except ConnectionResetError as e:
        #     logger.error("Connection Reset Error")
        #     self.engine.dispose()
        #     self.engine = get_connection(filepath = config["db_config_path"],profile= config["db_profile"])
        #     logger.info("Connection Reset Successful")
        #     logger.info("Re-executing the query")
        #     self.sql_query_to_pandas_dataframe(engine=self.engine,query=query,logger=logger)
        except Exception as e:
            self.logger.error(f"SQL Query Execution Failed with error -> {str(e)}", exc_info=True)  
            raise

    def log_table_updation(self, entry_type :str, stream_id :int, file_name :str, load_status :str, error_code :str = None, error_description :str = None):
        """
        """
        try:
            error_code = "NULL" if error_code is None else error_code
            error_description = "NULL" if error_description is None else error_description
            self.logger.info("Executing log_table_updation method")
            if entry_type == "insert":
                log_query = f"""
                INSERT INTO fusion_log.saas_ingstn_log (stream_id, file_name, load_status, error_code, error_description)
                VALUES ({stream_id}, '{file_name}', '{load_status}', '{error_code}', '{error_description}')
                """
            elif entry_type == "update":
                log_query = f"""
                UPDATE fusion_log.saas_ingstn_log
                SET load_status = '{load_status}', error_code = '{error_code}', error_description = '{error_description}'
                WHERE stream_id = {stream_id} AND file_name = '{file_name}'
                """
            else: raise ValueError("Invalid entry_type. Must be 'insert' or 'update'")
            self.engine.execute(text(log_query))
            self.logger.info("Log table updated successfully")
        except Exception as e:
            self.logger.error(f"Failed to execute log_table_updation method with error -> {str(e)}", exc_info=True)
            raise

    def move_s3_files(self, source_bucket :str, destination_bucket :str, source_key :str, destination_key :str):
        """
        """
        try:
            self.logger.info("Executed move_s3_files method")
            self.logger.info(f"sourcebucket:{source_bucket} \n destinationbucket:{destination_bucket} \n sourcekey:{source_key} \n destinationkey:{destination_key}")
            self.s3_session.s3_to_s3_move(source_bucket=source_bucket, dest_bucket=destination_bucket, source_key=source_key, dest_key=destination_key)
            self.logger.info("File Moved Successfully")
        except Exception as e:
            self.logger.error(f"Failed to execute move_s3_files method with error -> {str(e)}",exc_info=True)
            raise

    def metadata_filenames_fetcher(self) -> pd.DataFrame:
        """
        """
        try :
            self.logger.info("Executing metadata_filenames_fetcher method")
            query = f"""
            SELECT subquery.stream_id,subquery.stream_name,subquery.file_name_pattern,subquery.landing_path,subquery.rejected_path,subquery.processing_path,subquery.archive_path FROM (
            SELECT stream_id, stream_name, file_name_pattern, landing_path, rejected_path,processing_path,archive_path, ROW_NUMBER() OVER (
            PARTITION BY stream_id ORDER BY saas_ingstn_stream_control.created_dt DESC, saas_ingstn_stream_control.updated_dt DESC) AS rn 
            FROM fusion_metadata.saas_ingstn_stream_control 
            WHERE enable_flg = True) 
            AS subquery WHERE rn = 1;
            """
            required_files_metadata :pd.DataFrame = self.sql_query_to_pandas_dataframe(engine=self.engine, query= query)
            return required_files_metadata 
        except Exception as e:
            self.logger.error(f"Failed to execute metadata_filenames_fetcher method with error -> {str(e)}",exc_info=True)
            raise
        
    def source_file_fetcher(self):
        """
        """
        try:
            self.logger.info("Executing source_file_fetcher method")
            from s3_connector import S3Connector
            s3_connector :object = S3Connector(logger=logger, profile_name=config["s3_profile_name"]).s3_client
            s3_response = s3_connector.list_objects_v2(Bucket = config["s3_bucket_name"],Prefix= self.s3_landing_prefix, Delimiter='/')
            contents = s3_response.get("Contents",None)
            if contents:
                for content in contents:
                    key :str= content.get("Key",None)
                    if key:
                        file_name = key.split("/")[-1]
                        filepattern = file_name.split("-")[0]
                        file_extension = file_name.split(".")[-1]
                        if filepattern in self.file_names_pattern:
                            file_types = {"txt": "text", "xlsx": "excel"}
                            file_type = file_types.get(file_extension, file_extension)
                            data_frame = self.s3_session.getobject_s3(bucket_name=config["s3_bucket_name"],key= key ,file_type=file_type)
                            self.column_metadata_validation(df=data_frame, file_pattern=filepattern, file_name=file_name, s3_key=key)
                    else:raise KeyError(f"Key missing in S3 response \n Contents: {contents}")
            else:raise KeyError(f"Contents missing in S3 response \n Response: {s3_response}")
        except Exception as e:
            self.logger.error(f"Failed to execute source_file_fetcher method with error -> {str(e)}",exc_info=True)
            raise

    def column_metadata_validation(self, df :pd.DataFrame, file_pattern :str, s3_key :str, file_name :str):
        """
        """
        try:
            self.logger.info(f"Executing column_metadata_validation method for file {file_name}")
            metadata :list= self.files_metadata.loc[self.files_metadata["file_name_pattern"] == file_pattern, ["stream_id","stream_name","processing_path","rejected_path","archive_path"]].values[0]
            stream_id :int = metadata[0]
            stream_name :str = metadata[1]
            processing_path :str = metadata[2]
            rejected_path :str = metadata[3]
            archival_path :str = metadata[-1]
            column_mapping_metadata_query = f"""
            SELECT subquery.src_col_nm FROM (SELECT src_col_nm, ROW_NUMBER() OVER 
            (PARTITION BY src_col_nm ORDER BY saas_ingstn_field_mapping.created_dt DESC, saas_ingstn_field_mapping.updated_dt DESC) AS rn 
            FROM fusion_metadata.saas_ingstn_field_mapping 
            WHERE enable_flg = True AND stream_id = {stream_id} AND stream_name = '{stream_name}' AND src_col_nm IS NOT NULL
            ) AS subquery WHERE rn = 1;
            """
            log_metadata_query = f"""SELECT load_status from fusion_log.saas_ingstn_log WHERE stream_id = {stream_id} AND file_name = '{file_name}' ORDER BY load_dt DESC; """
            value :pd.DataFrame = self.sql_query_to_pandas_dataframe(query=log_metadata_query ,engine=self.engine)
            entry_type = "insert" if value.empty else "update"
            if value.empty:validation_to_be_performed = True
            else:
                validation_to_be_performed = False
                previous_load_status :str = value["load_status"][0]
                if previous_load_status.lower() == "validation_success":
                    destination_path = processing_path
                    self.logger.info(f"{previous_load_status} for {file_name} in Previous run \n Moving to Processing Path - {config['s3_bucket_name']}/{destination_path}")
                elif previous_load_status.lower() == "ingestion_success":
                    destination_path = archival_path
                    self.logger.info(f"{previous_load_status} for {file_name} in Previous run \n Moving to Archival Path - {config['s3_bucket_name']}/{archival_path}")
                else:
                    validation_to_be_performed = True
                    self.logger.info(f"{previous_load_status} for {file_name} in Previous run \n Re-validating the {file_name}")
            if validation_to_be_performed:
                metadata_dataframe :pd.DataFrame = self.sql_query_to_pandas_dataframe(engine=self.engine, query= column_mapping_metadata_query)
                metadata_columns :set = set(metadata_dataframe["src_col_nm"])
                source_columns :set = {col.lower() for col in df.columns}
                validation :bool= metadata_columns == source_columns
                destination_path :str = processing_path if validation else rejected_path
                load_status = "validation_success" if validation else "validation_failed"
                self.logger.info(f"{load_status.title().replace('_',' ')} for {file_name} \n Moving to {'Processing Path' if validation else 'Rejected Path'} {config['s3_bucket_name']}/{destination_path}")
                self.log_table_updation(entry_type=entry_type, load_status=load_status, stream_id=stream_id, file_name=file_name)
            self.move_s3_files(source_bucket=config["s3_bucket_name"], destination_bucket=config["s3_bucket_name"], source_key=s3_key, destination_key=destination_path+file_name)
        except Exception as e:
            self.logger.error(f"Failed to execute column_metadata_validation method for {file_name} with error -> {str(e)}",exc_info=True)
            raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', nargs=1, help="JSON file to be processed", type=argparse.FileType('r'))
    arguments = parser.parse_args()
    config :dict = json.load(arguments.infile[0])
    utils_path=config.get("utils_path",None)
    if not utils_path:
        raise ValueError("Utils Path not provided")
    sys.path.insert(0,utils_path)
    from utils import setup_logger, send_email_notification
    log_filename = str(arguments.infile[0].name).split('/')[-1].replace('json', 'log')
    log_filename = str(log_filename.replace(".log",f"_{datetime.today().strftime('%Y_%m_%d_%H_%M_%S')}.log"))
    log_path = os.path.join(config["log_file"],log_filename)
    logger = setup_logger(log_path)
    try:
        logger.info("Validation Started")
        Validation(logger=logger)
        logger.info("Validation completed")
        send_email_notification(message=f"Validation Sucessfull \n Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name}", subject=f"POC Testing | SUCCESS | {config['environment']} | {config.get('source','api')} Validation",log_path=log_path,logger=logger,add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))
        sys.exit(0)
    except Exception as e:
        message = f"Error -> {str(e)} \n Traceback -> {traceback.format_exc()}"
        logger.error(f"Validation Failed with error -> {str(e)}",exc_info=True)
        log_file_size= os.path.getsize(log_path)
        file_size=log_file_size / (1024 * 1024)
        if file_size < 1:
            send_email_notification(message=f"Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name} \n Exception -> {e} occured \n {traceback.format_exc()}", subject=f" POC Testing | FATAL | {config['environment']} | {config.get('source','api')} Validtion",log_path=log_path,logger=logger,add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))
        else:
            send_email_notification(message=f"Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name} \n Exception -> {e} occured \n Log File couldn't be attached with this mail due to file size limit being exceeded, Log Path-> {log_path} \n {traceback.format_exc()}", subject=f"POC Testing | FATAL | {config['environment']} | {config.get('source','api')} Validation",logger=logger,add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))
        sys.exit(1)