#######################################################
#Name: Kola Devi Revanth
#Date: 03-04-2025
#Version: 1.0
#Version Comments: Initial Version
#Objective: Performs Validation aganist Files obtained from S3 with the metadata table available in Database and proceed accordingly as per validation status
#userstory:
########################################################

#### Example Config File ####
"""
{
    "environment":"Environment of Server script being hosted in",
    "source":"Source Name",
    "db_config_path":"Path to where database connection details are saved",
    "db_profile":"database profile name",
    "metadata_fetcher_sql_path":"Path to Metadata Fetcher SQL File",
    "column_mapping_sql_path":"Path to Column Mapping Data Fetcher SQL File",
    "log_metadata_sql_path":"Path to Log Table Data Fetcher SQL File",
    "log_insert_sql_path":"Path to Insert Query SQL File",
    "s3_bucket_name":"S3 Bucket Name",
    "s3_profile_name":"S3 Profile Name",
    "utils_path":"Path to utility folder where modules are deployed",
    "log_file":"Path to Log file folder where this script execution will be logged and saved",
    "add_on_email_stake_holders":"Additional email recipients to receive Success/Failure email along with pre-defined recipients"
}
"""


#### Importing Required Packages ####
import pandas as pd
import argparse
from datetime import datetime
import json
import traceback
import sys,os

class Validation() :
    """
    A class to perform data validation, log updates, and file movement for a data pipeline.

    This class is designed to validate the data files fetched from an S3 bucket against the metadata stored in a database.

    Retry mechanism is enabled for **S3** and **Database** connection methods to handle transient errors.

    The class performs the following operations:
        - Establishes an S3 connection.
        - Establishes connection the database.
        - Fetches metadata from the database.
        - Fetches files matching with the pattern in metadata table from the S3 landing directory.
        - Extracts the data as `pd.DataFrame` from the file to proceed with Validation
        - Fetches log table data for the files fetched from S3 to check previous run status if any.
        - Column Metadata Validation is performed to check if the columns in the file match the metadata columns.
        - Logs the validation status and error codes with error description in the log table with respective file names.
        - Moves files to the processing or rejected path based on the validation status.

    **_Note_**
    - This class makes use of **_S3Operations_** and **_S3Connector_** classes imported from `s3_operations` and `s3_connector` Modules respectively.
    - This class makes use of **_get_connection_** method imported from `redshift_connector` Module.
    - This class makes use of **_setup_logger_** and **_send_email_notification_** functions imported from `utils` Module.
    - This class executes queries on database using only `.sql` files provide in config.
    """

    def __init__(self, logger ,config) -> None:
        """
        Initializes the Validation class by setting up logging, connecting to S3 and the database,
        and fetching metadata.

        Based on the necessary config provided in the `JSON` file, this class will perform the following operations:

        Parameters:
            logger (object) : Logger instance for logging messages.
            config (dict)   : Configuration dictionary containing necessary parameters.

        Raises:
            Exception : If S3 or database connection fails.
        """
        try:
            self.logger = logger
            self.config = config
            self.s3_profile_name = self.config["s3_profile_name"]
            self.s3_bucket_name = self.config["s3_bucket_name"]
            self.adjust_logging_levels(logger=self.logger)
            from s3_operations import S3Operations # Importing S3Operations class from s3_operations.py
            self.s3_session :object = S3Operations(logger=self.logger, profile_name=self.s3_profile_name ,partition="N")
            self.logger.info("S3 Connection Successful")
        except Exception as e :
            self.logger.error(f"S3 Connection Failed with error -> {str(e)}", exc_info=True)
            raise
        try:
            from redshift_connector import get_connection
            self.engine :object= get_connection(filepath=self.config["db_config_path"], profile=self.config["db_profile"], logger=self.logger)
            self.files_metadata :pd.DataFrame  = sql_query_executor(query=sql_file_reader(logger=self.logger, sql_file_path=self.config["metadata_fetcher"]).format(metadata_stream_control_table=config["metadata_stream_control_table"]), logger=self.logger, engine=self.engine)
            self.logger.info("Metadata Details fetched successfully")
            rounded_minutes = "00" if datetime.now().minute <= 30 else "30"
            self.batch_id = datetime.now().strftime(f"%Y%m%d%H{rounded_minutes}")
            self.s3_landing_prefix :str= self.files_metadata["landing_path"].unique()[0]
            self.file_names_pattern :list = self.files_metadata["file_name_pattern"].tolist()
            self.source_file_fetcher()
        except Exception as e:
            self.logger.error(f"Database Connection Failed with error -> {str(e)}", exc_info=True)
            raise

    @staticmethod
    def adjust_logging_levels(logger :object) -> None:
        """
        This is a static method that adjusts the logging levels to suppress verbose logs from boto3 and related libraries.

        Parameters:
            logger (object): Logger instance.
        """
        import logging
        logging.getLogger('boto3').setLevel(logging.WARNING)
        logging.getLogger('botocore').setLevel(logging.WARNING)
        logging.getLogger('s3transfer').setLevel(logging.WARNING)
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logger.info("Logger setup completed. Debug logs from boto3 are suppressed.")

    def move_s3_files(self, source_bucket :str, destination_bucket :str, source_key :str, destination_key :str) -> None:
        """
        Moves files from one S3 bucket to another.

        This Method will first copy the file from **Source Path** to **Destination Path** and file will be **_DELETED_** in **Source Path**

        **_Note_**
        - This method makes use of **_s3_to_s3_move_** method imported from `S3_Operations` Module.

        Parameters:
            source_bucket (str)      : Source S3 bucket name.
            destination_bucket (str) : Destination S3 bucket name.
            source_key (str)         : File key in the source bucket.
            destination_key (str)    : File key in the destination bucket.

        Raises:
            Exception : If the file move operation fails.
        """
        try:
            self.logger.info("Executed move_s3_files method")
            self.logger.info(f"sourcebucket:{source_bucket} \n destinationbucket:{destination_bucket} \n sourcekey:{source_key} \n destinationkey:{destination_key}")
            self.s3_session.s3_to_s3_move(source_bucket=source_bucket, dest_bucket=destination_bucket, source_key=source_key, dest_key=destination_key)
            self.logger.info("File Moved Successfully")
        except Exception as e:
            self.logger.error(f"Failed to execute move_s3_files method with error -> {str(e)}",exc_info=True)
            raise

    def source_file_fetcher(self) -> None:
        """
        Fetches files from S3 landing directory, matches file patterns, and triggers validation.

        Raises:
            KeyError  : If S3 reponse is missing with any required key values
            Exception : If any issue occurs while fetching files from S3.
        """
        try:
            self.logger.info("Executing source_file_fetcher method")
            matched_files = []
            from s3_connector import S3Connector # Importing S3Connector class from s3_connector.py
            s3_connector :object = S3Connector(logger=logger, profile_name=self.s3_profile_name).s3_client
            s3_response = s3_connector.list_objects_v2(Bucket=self.s3_bucket_name, Prefix= self.s3_landing_prefix, Delimiter='/')
            contents = s3_response.get("Contents",None)
            if contents:
                for content in contents:
                    key :str= content.get("Key",None)
                    if key:
                        file_name = key.split("/")[-1]
                        filepattern = file_name.split("-")[0]
                        file_extension = file_name.split(".")[-1]
                        if filepattern in self.file_names_pattern:
                            matched_files.append(filepattern)
                            file_types = {"txt": "text", "xlsx": "excel"}
                            file_type = file_types.get(file_extension, file_extension) # Defaults to file_extension if not found in dictionary: file_types
                            data_frame = self.s3_session.getobject_s3(bucket_name=self.s3_bucket_name,key= key ,file_type=file_type)
                            self.column_metadata_validation(df=data_frame, file_pattern=filepattern, file_name=file_name, s3_key=key)
                    else:
                        self.logger.warning(f"No Files found in s3://{self.s3_bucket_name}/{self.s3_landing_prefix}")
            else:
                raise KeyError(f"Contents missing in S3 response \n Response: {s3_response}")
            no_files_processed = set(self.file_names_pattern) - set(matched_files)
            if no_files_processed:
                log_file_size= os.path.getsize(log_path)
                file_size = log_file_size / (1024 * 1024)
                if set(no_files_processed) == set(self.file_names_pattern):
                    alert_message = f"No Files Found in S3 Landing Directory -> s3://{self.s3_bucket_name}/{self.s3_landing_prefix} \n Please check the S3 Landing Directory / Metadata Table  \n Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name} \n Log Path-> {log_path}"
                else:
                    alert_message = f"Any Files with below pattern are not found in S3 Landing Directory -> s3://{self.s3_bucket_name}/{self.s3_landing_prefix} \n Pattern - > {','.join(map(str,no_files_processed))} \n Please check the S3 Landing Directory \n Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name} \n Log Path-> {log_path}"
                self.logger.warning(alert_message)
                send_email_notification(message=alert_message, subject=f"{source} | Validation Script | WARNING | {environment}",log_path=log_path if file_size <= 1 else None,logger=self.logger,add_on_email_stake_holders=self.config.get("add_on_email_stake_holders", None))
                if set(no_files_processed) == set(self.file_names_pattern):
                    self.logger.warning("Execution exited as no Files found in Landing Directory")
                    sys.exit(0) # Exits with error code 0 (Success)
        except Exception as e:
            self.logger.error(f"Failed to execute source_file_fetcher method with error -> {str(e)}",exc_info=True)
            raise

    def column_metadata_validation(self, df :pd.DataFrame, file_pattern :str, s3_key :str, file_name :str) -> None:
        """
        Validates column names of the file against the metadata table mapped column names and moves files accordingly.

        This method performs the following operations:
        - Fetches metadata from the metadata table based on the file pattern.
        - Fetches log table data for the file to check the previous run status.
        - If previous run exists for the file:
            - If previous run status is **_Validation_Success_** / **_Ingestion_Success_**, file will be moved to **rejected path** without performing validation.
            - If previous run status is **_Validation_Failed_** / **__Ingestion_Failed__**, file will be re-validated.
        - Column Metadata Validation is performed to check if the columns in the file match the metadata columns.
            - If validation **_fails_**, file is moved to **rejected path**.
            - If validation **_passes_**, file is moved to **processing path**.
        - Above process is same for new files for which no previous run exists.
        - Logs the validation status and error codes with error description in the log table with respective file names.

        Parameters:
            df (pd.DataFrame)  : DataFrame containing the file data.
            file_pattern (str) : Pattern of the file name.
            s3_key (str)       : S3 file key.
            file_name (str)    : Name of the file.

        Raises:
            Exception : If validation fails or metadata fetching fails.
        """
        try:
            self.logger.info(f"Executing column_metadata_validation method for file {file_name}")
            if self.files_metadata.loc[self.files_metadata["file_name_pattern"] == file_pattern].empty:
                raise KeyError(f"File Pattern {file_pattern} not found in Metadata Table")
            required_columns = ["stream_id", "stream_name", "processing_path", "rejected_path", "channel_name", "archive_path"]
            if not all(column in self.files_metadata.columns for column in required_columns):
                raise KeyError("One or more required columns are missing in the metadata")
            metadata :list = self.files_metadata.loc[self.files_metadata["file_name_pattern"] == file_pattern, required_columns].values[0] #
            stream_id :int = metadata[0]
            stream_name :str = metadata[1]
            processing_path :str = metadata[2]
            rejected_path :str = metadata[3]
            channel_name :str = metadata[4]
            archival_path :str = metadata[-1]
            error_code = "NULL"
            error_description = "NULL"
            if df.empty or df.isna().all().all():
                source_count = 0
                destination_path = archival_path
                load_status = "empty_file"
                self.logger.info(f"Empty File -> {file_name} \n Moving to Rejected Path - {self.s3_bucket_name}/{destination_path}")
            else:
                source_count = df.shape[0]
                column_mapping_metadata_query = sql_file_reader(logger=self.logger, sql_file_path=self.config["column_mapping_fetcher"]).format(stream_id=stream_id, stream_name=stream_name,field_mapping_table=config["field_mapping_table"])
                log_metadata_query = sql_file_reader(logger=self.logger, sql_file_path=self.config["log_load_status_fetcher"]).format(stream_id=stream_id, file_name=file_name,log_table=config["log_table"])
                value :pd.DataFrame = sql_query_executor(query=log_metadata_query, logger=self.logger, engine=self.engine)
                validation_to_be_performed = True if value.empty else False
                if not value.empty:
                    previous_load_status :str = value["load_status"][0]
                    if previous_load_status.lower() in ["validation_success","ingestion_success"]:
                        load_status = "File already processed in previous run"
                        self.logger.info(f"{previous_load_status.title().replace('_',' ')} for {file_name} in Previous run \n Moving to Rejected Path - {self.s3_bucket_name}/{destination_path}")
                    else:
                        validation_to_be_performed = True
                        self.logger.info(f"{previous_load_status.title().replace('_',' ')} for {file_name} in Previous run \n Re-validating the {file_name}")
                if validation_to_be_performed:
                    metadata_dataframe :pd.DataFrame = sql_query_executor(query= column_mapping_metadata_query, logger=self.logger, engine=self.engine)
                    metadata_columns :set = set(metadata_dataframe["src_col_nm"])
                    source_columns :set = {col.lower() for col in df.columns}
                    validation :bool= metadata_columns == source_columns
                    file_mismatch, metadata_mismatch = metadata_columns - source_columns, source_columns - metadata_columns
                    if not validation:
                        if file_mismatch:
                            error_code = "Column_Mismatch_with_File"
                            error_description = f"{','.join(map(str,file_mismatch))} are not found in {file_name}"
                        elif metadata_mismatch:
                            error_code = "Column_Mismatch_with_Source_Metada"
                            error_description = f"New Columns - {','.join(map(str,metadata_mismatch))} are found in {file_name} which are not present in Column Mapping Metadata Table"
                destination_path :str = processing_path if validation else rejected_path
                load_status = "validation_success" if validation else "validation_failed"
                self.logger.info(f"{load_status.title().replace('_',' ')} for {file_name} \n Moving to {'Processing Path' if validation else 'Rejected Path'} {self.s3_bucket_name}/{destination_path}")
            query = sql_file_reader(logger=self.logger, sql_file_path=self.config["log_insert_query"]).format(
                log_table = config["log_table"],
                stream_id = stream_id,
                file_name = f"'{file_name}'" if file_name is not None else "NULL",
                load_status = f"'{load_status}'" if load_status is not None else "NULL",
                error_code = "NULL" if error_code == "NULL" else f"'{error_code}'",
                error_description = "NULL" if error_description == "NULL" else f"'{error_description}'",
                batch_id = self.batch_id,
                channel_name = f"'{channel_name}'",
                source_count = source_count
                )
            sql_query_executor(engine=self.engine, logger=self.logger, query=query)
            self.move_s3_files(source_bucket=self.s3_bucket_name, destination_bucket=self.s3_bucket_name, source_key=s3_key, destination_key=destination_path+file_name)
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
    from sql_file_reader import sql_file_reader
    from sql_query_executor import sql_query_executor
    source = config.get("source",None)
    environment = config.get("environment",None)
    if not environment:
        raise KeyError("environment Keyword is missing in Config File")
    if not source:
        raise KeyError("source Keyword is missing in Config File")
    log_filename = str(arguments.infile[0].name).split('/')[-1].replace('json', 'log')
    log_filename = str(log_filename.replace(".log",f"_{datetime.today().strftime('%Y_%m_%d_%H_%M_%S')}.log")) # Appending timestamp to log file name
    log_path = os.path.join(config["log_file"],log_filename)
    logger = setup_logger(log_path)
    try:
        logger.info("Execution Started")
        Validation(logger=logger, config=config)
        logger.info("Execution completed")
        send_email_notification(message=f"Execution Sucessfull \n Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name}", subject=f"{source} | Validation Script | SUCCESS | {environment}",log_path=log_path,logger=logger,add_on_email_stake_holders=config.get("add_on_email_stake_holders", None)) # Sends Success Email Notification
        sys.exit(0)  # Exits with error code 0 (Success)
    except Exception as e:
        logger.error(f"Validation Failed with error -> {str(e)}",exc_info=True)
        log_file_size= os.path.getsize(log_path)
        file_size = log_file_size / (1024 * 1024)
        log_message = f"\n Log File couldn't be attached with this mail due to file size limit being exceeded, Log Path-> {log_path}" if file_size > 1 else ""
        message = f"Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name} \n Exception -> {e} occured {'' if file_size < 1 else log_message}\n {traceback.format_exc()}"
        logpath = log_path if file_size <= 1 else None
        send_email_notification(message=message, subject=f"{source} | Validation Script | FATAL | {environment}",log_path=logpath,logger=logger,add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))
        sys.exit(1)  # Exits with error code 1 (Failure) if an exception occurs
