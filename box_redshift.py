#Developer Name: Kola Devi Revanth
#Date: 18-12-2024
#Version : 2.0
#Version Comments: Initial Version
#Objective: Box Module to handle CSV and Excel single and multiple files to one table only if column structure is same with enhanced logging and archival along with touch file creation
#userstory:
########################################################

#### Importing Necessary Packages ####
import sys,os
from datetime import datetime
from io import BytesIO
from boxsdk import JWTAuth, Client
import pandas as pd
import argparse
import json
import traceback

### Sample Config File ###
{
    "box_id":"Box id can be mutiple or single if multiple separate them by commas eg- 1,2,3",
    "source":"source name",
    "environment":"dev / Prod",
    "box_config_path":"Box config path",
    "file_names":"File name that has to be picked for ingestion if multiple files can be mentioned seperated by commas eg - test.csv,test2.csv",
    "ingestion_audit_field":"Name of the field that will be added in table as ingestion audit",
    "log_file_path":"log path where log file has to be created",
    "schema_name":"Target Schema name",
    "main_table":"Target Table name",
    "file_type":"csv / excel / text has to be mentioned",
    "stage_table":"Target Stage table name keep it empty if not required but it has be present in config",
    "data_origin":"Name of the column which will store source box file id as value in Table",
    "posting_agent":"Name of the column which will store source box file name as value in Table",
    "required_excel_features":"any extra attributes need to read_excel can be added here as a dict eg - {'method':'value'}",
    "required_csv_features":"any extra attributes need to read_csv can be added here as a dict eg - {'method':'value'}",
    "load_type":"incremental/truncate_and_load/fullload",
    "s3_touch_file_name":"Name of the touch file to be created in S3",
    "touch_file_type":"File type",
    "touch_file_s3_profile":"S3 profile where touch file has to be created",
    "touch_file_partition":"partition required in S3 prefix or not",
    "touch_file_s3_bucket_name":"S3 bucket name where touch file has to be created",
    "touch_file_s3_prefix_name":"S3 prefix where touch has to be created",
    "redshift_config":"Path to redshift configuration",
    "add_on_email_stake_holders":"Only to be added when extra email stake holders had to be included or else neglect this key value",
    "utils_path":"Path to the utility py file which has logging and alert feature on failure compulsory input in config",
    "redshift_profile":"Redshift profile ",
    "archive_folder":"This has to only be added in config is archival is required or else this field can be removed completely",
    "primary_key":"Only need for incremental should be kept as empty if not required config expects this parameter"
}


class DataFetcher:
    # A class to fetch files from Box API and ingest them to S3 or Redshift depending on the inputs provided
    def __init__(self, config, logger) -> None:
        """
        The Constructor for DataFetcher class.

        Parameters:
        config (dict): Configuration dictionary
        logger (Logger): Logger object
        engine (object): Redshift connection engine
        """
        self.config = config
        self.logger = logger

    def box_access(self, JWT_file_path):
        """
        A method to enable authentication to box api

        Parameters:
        JWT_file_path (str) : File path of the box config file sotred

        Returns:
        client (object) : Client object after sucessfull authentication
        """
        try:
            auth = JWTAuth.from_settings_file(JWT_file_path)
            client = Client(auth)
            self.logger.info("Box authentication has been established successfully")
            return client
        except Exception as e:
            self.logger.error(f"Failed to execute box_access method with error --> {e} {traceback.format_exc()}")
            raise

    def get_excel_box(self, client, folder_id):
        """
        A method to read excel file from box location and return the data frame it can handle multiple sheets or multiple files given that column structure is similar across all files to single table

        Parameters:
        client (object) : Client object initiated for box authentication
        folder_id (str) : Box folder id

        Returns:
        df (DataFrame) : DataFrame which holds data fetched from box location
        items (list)   : Box file id or ids if multiple files are provided
        """
        try:
            df = pd.DataFrame()
            items=[]
            for item in client.folder(folder_id).get_items():
                if item.type == "file" and item.name in self.config["file_names"].split(','):
                    box_file_name = client.file(item.id).get().name
                    self.logger.info(f"{box_file_name} has been downloaded from Box")
                    buffer = BytesIO()
                    client.file(item.id).download_to(buffer)
                    buffer.seek(0)
                    if self.config["required_excel_features"]:
                        if self.config["sheet_names"]:
                            for sheet in self.config["sheet_names"].split(','):
                                main_df = pd.read_excel(io=BytesIO(buffer.read()),sheet_name=sheet,**self.config["required_excel_features"])
                        else:main_df=pd.read_excel(io=BytesIO(buffer.read()),**self.config["required_excel_features"])
                    else:main_df=pd.read_excel(io=BytesIO(buffer.read()))
                    main_df["data_origin"]=item.id
                    main_df["posting_agent"]=item.name
                    df = pd.concat([df, main_df])
                    items.append(item)
            return df,items
        except Exception as e:
            self.logger.error(f"Failed to execute get_excel_box method with error --> {e} {traceback.format_exc()}")
            raise

    def get_csv_box(self, client, folder_id):
        """
        A method to read csv file from box location and return the data frame it can handle multiple files given that column structure is similar across all files to single table

        Parameters:
        client (object) : Client object initiated for box authentication
        folder_id (str) : Box folder id

        Returns:
        df (DataFrame) : DataFrame which holds data fetched from box location
        items (list)   : Box file id or ids if multiple files are provided
        """
        try:
            df = pd.DataFrame()
            items=[]
            for item in client.folder(folder_id).get_items():
                if item.type == "file" and item.name in self.config["file_names"].split(','):
                    box_file_name = client.file(item.id).get().name
                    self.logger.info(f"{box_file_name} has been downloaded from Box")
                    buffer = BytesIO()
                    client.file(item.id).download_to(buffer)
                    buffer.seek(0)
                    if "required_csv_features" in self.config:main_df = pd.read_csv(filepath_or_buffer=BytesIO(buffer.read()),**self.config["required_csv_features"])
                    else:main_df = pd.read_csv(filepath_or_buffer=BytesIO(buffer.read()))
                    main_df[self.config["data_origin"]]=item.id
                    main_df[self.config["posting_agent"]]=item.name
                    df=pd.concat([df,main_df])
                    items.append(item)
            return df,items
        except Exception as e:
            self.logger.error(f"Failed to execute get_csv_box method with error --> {e} {traceback.format_exc()}")
            raise

    def move_to_archive(self, client, item_list):
        """
        A method archive folders from one box location to other it can handle multiple files to only one single box location

        Parameters:
        client (object)  : Box authentication object
        item_list (list) : list of box file id or in case multiple files ids that has to be archieved

        Returns: None
        """
        for item in item_list:
            try:
                split_name = item.name.split('.')
                archival_name=f"{split_name[0]}_{day}-{month}-{year}.{split_name[-1]}"
                file_to_move = client.file(item.id).rename(archival_name)
                destination_folder_id = self.config["archive_folder"]
                destination_folder = client.folder(destination_folder_id)
                file_to_move.move(parent_folder=destination_folder)
                self.logger.info(f"file {file_to_move} moved to archive location")
            except Exception as e:
                self.logger.error(f"Failed to archive the fail with error --> {e} {traceback.format_exc()}")
                raise

    def upload_touch_file(self):
        """
        A method create a touch file in s3 location based on provided inputs

        Parameters:None

        Returns: None
        """
        try:
            self.logger.info("Executing upload_touch_file method to create a touch file in provided S3 location")
            from s3_operations import S3Operations
            file_name=config["s3_touch_file_name"]
            file_name=str(file_name).replace(".csv",f"-{datetime.now().strftime('%Y_%m_%d-%H_%M')}.csv")
            s3=S3Operations(logger=logger, profile_name=config["touch_file_s3_profile"], partition=config["touch_file_partition"])
            s3.upload_file(file_name=file_name, file_type=config["touch_file_type"], data=pd.DataFrame(), bucket=config["touch_file_s3_bucket_name"], prefix=config["touch_file_s3_prefix_name"])
            logger.info(f"Touch File created - {config['touch_file_s3_bucket_name']}/{config['touch_file_s3_prefix_name']}{file_name}")
        except Exception as e:
            logger.error(f"Failed to execute upload_touch_file method, error --> {e} {traceback.format_exc()}")
            raise

    def main(self):
        """
        A method to initiate Box ingestion based on the inputs provided in config

        Parameters: None

        Returns: None
        """
        try:
            client = self.box_access(self.config["box_config_path"])
            if self.config["file_type"].lower()=='csv' or self.config["file_type"].lower()=='text':
                df,items = self.get_csv_box(client, self.config["box_id"])
            elif self.config["file_type"].lower()=='excel':
                df,items = self.get_excel_box(client, self.config["box_id"])
            if not df.empty:
                if self.config["replace_space_in_column_name"].lower()=='y':
                    df.columns = pd.Series(df.columns).replace(' ', '_', regex=True).str.lower()
                else:
                    df.columns = pd.Series(df.columns).str.lower()
                df[self.config["ingestion_audit_field"]] = datetime.today()
                if "schema_name" and "main_table" in self.config:
                    Database(load_type=self.config["load_type"],logger=logger,config=self.config["redshift_config"],profile=self.config["redshift_profile"],data=df,schema=self.config["schema_name"],main_table_name=self.config["main_table"],stage_table_name=self.config["stage_table"],primary_key=self.config["primary_key"])
                if "archive_folder" in self.config:
                    self.move_to_archive(client,items)
                if "s3_touch_file_name" and "touch_file_s3_bucket_name" in self.config:
                    self.upload_touch_file()
                self.logger.info("Ingestion Completed")
                send_email_notification(message=f"Ingestion Sucessfull \n Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name}", subject=f"INFO - SUCCESS | {config['environment']} | {config.get('source','api')} Ingestion | BOX ID - {config['box_id']} | {config['schema_name']}.{config['main_table']} {config['redshift_profile']}",log_path=log_file,logger=logger,add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))
            else:self.logger.info("No data to ingest")
        except Exception as e:
            self.logger.error(f"Failed to execute main method in DataFetcher class , error --> {e} {traceback.format_exc()}")
            raise

if __name__ == "__main__":
    start = datetime.today().strftime("%Y-%m-%d_%H:%M:%S")
    year, month, day = datetime.today().strftime("%Y"), datetime.today().strftime("%m"), datetime.today().strftime("%d")
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', nargs=1, help="JSON file to be processed", type=argparse.FileType('r'))
    arguments = parser.parse_args()
    config = json.load(arguments.infile[0])
    parent_path = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0,config["utils_path"])
    from utils import setup_logger, send_email_notification
    from redshift_loader import Database
    log_filename = str(arguments.infile[0].name).split('/')[-1].replace('json', 'log')
    log_filename=str(log_filename.replace(".log",f"_{datetime.today().strftime('%Y_%m_%d_%H_%M_%S')}.log"))
    log_file=os.path.join(config["log_file_path"], log_filename)
    logger = setup_logger(log_file)
    logger.info("Ingestion Started")
    try:
        data_fetcher = DataFetcher(config, logger)
        sys.exit(data_fetcher.main())
    except Exception as e:
        logger.error(f"Exception occurred: {e} {traceback.format_exc()}")
        log_file_size= os.path.getsize(log_file)
        file_size=log_file_size / (1024 * 1024)
        if file_size < 1:
            send_email_notification(message=f"Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name} \n Exception -> {e} occured \n {traceback.format_exc()}", subject=f"FATAL | {config['environment']} | {config.get('source','api')} Ingestion | Box ID - {config['box_id']} | {config['schema_name']}.{config['main_table']} {config['redshift_profile']}",log_path=log_file,logger=logger,add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))
        else:
            send_email_notification(message=f"Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name} \n Exception -> {e} occured \n Log File couldn't be attached with this mail due to file size limit being exceeded, Log Path-> {log_file} \n {traceback.format_exc()}", subject=f"FATAL | {config['environment']} | {config.get('source','api')} Ingestion | Box ID - {config['box_id']} | {config['schema_name']}.{config['main_table']} {config['redshift_profile']}",logger=logger,add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))
