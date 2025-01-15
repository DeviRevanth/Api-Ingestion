######################################################
#Name: Kola Devi Revanth
#Date: 29-07-2024
#Version : 2.0
#Version Comments: Enhanced logging and alerts
#Objective: Box to S3 ingestion
#userstory:
########################################################

### Example Config File ###

"""{

    "source":"Source name for email alert",
    "environment":"Environment of the script location for email alert ",
    "box_config_path":"Path where box credentials are stored"
    "box_id":"provide the box source id",
    "s3_profile":"s3 profile",
    "log_file":"Log folder path no need to mention file name",
    "utils_path":"path where utils py file is placed to import functions in t",
    "s3_partition" : "is partiotion required or not y or n",
    "s3_prefix" : "need to provide the s3 prefix",
    "file_name": " need to provid the file name"

    }"""

#### Importing Necessary Packages ####
import sys
from datetime import datetime
import os
from io import BytesIO
from boxsdk import JWTAuth, Client
import traceback
import boto3
import argparse
import json

def box_access(JWT_file_path):
    """
    A method to authenticate with the box

    Parameter:
    JWT_file_path (str) : Box config path

    Returns:
    client (Object)     : Client object to access box files
    """
    try:
        logger.info("Executing box_access method")
        auth = JWTAuth.from_settings_file(JWT_file_path)
        client = Client(auth)
        logger.info("Box authentication has been established successfully")
        return client
    except Exception as e:
        logger.error(f"Failed to execute box_access method with error --> {e} {traceback.format_exc()}")
        raise

def box_s3(client, folder_id):
    """
    A method to download files from box and upload files into s3

    Parameter:
    client (object) : Client object which will be used to access box folder
    folder_id (str) : box folder id

    Returns:None
    """
    try:
        logger.info("Executing box_s3 method")
        for item in client.folder(folder_id).get_items():
            if item.type == "file" and item.name == config["file_name"]:
                buffer = BytesIO()
                client.file(item.id).download_to(buffer)
                buffer.seek(0)
                if config["s3_partition"].lower() == "y":
                    obj = config["s3_prefix"] + f"/year={year}/month={month}/day={day}/" + config["file_name"]
                else:
                    obj = config["s3_prefix"] +"/"+ config["file_name"]
                session = boto3.Session(profile_name=config["s3_profile"])
                s3 = session.client('s3')
                s3.upload_fileobj(buffer,config["bucket_name"],obj)
                logger.info(f"{config['file_name']} uploaded to S3 successfully")
                break  # Exit loop after finding the file
        else:
            logger.warning(f"File '{config['file_name']}' not found in Box folder with ID '{folder_id}'")
    except Exception as e:
        logger.error(f"Failed to execute box_s3 method with error --> {e} {traceback.format_exc()}")
        raise

def main():
    """
    A method to call other methods accordingly to proceed with ingestion
    """
    try:
        logger.info("Executing main method")
        client = box_access(config["box_config_path"])
        box_s3(client, config["box_id"])
        logger.info("Ingestion Completed")
        send_email_notification(message=f"Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name} \n Log Path-> {log_path} \n Ingestion Successful", subject=f"SUCCESS | {config['environment']} | {config.get('source','api')} Ingestion | Box ID - {config['box_id']} | {config['bucket_name']}/{config['s3_prefix']} | {config['s3_profile']}",log_path=log_path,logger=logger,add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))
    except Exception as e:
        logger.error(f"Failed to execute main method with error --> {e} {traceback.format_exc()}")
        raise

if __name__ == "__main__":
    start = datetime.today().strftime("%Y-%m-%d_%H:%M:%S")
    year, month, day = datetime.today().strftime("%Y"), datetime.today().strftime("%m"), datetime.today().strftime("%d")
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', nargs=1, help="JSON file to be processed", type=argparse.FileType('r'))
    arguments = parser.parse_args()
    config = json.load(arguments.infile[0])
    parent_path = os.path.dirname(os.path.abspath(__file__))
    utils_path=config["utils_path"]
    if 'utils' not in utils_path:utils_path=os.path.join(utils_path,"utils/")
    sys.path.append(utils_path)
    from utils import setup_logger, send_email_notification
    log_filename = str(arguments.infile[0].name).split('/')[-1].replace('json', 'log')
    log_filename=str(log_filename.replace(".log",f"_{datetime.today().strftime('%Y_%m_%d_%H_%M_%S')}.log"))
    log_path=os.path.join(config["log_file"], log_filename)
    logger = setup_logger(log_path)
    try:
        logger.info("Ingestion Started")
        sys.exit(main())
    except Exception as e:
        logger.error(f"Exception occurred --> {e} {traceback.format_exc()}")
        log_file_size= os.path.getsize(log_path)
        file_size=log_file_size / (1024 * 1024)
        if file_size < 1:
            send_email_notification(message=f"Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name} \n Log Path-> {log_path} \n Exception -> {e} occured \n {traceback.format_exc()}", subject=f"FATAL | {config['environment']} | {config.get('source','api')} Ingestion | BOX ID - {config['box_id']} | {config['bucket_name']}/{config['s3_prefix']} | {config['s3_profile']}",log_path=log_path,logger=logger,add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))
        else:
            send_email_notification(message=f"Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name} \n Exception -> {e} occured \n Log File couldn't be attached with this mail due to file size limit being exceeded, Log Path-> {log_path} \n {traceback.format_exc()}", subject=f"FATAL | {config['environment']} | {config.get('source','api')} Ingestion | BOX ID - {config['box_id']} | {config['bucket_name']}/{config['s3_prefix']} |{config['s3_profile']}",logger=logger,add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))
