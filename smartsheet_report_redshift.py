#######################################################
#Name: Kola Devi Revanth
#Date: 29-07-2024
#Version : 1.0
#Version Comments: Initial Version
#Objective: Smarsheet reports to Redshift truncate and load
#userstory:
########################################################

### Example Config File ###
"""{
    "source":"Source name for email alerts",
    "environment":"Environment of the script location for email alert",
    "sheet_id":"SmartSheet ID If multiple sheets to be ingested to single table then provide all as comma seperated values",
    "url":"API Url to access SmartSheet",
    "auth_token":"Token to access api url",
    "ingestion_audit_field":"Name of the audit column that will be added in redshift table",
    "redshift_config":"Path where redshift credentials are stored",
    "redshift_profile":"Redshift profile",
    "log_file":"Log folder path no need to mention file name",
    "replace_space_in_column_name":"If spaces in column names need to replaced or not provide y/n",
    "schema_name":"Schema Name",
    "data_origin":"Provide y/n based on need. If not provided by default it will create a column with name data_orgin(this is only to fix old ingestions should not be used for new developments).",
    "table_name / main_table":"Provide any one key and input as table name",
    "load_type":"If not provided in config by default it will be truncate_and_load",
    "posting_agent":"If posting agent is required provide the input values that will be stored in posting agent column",
    "utils_path":"path where utils py file is placed to import functions in t"
}"""

########### Importing Packages ###############
import requests,json
import pandas as pd
import argparse
import os,sys
from datetime import datetime
import traceback

def auth(id: str, token: str, page: int = 1) -> requests.Response:
    """
    Fetches paginated response from Smartsheet API.

    Parameters:
    id (str): Smartsheet ID to fetch data from.
    token (str): Authorization token for API access.
    page (int): Page number to fetch.

    Returns:
    response (requests.Response): The response object from the API for the given page.
    """
    try:
        logger.info("Executing auth method")
        headers = {"Authorization": f"Bearer {token}"}
        url = f"{config['url']}{id}?pageSize=500&page={page}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            logger.info(f"Page {page} fetched successfully.")
            return response
        else:
            logger.info("Authentication Failed")
            error_message = response.json().get("message", "No message provided")
            raise Exception(f"Authentication failed. Status code: {response.status_code}. Message: {error_message}")
    except Exception as e:
        logger.error(f"Failed to execute auth method, error --> {e} {traceback.format_exc()}")
        raise

def data(response):
    """
    A method to gather data from the response received by hitting with column names and data

    Parameter:
    response (str) : Response received by hitting API

    Returns:
    records (List) : List of values as column:Value
    """
    try:
        logger.info("Executing data method")
        data_cols = response.json()['columns']
        data_rows = response.json()['rows']
        value = config.get("smartsheet_parsing_value","value")
        empty_value=config.get("empty_value",'')
        columns={}
        for item in data_cols:
            columns.update({item['virtualId']:item['title']})
        records=[]
        for row in data_rows:
            record={}
            for cell in row['cells']:
                if value in cell:
                    record.update({columns[cell['virtualColumnId']]:cell[value]})
                else:
                    record.update({columns[cell['virtualColumnId']]:empty_value})
            records.append(record)
        logger.info("Records Fetched")
        return records
    except Exception as e:
        logger.error(f"Failed to execute data method in data method , error --> {e} {traceback.format_exc()}")
        raise

def dataframe_to_redshift(df):
    """
    A method to gather data from the response received by hitting with column names and data

    Parameter:
    df (DataFrame)  : Response received by hitting API

    Returns:None
    """
    try:
        logger.info("Executing dataframe_to_redshift method")
        replace_space_in_column_name=config.get("replace_space_in_column_name","y")
        if replace_space_in_column_name.lower()=='y':
            df.columns=pd.Series(df.columns).replace(' ','_',regex=True).str.lower()
        else:df.columns=pd.Series(df.columns).str.lower()
        posting_agent=config.get('posting_agent',None)
        if posting_agent:
            df["posting_agent"]=config["posting_agent"]
        drop_null_values=config.get("drop_null_values","n")
        if drop_null_values.lower()=='y':
            df=df.dropna(how='all')
        Database(load_type=config.get("load_type","truncate_and_load"),logger=logger,config=config["redshift_config"],profile=config["redshift_profile"],data=df,schema=config["schema_name"],main_table_name=table_name,stage_table_name=config.get("stage_table",None),primary_key=config.get("primary_key",None))
        if "touch_file_name" in config:
            pass
    except Exception as e:
        logger.error(f"Failed to execute main method in dataframe_to_redshift method , error --> {e} {traceback.format_exc()}")
        raise

def main():
    """
    Calls other functions to execute ingestion steps with pagination.

    Returns: None
    """
    try:
        logger.info("Executing main method")
        main_df = pd.DataFrame()
        for sheet in config["sheet_id"].split(','):
            page = 1
            while True:
                api_response = auth(sheet, config["auth_token"], page=page)
                page_df = pd.DataFrame(data(api_response))
                data_origin = config.get("data_origin", 'default')
                if data_origin.lower() == 'default':
                    page_df["data_orgin"] = sheet
                elif data_origin.lower() == 'y':
                    page_df["data_origin"] = sheet
                if "ingestion_audit_field" in config:
                    page_df[config["ingestion_audit_field"]] = datetime.today()
                main_df = pd.concat([main_df, page_df], ignore_index=True)
                if len(page_df)<500:
                    break  # No more data to fetch
                page += 1
        dataframe_to_redshift(main_df)
        send_email_notification(message=f"Ingestion Sucessfull \n Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name}", subject=f"INFO - SUCCESS | {config['environment']} | {config.get('source','api')} Ingestion | SmartSheet ID - {config['sheet_id']} | {config['schema_name']}.{table_name} {config['redshift_profile']}",log_path=log_path,logger=logger,add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))
        logger.info("Ingestion Completed")
    except Exception as e:
        logger.error(f"Failed to execute main method, error --> {e} {traceback.format_exc()}")
        raise

if __name__=="__main__":
    year, month, day = datetime.today().strftime("%Y"), datetime.today().strftime("%m"), datetime.today().strftime("%d")
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', nargs=1, help="JSON file to be processed", type=argparse.FileType('r'))
    arguments = parser.parse_args()
    config = json.load(arguments.infile[0])
    utils_path=config["utils_path"]
    if 'utils' not in utils_path:
        utils_path=os.path.join(utils_path,"utils/")
    sys.path.insert(0,utils_path)
    from utils import setup_logger, send_email_notification
    from redshift_loader import Database
    log_filename = str(arguments.infile[0].name).split('/')[-1].replace('json', 'log')
    log_filename=str(log_filename.replace(".log",f"_{datetime.today().strftime('%Y_%m_%d_%H_%M_%S')}.log"))
    log_path=os.path.join(config["log_file"],log_filename)
    logger = setup_logger(log_path)
    table_name=config.get("main_table")
    if not table_name:table_name=config["table_name"]
    try:
        logger.info("Ingestion Started")
        sys.exit(main())
    except Exception as e:
        logger.error(f"Exception occured-> {e}")
        log_file_size= os.path.getsize(log_path)
        file_size=log_file_size / (1024 * 1024)
        if file_size < 1:
            send_email_notification(message=f"Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name} \n Exception -> {e} occured \n {traceback.format_exc()}", subject=f"FATAL | {config['environment']} | {config.get('source','api')} Ingestion | SmartSheet ID - {config['sheet_id']} | {config['schema_name']}.{table_name} {config['redshift_profile']}",log_path=log_path,logger=logger,add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))
        else:
            send_email_notification(message=f"Script Path-> {os.path.abspath(__file__)} \n Config Path-> {arguments.infile[0].name} \n Exception -> {e} occured \n Log File couldn't be attached with this mail due to file size limit being exceeded, Log Path-> {log_path} \n {traceback.format_exc()}", subject=f"FATAL | {config['environment']} | {config.get('source','api')} Ingestion | SmartSheet ID - {config['sheet_id']} | {config['schema_name']}.{table_name} {config['redshift_profile']}",logger=logger,add_on_email_stake_holders=config.get("add_on_email_stake_holders", None))
