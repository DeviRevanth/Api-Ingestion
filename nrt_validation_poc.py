import pandas as pd 
import argparse
from datetime import datetime
import json
import sys,os

class Validation() :
    def __init__(self,logger) -> None:
        """
        """
        self.logger = logger
        try:
            from s3_operations import S3Operations
            self.s3_session :object = S3Operations(logger=logger,profile_name=config["s3_profile_name"])
            from s3_connector import S3Connector
            self.s3_connector :object = S3Connector(logger=logger,profile_name=config["s3_profile_name"]).s3_client
            logger.info("S3 Connection Successful")
        except Exception as e :
            logger.error("S3 Connection Failure")
            raise
        try:
            from database_connector import get_connection
            self.engine :object  = get_connection(filepath = config["db_config_path"],profile= config["db_profile"])
            logger.info("RDS Connection Successful")
            self.files_metadata :pd.DataFrame  = self.metadata_filenames_fetcher()
            self.file_names_pattern :list = self.files_metadata["file_name_pattern"]
        except Exception as e:
            logger.error("RDS Connection Failure")
            raise

    @staticmethod
    def sql_query_to_pandas_dataframe(engine :object ,query :str ,logger :object) -> pd.DataFrame:
        """
        """
        try:
            df = pd.read_sql(query,engine) 
            return df
        except Exception as e:
            pass

    def column_metadata_validation(self, df :pd.DataFrame, file :str):
        """
        """
        try:
            stream_id :int= self.files_metadata.loc[self.files_metadata["file_name_pattern"] == file, 'stream_id'].values[0]
            column_metadata_query = f"""
            SELECT subquery.src_col_nm FROM (
            SELECT *, ROW_NUMBER() OVER (
            PARTITION BY src_col_nm ORDER BY saas_ingstn_field_mapping.created_dt DESC, saas_ingstn_field_mapping.updated_dt DESC) AS rn 
            FROM fusion_metadata.saas_ingstn_field_mapping 
            WHERE enable_flg = True AND stream_id = {stream_id} AND src_col_nm IS NOT NULL) 
            AS subquery WHERE rn = 1;
            """
            metadata_dataframe :pd.DataFrame = self.sql_query_to_pandas_dataframe(logger=self.logger, engine=self.engine, query= column_metadata_query)
            metadata_columns :set = set(metadata_dataframe["src_col_nm"]) 
            source_columns :set = set(pd.Series(df.columns).str.lower())
            validation :bool= metadata_columns == source_columns
            if validation == True:
                pass
            else:
                pass
        except Exception as e:
            pass

    def source_file_fetcher(self):
        """
        """
        try:
            for file in self.file_names_pattern:
                s3_response = self.s3_connector.list_objects_v2(Bucket = config["s3_bucket_name"],Prefix= config["s3_prefix_name"], Delimiter='/')
                contents = s3_response.get("Contents",None)
                if contents:
                    for content in contents:
                        key = content.get("Key",None)
                        if key:
                            if file in key:
                                file_extension = key.split("/")[-1]
                                file_type = "text" if file_extension == "txt" else file_extension
                                if file_extension == "xlsx":
                                    file_type = "excel"
                                data_frame = self.s3_session.getobject_s3(bucket_name=config["s3_bucket_name"],key= key ,file_type=file_type)
                                self.column_metadata_validation(df=data_frame,file = file)
                        else:
                            raise KeyError("")
                else:
                    raise KeyError("")
        except Exception as e:
            self.logger.error()
            raise

    def metadata_filenames_fetcher(self) -> pd.DataFrame:
        """
        """
        try :
            query = f"""
            SELECT subquery.stream_id,subquery.stream_name,subquery.file_name_pattern,subquery.processing_path,subquery.rejected_path FROM (
            SELECT *, ROW_NUMBER() OVER (
            PARTITION BY stream_id ORDER BY saas_ingstn_stream_control.created_dt DESC, saas_ingstn_stream_control.updated_dt DESC) AS rn 
            FROM fusion_metadata.saas_ingstn_stream_control 
            WHERE enable_flg = True) 
            AS subquery WHERE rn = 1;
            """
            required_files_metadata :pd.DataFrame = self.sql_query_to_pandas_dataframe(logger=self.logger, engine=self.engine, query= query)
            return required_files_metadata 
        except Exception as e:
            self.logger.error("")
            raise

    def log_metadata_files_fetcher(self, df :pd.DataFrame, ) -> pd.DataFrame:
        """
        """
        try:
            query = f"""SELECT load_status,error_code from fusion_log.saas_ingstn_log WHERE stream_id = {}"""
        except Exception as e:
            pass
        pass

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
        logger.info("NRT Saas Fusion Validation Execution Started")
        Validation(logger=logger)
        sys.exit(0)
        logger.info("NRT Saas Fusion Validation completed")
    except Exception as e:
        print(f"Error Connecting to the database:{e}")


