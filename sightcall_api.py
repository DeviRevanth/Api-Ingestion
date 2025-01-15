import json
import argparse
import pandas as pd
import os
from utils import setup_logger, get_connection,send_email_notification
import urllib3
from sqlalchemy import types
from datetime import datetime
import aiohttp
import asyncio
 
class DataFetcher:
    def __init__(self, config, engine, logger):
        self.config = config
        self.engine = engine
        self.logger = logger
 
    @staticmethod
    def sqlcol(dfparam):
        dtypedict = {}
        for i, j in zip(dfparam.columns, dfparam.dtypes):
            if "object" in str(j):
                dtypedict.update({i: types.VARCHAR(5000, collation='case_insensitive')})
        return dtypedict
 
    async def fetch_data(self,api_url, headers, session):
        all_data = []
        while api_url:
            async with session.get(api_url, headers=headers,ssl=False) as response:
                if response.status == 200:
                    json_data = await response.json()
                    current_page_data = json_data["data"]
                    if current_page_data:
                        all_data.extend(current_page_data)
                    next_link = json_data.get("links", {}).get("next")
                    api_url = next_link if next_link else None
                else:
                    self.logger.info(f"Failure Api response--> {response.status}")
                    break
        return all_data
     
    async def process_url(self, url, headers, region,name):
        async with aiohttp.ClientSession() as session:
            all_data = await self.fetch_data(url, headers, session)
            if all_data:
                df = pd.json_normalize(all_data)
                df.columns = pd.Series(df.columns).replace('attributes.', '', regex=True)
                df.columns = pd.Series(df.columns).replace(' ', '_', regex=True)
                df["source"] = region
                df = df.applymap(lambda x: str(x) if isinstance(x, list) else x)
                df['hvr_last_upd_tms'] = datetime.now()
                self.logger.info(f"{name} for {region} fetched - {df.shape[0]} records")
                return df
 
    async def run(self):
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        for url in self.config["search_url"]:
            tasks = []
            name = str(url.split("/")[-1].split("?")[0]).lower()
            main_df=pd.DataFrame()
            self.logger.info(f"Running Async calls for {name}")
            for header in self.config["headers"]:
                for region in header.keys():
                    tasks.append(self.process_url(url,header[region], region,name))
            result=await asyncio.gather(*tasks)
            main_df=pd.DataFrame()
            for res in result:main_df=pd.concat([main_df,res])
            self.logger.info(f"{name} count - {main_df.shape[0]}")
            print(main_df)
 
            # con=self.engine.connect().execution_options(autocommit=True)
            try:
                query=f"TRUNCATE TABLE {self.config['schema_name']}.{name}"
                # con.execute(query)
                self.logger.info(f"{self.config['schema_name']}.{name} has been truncated")
                # main_df.to_sql(name=name.lower(), schema=self.config["schema_name"], method='multi', chunksize=1500, if_exists='append', con=self.engine, index=False, dtype=self.sqlcol(main_df))
                main_df.to_csv(os.path.join(parent_path,f"{name}.csv"))
                self.logger.info(f"Data has been ingested to {self.config['schema_name']}.{name}")
            except Exception as e:
                if 'does not exist' in e:
                    self.logger.info(f"{self.config['schema_name']}.{name} does not exist")
                    # main_df.to_sql(name=name.lower(), schema=self.config["schema_name"], method='multi', chunksize=1500, if_exists='replace', con=self.engine, index=False, dtype=self.sqlcol(main_df))
                    self.logger.info(f"Created {self.config['schema_name']}.{name}")
 
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', nargs=1, help="JSON file to be processed", type=argparse.FileType('r'))
    arguments = parser.parse_args()
    config = json.load(arguments.infile[0])
    parent_path = os.path.dirname(os.path.abspath(__file__))
    log_filename = str(arguments.infile[0].name).split('/')[-1].replace('json', 'log')
    logger = setup_logger(os.path.join(parent_path, log_filename))
    logger.info("Ingestion Started")
    # conn = get_connection(config["config_path"], config["connection_profile"])
    urllib3.disable_warnings()
    try:
        data_fetcher = DataFetcher(config, '', logger)
        if hasattr(asyncio, 'run'):
            asyncio.run(data_fetcher.run())
        else:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(data_fetcher.run())
        logger.info("Ingestion Completed")
    except Exception as e:
        logger.error(f"Exception occurred - {e}")
        send_email_notification(message=f"Exception - {e} occured at {parent_path}", subject=f" SightCall Ingestion Failure | Dev ",email_stake_holders="",log_path=log_filename)
