#######################################################
#Name: Kola Devi Revanth
#Date: 28-03-2025
#Version : 1.0
#Version Comments: Initial Version
#Objective: SQL Query Executor Module
#userstory:
########################################################

#### Import Necessary Packages ####
from typing import Union
import pandas as pd

def sql_query_executor(engine :object, logger :object, query: str) -> Union[pd.DataFrame, None]:
    """
    This method executes a SQL query using the provided engine and returns the result as a `pd.DataFrame`.

    Parameters:
        engine (object) : SQLAlchemy engine object for Database connection.
        query (str)     : SQL query string.

    Returns:
        pd.DataFrame : A DataFrame containing the query result, or raises an exception if all retries fail.

        **None**  : For non-SELECT queries such as INSERT or UPDATE.
    """
    logger.info(f"Executing sql_query_executor method")
    if not query or not isinstance(query, str):
        raise ValueError("Query must be a non-empty string.")
    try:
        if query.strip().upper().startswith(("INSERT", "UPDATE", "DO")):
            engine.execute(query)
            logger.info(f"SQL Query execution successful")
            return None
        elif query.strip().upper().startswith("SELECT"):
            df = pd.read_sql(query, engine)
            logger.info(f"Fetched data into DataFrame using SQL Query")
            return df
        else:
            raise ValueError("Only SELECT, INSERT, and UPDATE queries are supported.")
    except Exception as e:
        logger.error(f"Error occurred in sql_query_executor method: {str(e)}")
        raise
