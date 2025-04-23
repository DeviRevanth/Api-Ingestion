#######################################################
#Name: Kola Devi Revanth
#Date: 28-03-2025
#Version : 1.0
#Version Comments: Initial Version
#Objective: SQL File reader module
#userstory:
########################################################

def sql_file_reader(logger :object ,sql_file_path :str) -> str:
    """
    Reads and returns the content of a `.sql` file.

    This method is designed to be used in scenarios where **SQL** queries
    are stored in external `.sql` files.

    Parameters:
        logger (object)     : The logging object used to log messages and errors.
        sql_file_path (str) : Path to the sql file to be read. Must end with `.sql`.

    Returns:
        query (str) :The content of the SQL file as a string.

    Raises:
        ValueError : If the file path provided is not `.sql` file.
        Exception  : If an error occurs during file reading or processing.
    """
    try:
        if sql_file_path.endswith(".sql"):
            logger.info("Executing sql_file_reader method")
            with open(sql_file_path, "r") as file:
                query = file.read()
            logger.info("SQL File read Successfully")
            return query
        else:raise ValueError(f"Invalid file path: {sql_file_path}. Expected a '.sql' file.")
    except Exception as e:
        logger.error(f"Failed to execute sql_file_reader method with error -> {str(e)}", exc_info=True)
        raise
