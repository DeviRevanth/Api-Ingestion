#######################################################
#Name: Naveen Marella
#Date: 29-10-2024
#Version : 1.0
#Version Comments: Initial Version
#Objective: API connector class that supports all different types of api authentication
#userstory:
########################################################

import configparser
from requests.auth import HTTPBasicAuth
import requests
import traceback

class api_connect:

    def __init__(self,logger,connection_path,connection_profile):
        self.logger=logger
        self.connection_path = connection_path
        self.connection_profile = connection_profile
        self.config=configparser.ConfigParser()
        self.config.read(self.connection_path)

    def basic_auth(self):
        """
        A method returns auth that used for all basic auth

        paramas : None
        Return (object) : authentication

        """
        try:
           #print(self.config[self.connection_profile]['username'],self.config[self.connection_profile]['password'])
           auth = HTTPBasicAuth(self.config[self.connection_profile]['username'],self.config[self.connection_profile]['password'])
           self.logger.info("Basic authentication sucessfully established")
           return auth
        except Exception as e:
            self.logger.error(f"Authentication failed due to error : {e}")
            raise


class ApiRequest:
    def __init__(self, logger) -> None:
        """
        The Constructor for ApiRequest class.

        Parameters:
        logger (Logger): Logger object
        """
        self.logger = logger
        self.logger.info("Initiating ApiRequest Class")

    def get_request(self, api_url, headers=None, additional_get_parameters=None):
        """
        Performs a GET request to the provided API URL and returns the response.

        Parameters:
        api_url (str): API endpoint URL
        headers (dict): Optional HTTP headers
        additional_get_parameters (dict): Optional GET parameters for the request

        Returns:
        response : Generate API response only if it is successful
        """
        try:
            self.logger.info("Executing get_request method")
            if additional_get_parameters:response = requests.get(api_url, headers=headers,**additional_get_parameters)
            else:response = requests.get(api_url, headers=headers)
            return self.reponse_handler(response, method="GET")
        except Exception as e:
            self.logger.error(f"Failed to execute get_request method: {e} {traceback.format_exc()}")
            raise

    def post_request(self, api_url, headers=None, additional_post_parameters=None):
        """
        Performs a POST request to the provided API URL and returns the response.

        Parameters:
        api_url (str): API endpoint URL
        headers (dict): Optional HTTP headers
        additional_post_parameters (dict): Optional POST parameters for the request

        Returns:
        response : Generate API response only if it is successful
        """
        try:
            self.logger.info("Executing post_request method")
            if additional_post_parameters:response = requests.post(api_url, headers=headers, **additional_post_parameters)
            else:response = requests.pose(api_url, headers=headers)
            print(response.json())
            return self.reponse_handler(response, method="PUT")
        except Exception as e:
            self.logger.error(f"Failed to execute post_request method: {e} {traceback.format_exc()}")
            raise

    def reponse_handler(self, response, method):
        """
        Handles the HTTP response, checking for success and logging accordingly.

        Parameters:
        response (requests.Response): HTTP response object
        method (str): The HTTP method used (e.g., "GET", "PUT")

        Returns:
        response : Generate API response
        """
        if response.status_code == 200:
            self.logger.info(f"{method} request successful.")
            return response
        else:
            self.logger.warning(f"{method} request failed with status code {response.status_code}.")
            try:
                error_message = response.json().get("message", "No message provided")
            except ValueError:
                error_message = "No valid JSON in response"
            error_info = f"{method} request failed. Status code: {response.status_code}. Message: {error_message}"
            self.logger.error(error_info)
            raise Exception(error_info)
