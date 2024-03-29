import configparser
import os

from snowflake.snowpark import Session
from snowflake.snowpark.functions import current_session
from os.path import join, dirname
from dotenv import load_dotenv


def getConfig():
    config = configparser.ConfigParser()
    config.read('utilities/properties.ini')
    return config


def getSFConn():
    dotenv_path = join(os.path.dirname(dirname(__file__)),'.env')
    load_dotenv(dotenv_path)

    # Get the credentials from .env
    connection_params = {
    "account": os.getenv('SF_ACCOUNT'),
    "user": os.getenv('SF_USER'),
    "password": os.getenv('SF_PASSWORD'),
    "warehouse": os.getenv('SF_WAREHOUSE'),
    "database": os.getenv('SF_DATABASE'),
    "schema": os.getenv('SF_SCHEMA'),
    "region": os.getenv('SF_REGION'),
    "rolename": os.getenv('SF_ROLENAME')
    }

    try:
        session = Session.builder.configs(connection_params).create()
        if session.create_dataframe([1]).select(current_session()).collect():
            print("Connection Successful")
            return session
    except:
        print("Unable to connect")
