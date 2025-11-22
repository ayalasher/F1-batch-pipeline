import os
from dotenv import load_dotenv
import pandas as pd
import requests
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(mesaage)s',
    filename='drivers_standings.log',
    filemode='a'
)

logger = logging.getLogger()


def extract_drivers():
    load_dotenv()
    complete_drivers_url = os.getenv("current_drivers_championship")
    response = requests.get(complete_drivers_url)
    response_json_version = response.json()
    response_dict_version = response_json_version["drivers_championship"]
    drivers_normalized = pd.json_normalize(
        response_dict_version,
        sep='_'
    )
    drivers_desired_columns = [
        'driver_name',
        'driver_surname', 
        'points',
        'position',
        'team_teamId' 
    ]
    final_data = drivers_normalized[drivers_desired_columns]
    logger.info("Drivers data extraction succesful")
    return final_data


def transform_drivers(final_data):
    load_dotenv()
    removed_null_values_final = final_data.dropna()
    return removed_null_values_final