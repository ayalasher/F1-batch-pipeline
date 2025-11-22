import os
from dotenv import load_dotenv
import pandas as pd
import requests
import logging



logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(mesaage)s',
    filename='constructors_standings.log',
    filemode='a'
)

logger = logging.getLogger()


def extract_constructors():
    load_dotenv()
    complete_constructors_url = os.getenv("current_constructors_championship")
    response = requests.get(complete_constructors_url)
    response_json_version = response.json()
    response_dict_version = response_json_version["constructors_championship"]
    constructor_normalized = pd.json_normalize(
        response_dict_version,
        sep='_'
    )
    constructors_desired_columns = [
          "teamId",
          "points",
          "position",
          "wins",
          "team_teamName" 
    ]
    final_data = constructor_normalized[constructors_desired_columns]
    logger.info("Constructors data extraction succesful")
    return final_data

def transform_constuctors(final_data):
    load_dotenv()
    removed_null_values_final = final_data.dropna()
    return removed_null_values_final