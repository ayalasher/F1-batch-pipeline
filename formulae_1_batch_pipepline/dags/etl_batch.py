from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import pandas as pd
import requests
import os
import logging
from sqlalchemy import create_engine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    filename='formula_one_pipeline_rewrite.log',
    filemode='a'
)

logger = logging.getLogger()

default_args = {
    'owner': 'ayalasher',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id="f1_etl_pipeline_rewrite",
    default_args=default_args,
    description='F1 Championship ETL Pipeline - No External Modules',
    start_date=datetime(2024, 11, 22),
    schedule="@weekly",  # Every Monday at 12 PM
    catchup=False,
    tags=['f1', 'etl', 'championship', 'rewrite']
) as dag:
    
    @task
    def extract_drivers():
        try:
            drivers_url = os.getenv("current_drivers_championship")
            if not drivers_url:
                raise ValueError("Driver championship URL not found in environment variables")
            
            response = requests.get(drivers_url)
            response.raise_for_status()
            response_json = response.json()
            response_dict = response_json["drivers_championship"]
            
           
            drivers_normalized = pd.json_normalize(response_dict, sep='_')
            
           
            drivers_desired_columns = [
                'driver_name',
                'driver_surname', 
                'points',
                'position',
                'team_teamId' 
            ]
            final_data = drivers_normalized[drivers_desired_columns]
            
            logger.info(f"Drivers data extraction successful - {len(final_data)} records extracted")
            
            
            return final_data.to_dict('records')
            
        except Exception as e:
            logger.error(f"Error extracting drivers data: {e}")
            raise
    
    @task
    def transform_drivers(drivers_data):
        try:
            
            df = pd.DataFrame(drivers_data)
            cleaned_df = df.dropna()
            
            logger.info(f"Drivers data transformation successful - {len(cleaned_df)} records after cleaning")
            return cleaned_df.to_dict('records')
            
        except Exception as e:
            logger.error(f"Error transforming drivers data: {e}")
            raise
    
    @task
    def extract_constructors():
        try:
            constructors_url = os.getenv("current_constructors_championship")
            if not constructors_url:
                raise ValueError("Constructors championship URL not found in environment variables")
            
            response = requests.get(constructors_url)
            response.raise_for_status()
            response_json = response.json()
            response_dict = response_json["constructors_championship"]
            constructors_normalized = pd.json_normalize(response_dict, sep='_')
            constructors_desired_columns = [
                "teamId",
                "points",
                "position",
                "wins",
                "team_teamName" 
            ]
            final_data = constructors_normalized[constructors_desired_columns]
            
            logger.info(f"Constructors data extraction successful - {len(final_data)} records extracted")
            return final_data.to_dict('records')
            
        except Exception as e:
            logger.error(f"Error extracting constructors data: {e}")
            raise
    
    @task
    def transform_constructors(constructors_data):
        try:
            df = pd.DataFrame(constructors_data)
            cleaned_df = df.dropna()
            
            logger.info(f"Constructors data transformation successful - {len(cleaned_df)} records after cleaning")
            return cleaned_df.to_dict('records')
            
        except Exception as e:
            logger.error(f"Error transforming constructors data: {e}")
            raise
    
    @task
    def load_drivers_to_database(transformed_drivers):
        try:
            df = pd.DataFrame(transformed_drivers)
            connection_string = (
                f"postgresql://{os.getenv('POSTGRES_USER')}:"
                f"{os.getenv('POSTGRES_PASSWORD')}@"
                f"{os.getenv('POSTGRES_HOST')}:"
                f"{os.getenv('POSTGRES_PORT')}/"
                f"{os.getenv('POSTGRES_DB')}"
            )
            engine = create_engine(connection_string)
            df.to_sql(
                name='drivers_championship',
                con=engine,
                if_exists='replace',  # Options: 'fail', 'replace', 'append'
                index=False,
                method='multi',
            )
            
            logger.info(f"Drivers data successfully loaded to database - {len(df)} records")
            
            engine.dispose()
            
            return f"Successfully loaded {len(df)} driver records"
            
        except Exception as e:
            logger.error(f"Error loading drivers data to database: {e}")
            raise
    
    @task
    def load_constructors_to_database(transformed_constructors):
        try:
            df = pd.DataFrame(transformed_constructors)
            connection_string = (
                f"postgresql://{os.getenv('POSTGRES_USER')}:"
                f"{os.getenv('POSTGRES_PASSWORD')}@"
                f"{os.getenv('POSTGRES_HOST')}:"
                f"{os.getenv('POSTGRES_PORT')}/"
                f"{os.getenv('POSTGRES_DB')}"
            )
            
            engine = create_engine(connection_string)
            
            df.to_sql(
                name='constructors_championship',
                con=engine,
                if_exists='replace',  # Options: 'fail', 'replace', 'append'
                index=False,
                method='multi',
            )
            
            logger.info(f"Constructors data successfully loaded to database - {len(df)} records")
            
            engine.dispose()
            
            return f"Successfully loaded {len(df)} constructor records"
            
        except Exception as e:
            logger.error(f"Error loading constructors data to database: {e}")
            raise
    drivers_extracted = extract_drivers()
    drivers_transformed = transform_drivers(drivers_extracted)
    drivers_loaded = load_drivers_to_database(drivers_transformed)

    constructors_extracted = extract_constructors()
    constructors_transformed = transform_constructors(constructors_extracted)
    constructors_loaded = load_constructors_to_database(constructors_transformed)

    drivers_extracted >> drivers_transformed >> drivers_loaded >> constructors_extracted >> constructors_transformed >> constructors_loaded