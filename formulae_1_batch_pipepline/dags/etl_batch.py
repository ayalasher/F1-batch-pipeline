from datetime import datetime, timedelta
from airflow.decorators import dag , task
import sys
import os
from sqlalchemy import create_engine
import pandas as pd
import logging
from dotenv import load_dotenv
load_dotenv()


logging.basicConfig(
    level=logging.INFO,
    format= '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    filename='formula_one_pipelinee.log',
    filemode='a'
)

logger = logging.getLogger()


"""
email operator is going to be intergrated
later on after testing and deploying the model to test
CI/CD and the email sending capabilities
"""

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from drivers.drivers import extract_drivers , transform_drivers
from constructors.constructors import extract_constructors , transform_constuctors

# default args
default_args = {
    'owner':'ayalasher',
    'retries':3,
    'retry_delay':timedelta(minutes=5)
}

@dag(
    dag_id='f1_etl_pipeline',
    default_args=default_args,
    description='F1 Championship ETL Pipeline',
    schedule="0 12 * * 1",
    start_date=datetime(2024, 11, 22),
    catchup=False,
    tags=['f1', 'etl', 'championship']
)
def formulae_one_etl():
    @task
    def extract_drivers_task():
        edt_df = extract_drivers()
        return edt_df.to_dict('records')
    @task
    def transform_drivers_task(driver_data):
        tdt_df = transform_drivers(driver_data)
        return tdt_df.to_dict('records')
    
    @task
    def extract_constructors_task():
        edt_df = extract_constructors()
        return edt_df.to_dict('records')
    @task
    def transform_constructors_task(constructors_data):
        tdt_df = transform_constuctors(constructors_data)
        return tdt_df.to_dict('drivr_records')
    
    @task
    def load_drivers_to_database(transformed_drivers):
        df = pd.DataFrame(transformed_drivers)
        connection_string = (
            f"postgresql://{os.getenv('POSTGRES_USER')}:"
            f"{os.getenv('POSTGRES_PASSWORD')}@"
            f"{os.getenv('POSTGRES_HOST')}:"
            f"{os.getenv('POSTGRES_PORT')}/"
            f"{os.getenv('POSTGRES_DB')}"
        )

        engine = create_engine(connection_string)
        try:
            df.to_sql(
               name='drivers_championship',
                con=engine,
                if_exists='replace',  # Options: 'fail', 'replace', 'append'
                index=False,
                method='multi',  
            )
            logging.info("drivers data has been succesfully loaded into the database")
        except Exception as e:
            logging.error(f"an error {e} occured when loading the data into the database")
            raise
        finally:
            engine.dispose()

    @task
    def load_constructors_to_database(transformed_constructors):
        df = pd.DataFrame(transformed_constructors)
        connection_string = (
            f"postgresql://{os.getenv('POSTGRES_USER')}:"
            f"{os.getenv('POSTGRES_PASSWORD')}@"
            f"{os.getenv('POSTGRES_HOST')}:"
            f"{os.getenv('POSTGRES_PORT')}/"
            f"{os.getenv('POSTGRES_DB')}"
        )
        
        engine = create_engine(connection_string)
        try:
            df.to_sql(
               name='constructors_championship',
                con=engine,
                if_exists='replace',  # Options: 'fail', 'replace', 'append'
                index=False,
                method='multi',  
            )
            logging.info("drivers data has been succesfully loaded into the database")
        except Exception as e:
            logging.error(f"an error {e} occured when trying to load data to the constructors table")
            raise
        finally:
            engine.dispose()

    drivers_extracted = extract_drivers_task()
    drivers_transformed = transform_drivers_task(drivers_extracted)
    drivers_loaded = load_drivers_to_database(drivers_transformed)
    
    # Constructors pipeline (runs in parallel)
    constructors_extracted = extract_constructors_task()
    constructors_transformed = transform_constructors_task(constructors_extracted)
    constructors_loaded = load_constructors_to_database(constructors_transformed)


f1_etl_dag = formulae_one_etl()