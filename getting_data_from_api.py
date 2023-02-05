from airflow import DAG
from ds_utils import PythonVirtualEnvOperatorCustomPip


DEFAULT_ARGS = {
                'owner': 'airflow',
                'depends_on_past': False,
                'start_date': datetime(2023, 2, 1),
                'email': ['alenna309@gmail.com'],
                'email_on_failure': True,
                'email_on_retry': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=60),
                }


dag = DAG('getting_data_from_api', 
          description='A simple DAG that gets data from api',
          default_args=DEFAULT_ARGS, 
          schedule_interval="* */12 * * *" # every 12 hours
         )


def getting_data():
    import vertica_python
    from airflow.hooks.base_hook import BaseHook
    from sqlalchemy import create_engine
    import requests
    import pandas as pd
    import json
    from datetime import datetime


    connection = BaseHook.get_connection("our_connection_to_database")
    connection = create_engine(
            'vertica+vertica_python://{user}:{password}@{server}:{port}/{database}'.format(
            user=str(connection.login),
            password=str(connection.password),
            server=str(connection.host),
            port=str(connection.port),
            database=str(connection.schema)
        )
    )
    
    conn = connection.connect()

    
    def to_sql(df):

        df.to_sql(
                'trash_getting_data_from_api',
                conn=vertica,
                schema='Abaybakova',
                index=False,
                if_exists='append'
                )
    

    # Получаем данные из api
    url = "https://random-data-api.com/api/cannabis/random_cannabis?size=10"
    data = requests.get(url).json()


    lst = []
    for element in (range(len(data))):
        
        formatted_data = pd.DataFrame(data[element].items()).T
        headers = formatted_data.iloc[0]
        formatted_data  = pd.DataFrame(formatted_data.values[1:], columns=headers)

        lst.append(formatted_data)


    final_data = pd.concat(lst)

    final_data['created_at'] = datetime.now()

    to_sql(final_data)
    
    
task = PythonVirtualEnvOperatorCustomPip(
                    task_id='getting_data',
                    dag=dag,
                    python_callable=getting_data,
                    python_version='3.7',
                    requirements=[
                        'pandas',
                        'vertica_python',
                        'sqlalchemy',
                        'sqlalchemy-vertica-python',

                    ],
                    use_dill=False
                                        )

    
task
