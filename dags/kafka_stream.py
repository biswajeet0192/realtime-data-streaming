from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 00, 00)
}

def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]

    return res

def stream_data():
    import json
    from kafka import KafkaProducer
    import time


    res = get_data()
    res = format_data(res)
    # print(json.dumps(res, indent=3))

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)

    producer.send('users_created', json.dumps(res).encode('utf-8'))
    
def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']} " \
          f"{location['city']}  {location['state']} {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['dob'] = res['dob']['date']
    data['phone'] = res['phone']
    data['registrered_date'] = res['registered']['date']
    data['picture'] = res['picture']['large']

    return data

# with DAG('user_automation', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
#     streaming_task = PythonOperator(
#         task_id = 'stream_data_from_api',
#         python_callable=stream_data
#     )

stream_data()