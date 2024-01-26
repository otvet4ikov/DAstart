import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'https://storage.yandexcloud.net/kc-startda/top-1m.csv'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_top_10_zone():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['domain_split'] = df['domain'].apply(lambda x: x.split('.', maxsplit=1))
    df['domain_name'] = df['domain_split'].apply(lambda x: x[0])
    df['domain_zone'] = df['domain_split'].apply(lambda x: x[1])
    top_10_zone = df.groupby('domain_zone', as_index=False).agg({'rank': 'count'}).sort_values('rank', ascending=False).head(10)
    with open('top_10_zone.csv', 'w') as f:
        f.write(top_10_zone.to_csv(index=False, header=False))


def get_most_len_domain():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['domain_len'] = df['domain'].apply(lambda x: len(x))
    longest_domain = df.sort_values('domain_len', ascending=False).domain.iloc[0]
    longest_domain = pd.DataFrame([longest_domain])
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv(index=False, header=False))

def get_airflow_place():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['domain_len'] = df['domain'].apply(lambda x: len(x))
    airflow_dom = df.sort_values('domain_len', ascending=False).reset_index()
    airflow_place = int(airflow_dom[airflow_dom['domain'] == "airflow.com"].reset_index().level_0+1)
    airflow_place = pd.DataFrame([airflow_place])
    with open('airflow_place.csv', 'w') as f:
        f.write(airflow_place.to_csv(index=False, header=False))
    
    
def print_data(ds):
    with open('top_10_zone.csv', 'r') as f:
        all_data = f.read()
    with open('longest_domain.csv', 'r') as f:
        longest_domain = f.read()
    with open('airflow_place.csv', 'r') as f:
        airflow_place = f.read()
    date = ds

    print(f'Top domains zone {date}')
    print(all_data)

    print(f'Longest domain mane {date}')
    print(longest_domain)
    
    print(f'Airflow place {date}')
    print(airflow_place)

default_args = {
    'owner': 'n.otvetchikov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 21),
}
schedule_interval = '01 12 * * *'

dag = DAG('lesson_2_otvetchikov', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10_zone',
                    python_callable=get_top_10_zone,
                    dag=dag)

t3 = PythonOperator(task_id='get_most_len_domain',
                        python_callable=get_most_len_domain,
                        dag=dag)
t4 = PythonOperator(task_id='get_airflow_place',
                    python_callable=get_airflow_place,
                    dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)
