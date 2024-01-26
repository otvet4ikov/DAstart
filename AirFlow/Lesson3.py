import pandas as pd
from airflow.decorators import dag, task
from datetime import timedelta
from datetime import datetime
import telegram
from airflow.models import Variable
from airflow.operators.python import get_current_context

path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 'n.otvetchikov'

default_args = {
    'owner': 'n.otvetchikov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 20)
}


# CHAT_ID = -4105131483
# try:
#     BOT_TOKEN = Variable.get('TOKEN')
# except:
#     BOT_TOKEN = ''


# def send_message(context):
#     date = context['ds']
#     dag_id = context['dag'].dag_id
#     message = f'Huge success! Dag {dag_id} completed on {date}'
#     if BOT_TOKEN != '':
#         bot = telegram.Bot(token=BOT_TOKEN)
#         bot.send_message(chat_id=CHAT_ID, text=message)
#     else:
#         pass
    
    
@dag(default_args=default_args, schedule_interval='0 12 * * *', catchup=False)
def lesson_3_otvetchikov():

    @task(retries=3)
    def parse_data(path, login):
        data = pd.read_csv(path)
        year = 1994 + hash(f'{login}') % 23
        data = data.query('Year == @year')
        
        return data

    @task(retries=3)
    def world_top_sales(data):
        world_sales_data = data
        top_world_sales = world_sales_data.sort_values("Global_Sales", ascending=False).Name.head(1).iloc[0]
        
        return {'Top world sales': top_world_sales}

    @task(retries=3)
    def EU_top_sales(data):
        EU_sales_data = data
        top_EU_sales = EU_sales_data.groupby('Genre', as_index=False).agg({'EU_Sales': 'sum'}).max().Genre
        
        return {'Top genre EU sales': top_EU_sales}

    @task(retries=3)
    def NA_top_sales(data):
        NA_sales_data = data
        top_NA_sales = NA_sales_data.query('NA_Sales > 1').groupby('Platform', as_index=False).agg({'Year': 'count'}).max().Platform
        
        return {'Top platform NA sales over 1 million': top_NA_sales}

    @task(retries=3)
    def JP_top_sales(data):
        JP_sales_data = data
        top_JP_sales = JP_sales_data.groupby('Publisher', as_index=False).agg({'JP_Sales': 'mean'}).max().Publisher
        
        return {'Top publisher JP average sales': top_JP_sales}

    @task(retries=3)
    def top_sales(data):
        sell_more_EU_than_JP = data
        sell_more_EU_than_JP_count = len(sell_more_EU_than_JP.query("EU_Sales > JP_Sales"))
        
        return {'The number of games that sell more in Europe than in Japan': sell_more_EU_than_JP_count}

    @task(retries=3, on_success_callback=send_message)
    def print_all_data(world_sales, EU_sales, NA_sales, JP_sales, EU_JP_sales):
        context = get_current_context()
        
        top_world_sales = world_sales['Top world sales']
        top_EU_sales = EU_sales['Top genre EU sales']
        top_NA_sales = NA_sales['Top platform NA sales over 1 million']
        top_JP_sales = JP_sales['Top publisher JP average sales']
        sell_more_EU_than_JP_count = EU_JP_sales['The number of games that sell more in Europe than in Japan']
        
        print(f'Top world sales: {top_world_sales}')
        print(f'Top genre EU sales: {top_EU_sales}')
        print(f'Top platform NA sales over 1 million: {top_NA_sales}')
        print(f'Top publisher JP average sales: {top_JP_sales}')
        print(f'The number of games that sell more in Europe than in Japan: {sell_more_EU_than_JP_count}')

    
    
    df = parse_data(path, login)
    world_sales = world_top_sales(df)
    EU_sales = EU_top_sales(df)
    NA_sales = NA_top_sales(df)
    JP_sales = JP_top_sales(df)
    EU_JP_sales = top_sales(df)
    print_all_data(world_sales, EU_sales, NA_sales, JP_sales, EU_JP_sales)
    
lesson_3_otvetchikov = lesson_3_otvetchikov()    