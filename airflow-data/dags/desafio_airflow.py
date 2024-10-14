from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
import sqlite3
import pandas as pd

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['maikon.pereira@indicium.tech'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt","w") as f:
        f.write(base64_message)
    return None
## Do not change the code above this line-----------------------##
# my code 
# Função que extrai dados do banco de dados SQLite e salva em CSV


def extract_data_to_csv():
    # Conectando ao db Northwind
    conn = sqlite3.connect('data/Northwind_small.sqlite')

    # Lê os dados da tabela "Order"
    query = " SELECT * FROM 'Order' "
    df = pd.read_sql(query, conn)

    # Salvando o resultado em um arquivo CSV
    df.to_csv('/mnt/c/Users/Maikon/Documents/Lighthouse/airflow_tooltorial/airflow-data/target/arquivo.csv', index=False)

    # Fechando a conexão ao db Northwind
    conn.close()
    return None
    
def process_orders():
    caminho_sqlite = r"data/Northwind_small.sqlite"
    
    conn = sqlite3.connect(caminho_sqlite)
    cursor = conn.cursor()

    # Query feita diretamente no banco
    query = """
    SELECT SUM(OrderDetail.Quantity) FROM OrderDetail
    JOIN 'Order' ON OrderDetail.OrderID = 'Order'.ID
    WHERE 'Order'.ShipCity = 'Rio de Janeiro';
    """
    cursor.execute(query)
    result = cursor.fetchone()

    with open('/mnt/c/Users/Maikon/Documents/Lighthouse/airflow_tooltorial/airflow-data/target/count.txt', 'w') as f:
        f.write(str(result[0]))
    conn.close()
    return None

## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Importando arquivo count
    with open('/mnt/c/Users/Maikon/Documents/Lighthouse/airflow_tooltorial/airflow-data/target/count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("/mnt/c/Users/Maikon/Documents/Lighthouse/airflow_tooltorial/airflow-data/target/final_output.txt","w") as f:
        f.write(base64_message)
    return None


with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """
    
    extract_orders_task = PythonOperator(
        task_id='extract_and_save_to_csv',
        python_callable=extract_data_to_csv,
        provide_context=True
    )
    
    
    calculate_quantity_task = PythonOperator(
        task_id='process_orders',
        python_callable=process_orders,
        provide_context=True
    )
    
    
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True
    )
    

    # Definindo a ordem de execução das tasks
    extract_orders_task >> calculate_quantity_task >> export_final_output