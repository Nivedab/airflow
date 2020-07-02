from datetime import timedelta

import airflow
import matplotlib
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

def database():
  user_name = 'add_user_name_here'
  pwd  = 'add_password_here'



  import pyexasol

  def connect_exasol(query):
    C = pyexasol.connect(dsn='90.92.980.290..634', user=user_name, password=pwd)
    return (C.export_to_pandas(query))

  def write_exasol(query, df, schema, tablename):
      C = pyexasol.connect(dsn='90.92.980.290..634', user=user_name, password=pwd)
      C.execute(query)
      C.import_from_pandas(df, (schema, tablename))
      print('Done. U should now find the table in Exasol',"\n")

  q = """SELECT DISTINCT name FROM customers.users;"""

  df = connect_exasol(q)

  # Creating a new table and filling it up with the data that is in python.
  creating_empty_table = """CREATE OR REPLACE TABLE USER_NIVEDA_SRIDHAR.test
                (Handle VARCHAR(2000))"""

  write_exasol(creating_empty_table, df, 'USER_NIVEDA_SRIDHAR', 'test')

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    #'email': ['user.name@domain.com'],
    #'email_on_failure': True,
    #'email_on_retry': False,
    'retries': 1,
    #'retry_delay': timedelta(minutes=1)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'adhoc':False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'trigger_rule': u'all_success'
}

dag = DAG(
    'socorates_daily',
    default_args=default_args,
    description='Automation for word analysis',
    schedule_interval=timedelta(days=1),
)

# t1, t2 and t3 are examples of tasks created by instantiating operators
#t1 = BashOperator(
 #   task_id='bash_example',
  #  # This works (has a space after)
   # bash_command= "/usr/local/airflow/dags/python_notebook.sh ",
   # bash_command='{{"/python_notebook.sh"}}',
    #dag=dag)

t1  = PythonOperator(dag=dag,
              task_id='basictry',
              provide_context=False,
              python_callable=database,
              #op_args=['arguments_passed_to_callable'],
              #op_kwargs={'keyword_argument':'which will be passed to function'}
               )








