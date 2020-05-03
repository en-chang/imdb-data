import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2020, 5, 1)
}

staging_dataset = 'bollywood_workflow_staging'
modeled_dataset = 'bollywood_workflow_modeled'

bq_query_start = 'bq query --use_legacy_sql=false '

create_acts_sql = 'CREATE OR REPLACE TABLE ' + modeled_dataset + '''.bollywoodActs AS
                      SELECT Name actor, Height_in_cm_ height_cm, "actress" AS job 
                      FROM ''' + staging_dataset + '''.bollywoodActor
                      UNION ALL
                      SELECT Name actor, Height_in_cm_ height_cm, "actor" AS job 
                      FROM ''' + staging_dataset + '.bollywoodActress' 

create_titles_sql =  'CREATE OR REPLACE TABLE ' + modeled_dataset +'''.bollywoodTitles AS 
                        SELECT DISTINCT Title title, Release_Month MMM, CAST(Release_Date AS INT64) AS DD, Year YYYY, 
                        CAST(Highest_Grosser_By_Year_in_crores_ AS NUMERIC) croresGrossed 
                        FROM ''' + staging_dataset + '.bollywood'

create_directs_sql = 'CREATE OR REPLACE TABLE ' + modeled_dataset + '''.bollywoodDirects AS
                        SELECT DISTINCT Director director, Title title
                        FROM ''' + staging_dataset + '.bollywood'

create_genres_sql = 'CREATE OR REPLACE TABLE ' + modeled_dataset + '''.bollywoodGenres AS
                        SELECT DISTINCT Title title, Genre genre
                        FROM ''' + staging_dataset + '''.bollywood
                        WHERE genre != "None"'''

create_cast_sql = 'CREATE OR REPLACE TABLE ' + modeled_dataset + '''.bollywoodCast AS
                        SELECT Title title, b.Cast names
                        FROM ''' + staging_dataset + '''.bollywood b
                        WHERE b.Cast != "unknown"'''


with models.DAG(
        'bollywood_workflow',
        schedule_interval=None,
        default_args=default_dag_args) as dag:

    create_staging = BashOperator(
            task_id='create_staging_dataset',
            bash_command='bq --location=US mk --dataset ' + staging_dataset)
    
    create_modeled = BashOperator(
            task_id='create_modeled_dataset',
            bash_command='bq --location=US mk --dataset ' + modeled_dataset,
            trigger_rule = 'all_done')
    
    load_actors = BashOperator(
            task_id='load_actors',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.bollywoodActor \
                         "gs://cs329e-imdb-data-csvs/bollywood-data/bollywoodActors.csv"',
            trigger_rule='all_done')

    load_actress = BashOperator(
            task_id='load_actress',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.bollywoodActress \
                         "gs://cs329e-imdb-data-csvs/bollywood-data/bollywoodActress.csv"',
            trigger_rule='one_success')
    
    load_bollywood = BashOperator(
            task_id='load_bollywood',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.bollywood \
                         "gs://cs329e-imdb-data-csvs/bollywood-data/bollywood.csv"',
            trigger_rule='one_success')
    
    create_bollywoodActs = BashOperator(
            task_id='create_bollywoodActs',
            bash_command=bq_query_start + "'" + create_acts_sql + "'")#, 
            #trigger_rule='one_success')
    
    create_bollywoodTitles = BashOperator(
            task_id='create_bollywoodTitles',
            bash_command=bq_query_start + "'" + create_titles_sql + "'")#, 
            #trigger_rule='one_success')
    
    create_bollywoodDirects = BashOperator(
            task_id='create_bollywoodDirects',
            bash_command=bq_query_start + "'" + create_directs_sql + "'")#, 
            #trigger_rule='one_success')
    
    create_bollywoodGenres = BashOperator(
            task_id='create_bollywoodGenres',
            bash_command=bq_query_start + "'" + create_genres_sql + "'")#, 
            #trigger_rule='one_success')
    
    create_bollywoodCast = BashOperator(
            task_id='create_bollywoodCast',
            bash_command=bq_query_start + "'" + create_cast_sql + "'", 
            trigger_rule='one_success')
    
    branch = DummyOperator(
            task_id='branch',
            trigger_rule='all_done')
    
    branch2 = DummyOperator(
            task_id='branch2',
            trigger_rule='all_done')
    
    branch3 = DummyOperator(
            task_id='branch3',
            trigger_rule='all_done')
    
    join = DummyOperator(
            task_id='join',
            trigger_rule='all_done')
    
    join2 = DummyOperator(
            task_id='join2',
            trigger_rule='all_done')
        
    join3 = DummyOperator(
            task_id='join3',
            trigger_rule='all_done')
    
    bollywoodCast_df = BashOperator(
            task_id='bollywoodCast_df',
            bash_command='python /home/jupyter/airflow/dags/bollywoodCast_beam_dataflow.py')
    
    bollywoodDirects_df = BashOperator(
            task_id='bollywoodDirects_df',
            bash_command='python /home/jupyter/airflow/dags/bollywoodDirects_beam_dataflow.py')

    bollywoodGenres_df = BashOperator(
            task_id='bollywoodGenres_df',
            bash_command='python /home/jupyter/airflow/dags/bollywoodGenres_beam_dataflow.py')
    
    bollywoodTitles_df = BashOperator(
            task_id='bollywoodTitles_df',
            bash_command='python /home/jupyter/airflow/dags/bollywoodTitles_beam_dataflow.py')
        
    create_staging >> create_modeled >> branch
    branch >> load_actors >> join
    branch >> load_actress >> join
    join >> create_bollywoodActs >> load_bollywood >> branch2
    branch2 >> create_bollywoodTitles >> join2
    branch2 >> create_bollywoodDirects >> join2
    branch2 >> create_bollywoodGenres >> join2
    branch2 >> create_bollywoodCast >> join2
    join2 >> branch3
    branch3 >> bollywoodCast_df >> join3
    branch3 >> bollywoodDirects_df >> join3
    branch3 >> bollywoodGenres_df >> join3
    branch3 >> bollywoodTitles_df >> join3
    join3
    