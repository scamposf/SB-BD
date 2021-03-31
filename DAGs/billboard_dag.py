from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator,DataProcPySparkOperator,DataprocClusterDeleteOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime,timedelta , date 
from airflow.operators import BashOperator 
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import PythonOperator
import time

fecha = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())
route = 'gs://billboard8817/descomprimido/charts.csv'

def my_function(*time_delay):
    time.sleep(time_delay[0])

    return None

DAG_VAR = Variable.get('project_id', deserialize_json = True)
proyecto = DAG_VAR['variable1']

DEFAULT_DAG_ARGS = {
    'owner':"sebas",
    'depends_on_past' : False,
    'start_date':fecha,
    'email_on_failure':False,
    'email_on_retry':False,
    'retries': 1,
    'retry_delay':timedelta(minutes=5),
    'project_id':proyecto
}

with DAG("billboard_music", default_args=DEFAULT_DAG_ARGS, schedule_interval = "37 4 * * *") as dag:

    dummy = DummyOperator(
        task_id = "prueba"
    )
    
    descomprimir = BashOperator(
        task_id = "descomprime",
        bash_command = "gcloud dataflow jobs run descomprimir --region=us-east4 --gcs-location gs://dataflow-templates/latest/Bulk_Decompress_GCS_Files --parameters inputFilePattern=gs://billboard8817/*.zip,outputDirectory=gs://billboard8817/Descompresed,outputFailureFile=gs://billboard8817/failure2"
        
    )

    crear_cluster = DataprocClusterCreateOperator(

        task_id = "create_cluster",
        cluster_name = "ephemeral-spark-cluster-{{ds_nodash}}",
        master_machine_type="n1-standard-1",
        master_disk_size=50,
        worker_machine_type="n1-standard-1",
        worker_disk_size=50,
        num_workers=2,
        region="us-west1",
        zone="us-west1-a",
        image_version='1.4'
    )

    pyspark = DataProcPySparkOperator(
        task_id = "run_pyspark",
        main = route,
        cluster_name="ephemeral-spark-cluster-{{ds_nodash}}",
        region="us-west1"
    )

    borrar_cluster = DataprocClusterDeleteOperator(

        task_id ="borrar_cluster",
        cluster_name="ephemeral-spark-cluster-{{ds_nodash}}",
        region="us-west1",
        trigger_rule = TriggerRule.ALL_DONE
    )
    
    dummy_final = DummyOperator(
        task_id = "prueba_final"
    )

    delay = PythonOperator(
	    task_id = "delay1",
        python_callable = my_function,
        op_args=[200]
    )

    dummy.dag = dag
    dummy>>descomprimir>>delay>>crear_cluster>>pyspark>>borrar_cluster>>dummy_final
