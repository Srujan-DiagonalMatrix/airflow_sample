from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
import mig_csdr_re_spark_xml_process as re_validate_xml

# argument file variables paths
spark_script = 'main.py'
json = 'review_and_evaluation_config.json'
logFile = 'csdr_status.log'

scriptFile = '/opt/scripts/mig/csdr/csdr_xml_validation/app/{}'.format(spark_script)
xmlFilePath = 'hdfs://migration/data/raw/csdr/settlement_internalisation/to_process/'
jsonPath = '/opt/scripts/csdr/mig/conf/{}'.format(json)
logPath = 'hdfs://migration/data/raw/csdr/stage_status/{}'.format(logFile)

xml_validation = 'sudo python {} -f {} -j {} -l {}'.format(scriptFile, xmlFilePath, jsonPath, logPath)

args = {
    'owner':'Airflow',
    'start_date': days_ago(1)
}

with DAG(dag_id='mig_csdr_re_xml_validation_spark_xml_process', description='CSDR xml re validation spark process', default_args=args, schedule_interval='* * */1 * *') as dag:

    start = DummyOperator(
        task_id='start'
    )

    post_ingestion = SSHOperator(
        ssh_conn_id='zaloni',
        task_id='esml_feedback_process',
        command=xml_validation
    )

    start >> post_ingestion #>> re_validate_xml
