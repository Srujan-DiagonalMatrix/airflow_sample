from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator

# argument file variables paths
spark_script = 'main.py'
json = 'esma_feedback_xml_validation_config.json'
logFile = 'csdr_status.log'

xmlFilePath = 'hdfs://migration/data/raw/csdr/settlement_internalisation/to_process/'
scriptFile = '/opt/scripts/mig/csdr/csdr_xml_validation/app/{}'.format(spark_script)
jsonPath = '/opt/scripts/csdr/mig/conf/{}'.format(json)
logPath = 'hdfs://migration/data/raw/csdr/stage_status/{}'.format(logFile)

xml_validation = 'sudo sh {} -f {} -j {} -l {}'.format(scriptFile, xmlFilePath, jsonPath, logPath)

args = {
    'owner':'Airflow',
    'start_date': days_ago(1)
}

with DAG(dag_id='mig_csdr_setins_feedback_xml_validation', description='CSDR xml feedback validation', default_args=args, schedule_interval='* * */1 * *') as dag:

    start = DummyOperator(
        task_id='start'
    )

    xml_validation_process = SSHOperator(
        ssh_conn_id='zaloni',
        task_id='esml_feedback_process',
        command=xml_validation
    )

    start >> xml_validation_process
