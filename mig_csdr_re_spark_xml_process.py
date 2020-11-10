from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator

# argument file variables paths
spark_script = 'spark_app.py'
jar = 'spark-xml_2.10-0.4.1.jar'
json = 'review_and_evaluation_config.json'
logFile = 'csdr_status.log'

scriptFile = '/opt/scripts/mig/csdr/csdr_spark/app/{}'.format(spark_script)
jarPath = '/opt/scripts/mig/csdr/lib/{}'.format(jar)
xmlFilePath = 'hdfs://mig/data/raw/csdr/review_evaluation/to_process/'
jsonPath = '/opt/scripts/csdr/mig/csdr_spark_xml_process/conf/{}'.format(json)
logPath = 'hdfs://migration/data/raw/csdr/stage_status/{}'.format(logFile)
a
xml_validation = 'sudo sh client_spark_submit.sh {} {} {} {} {}'.format(scriptFile, jarPath, xmlFilePath, jsonPath, logPath)

args = {
    'owner':'Airflow',
    'start_date': days_ago(1)
}

with DAG(dag_id='mig_csdr_re_spark_xml_process', description='CSDR xml re validation process', default_args=args, schedule_interval='* * */1 * *') as dag:

    start = DummyOperator(
        task_id='start'
    )

    xml_validation_process = SSHOperator(
        ssh_conn_id='zaloni',
        task_id='esml_feedback_process',
        command=xml_validation
    )

    start >> xml_validation_process
