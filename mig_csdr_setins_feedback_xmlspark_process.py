from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator

# argument file variables paths
spark_script = 'spark_app.py'
jar = 'spark-xml_2.10-0.4.1.jar'
json = 'esma_feedback_report_config.json'
logFile = 'csdr_status.log'

xmlFilePath = 'hdfs://mig/data/raw/csdr/esma_feedback/to_process/'
scriptFile = '/opt/scripts/mig/csdr/csdr_spark/app/{}'.format(spark_script)
jarPath = '/opt/scripts/mig/csdr/lib/{}'.format(jar)
jsonPath = '/opt/scripts/csdr/mig/csdr_spark_xml_process/conf/{}'.format(json)
logPath = 'hdfs://migration/data/raw/csdr/stage_status/{}'.format(logFile)

spark_submit_script = 'sudo sh client_spark_submit.sh {} {} {} {} {}'.format(scriptFile, jarPath, xmlFilePath, jsonPath, logPath)

args = {
    'owner':'Airflow',
    'start_date': days_ago(1)
}

with DAG(dag_id='mig_csdr_setins_feedback_xmlspark_process', description='CSDR xml feedback process', default_args=args, schedule_interval='* * */1 * *') as dag:

    start = DummyOperator(
        task_id='start'
    )

    feedback_process = SSHOperator(
        ssh_conn_id='zaloni',
        task_id='esml_feedback_process',
        command=spark_submit_script
    )

    start >> feedback_process
