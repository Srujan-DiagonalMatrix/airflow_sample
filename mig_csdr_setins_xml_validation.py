from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import HdfsSensor
from airflow.contrib.operators.ssh_operator import SSHOperator

# argument file variables paths
pyScript = 'main.py'
xml_File = 'SPV_1000001858_2018-09-04_11-17-14_NCAGB_DATISR_CSDR9_DE-4PQUHN3JPFGFNF3BB653-2018-Q2.xml'
json_File = 'set_ins_xml_validation_config.json'
log_file = 'csdr_status.log'

pyFile = '/opt/scripts/mig/csdr/csdr_xml_validation/app/{}'.format(pyScript)
xmlFile = 'hdfs://migration/data/raw/csdr/settlement_internalisation/to_validate/{}'.format(xml_File)
jsonFile = '/opt/scripts/mig/csdr/csdr_xml_validation/app/resources/{}'.format(json_File)
logFile = 'hdfs://migration/data/raw/csdr/stage_status/{}'.format(log_file)

command = "sudo python {} -f {} -j {} -l {}".format(pyFile, xmlFile, jsonFile, logFile)
hdfs_dir = xmlFile

args = {
    'owner':'Airflow',
    'start_date': days_ago(1)
}

with DAG(dag_id='mig_csdr_setins_xml_validation', description='CSDR XML validation', default_args=args, schedule_interval='* * */1 * *') as dag:

    start = DummyOperator(
        task_id='start'
    )

    fileCheckSensor = HdfsSensor(
        task_id='source_data_sensor',
        hdfs_conn_id='webhdfs_default',
        filepath=hdfs_dir
    )

    xmlFileValidation = SSHOperator(
        ssh_conn_id='zaloni',
        task_id='xmlFile_Validation',
        command=command
    )

    start >> fileCheckSensor >> xmlFileValidation
