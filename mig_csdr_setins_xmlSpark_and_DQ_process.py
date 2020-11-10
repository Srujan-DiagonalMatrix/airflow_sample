from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.sensors import HdfsSensor

# argument file variables paths
script = 'run_csdr_spark.sh'
xml_File = 'SPV_1000001858_2018-09-04_11-17-14_NCAGB_DATISR_CSDR9_DE-4PQUHN3JPFGFNF3BB653-2018-Q2.xml'
ScriptArg = 'settlement_internalisation'
scriptFile = '/opt/scripts/mig/csdr/csdr_spark/shell_script/{}'.format(script)
xmlFile = '/mig/data/raw/csdr/settlement_internalisation/to_process/{}'.format(xml_File)
csdrDQ_path = '/opt/scripts/csdr/mig/csdr_data_quality_framework/'
csdrDQ_sscript = '{}shell_script/run_csdr_dq2.sh'.format(csdrDQ_path)
csdrDQ_json = '{}app/resources/settlement_internalisation.json'.format(csdrDQ_path)
csdrDQG_log = 'hdfs://migration/data/raw/csdr/stage_status/'

hdfs_dir = xmlFile

hqlPath = '/opt/scripts/mig/csdr/hive_scripts/'
xmlCommand = "sudo sh {} {}".format(scriptFile, ScriptArg)
hqlCommand = 'hive -f {}populate_consumed_mv_tables.hql'.format(hqlPath)
DQcommand = 'sudo sh {} -j {} -l {}csdr_status.log'.format(csdrDQ_sscript, csdrDQ_json, csdrDQG_log)

args = {
    'owner':'Airflow',
    'start_date': days_ago(1)
}

with DAG(dag_id='mig_csdr_setins_xmlSpark_and_DQ_process', description='CSDR xml Data Quality checking process', default_args=args, schedule_interval='* * */1 * *') as dag:

    start = DummyOperator(
        task_id='start'
    )

    xmlFileCheckSensor = HdfsSensor(
        task_id='xmlFile_sensor',
        hdfs_conn_id='webhdfs_default',
        filepath=xmlFile
    )

    xmlSpark_DQ_process = SSHOperator(
        ssh_conn_id='zaloni',
        task_id='xmlSpark_DQ_process',
        command=xmlCommand
    )

    refreshMV_hiveQuery = SSHOperator(
        ssh_conn_id='zaloni',
        task_id='refreshMV_hiveQuery',
        command=hqlCommand
    )

    data_quality = SSHOperator(
        ssh_conn_id='zaloni',
        task_id='data_quality',
        command=DQcommand
    )

    start >> xmlFileCheckSensor >> xmlSpark_DQ_process >> refreshMV_hiveQuery >> data_quality
