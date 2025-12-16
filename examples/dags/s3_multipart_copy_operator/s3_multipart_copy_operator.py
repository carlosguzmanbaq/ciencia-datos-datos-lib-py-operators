"""
   DAG modelo para explicar el funcionanmiento del operador `S3MultipartCopyOperator`
"""
import pendulum

# pylint: disable=import-error

from airflow import DAG
from airflow.models import Variable

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.empty import EmptyOperator

from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule


from airflow_operators.s3_multipart_copy_operator.s3_multipart_copy_operator import S3MultipartCopyOperator


# =====================================================================================
#  VARIABLES DE AMBIENTE
# =====================================================================================

ENV = Variable.get('env')

COUNTRY = "CO"
TEAM = "PRAGMA"
DOMAIN = "CIENCIA_DATOS"
SUBDOMAIN = "EXAMPLE"
PRODUCT = "MULTIPARTCOPYOPERATOR"
FREQUENCY = 'DI'


# =====================================================================================
#  ARGUMENTOS DEL DAG
# =====================================================================================

DAG_ID = f"{COUNTRY}_{TEAM}_{DOMAIN}_{SUBDOMAIN}_{PRODUCT}{FREQUENCY}"
DAG_TIMEZONE = pendulum.timezone('America/Bogota')

DAG_DESCRIPTION = """
Dag dummy para realizar las pruebas de operador `S3MultipartCopyOperator`
"""

# =====================================================================================
#  CUENTAS Y CONEXIONES AWS
# =====================================================================================

ACCOUNT = {
    "PRAGMA": {
        "pdn": "123456789"
    },
}

PRAGMA_CONNECTION_ID = "pragma_productos"

# =====================================================================================
#  S3 Sensors
# =====================================================================================

BUCKET_PRAGMA_DATA = f"co-pragma-productos-data-{ACCOUNT[DOMAIN][ENV]}-{ENV}"
BUCKET_PRAGMA_OUTPUT = f"co-pragma-productos-output-{ACCOUNT[DOMAIN][ENV]}-{ENV}"

# Rutas base
BASE_PATH = "dummy/S3MultipartCopyOperator"
ORIGIN_PATH = f"{BASE_PATH}/origin"
DEST_PATH = f"{BASE_PATH}/destination"

# Archivos de origen
PREFIX_LIGHT_FILE_KEY = f"{ORIGIN_PATH}/under5Gb/file1.csv"
PREFIX_HEAVY_FILE_KEY = f"{ORIGIN_PATH}/greater5Gb/part-00000-f72c0fc6-2ab5-4997-bfb3-99ade29afc3f-c000.csv"

# Archivos de destino
DEST_LIGHT_FILE_KEY = f"{DEST_PATH}/under5Gb/file1.csv"
DEST_HEAVY_FILE_KEY = f"{DEST_PATH}/greater5Gb/file1.csv"

# Ingest Sensors
INGEST_SENSORS_LIGHT = {
    "task_id": "sensor_light_pragma",
    "bucket_name": BUCKET_PRAGMA_DATA,
    "aws_conn_id": PRAGMA_CONNECTION_ID,
    "prefix_key": PREFIX_LIGHT_FILE_KEY,
}

INGEST_SENSORS_HEAVY = {
    "task_id": "sensor_heavy_pragma",
    "bucket_name": BUCKET_PRAGMA_DATA,
    "aws_conn_id": PRAGMA_CONNECTION_ID,
    "prefix_key": PREFIX_HEAVY_FILE_KEY,
}

# Sensor Parameters
SENSOR_TIMEOUT = 60*60
POKE_INTERVAL = 60*5
SENSOR_MODE = "reschedule"

# =====================================================================================
#  FUNCIONES AUXILIARES
# =====================================================================================

def create_s3_sensor(task_id, bucket_key, wildcard_match=True) -> S3KeySensor:
    """Crea un sensor S3 con configuración estándar"""
    return S3KeySensor(
        task_id=task_id,
        poke_interval=POKE_INTERVAL,
        timeout=SENSOR_TIMEOUT,
        retries=0,
        wildcard_match=wildcard_match,
        bucket_key=bucket_key,
        bucket_name=BUCKET_PRAGMA_DATA,
        aws_conn_id=PRAGMA_CONNECTION_ID,
        mode=SENSOR_MODE,
        deferrable=True,
        soft_fail=True,
    )

# =====================================================================================
#  DEFINICIÓN DEL DAG
# =====================================================================================

with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2025, 1, 1, tz=DAG_TIMEZONE),
    schedule_interval=None,
    description=DAG_DESCRIPTION,
    doc_md=DAG_DESCRIPTION,
    catchup=False,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    with TaskGroup(group_id='S3SimpleCopy') as simple_copy_group:

        s3_sensor_light_task = create_s3_sensor(
            task_id=INGEST_SENSORS_LIGHT['task_id'],
            bucket_key=INGEST_SENSORS_LIGHT['prefix_key']
        )

        s3_copy_light_task = S3MultipartCopyOperator(
            task_id="copy_light_file",
            source_bucket_name=BUCKET_PRAGMA_DATA,
            source_bucket_key=PREFIX_LIGHT_FILE_KEY,
            dest_bucket_name=BUCKET_PRAGMA_OUTPUT,
            dest_bucket_key=DEST_LIGHT_FILE_KEY,
            aws_conn_id=PRAGMA_CONNECTION_ID,
        )

        s3_sensor_light_copy_task = create_s3_sensor(
            task_id="sensor_light_copy_verification",
            bucket_key=DEST_LIGHT_FILE_KEY,
            wildcard_match=False
        )

        s3_sensor_light_task >> s3_copy_light_task >> s3_sensor_light_copy_task

    with TaskGroup(group_id='S3MultipartCopy') as simple_multipart_group:

        s3_sensor_heavy_task = create_s3_sensor(
            task_id=INGEST_SENSORS_HEAVY['task_id'],
            bucket_key=INGEST_SENSORS_HEAVY['prefix_key']
        )

        s3_copy_heavy_task = S3MultipartCopyOperator(
            task_id="copy_heavy_file",
            source_bucket_name=BUCKET_PRAGMA_DATA,
            source_bucket_key=PREFIX_HEAVY_FILE_KEY,
            dest_bucket_name=BUCKET_PRAGMA_OUTPUT,
            dest_bucket_key=DEST_HEAVY_FILE_KEY,
            aws_conn_id=PRAGMA_CONNECTION_ID,
            part_size_mb=200,
        )

        s3_sensor_heavy_copy_task = create_s3_sensor(
            task_id="sensor_heavy_copy_verification",
            bucket_key=DEST_HEAVY_FILE_KEY,
            wildcard_match=False
        )

        s3_sensor_heavy_task >> s3_copy_heavy_task >> s3_sensor_heavy_copy_task

    start >> [simple_copy_group, simple_multipart_group] >> end
