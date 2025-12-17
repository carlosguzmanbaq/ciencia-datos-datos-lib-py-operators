"""
Ejemplo de DAG usando los operadores FileFerry.

Este DAG un flujo de referencia para el uso de las operaciones
de subida de archivos desde S3 hacia un servidor SFTP.
"""
import json
import pendulum

# pylint: disable=import-error, pointless-statement, invalid-name
from airflow_operators.file_ferry_operator.file_ferry_operator import FileFerryOperator
from airflow_sensors.file_ferry_sensor.file_ferry_sensor import FileFerryCompletionSensor

from airflow import DAG
from airflow.models import Variable

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator

from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup


# =====================================================================================
#  VARIABLES DE AMBIENTE
# =====================================================================================

ENV = Variable.get("env")

COUNTRY = 'CO'
TEAM = 'PRAGMA'
DOMAIN = 'CIENCIA_DATOS'
SUBDOMAIN = 'DATOS'
PRODUCT = 'FILEFERRY'

DAG_TIMEZONE = pendulum.timezone('America/Bogota')

# =====================================================================================
#  CONFIGURACIÓN DEL DAG
# =====================================================================================

DAG_ID = f"{COUNTRY}_{TEAM}_{DOMAIN}_{SUBDOMAIN}_{PRODUCT}"

DAG_DESCRIPTION = """
Dag dummy para realizar las pruebas de operador `FileFerryOperator`
"""

# =====================================================================================
#  CUENTAS Y CONEXIONES AWS
# =====================================================================================
ACCOUNT = {
    "PRAGMA": {
        "pdn": "123456789"
    },
}

PRAGMA_CONNECTION_ID = "pragma"


# =====================================================================================
# CONECTOR TRANSFER FAMILY
# =====================================================================================

CONNECTOR_ID = "c-123456789"

# =====================================================================================
# BUCKETS S3
# =====================================================================================

PARTNER = "file_ferry"
LIST_DIRECTORY_PREFIX = "list_file_ferry"
BUCKET_SOURCE = f"co-pragma -sandbox-input-{ACCOUNT[SUBDOMAIN][ENV]}-{ENV}"
PREFIX_SOURCE = f"{PARTNER}/to_transfer/from_cloud/"
LIST_DIRECTORY_PATH = f"/{BUCKET_SOURCE}/{PARTNER}/{LIST_DIRECTORY_PREFIX}"

# =====================================================================================
# SFTP PATHS
# =====================================================================================

SFTP_DESTINATION = "/sftp_root/uploads/file_ferry/cloud_test"

# =====================================================================================
#  FUNCIONES DE UTILIDAD
# =====================================================================================

def prepare_file_paths(**context):
    """
    Prepara una lista de rutas de archivos para ser enviadas al FileFerryOperator.
    """
    s3_paths = context['task_instance'].xcom_pull(task_ids='PreprocessGroup.list_s3_files')

    # Se descarta el primer elemento porque es el prefix
    s3_paths = s3_paths[1:]

    # Retornar los elementos de la lista s3_paths concatenados con el BUCKET_SOURCE
    return [f"/{BUCKET_SOURCE}/{path}" for path in s3_paths]

def extract_transfer_ids(**context):
    """Extrae transfer_ids del resultado de upload."""
    upload_result = context['task_instance'].xcom_pull(
        task_ids='FileFerryGroup.TransferGroup.upload_files_to_sftp'
    )

    print("Upload result: %s", upload_result)

    transfer_ids = []

    if 'batch_results' in upload_result:
        for result in upload_result['batch_results']:
            if 'transfer_id' in result:
                transfer_ids.append(result['transfer_id'])

    print("Transfer ID DAG: %s", transfer_ids)
    return transfer_ids

def check_validation_results(**context):
    """
    Valida los resultados de las transferencias y decide el flujo
    """
    transfer_results = context['task_instance'].xcom_pull(
        task_ids='FileFerryGroup.ValidationGroup.get_transfer_status'
    )

    print(f'Validando resultados: {transfer_results} TYPE: {type(transfer_results)}')

    if len(transfer_results["completed_files"]) > 0 and len(transfer_results["failed_files"]) > 0:
        print("Hay transferencias de archivos fallidas")
        return 'FileFerryGroup.DeleteGroup.get_s3_paths'

    if len(transfer_results["completed_files"]) == 0 and len(transfer_results["failed_files"]) > 0:
        print("Todas las transferencias de archivos fallaron")
        return 'FileFerryGroup.transfer_validation'

    print("Todas las transferencias de archivos fueron completadas")
    return 'FileFerryGroup.DeleteGroup.get_s3_paths'


def convert_to_s3_paths(**context) -> list:
    """
    Convierte una lista de rutas locales a rutas de S3.
    """
    transfer_results = context['task_instance'].xcom_pull(
        task_ids='FileFerryGroup.ValidationGroup.get_transfer_status'
    )

    if len(transfer_results["completed_files"]) == 0:
        print("No hay lista de archivos completados")
        return transfer_results

    # Para obtener el bucker_name de una esctructura como esta: /bucket_name/prefix
    transfer_results["bucket_name"] = transfer_results["completed_files"][0].split("/")[1]

    # Quitar el bucket_name de la lista de archivos completados
    transfer_results["completed_files"] = [
        f.split("/", 2)[2] for f in transfer_results["completed_files"]
    ]

    return transfer_results

def delete_files_s3(**context):
    """
    Elimina una lista de archivos en un bucket en S3
    """
    hook = S3Hook(aws_conn_id=PRAGMA_CONNECTION_ID)

    bucket_name = context['task_instance'].xcom_pull(
        task_ids='FileFerryGroup.DeleteGroup.get_s3_paths'
    )['bucket_name']

    keys = context['task_instance'].xcom_pull(
        task_ids='FileFerryGroup.DeleteGroup.get_s3_paths'
    )['completed_files']

    print("Eliminando de S3 los archivos completados")
    hook.delete_objects(bucket=bucket_name, keys=keys)

def read_s3_file(**context):
    """
    Lee un archivo de S3 y lo retorna como un string
    """
    hook = S3Hook(aws_conn_id=PRAGMA_CONNECTION_ID)

    file_name = context['task_instance'].xcom_pull(
        task_ids='FileFerryGroup.ListingGroup.list_directory'
    )['output_filename']

    key = f"{PARTNER}/{LIST_DIRECTORY_PREFIX}/{file_name}"

    print(f"Reading file: {key}")
    return hook.read_key(key=key, bucket_name=BUCKET_SOURCE)

def get_data_to_notification(**context):
    """
    Obtiene los datos de la notificación
    """
    transfer_results = context['task_instance'].xcom_pull(
        task_ids='FileFerryGroup.ValidationGroup.get_transfer_status'
    )

    print(f"Completed Files: {transfer_results['completed_files']}")
    print(f"Failed Files: {transfer_results['failed_files']}")

    failed_results = []
    for result in transfer_results['results']:
        for file in result['file_results']:
            if file["status"] == "FAILED":
                failed_results.append(file)

    if len(transfer_results["completed_files"]) > 0 and len(transfer_results["failed_files"]) > 0:
        print("Hay transferencias de archivos fallidas")

        return {
            "status": "PARTIALLY_COMPLETED",
            "message": "Hay transferencias de archivos fallidas",
            "results": failed_results
        }

    if len(transfer_results["completed_files"]) == 0 and len(transfer_results["failed_files"]) > 0:
        print("Todas las transferencias de archivos fallaron")

        return {
            "status": "FAILED",
            "message": "Todas las transferencias de archivos fallaron",
            "results": failed_results
        }

    print("Todas las transferencias de archivos fueron completadas")
    return {
        "status": "COMPLETED",
        "message": "Todas las transferencias de archivos fueron completadas",
        "listing_results": json.loads(
            context['task_instance'].xcom_pull(
                task_ids='FileFerryGroup.ListingGroup.read_listing'
            )
        )
    }


# =====================================================================================
#  DEFINICIÓN DEL DAG
# =====================================================================================

with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2025, 1, 1, tz=DAG_TIMEZONE),
    description=DAG_DESCRIPTION,
    doc_md=DAG_DESCRIPTION,
    catchup=False,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule=TriggerRule.ALL_DONE)

    with TaskGroup(group_id='PreprocessGroup') as preprocess_group:
        s3_files = S3ListOperator(
            task_id="list_s3_files",
            bucket=BUCKET_SOURCE,
            prefix=PREFIX_SOURCE,
            aws_conn_id=PRAGMA_CONNECTION_ID,
        )

        prepare_files = PythonOperator(
            task_id='prepare_file_paths',
            python_callable=prepare_file_paths
        )

        s3_files >> prepare_files

    with TaskGroup(group_id='FileFerryGroup') as file_ferry_group:
        with TaskGroup(group_id='TransferGroup') as transfer_group:
            # === Upload simple S3 → SFTP ===
            upload_files = FileFerryOperator(
                task_id='upload_files_to_sftp',
                operation='upload',
                environment=ENV,
                connector_id=CONNECTOR_ID,
                aws_conn_id=PRAGMA_CONNECTION_ID,
                files="{{ task_instance.xcom_pull(task_ids='PreprocessGroup.prepare_file_paths') }}",
                destination_path=SFTP_DESTINATION
            )

            get_transfer_ids = PythonOperator(
                task_id='extract_transfer_ids',
                python_callable=extract_transfer_ids
            )

            # === Sensor para esperar completación de transferencias ===
            wait_upload_completion = FileFerryCompletionSensor(
                task_id='wait_upload_completion',
                environment=ENV,
                connector_id=CONNECTOR_ID,
                aws_conn_id=PRAGMA_CONNECTION_ID,
                transfer_ids="{{ task_instance.xcom_pull(task_ids='FileFerryGroup.TransferGroup.extract_transfer_ids') }}",
                poke_interval=10,
                timeout=30,
            )


            # Flujo de dependencias
            upload_files >> get_transfer_ids
            get_transfer_ids >> wait_upload_completion

        with TaskGroup(group_id='ValidationGroup') as validation_group:

            # === Get status de transferencias ===
            get_transfer_status = FileFerryOperator(
                task_id='get_transfer_status',
                operation='get_status',
                environment=ENV,
                connector_id=CONNECTOR_ID,
                aws_conn_id=PRAGMA_CONNECTION_ID,
                transfer_ids="{{ task_instance.xcom_pull(task_ids='FileFerryGroup.TransferGroup.extract_transfer_ids') }}",
            )

            # Validación de resultados con BranchPythonOperator
            validation_check = BranchPythonOperator(
                task_id='check_validation_results',
                python_callable=check_validation_results,
                trigger_rule=TriggerRule.ALL_SUCCESS
            )

            get_transfer_status >> validation_check

        with TaskGroup(group_id='DeleteGroup') as delete_group:
            get_s3_paths = PythonOperator(
                task_id='get_s3_paths',
                python_callable=convert_to_s3_paths
            )

            delete_files = PythonOperator(
                task_id='delete_completed_files',
                python_callable=delete_files_s3
            )

            # Validación de resultados con BranchPythonOperator
            validation_check = BranchPythonOperator(
                task_id='check_validation_results',
                python_callable=check_validation_results,
                trigger_rule=TriggerRule.ALL_SUCCESS
            )

            get_s3_paths >> delete_files
            get_s3_paths >> delete_files >> validation_check

        with TaskGroup(group_id='ListingGroup') as listing_group:
            # === listar directorio en el SFTP ===
            list_directory = FileFerryOperator(
                task_id='list_directory',
                operation='list_directory',
                environment=ENV,
                connector_id=CONNECTOR_ID,
                aws_conn_id=PRAGMA_CONNECTION_ID,
                sftp_path=SFTP_DESTINATION,
                output_directory_path=LIST_DIRECTORY_PATH
            )

            list_directory_results = PythonOperator(
                task_id='read_listing',
                python_callable=read_s3_file
            )

            list_directory >> list_directory_results

        transfer_validation = EmptyOperator(
            task_id="transfer_validation",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
        )
        transfer_group >> validation_group >> transfer_validation
        validation_group >> delete_group >> listing_group
        delete_group >> transfer_validation

    with TaskGroup(
        group_id='NotificationGroup',
        default_args={'trigger_rule': TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS}
    ) as notification_group:

        data_to_notification = PythonOperator(
            task_id='data_to_notification',
            python_callable=get_data_to_notification
        )

        send_notification = EmptyOperator(
            task_id="send_notification"
        )

        data_to_notification >> send_notification

    # Flujo de dependencias
    start >> preprocess_group >> file_ferry_group >> notification_group >> end
    transfer_validation >> notification_group >> end
