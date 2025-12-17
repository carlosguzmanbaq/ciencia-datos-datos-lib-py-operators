"""
Custom Airflow Operator para invocar la Lambda File Ferry.

Este operador permite ejecutar operaciones de transferencia de archivos
entre S3 y SFTP usando la función Lambda file_ferry.
"""
import json
import logging
from typing import Dict, Any, List, Optional, Union

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook
from airflow.utils.context import Context
from airflow.exceptions import AirflowException


logger = logging.getLogger(__name__)


class FileFerryOperator(BaseOperator):
    """
    Operador personalizado para ejecutar operaciones de transferencia de archivos
    usando la función Lambda file_ferry.

    Soporta las siguientes operaciones:
    - upload: S3 → SFTP
    - download: SFTP → S3
    - delete: Eliminar archivos SFTP
    - list_directory: Listar directorio SFTP
    - get_status: Consultar estado de transferencias
    """

    # Configuración base del nombre de la función Lambda
    LAMBDA_BASE_NAME = 'file_ferry'

    template_fields = [
        'operation', 'environment', 'files', 'connector_id', 'destination_path',
        's3_destination_path', 'sftp_path', 'transfer_ids', 'output_directory_path'
    ]

    def __init__(
        self,
        operation: str,
        environment: str,
        connector_id: str,
        aws_conn_id: str,
        files: Optional[List[str]] = None,
        destination_path: Optional[str] = None,
        s3_destination_path: Optional[str] = None,
        sftp_path: Optional[str] = None,
        transfer_ids: Optional[List[str]] = None,
        max_items: Optional[int] = None,
        output_directory_path: Optional[str] = None,
        region_name: str = 'us-east-1',
        **kwargs
    ):
        """
        Inicializa el operador FileFerry.

        Args:
            operation: Tipo de operación (upload, download, delete, etc.)
            environment: Environment (dev, qc, pdn)
            aws_conn_id: ID conexión AWS (obligatorio)
            connector_id: ID del conector SFTP
            files: Lista de archivos para transferir
            destination_path: Ruta destino para uploads
            s3_destination_path: Ruta S3 destino para downloads
            sftp_path: Ruta SFTP para list_directory
            transfer_ids: Lista de IDs para get_status múltiple
            max_items: Máximo elementos para list_directory
            output_directory_path: Directorio salida para list_directory
            region_name: Región AWS
        """
        super().__init__(**kwargs)

        self.operation = operation
        self.environment = environment.lower()
        self.connector_id = connector_id
        self.files = files or []
        self.destination_path = destination_path
        self.s3_destination_path = s3_destination_path
        self.sftp_path = sftp_path
        self.transfer_ids = transfer_ids
        self.max_items = max_items
        self.output_directory_path = output_directory_path
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name

        # Construir nombre de la función Lambda dinámicamente
        self.lambda_function_name = f"{self.LAMBDA_BASE_NAME}-{self.environment}"

        # Validar operación
        valid_operations = {
            'upload', 'download', 'delete', 'list_directory',
            'get_status'
        }
        if operation not in valid_operations:
            raise ValueError(f"Invalid operation: {operation}. Valid: {valid_operations}")

    def execute(self, context: Context) -> Dict[str, Any]:
        """
        Ejecuta la operación de transferencia invocando la Lambda.

        Args:
            context: Contexto de ejecución de Airflow

        Returns:
            Dict con el resultado de la operación
        """
        logger.info("Executing FileFerry operation: %s", self.operation)

        # Crear hook de Lambda
        lambda_hook = LambdaHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name
        )

        # Construir payload para la Lambda
        payload = self._build_payload()

        logger.info("Invoking Lambda %s with payload: %s", self.lambda_function_name, payload)

        try:
            # Invocar Lambda
            response = lambda_hook.invoke_lambda(
                function_name=self.lambda_function_name,
                payload=json.dumps(payload),
                invocation_type='RequestResponse'
            )

            # Procesar respuesta
            result = self._process_response(response)

            logger.info("FileFerry operation completed successfully")
            return result

        except Exception as e:
            logger.error("FileFerry operation failed: %s", str(e))
            raise AirflowException(f"FileFerry operation failed: {str(e)}") from e


    def _normalize_list(self, list_to_normalize: Union[str, List[str]]):
        """
        Normaliza list_to_normalize para manejar strings de templating de Airflow.

        Args:
            list_to_normalize: Lista de strings o string que representa una lista

        Returns:
            Lista normalizada de list_to_normalize o None
        """

        if not list_to_normalize:
            return None

        # Si ya es una lista, devolverla tal como está
        if isinstance(list_to_normalize, list):
            return list_to_normalize

        # Si es un string, intentar convertirlo a lista
        if isinstance(list_to_normalize, str):
            # Intentar parsear como literal de Python

            cleaned = list_to_normalize.strip("[]'\"")
            items = [item.strip().strip("'\"") for item in cleaned.split(',') if item.strip()]

            return items if items else None

        return None


    def _build_payload(self) -> Dict[str, Any]:
        """
        Construye el payload para la invocación de Lambda.

        Returns:
            Dict con los parámetros para la Lambda
        """
        payload = {
            'operation': self.operation
        }

        # Agregar parámetros según la operación
        if self.connector_id:
            payload['connector_id'] = self.connector_id

        if self.files:
            self.files = self._normalize_list(self.files)
            payload['files'] = self.files

        if self.destination_path:
            payload['destination_path'] = self.destination_path

        if self.s3_destination_path:
            payload['s3_destination_path'] = self.s3_destination_path

        if self.sftp_path:
            payload['sftp_path'] = self.sftp_path

        if self.transfer_ids:
            self.transfer_ids = self._normalize_list(self.transfer_ids)
            payload['transfer_ids'] = self.transfer_ids

        if self.max_items is not None:
            payload['max_items'] = self.max_items

        if self.output_directory_path:
            payload['output_directory_path'] = self.output_directory_path

        return payload

    def _process_get_status_result(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Procesa el resultado de get_status para extraer listas de archivos por estado.

        Args:
            data: Datos del resultado de get_status

        Returns:
            Dict con el resultado original más las listas completed_files y failed_files
        """
        completed_files = []
        failed_files = []

        # Procesar results
        if 'results' in data:
            for result in data['results']:
                for file_result in result.get('file_results', []):
                    file_path = file_result.get('file_path')
                    status = file_result.get('status', '').upper()

                    if status == 'COMPLETED':
                        completed_files.append(file_path)
                    elif status == 'FAILED':
                        failed_files.append(file_path)

        # Agregar las listas al resultado original
        result = data.copy()
        result['completed_files'] = completed_files
        result['failed_files'] = failed_files

        return result

    def _process_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Procesa la respuesta de la Lambda.

        Args:
            response: Respuesta de la invocación Lambda

        Returns:
            Dict con el resultado procesado

        Raises:
            AirflowException: Si la Lambda retorna error
        """
        # Extraer payload de la respuesta
        if 'Payload' in response:
            payload_str = response['Payload'].read().decode('utf-8')
            payload = json.loads(payload_str)
        else:
            raise AirflowException("No payload in Lambda response")

        # Verificar código de estado HTTP
        status_code = payload.get('statusCode', 500)
        if status_code != 200:
            error_body = json.loads(payload.get('body', '{}'))
            error_msg = error_body.get('error', {}).get('message', 'Unknown error')
            raise AirflowException(f"Lambda returned error {status_code}: {error_msg}")

        # Extraer datos del body
        body = json.loads(payload.get('body', '{}'))

        if not body.get('success', False):
            error_msg = body.get('error', {}).get('message', 'Unknown error')
            raise AirflowException(f"FileFerry operation failed: {error_msg}")

        data = body.get('data', {})

        # Si es una operación get_status, procesar el resultado para extraer listas de archivos
        if self.operation == 'get_status':
            data = self._process_get_status_result(data)

        return data
