"""
Sensor personalizado para monitorear el estado de transferencias FileFerry.

Este sensor permite esperar hasta que una o múltiples transferencias
se completen antes de continuar con el flujo del DAG.
"""
import json
from typing import Dict, Any, List, Union

from airflow.sensors.base import BaseSensorOperator
from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook
from airflow.utils.context import Context
from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin


class FileFerryTransferSensor(BaseSensorOperator, LoggingMixin):
    """
    Sensor que monitorea el overall_status de transferencias FileFerry.

    Valida únicamente el campo 'overall_status' de cada transferencia,
    sin entrar en detalles de archivos individuales.
    """

    template_fields = ['environment', 'transfer_ids', 'connector_id']

    # Configuración base del nombre de la función Lambda
    LAMBDA_BASE_NAME = 'file_ferry'

    def __init__(
        self,
        environment: str,
        connector_id: str,
        aws_conn_id: str,
        transfer_ids: List[str],
        expected_status: Union[str, List[str]] = 'COMPLETED',
        fail_on_error: bool = True,
        region_name: str = 'us-east-1',
        poke_interval: int = 60,  # 1 minuto por defecto
        timeout: int = 60 * 60 * 2,  # 2 horas por defecto
        **kwargs
    ):
        """
        Inicializa el sensor FileFerry.

        Args:
            environment: Environment (dev, staging, prod)
            transfer_ids: Lista de IDs de transferencias a monitorear
            connector_id: ID del conector
            expected_status: Estado(s) esperado(s). Puede ser string o lista de strings
                           (ej: 'COMPLETED' o ['COMPLETED', 'PARTIALLY_COMPLETED'])
            fail_on_error: Si fallar cuando transferencia tiene error
            aws_conn_id: ID conexión AWS
            region_name: Región AWS
            poke_interval: Intervalo entre verificaciones (segundos)
            timeout: Timeout total del sensor (segundos)
        """
        super().__init__(
            poke_interval=poke_interval,
            timeout=timeout,
            **kwargs
        )
        self.environment = environment.lower()
        self.transfer_ids = transfer_ids
        self.connector_id = connector_id
        # Normalizar expected_status a lista de estados en mayúsculas
        if isinstance(expected_status, str):
            self.expected_statuses = [expected_status.upper()]
        else:
            self.expected_statuses = [status.upper() for status in expected_status]
        self.fail_on_error = fail_on_error
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name

        # Construir nombre de la función Lambda dinámicamente
        self.lambda_function_name = f"{self.LAMBDA_BASE_NAME}-{self.environment}"

        # Validar parámetros
        if not transfer_ids:
            raise ValueError("Debe especificar transfer_ids")

        if not connector_id:
            raise ValueError("connector_id es requerido")


    def poke(self, context: Context) -> bool:
        """
        Verifica el estado de las transferencias.

        Args:
            context: Contexto de ejecución de Airflow

        Returns:
            True si todas las transferencias están en el estado esperado
        """

        self.transfer_ids = self._normalize_transfer_ids(self.transfer_ids)

        self.log.info("Checking transfer status - IDs: %s, TYPE: %s", self.transfer_ids, type(self.transfer_ids))

        try:
            # Obtener estado actual
            status_data = self._get_transfer_status()

            # Verificar estado de las transferencias
            return self._check_multiple_transfer_status(status_data)

        except Exception as e:
            self.log.error("Error checking transfer status: %s", str(e))
            if self.fail_on_error:
                raise AirflowException(f"Error checking transfer status: {str(e)}") from e
            return False


    def _normalize_transfer_ids(self, transfer_ids: Union[str, List[str]]):
        """
        Normaliza transfer_ids para manejar strings de templating de Airflow.

        Args:
            transfer_ids: Lista de IDs o string que representa una lista

        Returns:
            Lista normalizada de transfer_ids o None
        """

        if not transfer_ids:
            return None

        # Si ya es una lista, devolverla tal como está
        if isinstance(transfer_ids, list):
            return transfer_ids

        # Si es un string, intentar convertirlo a lista
        if isinstance(transfer_ids, str):
            # Intentar parsear como literal de Python

            cleaned = transfer_ids.strip("[]'\"")
            ids = [id.strip().strip("'\"") for id in cleaned.split(',') if id.strip()]

            return ids if ids else None

        return None


    def _get_transfer_status(self) -> Dict[str, Any]:
        """
        Obtiene el estado de las transferencias desde la Lambda.

        Returns:
            Dict con el estado de las transferencias
        """
        # Crear hook de Lambda
        lambda_hook = LambdaHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name
        )

        # Construir payload
        payload = {
            'operation': 'get_status',
            'transfer_ids': self.transfer_ids,
            'connector_id': self.connector_id
        }

        # Invocar Lambda
        response = lambda_hook.invoke_lambda(
            function_name=self.lambda_function_name,
            payload=json.dumps(payload),
            invocation_type='RequestResponse'
        )

        # Procesar respuesta
        if 'Payload' in response:
            payload_str = response['Payload'].read().decode('utf-8')
            lambda_response = json.loads(payload_str)
        else:
            raise AirflowException("No payload in Lambda response")

        # Verificar código de estado
        status_code = lambda_response.get('statusCode', 500)
        if status_code != 200:
            error_body = json.loads(lambda_response.get('body', '{}'))
            error_msg = error_body.get('error', {}).get('message', 'Unknown error')
            raise AirflowException(f"Lambda returned error {status_code}: {error_msg}")

        # Extraer datos
        body = json.loads(lambda_response.get('body', '{}'))
        if not body.get('success', False):
            error_msg = body.get('error', {}).get('message', 'Unknown error')
            raise AirflowException(f"Get status operation failed: {error_msg}")

        return body.get('data', {})



    def _check_multiple_transfer_status(self, status_data: Dict[str, Any]) -> bool:
        """
        Verifica el overall_status de múltiples transferencias.

        Args:
            status_data: Datos de estado de las transferencias

        Returns:
            True si todas las transferencias tienen overall_status en estados esperados
        """
        results = status_data.get('results', [])

        if not results:
            self.log.warning("No transfer results found")
            return False

        accepted_count = 0
        failed_count = 0
        found_transfers = set()

        for result in results:
            transfer_id = result.get('transfer_id', 'unknown')
            current_status = result.get('overall_status', '').upper()

            # Solo procesar transferencias que están en nuestra lista
            if transfer_id in self.transfer_ids:
                found_transfers.add(transfer_id)

                self.log.info("Transfer %s overall_status: %s", transfer_id, current_status)

                if current_status in self.expected_statuses:
                    accepted_count += 1
                elif current_status == 'FAILED':
                    failed_count += 1
                    if self.fail_on_error and 'FAILED' not in self.expected_statuses:
                        raise AirflowException(f"Transfer {transfer_id} failed with overall_status: {current_status}")

        # Verificar que encontramos todas las transferencias solicitadas
        missing_transfers = set(self.transfer_ids) - found_transfers
        if missing_transfers:
            self.log.warning("Transfers not found in results: %s", list(missing_transfers))
            return False

        total_transfers = len(self.transfer_ids)
        self.log.info("Transfer overall_status summary: %s/%s in accepted states %s, %s failed",
                   accepted_count, total_transfers, self.expected_statuses, failed_count)

        # Todas las transferencias deben estar en alguno de los estados esperados
        return accepted_count == total_transfers


class FileFerryCompletionSensor(FileFerryTransferSensor):
    """Sensor especializado para esperar completación de transferencias."""

    def __init__(self, **kwargs):
        kwargs.setdefault('expected_status', ['COMPLETED', 'PARTIALLY_COMPLETED', 'FAILED'])
        kwargs.setdefault('fail_on_error', True)
        super().__init__(**kwargs)


class FileFerryFailureSensor(FileFerryTransferSensor):
    """Sensor especializado para detectar fallos en transferencias."""

    def __init__(self, **kwargs):
        kwargs.setdefault('expected_status', 'FAILED')
        kwargs.setdefault('fail_on_error', False)
        super().__init__(**kwargs)
