# FileFerryTransferSensor

El `FileFerryTransferSensor` es un sensor base para monitorear el estado de transferencias FileFerry hasta que alcancen un estado específico.

## Features

- Monitoreo del overall_status de transferencias
- Soporte para múltiples estados esperados
- Configuración flexible de intervalos y timeouts
- Manejo de errores configurable

## Parámetros de inicialización

| Parámetro         | Tipo                  | Descripción                                                       |
|-------------------|-----------------------|-------------------------------------------------------------------|
| `environment`     | str                   | Ambiente (dev, qc, pdn)                                           |
| `connector_id`    | str                   | ID del conector SFTP                                              |
| `aws_conn_id`     | str                   | ID de conexión AWS                                                |
| `transfer_ids`    | List[str]             | Lista de IDs de transferencias a monitorear                       |
| `expected_status` | Union[str, List[str]] | Estado(s) esperado(s) (predeterminado: 'COMPLETED')               |
| `fail_on_error`   | bool                  | Si fallar cuando transferencia tiene error (predeterminado: True) |
| `region_name`     | str                   | Región AWS (predeterminado: 'us-east-1')                          |
| `poke_interval`   | int                   | Intervalo entre verificaciones en segundos (predeterminado: 60)   |
| `timeout`         | int                   | Timeout total del sensor en segundos (predeterminado: 7200)       |


## Ejemplo de uso

```python
from file_ferry import FileFerryTransferSensor

wait_for_transfers = FileFerryTransferSensor(
    task_id='wait_for_file_transfers',
    environment='pdn',
    connector_id='sftp_connector_001',
    aws_conn_id='aws_default',
    transfer_ids=['{{task_instance.xcom_pull(task_ids="upload_files")["transfer_ids"]}}'],
    expected_status=['COMPLETED', 'PARTIALLY_COMPLETED'],
    poke_interval=30,
    timeout=3600,
)
```

# FileFerryCompletionSensor

El `FileFerryCompletionSensor` es un sensor especializado que hereda de `FileFerryTransferSensor` para esperar la completación de transferencias.

## Features

- Configuración predeterminada para estados de completación
- Incluye estados: COMPLETED, PARTIALLY_COMPLETED, FAILED
- Falla automáticamente en errores

## Ejemplo de uso

```python
from file_ferry import FileFerryCompletionSensor

wait_completion = FileFerryCompletionSensor(
    task_id='wait_for_completion',
    environment='pdn',
    connector_id='sftp_connector_001',
    aws_conn_id='aws_default',
    transfer_ids=['transfer_123', 'transfer_456'],
)
```

# FileFerryFailureSensor

El `FileFerryFailureSensor` es un sensor especializado que hereda de `FileFerryTransferSensor` para detectar fallos en transferencias.

## Features

- Configuración predeterminada para detectar fallos
- Estado esperado: FAILED
- No falla automáticamente (fail_on_error=False)

## Ejemplo de uso

```python
from file_ferry import FileFerryFailureSensor

detect_failures = FileFerryFailureSensor(
    task_id='detect_transfer_failures',
    environment='pdn',
    connector_id='sftp_connector_001',
    aws_conn_id='aws_default',
    transfer_ids=['transfer_123', 'transfer_456'],
)
```

## Flujo completo de ejemplo

```python
from airflow import DAG
from file_ferry import FileFerryOperator, FileFerryCompletionSensor

# Upload archivos
upload_task = FileFerryOperator(
    task_id='upload_files',
    operation='upload',
    environment='pdn',
    connector_id='sftp_connector_001',
    aws_conn_id='aws_default',
    files=[
        '/bucket/file1.csv',
        '/bucket/file2.csv'
    ],
    destination_path='/remote/uploads/',
)

# Esperar completación
wait_completion = FileFerryCompletionSensor(
    task_id='wait_upload_completion',
    environment='pdn',
    connector_id='sftp_connector_001',
    aws_conn_id='aws_default',
    transfer_ids='{{task_instance.xcom_pull(task_ids="upload_files")["transfer_ids"]}}',
)

upload_task >> wait_completion
```