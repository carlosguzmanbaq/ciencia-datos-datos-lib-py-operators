# FileFerryOperator

El `FileFerryOperator` es un operador personalizado para ejecutar operaciones de transferencia de archivos usando la función Lambda File Ferry.

# Features

- Soporte para múltiples operaciones: upload, download, delete, list_directory, get_status
- Transferencias entre S3 y SFTP
- Configuración dinámica del nombre de la función Lambda por ambiente
- Manejo de listas de archivos y IDs de transferencia
- Procesamiento automático de respuestas de get_status

# Operaciones soportadas

- **upload**: Transferir archivos de S3 a SFTP
- **download**: Transferir archivos de SFTP a S3
- **delete**: Eliminar archivos en SFTP
- **list_directory**: Listar contenido de directorio SFTP
- **get_status**: Consultar estado de transferencias

# Parámetros de inicialización

| Parámetro               | Tipo      | Descripción                                                              |
|-------------------------|-----------|--------------------------------------------------------------------------|
| `operation`             | str       | Tipo de operación (upload, download, delete, list_directory, get_status) |
| `environment`           | str       | Ambiente (dev, qc, pdn)                                                  |
| `connector_id`          | str       | ID del conector SFTP                                                     |
| `aws_conn_id`           | str       | ID de conexión AWS                                                       |
| `files`                 | List[str] | Lista de archivos para transferir (opcional)                             |
| `destination_path`      | str       | Ruta destino para uploads (opcional)                                     |
| `s3_destination_path`   | str       | Ruta S3 destino para downloads (opcional)                                |
| `sftp_path`             | str       | Ruta SFTP para list_directory (opcional)                                 |
| `transfer_ids`          | List[str] | Lista de IDs para get_status (opcional)                                  |
| `max_items`             | int       | Máximo elementos para list_directory (opcional)                          |
| `output_directory_path` | str       | Directorio salida para list_directory (opcional)                         |
| `region_name`           | str       | Región AWS (predeterminado: 'us-east-1')                                 |


## Ejemplos de uso

### Upload de archivos S3 a SFTP
```python
from file_ferry import FileFerryOperator

upload_files = FileFerryOperator(
    task_id='upload_files_to_sftp',
    operation='upload',
    environment='pdn',
    connector_id='sftp_connector_001',
    aws_conn_id='aws_default',
    files=[
        '/my-bucket/data/file1.csv',
        '/my-bucket/data/file2.csv'
    ],
    destination_path='/remote/path/'
)
```

### Download de archivos SFTP a S3
```python
download_files = FileFerryOperator(
    task_id='download_files_from_sftp',
    operation='download',
    environment='pdn',
    connector_id='sftp_connector_001',
    aws_conn_id='aws_default',
    files=[
        '/remote/path/file1.csv',
        '/remote/path/file2.csv'
    ],
    s3_destination_path='/my-bucket/downloads/'
)
```

### Consultar estado de transferencias
```python
check_status = FileFerryOperator(
    task_id='check_transfer_status',
    operation='get_status',
    environment='pdn',
    connector_id='sftp_connector_001',
    aws_conn_id='aws_default',
    transfer_ids=[
        'transfer_123',
        'transfer_456'
    ]
)
```