# S3MultipartCopyOperator

El `S3MultipartCopyOperator` es un operador personalizado para copiar objetos S3 de cualquier tamaño de manera eficiente. Utiliza automáticamente el método de copia más apropiado según el tamaño del archivo.

## Features

- Copia automática inteligente basada en tamaño de archivo
- Copy simple para archivos ≤ 5GB (más rápido)
- Multipart copy para archivos > 5GB (maneja archivos grandes)
- Verificación de integridad automática
- Métricas detalladas de la operación
- Manejo robusto de errores con limpieza automática

## Parámetros de inicialización

| Parámetro            | Tipo | Descripción                                                          |
| -------------------- | ---- | -------------------------------------------------------------------- |
| `source_bucket_name` | str  | Nombre del bucket S3 origen                                          |
| `source_bucket_key`  | str  | Clave (path) del archivo origen en S3                                |
| `dest_bucket_name`   | str  | Nombre del bucket S3 destino                                         |
| `dest_bucket_key`    | str  | Clave (path) del archivo destino en S3                               |
| `aws_conn_id`        | str  | ID de conexión AWS (predeterminado: 'aws_default')                   |
| `part_size_mb`       | int  | Tamaño de cada parte en MB para multipart copy (predeterminado: 100) |
| `acl_policy`         | str  | Política ACL para el archivo destino (opcional)                      |

## Estrategias de copia

### 1. Copy Simple (≤ 5GB)
Para archivos de 5GB o menos, utiliza `copy_object` de S3 que es más rápido y eficiente.

### 2. Multipart Copy (> 5GB)
Para archivos mayores a 5GB, utiliza multipart copy que:
- Divide el archivo en partes de 100MB (configurable)
- Copia cada parte en paralelo
- Ensambla las partes al final
- Permite manejar archivos de cualquier tamaño

### Optimización del tamaño de partes

El tamaño de las partes afecta directamente el **throughput** (velocidad de transferencia de datos):

- **100 MB (predeterminado)**: Óptimo para archivos de 5-20 GB
- **200 MB**: Recomendado para archivos de 20-100 GB - reduce el número total de partes manteniendo buen rendimiento
- **500 MB**: Para archivos de 100+ GB - partes más grandes mejoran el throughput
- **1 GB**: Para archivos muy grandes (> 500 GB) - maximiza throughput y minimiza overhead

**Throughput** = Velocidad de transferencia de datos (MB/s). Partes más grandes reducen el overhead de red y mejoran la velocidad total de copia.

## Ejemplo de uso

```python
from s3_multipart_copy_operator import S3MultipartCopyOperator

# Copia simple (archivo pequeño)
copy_small_file = S3MultipartCopyOperator(
    task_id='copy_small_file',
    source_bucket_name='source-bucket',
    source_bucket_key='data/small-file.csv',
    dest_bucket_name='dest-bucket',
    dest_bucket_key='backup/small-file.csv',
    aws_conn_id='aws_default',
)

# Copia multipart (archivo grande)
copy_large_file = S3MultipartCopyOperator(
    task_id='copy_large_file',
    source_bucket_name='source-bucket',
    source_bucket_key='data/large-file.zip',
    dest_bucket_name='dest-bucket',
    dest_bucket_key='backup/large-file.zip',
    part_size_mb=500,  # Partes de 500MB para archivos grandes
    acl_policy='bucket-owner-full-control',  # Política ACL opcional
    aws_conn_id='aws_default',
)
```

## Métricas XCom

El operador almacena métricas detalladas en XCom bajo la clave `copy_metrics`:

```python
{
    "source_size_bytes": 1073741824,
    "dest_size_bytes": 1073741824,
    "duration_seconds": 45.67,
    "copy_method": "copy_object",
    "source_path": "s3://source-bucket/data/file.zip",
    "dest_path": "s3://dest-bucket/backup/file.zip"
}
```

## Verificación de integridad

El operador verifica automáticamente que:
- El archivo destino existe
- El tamaño del archivo destino coincide exactamente con el origen
- Lanza `AirflowException` si hay discrepancias

## Manejo de errores

- **Archivo origen no encontrado**: Lanza excepción inmediatamente
- **Error en multipart copy**: Ejecuta `abort_multipart_upload` para limpiar partes incompletas
- **Error de integridad**: Lanza excepción con detalles del problema
- **Errores de AWS**: Propaga excepciones específicas de boto3

## Dependencias

- Apache Airflow
- Amazon S3
- boto3 (AWS SDK para Python)
- Permisos S3: `s3:GetObject`, `s3:PutObject`, `s3:AbortMultipartUpload`
