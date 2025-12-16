# S3MultipartCopyOperator

Operador custom de Airflow para copiar archivos S3 de cualquier tamaño de manera eficiente.

## 🎯 Funcionalidad

- **Archivos ≤ 5GB**: Usa `copy_object` simple
- **Archivos > 5GB**: Usa `multipart copy` automáticamente
- **Verificación de integridad**: Compara tamaños origen/destino
- **Soporte cross-account**: ACL policies configurables
- **Métricas XCom**: Duración, método usado, tamaños

## 📋 Parámetros

| Parámetro | Tipo | Requerido | Descripción |
|-----------|------|-----------|-------------|
| `source_bucket_name` | str | ✅ | Bucket origen |
| `source_bucket_key` | str | ✅ | Key del archivo origen |
| `dest_bucket_name` | str | ✅ | Bucket destino |
| `dest_bucket_key` | str | ✅ | Key del archivo destino |
| `aws_conn_id` | str | ❌ | Conexión AWS (default: aws_default) |
| `part_size_mb` | int | ❌ | Tamaño de parte en MB (default: 100) |
| `acl_policy` | str | ❌ | ACL policy para cross-account |

## 🚀 Uso

```python
from src.airflow_operators.s3_multipart_copy_operator import S3MultipartCopyOperator

# Copia simple
copy_task = S3MultipartCopyOperator(
    task_id='copy_file',
    source_bucket_name='mi-bucket-origen',
    source_bucket_key='data/archivo.csv',
    dest_bucket_name='mi-bucket-destino',
    dest_bucket_key='backup/archivo.csv'
)

# Copia cross-account con ACL
copy_cross_account = S3MultipartCopyOperator(
    task_id='copy_cross_account',
    source_bucket_name='bucket-cuenta-a',
    source_bucket_key='data/archivo-grande.zip',
    dest_bucket_name='bucket-cuenta-b',
    dest_bucket_key='data/archivo-grande.zip',
    acl_policy='bucket-owner-full-control',
    part_size_mb=200
)
```

## 📊 Métricas XCom

El operador genera métricas en XCom con key `copy_metrics`:

```python
{
    "source_size_bytes": 7516192768,
    "dest_size_bytes": 7516192768,
    "duration_seconds": 45.23,
    "copy_method": "create_multipart_upload",
    "source_path": "s3://origen/archivo.zip",
    "dest_path": "s3://destino/archivo.zip"
}
```

## ⚠️ Consideraciones

- **Permisos**: Requiere `s3:GetObject` en origen y `s3:PutObject` en destino
- **Cross-account**: Usar `acl_policy='bucket-owner-full-control'`
- **Archivos grandes**: Ajustar `part_size_mb` según necesidades
- **Timeouts**: Considerar timeouts de Airflow para archivos muy grandes

## 🧪 Testing

```bash
# Tests específicos del operador
pytest tests/s3multipartcopyoperator/ -v

# Con cobertura
pytest tests/s3multipartcopyoperator/ --cov=src/airflow_operators/s3_multipart_copy_operator --cov-report=html
```