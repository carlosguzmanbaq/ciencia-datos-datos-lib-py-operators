# Operadores y Sensores Custom de Airflow

Repositorio centralizado de operadores y sensores custom de Apache Airflow para resolver necesidades transversales de manera reutilizable en proyectos de ciencia de datos.

## 🎯 Propósito

Este repositorio contiene componentes custom de Airflow diseñados para:
- Resolver necesidades comunes entre diferentes proyectos
- Proporcionar funcionalidades especializadas no disponibles en operadores estándar
- Mantener consistencia y calidad en implementaciones transversales
- Facilitar reutilización y mantenimiento centralizado

## 📦 Operadores Disponibles

### S3MultipartCopyOperator
Copia archivos S3 de cualquier tamaño usando copy simple (≤5GB) o multipart copy (>5GB).
- **Ubicación**: `src/airflow_operators/s3_multipart_copy_operator/`
- **Casos de uso**: Migración de datos, backup, replicación cross-account
- **Características**: Verificación de integridad, soporte ACL, métricas XCom

### FileFerryOperator
Ejecuta operaciones de transferencia de archivos entre S3 y SFTP mediante Lambda File Ferry.
- **Ubicación**: `src/airflow_operators/file_ferry_operator/`
- **Casos de uso**: Upload/download S3↔SFTP, gestión de archivos remotos, sincronización de datos
- **Características**: Múltiples operaciones (upload, download, delete, list_directory, get_status), configuración por ambiente, manejo de listas de archivos

## 🔍 Sensores Disponibles

### FileFerryTransferSensor
Monitorea el estado de transferencias FileFerry hasta alcanzar un estado específico.
- **Ubicación**: `src/airflow_sensors/file_ferry_sensor/`
- **Casos de uso**: Esperar completación de transferencias, validar estados de archivos
- **Características**: Soporte múltiples estados esperados, configuración de timeouts, manejo de errores

### FileFerryCompletionSensor
Sensor especializado para esperar completación de transferencias (hereda de FileFerryTransferSensor).
- **Ubicación**: `src/airflow_sensors/file_ferry_sensor/`
- **Casos de uso**: Validar finalización exitosa de uploads/downloads
- **Características**: Estados predeterminados (COMPLETED, PARTIALLY_COMPLETED, FAILED), falla automática en errores

### FileFerryFailureSensor
Sensor especializado para detectar fallos en transferencias (hereda de FileFerryTransferSensor).
- **Ubicación**: `src/airflow_sensors/file_ferry_sensor/`
- **Casos de uso**: Detectar y manejar transferencias fallidas
- **Características**: Estado esperado FAILED, no falla automáticamente

## 🧪 Testing

```bash
# Ejecutar todos los tests
pytest

# Test específico con cobertura
pytest tests/s3multipartcopyoperator/ --cov=src/airflow_operators/s3_multipart_copy_operator --cov-report=html

# Cobertura
./htmlcov/index.html
```

## 📁 Estructura

```bash
tree -I 'virtual|__pycache__|*.pyc|*.pyo|*.egg-info|.pytest_cache|*.egg|dist|build|.coverage|cov_html|htmlcov|*.so|node_modules|derby.log|catalog|localfs_quality|metastore_db|spark-warehouse' -L 4
```

```bash
.
├── README.md
├── examples
│   └── dags
│       ├── file_ferry
│       │   ├── download
│       │   └── upload
│       └── s3_multipart_copy_operator
│           ├── docs
│           └── s3_multipart_copy_operator.py
├── pytest.ini
├── requirements.txt
├── src
│   ├── __init__.py
│   ├── airflow_operators
│   │   ├── __init__.py
│   │   ├── file_ferry_operator
│   │   │   ├── README.md
│   │   │   ├── __init__.py
│   │   │   └── file_ferry_operator.py
│   │   └── s3_multipart_copy_operator
│   │       ├── README.md
│   │       ├── __init__.py
│   │       └── s3_multipart_copy_operator.py
│   └── airflow_sensors
│       ├── __init__.py
│       └── file_ferry_sensor
│           ├── README.md
│           ├── __init__.py
│           └── file_ferry_sensor.py
└── tests
    ├── __init__.py
    ├── file_ferry
    │   ├── __init__.py
    │   ├── test_file_ferry_operator.py
    │   └── test_file_ferry_sensor.py
    └── test_s3_multipart_copy_operator
        ├── __init__.py
        └── test_s3_multipart_copy_operator.py
```

## 🤝 Contribución

1. Crear branch: `git checkout -b feature/nuevo-operador`
2. Seguir estructura estándar en `src/airflow_operators/`
3. Incluir tests en `tests/test_nuevo_operador/`
4. Documentar en README específico
5. Agregar ejemplo en `examples/dags/`
6. Ejecutar tests: `pytest tests/test_nuevo_operador/`

## 📋 Estándares

- **Cobertura mínima**: 80%
- **Documentación**: README por operador
- **Tests**: Independientes por operador
- **Ejemplos**: DAG funcional por operador
- **Logging**: Español o Ingles informativo
- **XCom**: Métricas de ejecución (Opcional)