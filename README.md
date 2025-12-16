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
├── README.md
├── examples
│   └── dags
│       └── s3_multipart_copy_operator
│           ├── docs
│           └── s3_multipart_copy_operator.py
├── pytest.ini
├── requirements.txt
├── src
│   ├── __init__.py
│   ├── airflow_operators
│   │   ├── __init__.py
│   │   └── s3_multipart_copy_operator
│   │       ├── README.md
│   │       ├── __init__.py
│   │       └── s3_multipart_copy_operator.py
│   └── airflow_sensors
│       └── __init__.py
└── tests
    ├── __init__.py
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