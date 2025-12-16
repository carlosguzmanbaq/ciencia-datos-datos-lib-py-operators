Initial Commit

``` bash
ciencia-datos-datos-lib-py-operators/
├── README.md                           # Documentación principal del repositorio
├── requirements.txt                    # Dependencias principales
├── .gitignore                         # Archivos a ignorar en Git
├── .github/                           # Configuración de GitHub Actions
│   └── workflows/
│       ├── ci.yml                     # Pipeline de CI/CD
│       └── release.yml                # Pipeline de releases
├── src/                               # Código fuente principal
│   └── airflow_operators/             # Paquete principal
│       ├── __init__.py               # Inicialización del paquete
│       ├── s3_multipart_copy_operator/              # Operador para S3
│       │   ├── __init__.py
│       │   ├── README.md             # Documentación específica del operador
│       │   ├── operator.py           # Implementación del operador
│       │   └── hooks.py              # Hooks específicos si es necesario
├── tests/                            # Tests independientes por operador
│   ├── __init__.py
│   ├── conftest.py                   # Configuración común de pytest
│   ├── test_s3_operator/             # Tests del operador S3
│   │   ├── __init__.py
│   │   ├── test_operator.py
│   │   ├── test_hooks.py
│   │   └── fixtures/                 # Datos de prueba
│   │       └── sample_data.json
│   ├── test_redshift_operator/       # Tests del operador Redshift
│   │   ├── __init__.py
│   │   ├── test_operator.py
│   │   ├── test_hooks.py
│   │   └── fixtures/
│   │       └── sample_queries.sql
│   ├── test_email_operator/          # Tests del operador Email
│   │   ├── __init__.py
│   │   ├── test_operator.py
│   │   └── fixtures/
│   │       └── sample_templates/
│   └── test_slack_operator/          # Tests del operador Slack
│       ├── __init__.py
│       ├── test_operator.py
│       ├── test_utils.py
│       └── fixtures/
│           └── sample_messages.json
├── docs/                             # Documentación adicional
│   ├── index.md
│   ├── installation.md
│   ├── usage.md
├── examples/                         # Ejemplos de uso
│   ├── dags/                        # DAGs de ejemplo
│   │   ├── example_s3_dag.py
│   │   ├── example_redshift_dag.py
│   │   ├── example_email_dag.py
│   │   └── example_slack_dag.py
```