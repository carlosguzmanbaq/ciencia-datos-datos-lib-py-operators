import math
import time

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class S3MultipartCopyOperator(BaseOperator):
    """
    Custom Operator para copiar objetos S3 de cualquier tamaño.
    Utiliza copy simple para archivos <= 5 GB
    y multipart copy para archivos mayores a 5 GB.
    """

    template_fields = ('source_bucket_name', 'source_bucket_key', 'dest_bucket_name', 'dest_bucket_key', 'aws_conn_id', 'part_size')
    BYTES_PER_MB = 1024 * 1024
    GB_THRESHOLD = 5 * 1024 * 1024 * 1024  # 5GB en bytes

    def __init__(
        self,
        source_bucket_name: str,
        source_bucket_key: str,
        dest_bucket_name: str,
        dest_bucket_key: str,
        *args,
        aws_conn_id: str = "aws_default",
        part_size_mb: int = 100,
        acl_policy: str = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.source_bucket_name = source_bucket_name
        self.source_bucket_key = source_bucket_key
        self.dest_bucket_name = dest_bucket_name
        self.dest_bucket_key = dest_bucket_key
        self.aws_conn_id = aws_conn_id
        self.part_size = part_size_mb * self.BYTES_PER_MB
        self.acl_policy = acl_policy

    def execute(self, context):
        start_time = time.time()

        # S3Hook para hacer la conexión
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        s3_client = s3_hook.get_conn()

        clean_source_key = self.source_bucket_key.lstrip('/')
        source_size = self._get_file_size(s3_client, clean_source_key)

        # Determinar método de copia según tamaño
        if source_size <= self.GB_THRESHOLD:
            copy_method = "copy_object"
            self._simple_copy(s3_client, clean_source_key)
        else:
            copy_method = "create_multipart_upload"
            self._multipart_copy(s3_client, clean_source_key, source_size)

        # Verificar integridad del archivo copiado
        self._verify_integrity(s3_client, source_size)

        # Calcular duración y preparar métricas para XCom
        duration = time.time() - start_time

        xcom_data = {
            "source_size_bytes": source_size,
            "dest_size_bytes": source_size,
            "duration_seconds": round(duration, 2),
            "copy_method": copy_method,
            "source_path": f"s3://{self.source_bucket_name}/{clean_source_key}",
            "dest_path": f"s3://{self.dest_bucket_name}/{self.dest_bucket_key}"
        }

        context['task_instance'].xcom_push(key='copy_metrics', value=xcom_data)
        self.log.info(f"Copia completada en {duration:.2f}s usando {copy_method}")

    def _get_file_size(self, s3_client, clean_source_key):
        """Obtiene el tamaño del archivo origen y registra información"""
        try:
            response = s3_client.head_object(
                Bucket=self.source_bucket_name,
                Key=clean_source_key
            )
            source_size = response['ContentLength']
            file_size_mb = source_size / self.BYTES_PER_MB
            filename = clean_source_key.split('/')[-1]
            self.log.info(f"Archivo origen encontrado {filename}, tamaño: {source_size} bytes ({file_size_mb:.2f} MB)")
            return source_size
        except Exception as e:
            raise AirflowException(f"No se puede acceder al archivo origen: {str(e)}") from e

    def _simple_copy(self, s3_client, clean_source_key):
        """Ejecuta copia simple para archivos <= 5GB"""
        self.log.info("Archivo <= 5GB, usando copy_object simple")
        copy_params = {
            "CopySource": {"Bucket": self.source_bucket_name, "Key": clean_source_key},
            "Bucket": self.dest_bucket_name,
            "Key": self.dest_bucket_key,
        }
        if self.acl_policy:
            copy_params["ACL"] = self.acl_policy

        s3_client.copy_object(**copy_params)

    def _multipart_copy(self, s3_client, clean_source_key, file_size):
        """Ejecuta copia multipart para archivos > 5GB"""
        self.log.info("Archivo > 5GB, iniciando multipart copy...")

        try:
            # 1. Iniciar la carga multipart
            multipart_params = {
                "Bucket": self.dest_bucket_name,
                "Key": self.dest_bucket_key,
            }
            if self.acl_policy:
                multipart_params["ACL"] = self.acl_policy

            mpu = s3_client.create_multipart_upload(**multipart_params)
            upload_id = mpu["UploadId"]

            part_count = math.ceil(file_size / self.part_size)
            part_size_mb = self.part_size / self.BYTES_PER_MB
            self.log.info(f"Subiendo en {part_count} partes de {part_size_mb:.0f} MB cada una")

            parts = []
            for i in range(part_count):
                start = i * self.part_size
                end = min(start + self.part_size - 1, file_size - 1)
                part_mb = (end - start + 1) / self.BYTES_PER_MB

                self.log.info(f"Copiando parte {i+1}/{part_count}: {part_mb:.2f} MB")

                part = s3_client.upload_part_copy(
                    Bucket=self.dest_bucket_name,
                    Key=self.dest_bucket_key,
                    PartNumber=i + 1,
                    UploadId=upload_id,
                    CopySource={
                        "Bucket": self.source_bucket_name,
                        "Key": clean_source_key,
                    },
                    CopySourceRange=f"bytes={start}-{end}",
                )
                parts.append({"PartNumber": i + 1, "ETag": part["CopyPartResult"]["ETag"]})

            # 2. Completar la carga multipart
            s3_client.complete_multipart_upload(
                Bucket=self.dest_bucket_name,
                Key=self.dest_bucket_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            self.log.info("Multipart copy completada exitosamente")

        except Exception as e:
            self.log.error(f"Error en multipart copy: {str(e)}")
            try:
                s3_client.abort_multipart_upload(
                    Bucket=self.dest_bucket_name, Key=self.dest_bucket_key, UploadId=upload_id
                )
            except Exception:
                pass  # Si falla el abort, continuar con la excepción original
            raise AirflowException(f"Fallo la copia multipart: {str(e)}") from e

    def _verify_integrity(self, s3_client, expected_size):
        """Verifica que el tamaño del archivo destino coincida con el origen"""
        try:
            dest_response = s3_client.head_object(
                Bucket=self.dest_bucket_name,
                Key=self.dest_bucket_key
            )
            dest_size = dest_response['ContentLength']

            if dest_size != expected_size:
                raise AirflowException(
                    f"Error de integridad: tamaño origen ({expected_size}) != tamaño destino ({dest_size})"
                )

            self.log.info(f"Verificación de integridad exitosa: {dest_size} bytes")

        except Exception as e:
            if "Error de integridad" in str(e):
                raise
            raise AirflowException(f"Error verificando archivo destino: {str(e)}") from e
