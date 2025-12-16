"""Unit tests for the S3MultipartCopyOperator"""
import unittest
from unittest.mock import Mock, patch

from airflow.exceptions import AirflowException
from src.airflow_operators.s3_multipart_copy_operator.s3_multipart_copy_operator import S3MultipartCopyOperator


class TestS3MultipartCopyOperator(unittest.TestCase):
    """Test para validar que el operador use el método correcto según el tamaño del archivo"""

    def setUp(self):
        self.operator = S3MultipartCopyOperator(
            task_id='test_copy',
            source_bucket_name='source-bucket',
            source_bucket_key='source/file.txt',
            dest_bucket_name='dest-bucket',
            dest_bucket_key='dest/file.txt'
        )

    @patch('src.airflow_operators.s3_multipart_copy_operator.s3_multipart_copy_operator.S3Hook')
    def test_file_under_5gb_uses_simple_copy(self, mock_s3_hook):
        """Test que archivos <= 5GB usen copy_object simple"""
        mock_client = Mock()
        mock_s3_hook.return_value.get_conn.return_value = mock_client

        # Simular archivo de 3GB
        file_size = 3 * 1024 * 1024 * 1024
        mock_client.head_object.return_value = {'ContentLength': file_size}

        # Ejecutar
        self.operator.execute({'task_instance': Mock()})

        # Verificar que se usó copy_object
        mock_client.copy_object.assert_called_once()
        mock_client.create_multipart_upload.assert_not_called()

    @patch('src.airflow_operators.s3_multipart_copy_operator.s3_multipart_copy_operator.S3Hook')
    def test_file_over_5gb_uses_multipart_copy(self, mock_s3_hook):
        """Test que archivos > 5GB usen multipart copy"""
        mock_client = Mock()
        mock_s3_hook.return_value.get_conn.return_value = mock_client

        # Simular archivo de 7GB
        file_size = 7 * 1024 * 1024 * 1024
        mock_client.head_object.return_value = {'ContentLength': file_size}
        mock_client.create_multipart_upload.return_value = {"UploadId": "test-id"}
        mock_client.upload_part_copy.return_value = {"CopyPartResult": {"ETag": "etag"}}

        # Ejecutar
        self.operator.execute({'task_instance': Mock()})

        # Verificar que se usó multipart
        mock_client.copy_object.assert_not_called()
        mock_client.create_multipart_upload.assert_called_once()
        mock_client.complete_multipart_upload.assert_called_once()

    @patch('src.airflow_operators.s3_multipart_copy_operator.s3_multipart_copy_operator.S3Hook')
    def test_multipart_copy_error_handling(self, mock_s3_hook):
        """Test manejo de errores en multipart copy con abort_multipart_upload"""

        mock_client = Mock()
        mock_s3_hook.return_value.get_conn.return_value = mock_client

        file_size = 7 * 1024 * 1024 * 1024
        mock_client.head_object.return_value = {'ContentLength': file_size}
        mock_client.create_multipart_upload.return_value = {"UploadId": "test-id"}
        mock_client.upload_part_copy.side_effect = Exception("Error simulado")

        # Ejecutar y verificar excepción
        with self.assertRaises(AirflowException):
            self.operator.execute({'task_instance': Mock()})

        # Verificar que se llamó abort_multipart_upload
        mock_client.abort_multipart_upload.assert_called_once_with(
            Bucket="dest-bucket", Key="dest/file.txt", UploadId="test-id"
        )

    @patch('src.airflow_operators.s3_multipart_copy_operator.s3_multipart_copy_operator.S3Hook')
    def test_verify_integrity_size_mismatch(self, mock_s3_hook):
        """Test error de integridad cuando tamaños no coinciden"""

        mock_client = Mock()
        mock_s3_hook.return_value.get_conn.return_value = mock_client

        file_size = 3 * 1024 * 1024 * 1024
        mock_client.head_object.side_effect = [
            {'ContentLength': file_size},  # Archivo origen
            {'ContentLength': file_size - 1000}  # Archivo destino (diferente tamaño)
        ]

        with self.assertRaises(AirflowException) as context:
            self.operator.execute({'task_instance': Mock()})

        self.assertIn("Error de integridad", str(context.exception))

    @patch('src.airflow_operators.s3_multipart_copy_operator.s3_multipart_copy_operator.S3Hook')
    def test_verify_integrity_head_object_error(self, mock_s3_hook):
        """Test error al verificar archivo destino"""

        mock_client = Mock()
        mock_s3_hook.return_value.get_conn.return_value = mock_client

        file_size = 3 * 1024 * 1024 * 1024
        mock_client.head_object.side_effect = [
            {'ContentLength': file_size},  # Archivo origen
            Exception("No se puede acceder al destino")  # Error en destino
        ]

        with self.assertRaises(AirflowException) as context:
            self.operator.execute({'task_instance': Mock()})

        self.assertIn("Error verificando archivo destino", str(context.exception))

    @patch('src.airflow_operators.s3_multipart_copy_operator.s3_multipart_copy_operator.S3Hook')
    def test_get_file_size_error(self, mock_s3_hook):
        """Test error al obtener tamaño del archivo origen"""

        mock_client = Mock()
        mock_s3_hook.return_value.get_conn.return_value = mock_client

        # Simular error en head_object del archivo origen
        mock_client.head_object.side_effect = Exception("Archivo no encontrado")

        with self.assertRaises(AirflowException) as context:
            self.operator.execute({'task_instance': Mock()})

        self.assertIn("No se puede acceder al archivo origen", str(context.exception))

    @patch('src.airflow_operators.s3_multipart_copy_operator.s3_multipart_copy_operator.S3Hook')
    def test_multipart_copy_abort_fails(self, mock_s3_hook):
        """Test que cuando falla abort_multipart_upload, se propague la excepción original"""

        mock_client = Mock()
        mock_s3_hook.return_value.get_conn.return_value = mock_client

        file_size = 7 * 1024 * 1024 * 1024
        mock_client.head_object.return_value = {'ContentLength': file_size}
        mock_client.create_multipart_upload.return_value = {"UploadId": "test-id"}
        mock_client.upload_part_copy.side_effect = Exception("Error original")
        mock_client.abort_multipart_upload.side_effect = Exception("Error en abort")

        with self.assertRaises(AirflowException) as context:
            self.operator.execute({'task_instance': Mock()})

        # Verificar que se propague la excepción original, no la del abort
        self.assertIn("Fallo la copia multipart: Error original", str(context.exception))
        mock_client.abort_multipart_upload.assert_called_once()

    @patch('src.airflow_operators.s3_multipart_copy_operator.s3_multipart_copy_operator.S3Hook')
    def test_simple_copy_with_acl_cross_account(self, mock_s3_hook):
        """Test copia simple con ACL para cross-account"""
        operator = S3MultipartCopyOperator(
            task_id='test_cross_account_simple',
            source_bucket_name='source-bucket',
            source_bucket_key='source/file.txt',
            dest_bucket_name='dest-bucket-cross-account',
            dest_bucket_key='dest/file.txt',
            acl_policy='bucket-owner-full-control'
        )

        mock_client = Mock()
        mock_s3_hook.return_value.get_conn.return_value = mock_client

        # Simular archivo de 3GB
        file_size = 3 * 1024 * 1024 * 1024
        mock_client.head_object.return_value = {'ContentLength': file_size}

        # Ejecutar
        operator.execute({'task_instance': Mock()})

        # Verificar que se usó copy_object con ACL
        mock_client.copy_object.assert_called_once()
        call_args = mock_client.copy_object.call_args[1]
        self.assertEqual(call_args['ACL'], 'bucket-owner-full-control')
        self.assertEqual(call_args['Bucket'], 'dest-bucket-cross-account')
        self.assertEqual(call_args['Key'], 'dest/file.txt')

    @patch('src.airflow_operators.s3_multipart_copy_operator.s3_multipart_copy_operator.S3Hook')
    def test_multipart_copy_with_acl_cross_account(self, mock_s3_hook):
        """Test copia multipart con ACL para cross-account"""
        operator = S3MultipartCopyOperator(
            task_id='test_cross_account_multipart',
            source_bucket_name='source-bucket',
            source_bucket_key='source/large-file.zip',
            dest_bucket_name='dest-bucket-cross-account',
            dest_bucket_key='dest/large-file.zip',
            acl_policy='bucket-owner-full-control'
        )

        mock_client = Mock()
        mock_s3_hook.return_value.get_conn.return_value = mock_client

        # Simular archivo de 7GB
        file_size = 7 * 1024 * 1024 * 1024
        mock_client.head_object.return_value = {'ContentLength': file_size}
        mock_client.create_multipart_upload.return_value = {"UploadId": "test-id"}
        mock_client.upload_part_copy.return_value = {"CopyPartResult": {"ETag": "etag"}}

        # Ejecutar
        operator.execute({'task_instance': Mock()})

        # Verificar que se usó multipart con ACL
        mock_client.create_multipart_upload.assert_called_once()
        create_call_args = mock_client.create_multipart_upload.call_args[1]
        self.assertEqual(create_call_args['ACL'], 'bucket-owner-full-control')
        self.assertEqual(create_call_args['Bucket'], 'dest-bucket-cross-account')
        self.assertEqual(create_call_args['Key'], 'dest/large-file.zip')

        mock_client.complete_multipart_upload.assert_called_once()


if __name__ == '__main__':
    unittest.main()
