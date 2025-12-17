"""Unit tests for the FileFerryOperator."""
import json
import pytest
from unittest.mock import patch, MagicMock
from airflow.exceptions import AirflowException

from src.airflow_operators.file_ferry_operator.file_ferry_operator import FileFerryOperator


@pytest.fixture
def operator_args():
    """Fixture providing default arguments for FileFerryOperator."""
    return {
        "task_id": "test_file_ferry_operator",
        "operation": "upload",
        "environment": "dev",
        "connector_id": "test_connector",
        "aws_conn_id": "aws_default",
        "files": ["file1.txt", "file2.txt"],
        "destination_path": "/remote/path",
        "region_name": "us-east-1"
    }


def test_init(operator_args):
    """Test initialization of FileFerryOperator."""
    operator = FileFerryOperator(**operator_args)

    assert operator.operation == "upload"
    assert operator.environment == "dev"
    assert operator.connector_id == "test_connector"
    assert operator.files == ["file1.txt", "file2.txt"]
    assert operator.destination_path == "/remote/path"
    assert operator.lambda_function_name == "file_ferry-dev"


def test_init_invalid_operation():
    """Test initialization fails with invalid operation."""
    args = {
        "task_id": "test_operator",
        "operation": "invalid_operation",
        "environment": "dev",
        "connector_id": "test_connector",
        "aws_conn_id": "aws_default"
    }

    with pytest.raises(ValueError, match="Invalid operation"):
        FileFerryOperator(**args)


def test_normalize_list_with_list(operator_args):
    """Test _normalize_list with list input."""
    operator = FileFerryOperator(**operator_args)
    result = operator._normalize_list(["item1", "item2"])

    assert result == ["item1", "item2"]


def test_normalize_list_with_string(operator_args):
    """Test _normalize_list with string input."""
    operator = FileFerryOperator(**operator_args)
    result = operator._normalize_list("[item1,item2,item3]")

    assert result == ["item1", "item2", "item3"]


def test_normalize_list_empty(operator_args):
    """Test _normalize_list with empty input."""
    operator = FileFerryOperator(**operator_args)
    result = operator._normalize_list("")

    assert result is None


def test_build_payload_upload(operator_args):
    """Test _build_payload for upload operation."""
    operator = FileFerryOperator(**operator_args)
    payload = operator._build_payload()

    expected = {
        "operation": "upload",
        "connector_id": "test_connector",
        "files": ["file1.txt", "file2.txt"],
        "destination_path": "/remote/path"
    }

    assert payload == expected


def test_build_payload_download():
    """Test _build_payload for download operation."""
    args = {
        "task_id": "test_download",
        "operation": "download",
        "environment": "dev",
        "connector_id": "test_connector",
        "aws_conn_id": "aws_default",
        "files": ["remote_file.txt"],
        "s3_destination_path": "s3://bucket/path/"
    }

    operator = FileFerryOperator(**args)
    payload = operator._build_payload()

    expected = {
        "operation": "download",
        "connector_id": "test_connector",
        "files": ["remote_file.txt"],
        "s3_destination_path": "s3://bucket/path/"
    }

    assert payload == expected


def test_build_payload_list_directory():
    """Test _build_payload for list_directory operation."""
    args = {
        "task_id": "test_list",
        "operation": "list_directory",
        "environment": "dev",
        "connector_id": "test_connector",
        "aws_conn_id": "aws_default",
        "sftp_path": "/remote/directory",
        "max_items": 100
    }

    operator = FileFerryOperator(**args)
    payload = operator._build_payload()

    expected = {
        "operation": "list_directory",
        "connector_id": "test_connector",
        "sftp_path": "/remote/directory",
        "max_items": 100
    }

    assert payload == expected


def test_build_payload_get_status():
    """Test _build_payload for get_status operation."""
    args = {
        "task_id": "test_status",
        "operation": "get_status",
        "environment": "dev",
        "connector_id": "test_connector",
        "aws_conn_id": "aws_default",
        "transfer_ids": ["transfer_1", "transfer_2"]
    }

    operator = FileFerryOperator(**args)
    payload = operator._build_payload()

    expected = {
        "operation": "get_status",
        "connector_id": "test_connector",
        "transfer_ids": ["transfer_1", "transfer_2"]
    }

    assert payload == expected


def test_process_get_status_result(operator_args):
    """Test _process_get_status_result method."""
    operator_args["operation"] = "get_status"
    operator = FileFerryOperator(**operator_args)

    data = {
        "results": [
            {
                "transfer_id": "transfer_1",
                "file_results": [
                    {"file_path": "file1.txt", "status": "COMPLETED"},
                    {"file_path": "file2.txt", "status": "FAILED"}
                ]
            }
        ]
    }

    result = operator._process_get_status_result(data)

    assert result["completed_files"] == ["file1.txt"]
    assert result["failed_files"] == ["file2.txt"]
    assert "results" in result  # Original data preserved


@patch("src.airflow_operators.file_ferry_operator.file_ferry_operator.LambdaHook")
def test_execute_success(mock_lambda_hook, operator_args):
    """Test execute method success case."""
    # Setup mock
    mock_hook_instance = MagicMock()
    mock_response = {
        "Payload": MagicMock()
    }
    mock_response["Payload"].read.return_value = json.dumps({
        "statusCode": 200,
        "body": json.dumps({
            "success": True,
            "data": {"transfer_id": "test_transfer"}
        })
    }).encode('utf-8')
    mock_hook_instance.invoke_lambda.return_value = mock_response
    mock_lambda_hook.return_value = mock_hook_instance

    operator = FileFerryOperator(**operator_args)
    result = operator.execute({})

    # Verify hook was called correctly
    mock_hook_instance.invoke_lambda.assert_called_once()
    call_args = mock_hook_instance.invoke_lambda.call_args
    assert call_args[1]["function_name"] == "file_ferry-dev"

    # Verify result
    assert result == {"transfer_id": "test_transfer"}


@patch("src.airflow_operators.file_ferry_operator.file_ferry_operator.LambdaHook")
def test_execute_get_status_success(mock_lambda_hook, operator_args):
    """Test execute method for get_status operation."""
    # Setup for get_status operation
    operator_args["operation"] = "get_status"
    operator_args["transfer_ids"] = ["transfer_1"]

    # Setup mock
    mock_hook_instance = MagicMock()
    mock_response = {
        "Payload": MagicMock()
    }
    mock_response["Payload"].read.return_value = json.dumps({
        "statusCode": 200,
        "body": json.dumps({
            "success": True,
            "data": {
                "results": [
                    {
                        "transfer_id": "transfer_1",
                        "file_results": [
                            {"file_path": "file1.txt", "status": "COMPLETED"}
                        ]
                    }
                ]
            }
        })
    }).encode('utf-8')
    mock_hook_instance.invoke_lambda.return_value = mock_response
    mock_lambda_hook.return_value = mock_hook_instance

    operator = FileFerryOperator(**operator_args)
    result = operator.execute({})

    # Verify result includes processed file lists
    assert "completed_files" in result
    assert "failed_files" in result
    assert result["completed_files"] == ["file1.txt"]


@patch("src.airflow_operators.file_ferry_operator.file_ferry_operator.LambdaHook")
def test_execute_lambda_error(mock_lambda_hook, operator_args):
    """Test execute method when Lambda returns error."""
    # Setup mock
    mock_hook_instance = MagicMock()
    mock_response = {
        "Payload": MagicMock()
    }
    mock_response["Payload"].read.return_value = json.dumps({
        "statusCode": 500,
        "body": json.dumps({
            "error": {"message": "Lambda error"}
        })
    }).encode('utf-8')
    mock_hook_instance.invoke_lambda.return_value = mock_response
    mock_lambda_hook.return_value = mock_hook_instance

    operator = FileFerryOperator(**operator_args)

    with pytest.raises(AirflowException, match="Lambda returned error 500"):
        operator.execute({})


@patch("src.airflow_operators.file_ferry_operator.file_ferry_operator.LambdaHook")
def test_execute_operation_failed(mock_lambda_hook, operator_args):
    """Test execute method when operation fails."""
    # Setup mock
    mock_hook_instance = MagicMock()
    mock_response = {
        "Payload": MagicMock()
    }
    mock_response["Payload"].read.return_value = json.dumps({
        "statusCode": 200,
        "body": json.dumps({
            "success": False,
            "error": {"message": "Operation failed"}
        })
    }).encode('utf-8')
    mock_hook_instance.invoke_lambda.return_value = mock_response
    mock_lambda_hook.return_value = mock_hook_instance

    operator = FileFerryOperator(**operator_args)

    with pytest.raises(AirflowException, match="FileFerry operation failed"):
        operator.execute({})


@patch("src.airflow_operators.file_ferry_operator.file_ferry_operator.LambdaHook")
def test_execute_no_payload(mock_lambda_hook, operator_args):
    """Test execute method when Lambda response has no payload."""
    # Setup mock
    mock_hook_instance = MagicMock()
    mock_response = {}  # No Payload key
    mock_hook_instance.invoke_lambda.return_value = mock_response
    mock_lambda_hook.return_value = mock_hook_instance

    operator = FileFerryOperator(**operator_args)

    with pytest.raises(AirflowException, match="No payload in Lambda response"):
        operator.execute({})


@patch("src.airflow_operators.file_ferry_operator.file_ferry_operator.LambdaHook")
def test_execute_exception_handling(mock_lambda_hook, operator_args):
    """Test execute method exception handling."""
    # Setup mock to raise exception
    mock_hook_instance = MagicMock()
    mock_hook_instance.invoke_lambda.side_effect = Exception("Connection error")
    mock_lambda_hook.return_value = mock_hook_instance

    operator = FileFerryOperator(**operator_args)

    with pytest.raises(AirflowException, match="FileFerry operation failed"):
        operator.execute({})


def test_normalize_list_with_brackets(operator_args):
    """Test _normalize_list with bracketed string input."""
    operator = FileFerryOperator(**operator_args)
    result = operator._normalize_list("['item1', 'item2']")

    assert result == ["item1", "item2"]


def test_process_get_status_result_empty_results(operator_args):
    """Test _process_get_status_result with empty results."""
    operator_args["operation"] = "get_status"
    operator = FileFerryOperator(**operator_args)

    data = {"results": []}
    result = operator._process_get_status_result(data)

    assert result["completed_files"] == []
    assert result["failed_files"] == []


def test_normalize_list_invalid_type(operator_args):
    """Test _normalize_list with invalid type (not string or list)."""
    operator = FileFerryOperator(**operator_args)
    result = operator._normalize_list(123)  # Invalid type

    assert result is None


def test_build_payload_with_output_directory_path():
    """Test _build_payload includes output_directory_path when provided."""
    args = {
        "task_id": "test_list_with_output",
        "operation": "list_directory",
        "environment": "dev",
        "connector_id": "test_connector",
        "aws_conn_id": "aws_default",
        "sftp_path": "/remote/directory",
        "output_directory_path": "/local/output"
    }

    operator = FileFerryOperator(**args)
    payload = operator._build_payload()

    expected = {
        "operation": "list_directory",
        "connector_id": "test_connector",
        "sftp_path": "/remote/directory",
        "output_directory_path": "/local/output"
    }

    assert payload == expected