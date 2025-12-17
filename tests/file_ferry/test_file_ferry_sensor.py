"""Unit tests for the FileFerryTransferSensor."""
import json
import pytest
from unittest.mock import patch, MagicMock
from airflow.exceptions import AirflowException

from src.airflow_sensors.file_ferry_sensor.file_ferry_sensor import (
    FileFerryTransferSensor,
    FileFerryCompletionSensor,
    FileFerryFailureSensor
)


@pytest.fixture
def sensor_args():
    """Fixture providing default arguments for FileFerryTransferSensor."""
    return {
        "task_id": "test_file_ferry_sensor",
        "environment": "dev",
        "connector_id": "test_connector",
        "aws_conn_id": "aws_default",
        "transfer_ids": ["transfer_1", "transfer_2"],
        "expected_status": "COMPLETED",
        "fail_on_error": True,
        "region_name": "us-east-1"
    }


def test_init(sensor_args):
    """Test initialization of FileFerryTransferSensor."""
    sensor = FileFerryTransferSensor(**sensor_args)

    assert sensor.environment == "dev"
    assert sensor.connector_id == "test_connector"
    assert sensor.transfer_ids == ["transfer_1", "transfer_2"]
    assert sensor.expected_statuses == ["COMPLETED"]
    assert sensor.fail_on_error is True
    assert sensor.lambda_function_name == "file_ferry-dev"


def test_init_with_multiple_expected_statuses(sensor_args):
    """Test initialization with multiple expected statuses."""
    sensor_args["expected_status"] = ["COMPLETED", "PARTIALLY_COMPLETED"]
    sensor = FileFerryTransferSensor(**sensor_args)

    assert sensor.expected_statuses == ["COMPLETED", "PARTIALLY_COMPLETED"]


def test_init_missing_transfer_ids():
    """Test initialization fails when transfer_ids is empty."""
    args = {
        "task_id": "test_sensor",
        "environment": "dev",
        "connector_id": "test_connector",
        "aws_conn_id": "aws_default",
        "transfer_ids": [],
    }

    with pytest.raises(ValueError, match="Debe especificar transfer_ids"):
        FileFerryTransferSensor(**args)


def test_init_missing_connector_id():
    """Test initialization fails when connector_id is empty."""
    args = {
        "task_id": "test_sensor",
        "environment": "dev",
        "connector_id": "",
        "aws_conn_id": "aws_default",
        "transfer_ids": ["transfer_1"],
    }

    with pytest.raises(ValueError, match="connector_id es requerido"):
        FileFerryTransferSensor(**args)


def test_normalize_transfer_ids_list(sensor_args):
    """Test _normalize_transfer_ids with list input."""
    sensor = FileFerryTransferSensor(**sensor_args)
    result = sensor._normalize_transfer_ids(["id1", "id2"])

    assert result == ["id1", "id2"]


def test_normalize_transfer_ids_string(sensor_args):
    """Test _normalize_transfer_ids with string input."""
    sensor = FileFerryTransferSensor(**sensor_args)
    result = sensor._normalize_transfer_ids("id1,id2,id3")

    assert result == ["id1", "id2", "id3"]


def test_normalize_transfer_ids_empty(sensor_args):
    """Test _normalize_transfer_ids with empty input."""
    sensor = FileFerryTransferSensor(**sensor_args)
    result = sensor._normalize_transfer_ids("")

    assert result is None


@patch("src.airflow_sensors.file_ferry_sensor.file_ferry_sensor.LambdaHook")
def test_get_transfer_status_success(mock_lambda_hook, sensor_args):
    """Test _get_transfer_status method success case."""
    # Setup mock
    mock_hook_instance = MagicMock()
    mock_response = {
        "Payload": MagicMock()
    }
    mock_response["Payload"].read.return_value = json.dumps({
        "statusCode": 200,
        "body": json.dumps({
            "success": True,
            "data": {"results": []}
        })
    }).encode('utf-8')
    mock_hook_instance.invoke_lambda.return_value = mock_response
    mock_lambda_hook.return_value = mock_hook_instance

    sensor = FileFerryTransferSensor(**sensor_args)
    result = sensor._get_transfer_status()

    # Verify hook was called correctly
    mock_hook_instance.invoke_lambda.assert_called_once()
    call_args = mock_hook_instance.invoke_lambda.call_args
    assert call_args[1]["function_name"] == "file_ferry-dev"

    # Verify result
    assert result == {"results": []}


@patch("src.airflow_sensors.file_ferry_sensor.file_ferry_sensor.LambdaHook")
def test_get_transfer_status_lambda_error(mock_lambda_hook, sensor_args):
    """Test _get_transfer_status method when Lambda returns error."""
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

    sensor = FileFerryTransferSensor(**sensor_args)

    with pytest.raises(AirflowException, match="Lambda returned error 500"):
        sensor._get_transfer_status()


def test_check_multiple_transfer_status_success(sensor_args):
    """Test _check_multiple_transfer_status when all transfers are completed."""
    sensor = FileFerryTransferSensor(**sensor_args)
    status_data = {
        "results": [
            {"transfer_id": "transfer_1", "overall_status": "COMPLETED"},
            {"transfer_id": "transfer_2", "overall_status": "COMPLETED"}
        ]
    }

    result = sensor._check_multiple_transfer_status(status_data)
    assert result is True


def test_check_multiple_transfer_status_partial(sensor_args):
    """Test _check_multiple_transfer_status when some transfers are pending."""
    sensor = FileFerryTransferSensor(**sensor_args)
    status_data = {
        "results": [
            {"transfer_id": "transfer_1", "overall_status": "COMPLETED"},
            {"transfer_id": "transfer_2", "overall_status": "IN_PROGRESS"}
        ]
    }

    result = sensor._check_multiple_transfer_status(status_data)
    assert result is False


def test_check_multiple_transfer_status_failed(sensor_args):
    """Test _check_multiple_transfer_status when transfer fails."""
    sensor = FileFerryTransferSensor(**sensor_args)
    status_data = {
        "results": [
            {"transfer_id": "transfer_1", "overall_status": "FAILED"}
        ]
    }

    with pytest.raises(AirflowException, match="Transfer transfer_1 failed"):
        sensor._check_multiple_transfer_status(status_data)


def test_check_multiple_transfer_status_missing_transfers(sensor_args):
    """Test _check_multiple_transfer_status when transfers are missing."""
    sensor = FileFerryTransferSensor(**sensor_args)
    status_data = {
        "results": [
            {"transfer_id": "transfer_1", "overall_status": "COMPLETED"}
            # transfer_2 is missing
        ]
    }

    result = sensor._check_multiple_transfer_status(status_data)
    assert result is False


@patch.object(FileFerryTransferSensor, "_get_transfer_status")
@patch.object(FileFerryTransferSensor, "_check_multiple_transfer_status")
def test_poke_success(mock_check_status, mock_get_status, sensor_args):
    """Test poke method success case."""
    mock_get_status.return_value = {"results": []}
    mock_check_status.return_value = True

    sensor = FileFerryTransferSensor(**sensor_args)
    result = sensor.poke({})

    assert result is True
    mock_get_status.assert_called_once()
    mock_check_status.assert_called_once()


@patch.object(FileFerryTransferSensor, "_get_transfer_status")
def test_poke_exception_with_fail_on_error(mock_get_status, sensor_args):
    """Test poke method when exception occurs and fail_on_error is True."""
    mock_get_status.side_effect = Exception("Test error")

    sensor = FileFerryTransferSensor(**sensor_args)

    with pytest.raises(AirflowException, match="Error checking transfer status"):
        sensor.poke({})


@patch.object(FileFerryTransferSensor, "_get_transfer_status")
def test_poke_exception_without_fail_on_error(mock_get_status, sensor_args):
    """Test poke method when exception occurs and fail_on_error is False."""
    mock_get_status.side_effect = Exception("Test error")
    sensor_args["fail_on_error"] = False

    sensor = FileFerryTransferSensor(**sensor_args)
    result = sensor.poke({})

    assert result is False


def test_completion_sensor_initialization():
    """Test FileFerryCompletionSensor initialization."""
    sensor = FileFerryCompletionSensor(
        task_id="test_completion",
        environment="dev",
        connector_id="test_connector",
        aws_conn_id="aws_default",
        transfer_ids=["transfer_1"]
    )

    assert sensor.expected_statuses == ["COMPLETED", "PARTIALLY_COMPLETED", "FAILED"]
    assert sensor.fail_on_error is True


def test_failure_sensor_initialization():
    """Test FileFerryFailureSensor initialization."""
    sensor = FileFerryFailureSensor(
        task_id="test_failure",
        environment="dev",
        connector_id="test_connector",
        aws_conn_id="aws_default",
        transfer_ids=["transfer_1"]
    )

    assert sensor.expected_statuses == ["FAILED"]
    assert sensor.fail_on_error is False


def test_normalize_transfer_ids_with_brackets(sensor_args):
    """Test _normalize_transfer_ids with bracketed string input."""
    sensor = FileFerryTransferSensor(**sensor_args)
    result = sensor._normalize_transfer_ids("['id1', 'id2', 'id3']")

    assert result == ["id1", "id2", "id3"]


@patch("src.airflow_sensors.file_ferry_sensor.file_ferry_sensor.LambdaHook")
def test_get_transfer_status_operation_failed(mock_lambda_hook, sensor_args):
    """Test _get_transfer_status when operation success is False."""
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

    sensor = FileFerryTransferSensor(**sensor_args)

    with pytest.raises(AirflowException, match="Get status operation failed"):
        sensor._get_transfer_status()


def test_check_multiple_transfer_status_no_results(sensor_args):
    """Test _check_multiple_transfer_status when no results are returned."""
    sensor = FileFerryTransferSensor(**sensor_args)
    status_data = {"results": []}

    result = sensor._check_multiple_transfer_status(status_data)
    assert result is False


def test_check_multiple_transfer_status_failed_allowed(sensor_args):
    """Test _check_multiple_transfer_status when FAILED is in expected statuses."""
    sensor_args["expected_status"] = ["COMPLETED", "FAILED"]
    sensor = FileFerryTransferSensor(**sensor_args)
    status_data = {
        "results": [
            {"transfer_id": "transfer_1", "overall_status": "FAILED"},
            {"transfer_id": "transfer_2", "overall_status": "COMPLETED"}
        ]
    }

    result = sensor._check_multiple_transfer_status(status_data)
    assert result is True


def test_normalize_transfer_ids_invalid_type(sensor_args):
    """Test _normalize_transfer_ids with invalid type (not string or list)."""
    sensor = FileFerryTransferSensor(**sensor_args)
    result = sensor._normalize_transfer_ids(123)  # Invalid type

    assert result is None


@patch("src.airflow_sensors.file_ferry_sensor.file_ferry_sensor.LambdaHook")
def test_get_transfer_status_no_payload_key(mock_lambda_hook, sensor_args):
    """Test _get_transfer_status when Lambda response has no Payload key."""
    # Setup mock
    mock_hook_instance = MagicMock()
    mock_response = {}  # No Payload key
    mock_hook_instance.invoke_lambda.return_value = mock_response
    mock_lambda_hook.return_value = mock_hook_instance

    sensor = FileFerryTransferSensor(**sensor_args)

    with pytest.raises(AirflowException, match="No payload in Lambda response"):
        sensor._get_transfer_status()