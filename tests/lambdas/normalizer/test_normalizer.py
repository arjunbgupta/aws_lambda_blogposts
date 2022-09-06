import pytest
from unittest.mock import Mock
import os
import json
import pandas as pd
from lambdas.normalizer.normalizer import Normalizer


@pytest.fixture(scope="session")
def lambda_event(request):
    """Return the json event as a dictionnary."""
    with open(os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "test_data", "sample_event.json"), "r"
    ) as f:
        event = json.load(f)

    return event


@pytest.fixture(scope="session")
def config_path(request):
    """Return the path of the normalizer config."""
    return os.path.join(
        request.config.rootdir, "lambdas", "wwc", "config", "normalizer_config.yaml"
    )


@pytest.fixture(scope="session")
def expected_normalized_df(request):
    """Return the expected normalized DataFrame of the test_matches.json file."""
    file_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "test_data", "expected_normalized.csv"
    )

    return pd.read_csv(file_path)


def test_normalize_data(config_path, expected_normalized_df):
    # Arrange
    # Read json data file
    with open(os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "test_data", "test_matches.json"), "r"
    ) as f:
        json_matches = json.load(f)

    mock_client = Mock()

    # Initialize WWC normalizer
    lambda_handler = Normalizer(
        mock_client,
        "test_raw_bucket",
        "test_normalized_bucket",
        config_path
    )

    # Act: call the normalize_data function
    actual = lambda_handler.normalize_data(json_matches)

    # Assert
    pd.testing.assert_frame_equal(actual, expected_normalized_df)


def test_add_metadata(config_path, expected_normalized_df):
    # Arrange
    mock_client = Mock()

    # Initialize WWC normalizer
    lambda_handler = Normalizer(
        mock_client,
        "test_raw_bucket",
        "test_normalized_bucket",
        config_path
    )

    # Act: call the add_metadata function
    lambda_handler.add_metadata(expected_normalized_df)

    # Assert
    assert (expected_normalized_df["division"] == lambda_handler.config["division"]).all()
    assert expected_normalized_df["normalization_datetime"].notnull().all()
    assert (expected_normalized_df["normalization_datetime"] == expected_normalized_df["normalization_datetime"][0]).all()
    assert expected_normalized_df["normalization_uuid"].notnull().all()
    assert (expected_normalized_df["normalization_uuid"] == expected_normalized_df["normalization_uuid"][0]).all()


def test_normalizer_happy_path(config_path, lambda_event, lambda_context, caplog):
    # Arrange
    # Read json data file as a string to mock the s3_client.get_object() call
    mock_body = Mock()
    with open(os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "test_data", "test_matches.json"), "r"
    ) as f:
        mock_body_content = f.read()
    mock_body.read.return_value.decode.return_value = mock_body_content

    mock_client = Mock()
    mock_client.get_object.return_value = {"Body": mock_body}

    # Initialize WWC normalizer
    lambda_handler = Normalizer(
        mock_client,
        "test_raw_bucket",
        "test_normalized_bucket",
        config_path
    )

    # Act: call the lambda handler
    lambda_handler(lambda_event, lambda_context)

    # Assert: verify execution was completed successfully
    mock_client.get_object.assert_called_once()
    mock_client.put_object.assert_called_once()
    mock_client.copy_object.assert_called_once()
    mock_client.delete_object.assert_called_once()

    # Assert logging
    assert "Successful normalization." in caplog.text
