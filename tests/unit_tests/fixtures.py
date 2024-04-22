from unittest.mock import Mock, mock_open, patch

import pytest


@pytest.fixture(scope="function")
def mock_emr_client():
    with patch("emrflow.deployment.emr.boto3.client") as mock_client:
        yield mock_client


@pytest.fixture(scope="function")
def mock_subprocess_run():
    with patch("subprocess.run") as mock_subprocess_run:
        yield mock_subprocess_run


@pytest.fixture
def mock_open():
    with patch("builtins.open") as mock_open:
        yield mock_open


@pytest.fixture
def mock_print_s3_gz():
    with patch("emrflow.utils.print_s3_gz") as mock_print_s3_gz:
        yield mock_print_s3_gz
