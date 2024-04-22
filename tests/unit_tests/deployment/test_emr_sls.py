"""Test cases for the EMRServerless class"""

from unit_tests.fixtures import mock_emr_client, mock_print_s3_gz

from emrflow.deployment.emr_sls import EMRServerless


def test_get_job_run(mock_emr_client):
    """Test get_job_run method"""

    mock_emr_client.return_value.get_job_run.return_value = {
        "jobRun": {"jobId": "123", "state": "COMPLETED"}
    }

    emr_serverless = EMRServerless("application_id", "job_role")
    result = emr_serverless.get_job_run("job_run_id")

    mock_emr_client.return_value.get_job_run.assert_called_once_with(
        applicationId="application_id", jobRunId="job_run_id"
    )

    assert result == {"jobId": "123", "state": "COMPLETED"}


def test_get_dashboard_for_job_run(mock_emr_client):
    """Test get_job_run method"""

    mock_emr_client.return_value.get_dashboard_for_job_run.return_value = {
        "url": "http://dashboard.com"
    }

    emr_serverless = EMRServerless("application_id", "job_role")
    result = emr_serverless.get_dashboard_for_job_run("job_run_id")

    mock_emr_client.return_value.get_dashboard_for_job_run.assert_called_once_with(
        applicationId="application_id", jobRunId="job_run_id"
    )

    assert result == "http://dashboard.com"


def test_list_job_runs(mock_emr_client):
    """Test list_job_runs method"""

    mock_emr_client.return_value.list_job_runs.return_value = {
        "jobRuns": [{"jobId": "123", "state": "COMPLETED"}]
    }

    emr_serverless = EMRServerless("application_id", "job_role")
    result = emr_serverless.list_job_runs(10, ["COMPLETED"])

    mock_emr_client.return_value.list_job_runs.assert_called_once_with(
        applicationId="application_id", maxResults=10, states=["COMPLETED"]
    )

    assert result == [{"jobId": "123", "state": "COMPLETED"}]


def test_cancel_job_run(mock_emr_client):
    """Test cancel_job_run method"""

    mock_emr_client.return_value.cancel_job_run.return_value = {
        "jobId": "123",
        "state": "CANCELLED",
    }

    emr_serverless = EMRServerless("application_id", "job_role")
    result = emr_serverless.cancel_job_run("job_run_id")

    mock_emr_client.return_value.cancel_job_run.assert_called_once_with(
        applicationId="application_id", jobRunId="job_run_id"
    )

    assert result == {"jobId": "123", "state": "CANCELLED"}


def test_run_job(mock_emr_client):
    """Test run_job method"""

    mock_emr_client.return_value.start_job_run.return_value = {"jobRunId": "456"}

    emr_serverless = EMRServerless("application_id", "job_role")

    result = emr_serverless.run_job(
        job_name="test_job",
        entry_point_uri="s3://entry_point_uri",
        wait=False,
        show_logs=False,
        s3_code_uri="s3://code_uri",
        s3_logs_uri="s3://logs_uri",
        execution_timeout=60,
        ping_duration=10,
        tags=["tag1:tag1", "tag2:tag2"],
        src_dest_uri={"src1": "dest1", "src2": "dest2"},
    )

    expected_tags = {"tag1": "tag1", "tag2": "tag2", "utility": "emrflow"}
    expected_job_driver = {
        "sparkSubmit": {"entryPoint": "s3://code_uri/s3://entry_point_uri"}
    }
    expected_config_overrides = {
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {"logUri": "s3://logs_uri"}
        }
    }

    mock_emr_client.return_value.start_job_run.assert_called_once_with(
        applicationId="application_id",
        executionRoleArn="job_role",
        name="test_job",
        jobDriver=expected_job_driver,
        configurationOverrides=expected_config_overrides,
        tags=expected_tags,
        executionTimeoutMinutes=60,
    )

    assert result == "456"


def test_get_artifacts(mock_emr_client):
    """Test get_artifacts method"""
    spark_submit_parameters = "--conf spark.submit.pyFiles=s3://bucket1/file1.py,s3://bucket2/file2.py --conf spark.archives=s3://bucket1/archive1.zip#archive1.zip --conf spark.jars=s3://bucket1/jar1.jar,s3://bucket2/jar2.jar --conf spark.files=s3://bucket1/file1.txt,s3://bucket2/file2.txt"
    emr_serverless = EMRServerless("application_id", "job_role")
    result = emr_serverless.get_artifacts(spark_submit_parameters)

    assert result == [
        "s3://bucket1/file1.py",
        "s3://bucket2/file2.py",
        "s3://bucket1/archive1.zip",
        "s3://bucket1/jar1.jar",
        "s3://bucket2/jar2.jar",
        "s3://bucket1/file1.txt",
        "s3://bucket2/file2.txt",
    ]


def test_job_tracking(mock_emr_client):
    mock_emr_client.return_value.get_job_run.return_value = {
        "jobRun": {
            "jobId": "123",
            "state": "COMPLETED",
            "configurationOverrides": {
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {"logUri": "s3://logs_uri"}
                }
            },
        }
    }
    emr_serverless = EMRServerless("application_id", "job_role")
    job_done, status, response = emr_serverless.job_tracking(
        "job_run_id", show_logs=False, ping_duration=1
    )

    assert status == "COMPLETED"


def test_show_logs(mock_emr_client, mock_print_s3_gz):
    mock_emr_client.return_value.get_job_run.return_value = {
        "jobRun": {
            "jobId": "123",
            "state": "COMPLETED",
            "configurationOverrides": {
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {"logUri": "s3://logs_uri"}
                }
            },
        }
    }

    mock_print_s3_gz.return_value = 0

    emr_serverless = EMRServerless("application_id", "job_role")
    return_val = emr_serverless.show_logs("123", 0)

    assert return_val == 0
