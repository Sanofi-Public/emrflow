"""EMR Serverless class"""

from os.path import join
from typing import Dict, List, Optional, Tuple

import rich

from emrflow.deployment.emr import EMR
from emrflow.utils import convert_to_dict


class EMRServerless(EMR):
    """
    EMR Serverless class
    """

    def __init__(
        self,
        application_id: str,
        job_role: str,
        region: str = "",
    ) -> None:
        super().__init__(
            application_cluster_id=application_id,
            job_role=job_role,
            region=region,
            emr_type="emr-serverless",
        )
        self.s3_job_log_uri, self.err_log_uri = self.__get_s3_log_uri(
            "s3_logs_uri", "job_run_id"
        )

    def __get_s3_log_uri(self, s3_logs_uri: str, job_run_id: str) -> Tuple[str, str]:
        """
        Get s3 log uri
        s3_logs_uri: str : s3 logs uri
        application_cluster_id: str : application cluster id
        job_run_id: str : job run id

        return: str : s3_job_log_uri, err_log_uri
        """
        s3_job_log_uri = join(
            f"{s3_logs_uri}",
            "applications",
            self.application_cluster_id,
            "jobs",
            job_run_id,
            "SPARK_DRIVER",
            "stdout.gz",
        )

        err_log_uri = join(
            f"{s3_logs_uri}",
            "applications",
            self.application_cluster_id,
            "jobs",
            job_run_id,
            "SPARK_DRIVER",
            "stderr.gz",
        )

        return s3_job_log_uri, err_log_uri

    def __entry_point(
        self, job_driver: Dict, s3_code_uri: str, entry_point_uri: str
    ) -> Dict:
        job_driver["sparkSubmit"]["entryPoint"] = f"{s3_code_uri}/{entry_point_uri}"
        return job_driver

    def __spark_submit_parameters(
        self, job_driver: Dict, src_dest_uri: Dict, spark_submit_opts: str
    ) -> Dict:
        """
        Spark submit parameters
        job_driver: dict : job driver
        src_dest_uri: Dict : dict containing key as src path and value as dest path in S3
        spark_submit_opts: str : spark submit options

        return: dict : job_driver
        """

        if spark_submit_opts:
            for key, value in src_dest_uri.items():
                spark_submit_opts = spark_submit_opts.replace(key, value)

            job_driver["sparkSubmit"][
                "sparkSubmitParameters"
            ] = spark_submit_opts.strip()
        return job_driver

    def __entry_point_arguments(
        self, job_driver: Dict, entry_point_arguments: List[str]
    ) -> Dict:
        if entry_point_arguments:
            job_driver["sparkSubmit"]["entryPointArguments"] = entry_point_arguments
        return job_driver

    def __configure_overrides(self, config_overrides: Dict, s3_logs_uri: str) -> Dict:
        if s3_logs_uri:
            config_overrides = {
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {"logUri": s3_logs_uri}
                }
            }
        return config_overrides

    def get_job_run(self, job_run_id: str) -> Dict:
        """
        Get job run details for a given job run id
        job_run_id: str : job run id
        """
        job_run_response = self.emr_client.get_job_run(
            applicationId=self.application_cluster_id, jobRunId=job_run_id
        )
        return job_run_response.get("jobRun")

    def get_dashboard_for_job_run(self, job_run_id: str) -> str:
        """
        Get dashboard for a given job run id
        job_run_id: str : job run id

        return: str : dashboard url
        """
        job_dashbord_response = self.emr_client.get_dashboard_for_job_run(
            applicationId=self.application_cluster_id, jobRunId=job_run_id
        )
        return job_dashbord_response.get("url")

    def list_job_runs(self, max_results: int, states: List) -> List:
        """
        List job runs
        max_results: int : maximum results
        states: List : states

        return: List : job_runs
        """
        job_runs_response = self.emr_client.list_job_runs(
            applicationId=self.application_cluster_id,
            maxResults=max_results,
            states=states,
        )
        return job_runs_response.get("jobRuns")

    def cancel_job_run(self, job_run_id: str) -> Dict:
        """
        Cancel a job run
        job_run_id: str : job run id

        return: dict : cancel_job_run_response
        """
        cancel_job_run_response = self.emr_client.cancel_job_run(
            applicationId=self.application_cluster_id, jobRunId=job_run_id
        )
        return cancel_job_run_response

    def run_job(
        self,
        job_name: str,
        entry_point_uri: str,
        entry_point_arguments: Optional[List[str]] = None,
        spark_submit_opts: Optional[str] = None,
        wait: bool = True,
        show_logs: bool = False,
        s3_code_uri: str = None,
        s3_logs_uri: Optional[str] = None,
        execution_timeout: int = 0,
        ping_duration: int = 30,
        tags: Optional[List[str]] = None,
        src_dest_uri: Dict = None,
    ) -> str:
        """
        Submit a job to EMR Serverless
        job_name (str): Name of the job
        entry_point_uri (str): URI of the entry point
        entry_point_arguments (List[str]): Arguments for the entry point
        spark_submit_opts (str): Additional spark submit options
        wait (bool): Wait for the job to complete
        show_logs (bool): Show logs of the job
        s3_code_uri (str): S3 URI of the code
        s3_logs_uri (str): S3 URI of the logs
        execution_timeout (int): Timeout for the job
        ping_duration (int): Duration between pings
        tags (List[str]): Custom tags for the job
        src_dest_uri (Dict): Source and destination URI

        return: str : job_run_id
        """

        if show_logs and not s3_logs_uri:
            raise RuntimeError("--show_stdout requires --s3_logs_uri to be set.")

        job_driver = {"sparkSubmit": {}}
        job_driver = self.__entry_point(job_driver, s3_code_uri, entry_point_uri)
        job_driver = self.__spark_submit_parameters(
            job_driver, src_dest_uri, spark_submit_opts
        )
        job_driver = self.__entry_point_arguments(job_driver, entry_point_arguments)

        tags_dict = {}
        if tags:
            tags_dict = convert_to_dict(tags)

        config_overrides = {}
        config_overrides = self.__configure_overrides(config_overrides, s3_logs_uri)

        response = self.emr_client.start_job_run(
            applicationId=self.application_cluster_id,
            executionRoleArn=self.job_role,
            name=job_name,
            jobDriver=job_driver,
            configurationOverrides=config_overrides,
            tags={**tags_dict, **self.get_implicit_tags()},
            executionTimeoutMinutes=execution_timeout,
        )
        job_run_id = response.get("jobRunId")

        rich.print(f"Job submitted to EMR Serverless (Job Run ID: {job_run_id})")
        if not wait and not show_logs:
            return job_run_id

        rich.print("Waiting for job to complete...")
        self.s3_job_log_uri, self.err_log_uri = self.__get_s3_log_uri(
            s3_logs_uri, job_run_id
        )
        _, job_state, jr_response = self.job_tracking(
            job_run_id, show_logs, ping_duration
        )

        if job_state != "SUCCESS":
            rich.print(f"EMR Serverless job failed: {jr_response.get('stateDetails')}")
            raise Exception(f"Job {job_run_id} failed with state {job_state}")
        rich.print("Job completed successfully!")

        return job_run_id
