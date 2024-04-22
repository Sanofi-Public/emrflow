"""EMR class to interact with EMR"""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from time import sleep
from typing import Dict, List, Tuple

import boto3
import rich

from emrflow.utils import print_s3_gz, upload_package


class EMR(ABC):
    """
    EMR class to interact with EMR
    """

    def __init__(
        self,
        application_cluster_id: str,
        job_role: str,
        region: str,
        emr_type: str,
    ) -> None:
        """
        Initialize EMR class
        application_cluster_id: str : application cluster id
        job_role: str : job role
        region: str : region
        emr_type: str : emr type
        """

        self.application_cluster_id = application_cluster_id
        self.job_role = job_role
        self.s3_client = boto3.client("s3")
        self.s3_job_log_uri = ""
        self.err_log_uri = ""
        self.emr_type = emr_type

        self.emr_client = boto3.client(emr_type)
        if region:
            self.emr_client = boto3.client(emr_type, region_name=region)

    @abstractmethod
    def get_job_run(self, job_run_id: str) -> Dict:
        """Abstract Method to get job run"""
        pass

    @abstractmethod
    def get_dashboard_for_job_run(self, job_run_id: str) -> str:
        """Abstract method to dashboard for job run"""
        pass

    @abstractmethod
    def run_job(self, job_run_id: str) -> str:
        """Abstract method for job run"""
        pass

    @abstractmethod
    def list_job_runs(self, max_results: int, states: List) -> List:
        """Abstract method to list the jobs based on the states"""
        pass

    @abstractmethod
    def cancel_job_run(self, job_run_id: str) -> Dict:
        """Abstract method for cancelling the job run"""
        pass

    def get_implicit_tags(self) -> Dict:
        """Get implicit tags for the job run"""
        return {
            "utility": "emrflow",
        }

    def get_artifacts(self, spark_submit_parameters: str) -> List[str]:
        """
        Get artifacts
        s3_code_uri: str : s3 code uri
        spark_submit_parameters: str : spark submit parameters

        return: List : artifacts
        """
        # convert string to dict
        spark_submit_params = [
            item.split("=") for item in spark_submit_parameters.split("--conf ")
        ]
        spark_submit_params = {
            key.strip(): value.strip()
            for item_list in spark_submit_params
            if len(item_list) == 2
            for key, value in [item_list]
        }

        artifacts = []
        # find relevant artifacts from spark_submit_parameters
        if spark_submit_params.get("spark.submit.pyFiles"):
            artifacts.extend(spark_submit_params.get("spark.submit.pyFiles").split(","))
        if spark_submit_params.get("spark.archives"):
            artifacts.extend(
                [
                    archives.split("#")[0]
                    for archives in spark_submit_params.get("spark.archives").split(",")
                ]
            )
        if spark_submit_params.get("spark.jars"):
            artifacts.extend(spark_submit_params.get("spark.jars").split(","))
        if spark_submit_params.get("spark.files"):
            artifacts.extend(spark_submit_params.get("spark.files").split(","))
        if spark_submit_params.get("spark.jars.packages"):
            artifacts.extend(spark_submit_params.get("spark.jars.packages").split(","))

        return artifacts

    def upload_artifacts(
        self, s3_code_uri: str, artifacts: List[str], excludes: List[str]
    ) -> str:
        """
        Upload local artifacts to S3 bucket
        s3_code_uri: str : s3 code uri
        dist_dir: str : dist directory

        return: str : src_target
        """
        src_targets = upload_package(self.s3_client, s3_code_uri, artifacts, excludes)
        return src_targets

    def job_tracking(
        self, job_run_id: str, show_logs: bool, ping_duration: int
    ) -> Tuple[bool, str, dict]:
        """
        Track job run status and logs
        job_run_id: str : job run id
        show_logs: bool : show logs of the job run
        ping_duration: int : duration to ping the job run status

        return: bool : job_done
        return: str : job_state
        return: dict : jr_response
        """
        job_done = False
        job_state = "SUBMITTED"
        jr_response = {}
        log_read_pos = 0
        start_time = datetime.now()
        rich.print(f"Log Location for job: {job_run_id} :- \n {self.s3_job_log_uri}")
        rich.print(
            f"Std err:- Log Location for job: {job_run_id} :- \n {self.err_log_uri}"
        )

        while not job_done:
            jr_response = self.get_job_run(job_run_id)
            new_state = jr_response.get("state")
            if new_state != job_state:
                rich.print(f"Job state is now: {new_state}")
                job_state = new_state

            if datetime.now() - start_time >= timedelta(minutes=10):
                print(f"Dashboard: {self.get_dashboard_for_job_run(job_run_id)}")
                start_time = datetime.now()

            if show_logs:
                try:
                    log_read_pos = self.show_logs(job_run_id, log_read_pos=log_read_pos)
                except Exception as ex:
                    print(ex)

            job_done = new_state in [
                "SUCCESS",
                "FAILED",
                "CANCELLING",
                "CANCELLED",
                "COMPLETED",
            ]
            sleep(ping_duration)

        return job_done, jr_response.get("state"), jr_response

    def show_logs(self, job_run_id: str, log_read_pos: int) -> int:
        """
        Print logs of completed job_id
        job_run_id: str : job run id
        log_read_pos: int : log read position

        return: int : log_read_pos
        """
        jr_response = self.get_job_run(job_run_id)
        s3_logs_uri = (
            jr_response.get("configurationOverrides")
            .get("monitoringConfiguration")
            .get("s3MonitoringConfiguration")
            .get("logUri")
        )

        try:
            return print_s3_gz(
                self.s3_client,
                self.s3_job_log_uri.replace("s3_logs_uri", s3_logs_uri).replace(
                    "job_run_id", job_run_id
                ),
                last_position=log_read_pos,
            )
        except Exception as e:
            print("Error in printing logs")
            return print_s3_gz(
                self.s3_client,
                self.err_log_uri.replace("s3_logs_uri", s3_logs_uri).replace(
                    "job_run_id", job_run_id
                ),
                last_position=0,
            )
