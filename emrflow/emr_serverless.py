"""CLI and API for EMR Serverless"""

import json
from pathlib import Path
from typing import Dict, List, Optional

import rich
import typer
from typing_extensions import Annotated

from emrflow.deployment.emr_sls import EMRServerless
from emrflow.package.build_package import build_package

app = typer.Typer(pretty_exceptions_show_locals=False)
global_obj_dict = {"emr_serverless": None}


@app.callback(invoke_without_command=True)
def init_connection(
    config_path: Annotated[
        str,
        typer.Option(
            help="Serverless configuration .json file",
        ),
    ] = str(Path.home())
    + "/emr_serverless_config.json"
):
    """Initialize connection with EMR Serverless"""
    rich.print(f"~~Config Path: {config_path}~~")
    # Open and read the JSON config file
    with open(config_path, "r") as config_file:
        config = json.load(config_file)
    global_obj_dict["emr_serverless"] = EMRServerless(
        config["application_id"], config["job_role"], config["region"]
    )
    rich.print("Connection established!!")


@app.command()
def package_dependencies(
    output_dir: Annotated[
        str,
        typer.Option(
            help="Output directory to save the packaged dependencies",
        ),
    ] = str(Path.cwd())
    + "/dist",
    package_project: Annotated[
        Optional[bool],
        typer.Option(
            help="Do you want to package project containing .py/.json/yaml files?",
        ),
    ] = False,
    include_paths: Annotated[
        List[str],
        typer.Option(
            help="Directories/files to include in the project package",
        ),
    ] = [str(Path.cwd())],
    package_env: Annotated[
        Optional[bool],
        typer.Option(
            help="Build and Package the environment?",
        ),
    ] = False,
    env_type: Annotated[
        str,
        typer.Option(
            help="Type of environment 'Docker' or 'Conda'",
        ),
    ] = "conda",
    env_exec_cmd: Annotated[
        List[str],
        typer.Option(
            help="Execute command to install libraries when compiling environment. ex: --env-exec-cmd 'pip install -r requirements.txt'"
        ),
    ] = ["pip install poetry==1.7.1", "poetry install"],
    env_python_version: Annotated[
        float, typer.Option(help="Python version of environment")
    ] = 3.9,
    env_proxy: Annotated[
        str,
        typer.Option(
            help="Proxy endpoint",
        ),
    ] = "",
):
    """Package dependencies for the project"""
    build_package(
        output_dir=output_dir,
        package_project=package_project,
        include_paths=include_paths,
        package_env=package_env,
        env_type=env_type,
        env_exec_cmd=env_exec_cmd,
        env_python_version=env_python_version,
        env_proxy=env_proxy,
    )


@app.command()
def run(
    job_name: Annotated[str, typer.Option(help="Name of the Job")],
    entry_point: Annotated[
        str, typer.Option(help="Path of python file for the main entrypoint")
    ],
    spark_submit_parameters: Annotated[
        str, typer.Option(help="String containing spark submit options")
    ],
    s3_code_uri: Annotated[
        str, typer.Option(help="Location of s3 to copy project artifacts")
    ],
    s3_logs_uri: Annotated[str, typer.Option(help="Location of s3 to send logs")] = "",
    entry_point_arguments: Annotated[
        Optional[List[str]],
        typer.Option(
            help="Comma seperated string arguments during entrypoint execution",
        ),
    ] = None,
    execution_timeout: Annotated[
        Optional[int],
        typer.Option(
            help="The maximum duration for the job run to run. If the job run runs beyond this duration, it will be automatically cancelled.",
        ),
    ] = 0,
    ping_duration: Annotated[
        Optional[int],
        typer.Option(
            help="Ping duration (in sec) to check the status when job tracking is enabled",
        ),
    ] = 30,
    tags: Annotated[
        List[str],
        typer.Option(
            help="Add tags such as --tags key:value",
        ),
    ] = None,
    wait: Annotated[
        Optional[bool],
        typer.Option(
            help="Wait for the job to finish, remove this argument if you want to run job in background",
        ),
    ] = False,
    show_output: Annotated[
        bool,
        typer.Option(help="Show the output of logs after job is finished"),
    ] = False,
    exclude_paths: Annotated[
        List[str],
        typer.Option(
            help="File paths to be excluded during the upload process (Useful when reusing the artifacts already available in S3). e.g 'dist/pyspark_deps.tar.gz'",
        ),
    ] = [],
):
    """Run PySpark job on EMR Serverless"""
    rich.print("Running emr serverless application!!")
    if global_obj_dict["emr_serverless"] is None:
        rich.print(
            "Please run `init-connection` command to establish connection with EMR"
        )

    # get list of artifacts to upload
    artifacts = global_obj_dict["emr_serverless"].get_artifacts(
        spark_submit_parameters=spark_submit_parameters
    )

    # upload library dependencies, project modules and entry point to S3
    src_dest_uri = global_obj_dict["emr_serverless"].upload_artifacts(
        s3_code_uri=s3_code_uri,
        artifacts=artifacts + [entry_point],
        excludes=exclude_paths,
    )

    # Submit PySpark job to EMR Serverless
    response = global_obj_dict["emr_serverless"].run_job(
        job_name=job_name,
        entry_point_uri=entry_point,
        entry_point_arguments=entry_point_arguments,
        spark_submit_opts=spark_submit_parameters,
        wait=wait,
        show_logs=show_output,
        s3_code_uri=s3_code_uri,
        s3_logs_uri=s3_logs_uri,
        execution_timeout=execution_timeout,
        ping_duration=ping_duration,
        tags=tags,
        src_dest_uri=src_dest_uri,
    )
    return response


@app.command()
def list_job_runs(
    max_results: Annotated[
        int,
        typer.Option(
            help="The maximum number of job runs that can be listed",
        ),
    ],
    states: Annotated[
        Optional[List[str]],
        typer.Option(
            help="An optional filter for job run states. Note that if this filter contains multiple states, the resulting list will be grouped by the state. e.g. --states SUCCESSFUL --states FAILED",
        ),
    ] = None,
):
    """List job runs"""
    if global_obj_dict["emr_serverless"] is None:
        rich.print(
            "Please run `init-connection` command to establish connection with EMR"
        )
        raise typer.Exit()

    if states is None:
        states = [
            "SUBMITTED",
            "PENDING",
            "SCHEDULED",
            "RUNNING",
            "SUCCESS",
            "FAILED",
            "CANCELLING",
            "CANCELLED",
        ]
    response = global_obj_dict["emr_serverless"].list_job_runs(max_results, states)
    rich.print(response)
    return response


@app.command()
def get_job_run(
    job_id: Annotated[
        str,
        typer.Option(
            help="Job ID",
        ),
    ],
) -> Dict:
    """Get job run"""
    response = global_obj_dict["emr_serverless"].get_job_run(job_id)
    rich.print(response)
    return response


@app.command()
def cancel_job_run(
    job_id: Annotated[
        str,
        typer.Option(
            help="Job ID",
        ),
    ],
) -> Dict:
    """Cancel job run"""
    response = global_obj_dict["emr_serverless"].cancel_job_run(job_id)
    rich.print(response)
    return response


@app.command()
def get_dashboard_for_job_run(
    job_id: Annotated[
        str,
        typer.Option(
            help="Job ID",
        ),
    ],
) -> str:
    """Get dashboard for job run"""
    response = global_obj_dict["emr_serverless"].get_dashboard_for_job_run(job_id)
    rich.print(f"Job: {job_id}, DASHBOARD LINK: {response}")
    return response


@app.command()
def resume_job_tracking(
    job_id: Annotated[
        str,
        typer.Option(
            help="Job ID",
        ),
    ],
    show_output: Annotated[
        bool,
        typer.Option(help="Show the output of logs after job is finished"),
    ] = True,
    ping_duration: Annotated[
        Optional[int],
        typer.Option(
            help="Ping duration (in sec) that status when job tracking is enabled",
        ),
    ] = 30,
) -> Dict:
    """Resume job tracking"""
    _, _, jr_response = global_obj_dict["emr_serverless"].job_tracking(
        job_id, show_output, ping_duration
    )
    return jr_response


@app.command()
def get_logs(
    job_id: Annotated[
        str,
        typer.Option(
            help="Job ID",
        ),
    ],
) -> str:
    """Get logs for job run"""
    response = global_obj_dict["emr_serverless"].show_logs(job_id, log_read_pos=0)
    return response
