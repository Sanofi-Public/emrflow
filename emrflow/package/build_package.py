"""Build package for the project"""

from typing import List

import rich

from emrflow.package.create_env import create_conda_env, create_docker_env
from emrflow.package.project_dependency_src import create_packaged_dependency_src


def build_package(
    output_dir: str,
    package_project: bool,
    include_paths: List[str],
    package_env: bool,
    env_type: str,
    env_exec_cmd: List[str],
    env_python_version: str,
    env_proxy: str,
) -> int:
    """
    Build package for the project
    output_dir: str : output directory
    package_project: bool : package project
    include_paths: List[str] : project directory
    package_env: bool : package environment
    env_type: str : environment type
    env_exec_cmd: List[str] : environment execution command
    env_python_version: str : python version
    env_proxy: str : environment proxy

    return: return_code : int
    """

    rich.print("Building code package and required dependencies")

    # package project src dependencies
    if package_project:
        return_code = create_packaged_dependency_src(
            include_paths=include_paths,
            output_dir=output_dir,
        )

    if package_env:
        # compile conda environment
        rich.print(f"Building and packaging {env_type} environment...")
        if env_type == "conda":
            return_code = create_conda_env(
                python_version=str(env_python_version),
                proxy=env_proxy,
                output_dir=output_dir,
                include_paths=include_paths,
                exec_cmd=env_exec_cmd,
            )
        if env_type == "docker":
            return_code = create_docker_env(
                python_version=str(env_python_version),
                proxy=env_proxy,
                output_dir=output_dir,
                include_paths=include_paths,
                exec_cmd=env_exec_cmd,
            )

    return return_code
