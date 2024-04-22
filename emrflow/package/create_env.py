"""
This module is used to create conda or docker env from which the required dependencies
are packaged and saved in the output directory
"""

import os
from typing import List

import rich

from emrflow.utils import execute_bash_script


def create_conda_env(
    python_version: str, proxy: str, exec_cmd: List, output_dir: str, include_paths: str
) -> int:
    """
    Create conda environment
    python_version: str : python version
    proxy: str : proxy endpoint
    exec_cmd: List : execution command
    output_dir: str : output directory
    include_paths: List[str] : project directory

    return: return_code: int
    """

    rich.print(f"Additional commands provided:- {exec_cmd}")
    conda_runner = "conda run -n emr_runner"
    inject_cmd = ""

    for each_cmd in exec_cmd:
        inject_cmd += f"{conda_runner} {each_cmd};\n"

    only_paths = [
        path if os.path.isdir(path) else os.path.dirname(path) for path in include_paths
    ]

    conda_commnd = f"""
    set -e

    export HTTP_PROXY={proxy}
    export HTTPS_PROXY={proxy}
    export PATH=/opt/conda/bin:/opt/conda/envs/runner-emr-env/bin:{":".join(only_paths)}:$PATH

    # Download and install Miniconda
    if conda --version &> /dev/null; then
        echo "Conda is already installed"
    else
        echo "Conda is not installed. Installing!!"
        wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh
        /bin/bash ~/miniconda.sh -b -p /opt/conda
        rm -f ~/miniconda.sh
    fi

    mkdir -p {output_dir};

    conda create -n emr_runner python={python_version} -y;
    {inject_cmd}
    pip install conda-pack;
    conda pack -n emr_runner --ignore-missing-files -f -o {output_dir}/pyspark_deps.tar.gz;"""

    returncode = execute_bash_script(conda_commnd)
    if returncode != 0:
        raise Exception("Conda environment creation failed!!")
    return returncode


def create_docker_env(
    python_version: str,
    proxy: str,
    exec_cmd: List,
    output_dir: str,
    include_paths: List[str],
):
    """
    Create docker environment
    python_version: str : python version
    proxy: str : proxy endpoint
    exec_cmd: List : execution command
    output_dir: str : output directory
    include_paths: List[str] : project directory and paths to include

    return: return_code: int
    """

    rich.print(f"Additional commands provided:- {exec_cmd}")

    # Check if Docker is installed and running
    returncode = execute_bash_script("docker --version")

    if returncode != 0:
        rich.print("Docker is not installed or not running!!")
        raise Exception("Docker is not installed or not running!!")

    conda_runner = "RUN conda run -n runner-emr-env "
    inject_cmd = ""

    for each_cmd in exec_cmd:
        inject_cmd += f"{conda_runner} {each_cmd};\n"

    docker_commnd = f"""
    FROM public.ecr.aws/emr-serverless/spark/emr-6.14.0:latest AS builder

    ENV PATH="/opt/conda/bin:$PATH"
    ENV PATH="/opt/conda/envs/runner-emr-env/bin:$PATH"

    ENV HTTP_PROXY={proxy}
    ENV HTTPS_PROXY={proxy}

    USER root

    WORKDIR /build

    COPY {' '.join(include_paths)} .

    RUN yum install -y gcc openssl-devel bzip2-devel libffi-devel tar gzip wget make

    # Download and install Miniconda
    RUN wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh --no-check-certificate && \
        /bin/bash ~/miniconda.sh -b -p /opt/conda

    RUN conda create -n runner-emr-env python={python_version} -y --force
    RUN echo "conda activate runner-emr-env" > ~/.bashrc
    {inject_cmd}

    # export conda environment to zip
    RUN mkdir -p dist && pip install conda-pack && conda pack -n runner-emr-env --ignore-missing-files -f -o /build/dist/pyspark_deps.tar.gz;

    #Stage 2: Copy files to scratch image
    FROM scratch AS export
    COPY --from=builder /build/dist/pyspark_deps.tar.gz .
    """

    with open(f"{output_dir}/Dockerfile", "w") as file:
        file.write(docker_commnd)
    returncode = execute_bash_script(
        f"docker buildx build -f {output_dir}/Dockerfile --output type=local,dest={output_dir} ."
    )

    if returncode != 0:
        raise Exception("Docker build failed!!")
    return returncode
