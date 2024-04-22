from unittest.mock import Mock, call

from unit_tests.fixtures import mock_open, mock_subprocess_run


def test_create_docker_env(mock_subprocess_run, mock_open):
    from emrflow.package.create_env import create_docker_env

    mock_process = Mock()
    mock_process.returncode = 0
    mock_subprocess_run.side_effect = [mock_process] * 2

    return_code = create_docker_env(
        "3.8", "proxy", ["exec_cmd"], "output_dir", ["project_dir/**"]
    )

    expected_calls = [
        call(["bash", "-c", "docker --version"], check=True),
        call(
            [
                "bash",
                "-c",
                "docker buildx build -f output_dir/Dockerfile --output type=local,dest=output_dir .",
            ],
            check=True,
        ),
    ]

    mock_subprocess_run.assert_has_calls(expected_calls)
    mock_open.assert_called_once_with("output_dir/Dockerfile", "w")


def test_create_conda_env(mock_subprocess_run):
    from emrflow.package.create_env import create_conda_env

    mock_process = Mock()
    mock_process.returncode = 0
    mock_subprocess_run.return_value = mock_process

    # call the create_conda_env function
    return_code = create_conda_env(
        "3.8", "proxy", ["exec_cmd"], "output_dir", ["project_dir/**"]
    )
    # assert the execute_bash_script is called with the correct command
    expected_conda_commnd = f"""
    set -e

    export HTTP_PROXY=proxy
    export HTTPS_PROXY=proxy
    export PATH=/opt/conda/bin:/opt/conda/envs/runner-emr-env/bin:project_dir:$PATH

    # Download and install Miniconda
    if conda --version &> /dev/null; then
        echo "Conda is already installed"
    else
        echo "Conda is not installed. Installing!!"
        wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh
        /bin/bash ~/miniconda.sh -b -p /opt/conda
        rm -f ~/miniconda.sh
    fi

    mkdir -p output_dir;

    conda create -n emr_runner python=3.8 -y;
    conda run -n emr_runner exec_cmd;\n
    pip install conda-pack;
    conda pack -n emr_runner --ignore-missing-files -f -o output_dir/pyspark_deps.tar.gz;"""

    mock_subprocess_run.assert_called_once_with(
        ["bash", "-c", expected_conda_commnd], check=True
    )
    assert return_code == 0
