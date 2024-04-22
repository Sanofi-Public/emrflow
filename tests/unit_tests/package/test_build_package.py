from unittest.mock import Mock, patch

import pytest
from unit_tests.fixtures import mock_subprocess_run


@pytest.fixture(scope="function")
def mock_create_docker_env():
    with patch(
        "emrflow.package.create_env.create_docker_env"
    ) as mock_create_docker_env:
        yield mock_create_docker_env


@pytest.fixture(scope="function")
def mock_create_conda_env():
    with patch("emrflow.package.create_env.create_conda_env") as mock_create_conda_env:
        yield mock_create_conda_env


@pytest.fixture(scope="function")
def mock_create_packaged_dependency_src():
    with patch(
        "emrflow.package.project_dependency_src.create_packaged_dependency_src"
    ) as mock_create_packaged_dependency_src:
        yield mock_create_packaged_dependency_src


def test_build_package_with_python_package(
    mock_create_packaged_dependency_src, mock_subprocess_run
):
    from emrflow.package.build_package import build_package

    mock_create_packaged_dependency_src.return_value = 0

    return_code = build_package(
        "output_dir",
        True,
        ["project_dir/**"],
        False,
        "conda",
        ["exec_cmd"],
        "3.8",
        "proxy",
    )

    # assert the create_packaged_dependency_src is called with the correct command
    mock_create_packaged_dependency_src.assert_called_once_with(
        include_paths=["project_dir/**"], output_dir="output_dir"
    )


# def test_build_package_with_conda(mock_create_conda_env, mock_subprocess_run):
#     from emrflow.package.build_package import build_package

#     mock_create_conda_env.return_value = 0
#     mock_process = Mock()
#     mock_process.returncode = 0
#     mock_subprocess_run.return_value = mock_process

#     return_code = build_package(
#         "output_dir",
#         False,
#         "project_dir",
#         True,
#         [],
#         "conda",
#         ["exec_cmd"],
#         "3.8",
#         "proxy",
#     )

#     # assert the create_conda_env is called with the correct command
#     mock_create_conda_env.assert_called_once_with(
#         python_version="3.8",
#         proxy="proxy",
#         output_dir="output_dir",
#         project_dir="project_dir",
#         exec_cmd=["exec_cmd"],
#     )


# def test_build_package_with_docker(
#     mock_create_docker_env, mock_subprocess_run
# ):
#     from emrflow.package.build_package import build_package

#     mock_create_docker_env.return_value = 0
#     mock_process = Mock()
#     mock_process.returncode = 0
#     mock_subprocess_run.return_value = mock_process

#     return_code = build_package(
#         "output_dir",
#         False,
#         "project_dir",
#         True,
#         [],
#         "docker",
#         ["exec_cmd"],
#         "3.8",
#         "proxy",
#     )

#     # assert the create_conda_env is called with the correct command
#     mock_create_docker_env.assert_called_once_with(
#         python_version="3.8",
#         proxy="proxy",
#         output_dir="output_dir",
#         project_dir="project_dir",
#         exec_cmd=["exec_cmd"],
#     )
