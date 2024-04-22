import unittest
from unittest.mock import Mock

from unit_tests.fixtures import mock_subprocess_run


def test_create_packaged_dependency_src(mock_subprocess_run):
    from emrflow.package.project_dependency_src import create_packaged_dependency_src

    mock_process = Mock()
    mock_process.returncode = 0
    mock_subprocess_run.return_value = mock_process

    # call the create_packaged_dependency_src function with the correct arguments
    return_code = create_packaged_dependency_src(
        "output_dir",
        ["data_pipeline/**"],
    )
    # assert the execute_bash_script is called with the correct command

    expected_packaged_src_command = f"""

    mkdir -p output_dir;

    rm -f output_dir/project-dependency-src.zip

    # Add directories and files to the zip
    for path in data_pipeline/**; do
        if [ -e "$path" ]; then
            continue
        else
            echo "Path: '$path' does not exist. Please check the path and try again!"
            exit 1
        fi
    done

    zip -r  output_dir/project-dependency-src.zip data_pipeline/** \
        -x "*.git/**"  \
        -x "*.github/**"  \
        -x "*.vscode/**" \
        -x "*__pycache__*"

    echo "output_dir/project-dependency-src.zip created successfully!!"
    """

    assert return_code == 0
    mock_subprocess_run.assert_called_once_with(
        ["bash", "-c", expected_packaged_src_command], check=True
    )
