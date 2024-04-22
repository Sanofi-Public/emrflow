"""Create packaged dependency src"""

from typing import List

from emrflow.utils import execute_bash_script


def create_packaged_dependency_src(output_dir: str, include_paths=List) -> int:
    """
    Create project package including .py/.json/yaml files
    output_dir: str : output directory
    paths: str : path to include in the package

    return: return_code : int
    """

    packaged_src_command = f"""

    mkdir -p {output_dir};

    rm -f {output_dir}/project-dependency-src.zip

    # Add directories and files to the zip
    for path in {" ".join(include_paths)}; do
        if [ -e "$path" ]; then
            continue
        else
            echo "Path: '$path' does not exist. Please check the path and try again!"
            exit 1
        fi
    done

    zip -r  {output_dir}/project-dependency-src.zip {" ".join(include_paths)} \
        -x "*.git/**"  \
        -x "*.github/**"  \
        -x "*.vscode/**" \
        -x "*__pycache__*"

    echo "{output_dir}/project-dependency-src.zip created successfully!!"
    """
    returncode = execute_bash_script(packaged_src_command)
    if returncode != 0:
        raise Exception("Project dependency src creation failed!!")
    return returncode
