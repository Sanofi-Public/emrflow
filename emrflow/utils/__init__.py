"""Utility functions for EMRFLOW"""

import gzip
import os
import re
import subprocess
import sys
from pathlib import Path
from shutil import copyfile, copytree, ignore_patterns
from typing import Dict, List
from urllib.parse import urlparse

import boto3
import rich
from rich.progress import Progress, TotalFileSizeColumn


def parse_bucket_uri(uri: str) -> List[str]:
    """
    Parse the S3 URI and return the bucket and key
    uri: str : s3 uri

    return: List[str] : [bucket, key]
    """
    result = urlparse(uri, allow_fragments=False)
    return [result.netloc, result.path.strip("/")]


def mkdir(path: str):
    """
    Create a folder
    """
    try:
        os.mkdir(path)
    except FileExistsError:
        pass


def params_for(dict_params: str) -> str:
    """
    Return a set of string spark-submit parameters for the provided deployment type.
    """

    conf_items = {}
    for key, val in dict_params.items():
        conf_items[key] = val

    return " ".join([f"--conf {key}={val}" for key, val in conf_items.items()])


def convert_to_dict(list_params: List) -> Dict:
    """
    Convert a list of key-value pairs to a dictionary.
    list_params: List : list of key-value pairs

    return: Dict : dictionary
    """
    dict_items = {}
    if list_params is not None:
        for item in list_params:
            key, value = item.split(":")
            dict_items[key] = value
    return dict_items


def print_s3_gz(client: boto3.session.Session.client, s3_uri: str, last_position: int):
    """
    Only print the new logs appended to the gzip file from S3.
    """

    bucket, key = parse_bucket_uri(s3_uri)

    try:
        gz_file = client.get_object(Bucket=bucket, Key=key)
        with gzip.open(gz_file["Body"]) as data:
            # Move to the last known position in the file
            data.seek(last_position)

            # Read the new info appended to the file
            new_info = data.read().decode()
            if new_info != "":
                rich.print(new_info)

            # Update the last known position
            last_position = data.tell()

            return last_position
    except Exception as e:
        return 0


def upload_package(
    s3_client, s3_code_uri: str, local_uri: List[str], excludes_uri: List[str] = []
) -> Dict:
    """
    Upload local artifacts to S3 bucket
    s3_code_uri: str : s3 code uri
    local_uri: str : local directory

    return: str : s3_code_uri
    """
    bucket, prefix = parse_bucket_uri(s3_code_uri)
    src_target = {}
    abs_src_target = {}

    for uri in local_uri:
        if uri not in excludes_uri:
            src_target[uri] = os.path.join(prefix, uri)
            abs_src_target[uri] = os.path.join(s3_code_uri, uri)
        else:
            abs_src_target[uri] = os.path.join(s3_code_uri, uri)

    rich.print(f"Uploading dependencies: {src_target}")

    uploader = PrettyUploader(
        s3_client,
        bucket,
        src_target,
    )
    uploader.run()

    return abs_src_target


def execute_bash_script(command):
    """
    Run the Bash script and capture the output
    """
    try:
        proc = subprocess.run(["bash", "-c", command], check=True)
        return proc.returncode
    except subprocess.CalledProcessError as e:
        rich.print("subprocess.CalledProcessError", str(e))
        return 1


class PrettyUploader:
    """
    Upload files to S3 and show progress
    """

    def __init__(
        self,
        s3_client: boto3.session.Session.client,
        bucket: str,
        src_target: Dict[str, str],
    ):
        self._s3_client = s3_client
        self._bucket = bucket
        self._src_target = src_target
        self._totalsize = sum(
            [float(os.path.getsize(filename)) for filename in self._src_target.keys()]
        )
        self._seensize = 0
        self._progress = Progress(
            *Progress.get_default_columns(), TotalFileSizeColumn()
        )
        self._task = self._progress.add_task("Uploading...", total=self._totalsize)

    def run(self):
        """Upload files to s3"""
        with self._progress:
            for src, target in self._src_target.items():
                self._s3_client.upload_file(src, self._bucket, target, Callback=self)

    def __call__(self, bytes_amount):
        """Check the progress of the upload"""
        self._progress.update(self._task, advance=bytes_amount)
