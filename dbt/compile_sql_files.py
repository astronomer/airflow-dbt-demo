"""Compile SQL files from DBT."""

import dbt.main as dbt_runner
import os
from dbt.logger import log_manager
import boto3
import re
import argparse

AWS_PROFILE = os.getenv("DBT_AWS_PROFILE")
S3_BUCKET = os.getenv("S3_BUCKET")
PIPELINE_ENV = os.getenv("PIPELINE_ENV")
RAW_SCHEMA = f"{PIPELINE_ENV}_raw_data"
STAGED_SCHEMA = f"{PIPELINE_ENV}_analytics_staged"
DBT_PATH_COMMANDS = [
    "--profiles-dir",
    ".",
]
INCREMENTAL_TRANSACTION_FLAGS = [
    "--vars",
    '{"incremental_type":"transaction"}',
]
DBT_TAG_INFO = {
    "staged": {
        "schema": RAW_SCHEMA,
        "files_dir": "target/compiled/hush_sound/models/sources/diplo/staged",
        "file_suffix": "staged",
    },
    "dataproduct": {
        "schema": STAGED_SCHEMA,
        "files_dir": "target/compiled/hush_sound/models/workspaces/external",
        "file_suffix": "denormalized",
    },
    "external": {
        "schema": STAGED_SCHEMA,
        "files_dir": "target/compiled/hush_sound/models/workspaces/external",
        "file_suffix": "denormalized",
    },
}

def _get_s3_session():
    if AWS_PROFILE:
        sess = boto3.session.Session(profile_name=AWS_PROFILE)
    else:
        sess = boto3.session.Session()
    return sess.client("s3")


def build_dbt_compile_args(
    tags: list = None,
    models: list = None,
    full_refresh: bool = False,
    compile_all_models: bool = False,
) -> list[str]:
    """Build list of arguments to run DBT."""
    tags = tags or []
    models = models or []
    selects = []
    for tag in tags:
        selects.append(f'tag:{tag}')
    selects.extend(models)
    compile_args = ["compile"]
    if not compile_all_models and selects:
        compile_args.append('--select')
        compile_args.extend(selects)
    if full_refresh:
        compile_args.append('--full-refresh')
    compile_args.extend(DBT_PATH_COMMANDS)
    return compile_args


def build_streaming_files(s3_client, tag: str):
    tag_info = DBT_TAG_INFO.get(tag)
    if not tag_info:
        raise ValueError(f"Invalid tag: {tag}")
    dbt_args = build_dbt_compile_args(
        tags=[tag],
    )
    dbt_args.extend(INCREMENTAL_TRANSACTION_FLAGS)
    files_dir = tag_info["files_dir"]
    file_suffix = tag_info["file_suffix"]
    schema = tag_info["schema"]
    log_manager.disable()
    dbt_runner.handle(dbt_args)
    dir_list = os.listdir(files_dir)
    for file_name in dir_list:
        if file_name.endswith(f"_{file_suffix}.sql"):
            file_path = f"{files_dir}/{file_name}"
            with open(file_path) as sql_file:
                parsed_sql = re.sub(
                    rf'\b{schema}\S*\s',
                    "{TABLE_NAME}",
                    sql_file.read(),
                    count=1,
                )
                s3_client.put_object(
                    Body=parsed_sql,
                    Bucket=S3_BUCKET,
                    Key=f"dbt/compiled/{tag}/{file_name}"
                )


def push_schema_to_s3(s3_client) -> dict:
    """Get dict of the source YAML for DBT."""
    s3_client.upload_file(
        Filename='models/schema.yml',
        Bucket=S3_BUCKET,
        Key=f"dbt/config/schema.yml"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser("compile_sql_files")
    parser.add_argument(
        "dbt_tag",
        help="DBT tag to compile",
        type=str,
        choices=["staged", "dataproduct", "external"],
    )
    args = parser.parse_args()
    s3 = _get_s3_session()
    source_dict = push_schema_to_s3(s3)
    build_streaming_files(s3, args.dbt_tag)
