#!/usr/bin/env python36

# #####################################################################################################################
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.                                                 #
#                                                                                                                     #
#  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance     #
#  with the License. You may obtain a copy of the License at                                                          #
#                                                                                                                     #
#   http://www.apache.org/licenses/LICENSE-2.0                                                                        #
#                                                                                                                     #
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed   #
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for  #
#  the specific language governing permissions and limitations under the License.                                     #
# #####################################################################################################################

import base64
import contextlib
import grp
import json
import logging
import os
import pwd
import subprocess

import boto3
from botocore.config import Config

JUPYTER_ENV_FILE = "/etc/profile.d/jupyter-env.sh"
NOTEBOOKS = ["%%NOTEBOOKS%%"]
SOLUTION_ID = "SO0123"
SOLUTION_VERSION = "%%VERSION%%"
CLIENT_CONFIG = Config(
    retries={"max_attempts": 10, "mode": "standard"},
    user_agent_extra=f"AwsSolution/{SOLUTION_ID}/{SOLUTION_VERSION}",
)


logging.basicConfig(
    format="[%(levelname)s]\t%(asctime)s.%(msecs)dZ\t%(message)s\n",
    datefmt="%Y-%m-%dT%H:%M:%S",
    level=logging.INFO,
)
sagemaker_cli = boto3.client("sagemaker", config=CLIENT_CONFIG)
s3_cli = boto3.client("s3", config=CLIENT_CONFIG)


def get_tag(name, is_base64=False):
    with open("/opt/ml/metadata/resource-metadata.json", "r") as instance_metadata:
        metadata = instance_metadata.read()
    notebook_instance_arn = json.loads(metadata).get("ResourceArn")
    logging.debug("Notebook instance ARN is %s" % notebook_instance_arn)

    notebook_instance_tags = sagemaker_cli.list_tags(
        ResourceArn=notebook_instance_arn
    ).get("Tags")
    tag = next(
        iter(
            [
                tag.get("Value")
                for tag in notebook_instance_tags
                if tag.get("Key") == name
            ]
        ),
        None,
    )

    if is_base64:
        tag = base64.b64decode(tag).decode("utf-8")

    logging.info("Tag %s value is %s" % (name, tag))
    return tag


def set_jupyter_env_from_tag(name, is_base64=False):
    tag = get_tag(name, is_base64)

    with open(JUPYTER_ENV_FILE, "a") as env_file:
        env_file.write(f"export {name}={tag}\n")

    return tag


def clean_env_file():
    with contextlib.suppress(FileNotFoundError):
        os.remove(JUPYTER_ENV_FILE)


def restart_notebook_server():
    logging.info("Restarting Jupyter Server")
    result = subprocess.run(["initctl", "restart", "jupyter-server", "--no-wait"])
    if result.returncode != 0:
        logging.error("Failed to restart Jupyter Server")


def copy_files(
    source_bucket, source_prefix, destination, username="ec2-user", groupname="ec2-user"
):

    for notebook in NOTEBOOKS:
        key = f"{source_prefix}/{notebook}"

        local_path = os.path.join(
            destination, key.replace(source_prefix, "").strip("/")
        )

        logging.info("Downloading s3://%s/%s to %s" % (source_bucket, key, local_path))
        local_dir = os.path.dirname(local_path)
        os.makedirs(local_dir, exist_ok=True)
        response = s3_cli.get_object(Bucket=source_bucket, Key=key)

        with open(local_path, "wb") as local_file:
            local_file.write(response.get("Body").read())

    try:
        uid = pwd.getpwnam(username).pw_uid
        gid = grp.getgrnam(groupname).gr_gid
    except KeyError:
        return

    logging.info("Ensuring %s is owned by %s:%s" % (destination, username, groupname))
    result = subprocess.run(["chown", "-R", f"{username}:{groupname}", destination])
    if result.returncode != 0:
        logging.error("Failed to set notebook directory ownership")

    for root, directories, files in os.walk(destination):
        for f in files:
            os.chown(os.path.join(root, f), uid, gid)
            os.chmod(local_path, mode=0o664)


if __name__ == "__main__":
    clean_env_file()

    # Jupyter needs to know about the Forecast Bucket
    set_jupyter_env_from_tag("FORECAST_BUCKET", is_base64=True)

    # Copy the notebooks from the public S3 bucket to our notebook instance
    nb_bucket = get_tag("NOTEBOOK_BUCKET", is_base64=True)
    nb_prefix = get_tag("NOTEBOOK_PREFIX", is_base64=True)

    destination = os.path.join("/home/ec2-user/SageMaker", nb_prefix)
    copy_files(
        source_bucket=nb_bucket, source_prefix=nb_prefix, destination=destination
    )

    restart_notebook_server()
