import os

from ezt.util.helpers import get_s3_filesystem


def prepare_local(destination: str):
    """Function that creates the local destination folder if it does not exists."""
    if not os.path.isdir(f"{destination}"):
        os.makedirs(destination)
