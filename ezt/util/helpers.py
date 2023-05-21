import dis
import os

# from distutils.dir_util import copy_tree
import shutil
import sys

from adlfs import AzureBlobFileSystem
from s3fs import S3FileSystem
from yaml import Loader, load

from ezt.util.exceptions import EztAuthenticationException


def copy_starter(path, name) -> None:
    """
    Function that copies the standard starting library to the location where the user wants to initialize their ezt project.
    """
    from ezt.include.starter_project import PACKAGE_PATH as starter_project_directory

    IGNORE = "__pycache__"

    shutil.copytree(
        starter_project_directory, f"{path}/{name}", ignore=shutil.ignore_patterns(IGNORE)
    )


def parse_yaml(file: str) -> dict:
    """
    Function for parsing yaml configuration files and returningt hem as a python dictionary.
    """
    with open(file, "r") as f:
        result = load(f, Loader=Loader)

    return result


# def get_model_dependencies_all(model, model_name) -> list:
#     """
#     Function to analyze what function gets called with which parameters in order to analyze data lineage for python models.
#     """
#     deps = []
#     bytecode = dis.Bytecode(model)
#     instrs = list(reversed([instr for instr in bytecode]))
#     for (ix, instr) in enumerate(instrs):
#         if instr.opname == "CALL_FUNCTION":
#             load_func_instr = instrs[ix + instr.arg + 1]
#             func = load_func_instr.argval
#             set_param_instr = instrs[ix + instr.arg]
#             param = set_param_instr.argval
#             if func in "get_source":
#                 deps.append((param, model_name))
#             elif func in "get_model":
#                 deps.append((param, model_name))
#         elif instr.opname == "CALL_FUNCTION_KW":
#             load_func_instr = instrs[ix + instr.arg + 2]
#             func = load_func_instr.argval
#             set_param_instr = instrs[ix + instr.arg + 1]
#             param = set_param_instr.argval
#             if func in "get_source":
#                 deps.append((param, model_name))
#             elif func in "get_model":
#                 deps.append((param, model_name))

#     return deps


def get_model_dependencies(model):
    """
    Same as get_model_dependencies_all except it skips sources.
    Python 3.11 bytecode requires special handling.
    """
    deps = set()
    bytecode = dis.Bytecode(model)
    instrs = list(reversed([instr for instr in bytecode]))
    for ix, instr in enumerate(instrs):
        if sys.version_info.major == 3 and sys.version_info.minor == 11:
            if instr.opname == "CALL" and instrs[ix + 2].opname != "KW_NAMES":
                load_func_instr = instrs[ix + instr.arg + 2]
                func = load_func_instr.argval
                set_param_instr = instrs[ix + instr.arg + 1]
                param = set_param_instr.argval
                if isinstance(func, str):
                    if func in "get_model":
                        deps.add(param)

            elif instr.opname == "CALL" and instrs[ix + 2].opname == "KW_NAMES":
                load_func_instr = instrs[ix + instr.arg + 3]
                func = load_func_instr.argval
                set_param_instr = instrs[ix + instr.arg + 2]
                param = set_param_instr.argval
                if isinstance(func, str):
                    if func in "get_model":
                        deps.add(param)

        elif sys.version_info.major == 3 and sys.version_info.minor < 11:
            if instr.opname == "CALL_FUNCTION":
                load_func_instr = instrs[ix + instr.arg + 1]
                func = load_func_instr.argval
                set_param_instr = instrs[ix + instr.arg]
                param = set_param_instr.argval
                if isinstance(func, str):
                    if func in "get_model":
                        deps.add(param)
            elif instr.opname == "CALL_FUNCTION_KW":
                load_func_instr = instrs[ix + instr.arg + 2]
                func = load_func_instr.argval
                set_param_instr = instrs[ix + instr.arg + 1]
                param = set_param_instr.argval
                if isinstance(func, str):
                    if func in "get_model":
                        deps.add(param)

    return deps


def get_s3_filesystem() -> S3FileSystem:
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    return S3FileSystem(key=access_key, secret=secret_key)


def get_adls_filesystem() -> AzureBlobFileSystem:
    if (
        os.getenv("AZURE_STORAGE_ACCOUNT_NAME") is None
        or os.getenv("AZURE_STORAGE_TENANT_ID") is None
        or os.getenv("AZURE_STORAGE_CLIENT_ID") is None
        or os.getenv("AZURE_STORAGE_CLIENT_SECRET") is None
    ):
        raise EztAuthenticationException(
            "Environment variables AZURE_STORAGE_ACCOUNT_NAME, AZURE_STORAGE_TENANT_ID, AZURE_STORAGE_CLIENT_ID, AZURE_STORAGE_CLIENT_SECRET all need to be set to authenticate to ADLS. \
            Access key authentication not supported yet."
        )

    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    tenant_id = os.getenv("AZURE_STORAGE_TENANT_ID")
    client_id = os.getenv("AZURE_STORAGE_CLIENT_ID")
    client_secret = os.getenv("AZURE_STORAGE_CLIENT_SECRET")

    return AzureBlobFileSystem(
        account_name=account_name,
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
    )


def prepare_s3_path(path):
    if path.startswith("s3://"):
        pass
    else:
        path = f"s3://{path}"

    if path.endswith("/"):
        path = path[:-1]
    else:
        pass

    return path


def prepare_adls_path(path):
    if path.startswith("abfss://"):
        pass
    elif path.startswith("abfs://"):
        path = f"abfss://{path[8:]}"
    elif path.startswith("adl://"):
        path = f"abfss://{path[6:]}"
    else:
        path = f"abfss://{path}"

    if path.endswith("/"):
        path = path[:-1]
    else:
        pass

    return path


def prepare_adls_path_pqdataset(path):
    if path.startswith("abfss://"):
        path = path[8:]
    elif path.startswith("abfs://"):
        path = path[7:]
    elif path.startswith("adl://"):
        path = path[6:]
    else:
        pass

    if path.endswith("/"):
        path = path[:-1]
    else:
        pass

    return path
