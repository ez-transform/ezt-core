import dis
import os
from distutils.dir_util import copy_tree

from s3fs import S3FileSystem
from yaml import Loader, load


def copy_starter(path, name) -> None:
    from ezt.include.starter_project import PACKAGE_PATH as starter_project_directory

    """
    Function that copies the standard starting library to the location where the user wants to initialize their ezt project.
    """
    copy_tree(starter_project_directory, f"{path}/{name}")


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
    """
    deps = set()
    bytecode = dis.Bytecode(model)
    instrs = list(reversed([instr for instr in bytecode]))
    for (ix, instr) in enumerate(instrs):
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
