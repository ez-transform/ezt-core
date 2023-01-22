import fnmatch
import glob
import graphlib as gl
import os
from importlib.util import module_from_spec, spec_from_file_location
from typing import Union

# from matplotlib import pyplot as plt
from yaml import parse

from ezt.util.exceptions import (
    ConfigInitException,
    EztConfigException,
    GetModelException,
    GetSourceException,
)
from ezt.util.helpers import get_model_dependencies, parse_yaml
from ezt.util.validator import Validator


def unwrap(func):
    while hasattr(func, "__wrapped__"):
        func = func.__wrapped__
    return func


class Config:
    def __init__(self, project_dir):
        self.project_dir = project_dir

    @property
    def project(self) -> dict:
        """Returns the project yaml as a python dict."""

        try:
            return parse_yaml(f"{self.project_dir}/ezt_project.yml")
        except FileNotFoundError:
            raise ConfigInitException

    @property
    def sources(self) -> Union[dict, str]:
        """Returns the sources.yml as a python dict"""

        if "sources" in self.project:
            return self.project["sources"]
        else:
            source_path = f"{self.project_dir}/sources.yml"

            sources = parse_yaml(source_path)
            return sources

    @property
    def models(self):
        """Returns the models.yml as a python dict."""

        if "groups" in self.project["models"]:
            base_models_path = f'{self.project_dir}/{self.project["models"]["main_folder"]}'
            if os.path.exists(
                f'{self.project_dir}/{self.project["models"]["main_folder"]}/models.yml'
            ):
                master_models = parse_yaml(f"{base_models_path}/models.yml")
            else:
                master_models = {"models": []}

            for group in self.project["models"]["groups"]:
                yml_path_list = glob.glob(f"{base_models_path}/{group['name']}/models*.yml")
                if len(yml_path_list) == 1:
                    sub_models = parse_yaml(yml_path_list[0])
                    for model in sub_models["models"]:
                        model["group"] = group["name"]
                        master_models["models"].append(model)
                elif len(yml_path_list) > 1:
                    raise EztConfigException("Too many yml-files in sub-model directory.")
                else:
                    raise EztConfigException("No yml-file found for model configurations.")

            return master_models
        else:
            models_path = f'{self.project_dir}/{self.project["models"]["main_folder"]}/models.yml'
            models = parse_yaml(models_path)
            return models

    def get_model(self, model_name) -> dict:
        """Returns a specific model configuration by name."""

        for m in self.models["models"]:
            if m["name"] == model_name:
                return m
        else:
            raise GetModelException()

    def get_source(self, source_name) -> dict:
        """Returns a specific source configuration by name."""

        for s in self.sources["sources"]:
            if s["name"] == source_name:
                return s
        else:
            raise GetSourceException()

    @property
    def execution_order(self) -> gl.TopologicalSorter:
        """Returns the execution order for all the models specified in the project."""
        deps = {}

        for model in self.models["models"]:
            # import model
            df_model = self.import_model(model["name"])
            # file = f'{self.project_dir}/{self.project["models_folder"]}/{model["name"]}.py'
            # spec = importlib.util.spec_from_file_location(f"local_project.{model['name']}", file)
            # models = importlib.util.module_from_spec(spec)
            # spec.loader.exec_module(models)

            model_deps = get_model_dependencies(unwrap(df_model.df_model))
            deps[model["name"]] = model_deps

        ts = gl.TopologicalSorter(deps)
        return ts

    # @property
    # def _dag(self) -> nx.DiGraph:
    #     deps = []

    #     for model in self.models["models"]:
    #         # import model
    #         file = f'{self.project_dir}/{self.project["models_folder"]}/{model["name"]}.py'
    #         spec = importlib.util.spec_from_file_location(f"local_project.{model['name']}", file)
    #         models = importlib.util.module_from_spec(spec)
    #         spec.loader.exec_module(models)

    #         model_deps = get_model_dependencies_all(unwrap(models.df_model), model["name"])
    #         for t in model_deps:
    #             deps.append(t)

    #     g = nx.DiGraph()
    #     g.add_edges_from(deps)

    # return g

    @property
    def validation_result(self) -> dict:
        """Returns the validation result as a dictionary."""
        validation_results = {"failed": [], "success": []}
        validator = Validator(self.project, self.sources)
        validator.create_validation_results()
        for r in validator.results:
            if r.code == -1:
                validation_results["failed"].append(r)
            else:
                validation_results["success"].append(r)
        return validation_results

    def import_model(self, model_name: str):
        """Returns the user-specified model as a function."""
        model_dict = self.get_model(model_name)
        if "group" in model_dict.keys():
            file = f"{self.project_dir}/{self.project['models']['main_folder']}/{model_dict['group']}/{model_name}.py"
        else:
            file = f"{self.project_dir}/{self.project['models']['main_folder']}/{model_name}.py"
        spec = spec_from_file_location(f"local_project.{model_name}", file)
        df_model = module_from_spec(spec)
        spec.loader.exec_module(df_model)
        return df_model
