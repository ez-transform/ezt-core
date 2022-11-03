import os
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from distutils.log import error
from typing import Optional

from ezt.util.result import EztResult
from jsonschema import SchemaError, ValidationError
from jsonschema import validate as schema_validate
from rich.console import Console
from rich.style import Style
from yaml import Loader, load

# @dataclass
# class ValidationResult(ABC):

#     code: int
#     msg: str

#     @abstractmethod
#     def print_result(self):
#         pass


@dataclass
class ValidationResultSuccess(EztResult):
    """Represents a successful validation result."""

    def print_result(self):
        console = Console()
        console.print(f"\t{self.msg}")


@dataclass
class ValidationResultFailed(EztResult):
    """Represents a unsuccessful validation result."""

    traceback_msg: str

    def print_result(self):
        console = Console()
        error_style = "bold red"
        console.print(self.traceback_msg)
        console.print(f"\n\t{self.msg}", style=error_style)


@dataclass
class Validator:
    """
    Class that handles the validation of the yaml files included in the ezt project.
    """

    project_yml: dict = field(default_factory=dict)
    sources_yml: dict = field(default_factory=dict)
    results: list = field(default_factory=list)

    def _load_schema(self, schema_name: str) -> dict:
        """
        Loads the schema requested and returns it as a dict.
        """
        SCHEMAS_PATH = os.path.normpath(os.path.join(os.path.dirname(__file__), "yaml_schemas"))

        schemas = {
            "project_schema": "ezt_project_schema.yml",
            "sources_schema": "ezt_sources_schema.yml",
        }

        with open(f"{SCHEMAS_PATH}/{schemas[schema_name]}", "r") as f:
            return load(f, Loader=Loader)

    def _validate_yml(self, conf_dict, schema_name):
        """Validates the yml file agains a jsonschema."""
        schema_validate(conf_dict, self._load_schema(schema_name))

    def create_validation_results(self):

        # ezt_project.yml
        try:
            self._validate_yml(self.project_yml, "project_schema")
            result = ValidationResultSuccess(
                code=1, msg=":thumbs_up: ezt_project.yml found and successfully validated."
            )
            self.results.append(result)
        except FileNotFoundError:
            self.results.append(
                ValidationResultFailed(
                    code=-1,
                    msg=":no_entry: No ezt_project.yml file in current directory.",
                    traceback_msg=traceback.format_exc(),
                )
            )
        except ValidationError:
            self.results.append(
                ValidationResultFailed(
                    code=-1,
                    msg=":no_entry: Schema validation failed for ezt_project.yml.",
                    traceback_msg=traceback.format_exc(),
                )
            )
        except SchemaError:
            self.results.append(
                ValidationResultFailed(
                    code=-1,
                    msg=":no_entry: Internal error. Schema could not be opened.",
                    traceback_msg=traceback.format_exc(),
                )
            )
        except Exception:
            self.results.append(
                ValidationResultFailed(
                    code=-1,
                    msg=":no_entry: Unexpected error. See traceback for info.",
                    traceback_msg=traceback.format_exc(),
                )
            )

        # sources
        try:
            self._validate_yml(self.sources_yml, "sources_schema")
            self.results.append(
                ValidationResultSuccess(
                    code=1, msg=":thumbs_up: sources.yml found and successfully validated."
                )
            )
        except FileNotFoundError as e:
            self.results.append(
                ValidationResultFailed(
                    code=-1,
                    msg=":no_entry: No sources.yml file in current directory.",
                    traceback_msg=traceback.format_exc(),
                )
            )
        except ValidationError as e:
            self.results.append(
                ValidationResultFailed(
                    code=-1,
                    msg=":no_entry: Schema validation failed for sources.yml.",
                    traceback_msg=traceback.format_exc(),
                )
            )
        except SchemaError as e:
            self.results.append(
                ValidationResultFailed(
                    code=-1,
                    msg=":no_entry: Internal error. Schema could not be opened.",
                    traceback_msg=traceback.format_exc(),
                )
            )
        except:
            self.results.append(
                ValidationResultFailed(
                    code=-1,
                    msg=":no_entry: Unexpected error. See traceback for info.",
                    traceback_msg=traceback.format_exc(),
                )
            )
