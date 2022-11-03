import os
import traceback

# import sys
# import datafusion
from email.policy import default
from multiprocessing import Process, Queue

import rich_click as click
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.text import Text

from ezt.build.process_model import process_model
from ezt.build.run import Runner
from ezt.util import helpers
from ezt.util.config import Config
from ezt.util.exceptions import EztInputException


@click.group()
@click.pass_context
def ezt(ctx):
    """
    Welcome to ezt!
    """
    ctx.obj = Config(os.getcwd())


@ezt.command()
@click.argument("project_name")
@click.pass_obj
def init(config, project_name):
    """Create a new project in the current folder."""

    console = Console()

    helpers.copy_starter(config.project_dir, project_name)
    console.print(f"Project created at {config.project_dir}/{project_name}")


@ezt.command()
@click.pass_obj
def check(config):
    """
    Checks if a ezt project is initiated and working.
    """
    for res in config.validation_result["success"]:
        res.print_result()

    for res in config.validation_result["failed"]:
        res.print_result()


@ezt.command()
@click.pass_obj
def order(config):
    """Print out the execution order of your ezt models."""
    console = Console()
    if isinstance(config.project, dict):
        table = Table(title="Model execution order")
        table.add_column("Order num", style="cyan")
        table.add_column("Model name", style="bold blue")
        for count, model in enumerate(list(config.execution_order.static_order())):
            table.add_row(str(count + 1), model)
        console.print(table)
    else:
        console.print(":no_entry: No ezt_project.yml file in current directory.", style="bold red")
        raise SystemExit()


@ezt.command()
@click.pass_obj
@click.option("--validate", is_flag=True, help="Validate yaml files before running models.")
@click.option("--model-name", help="Provide the name of a specific model to run.", default=None)
@click.option(
    "--model-group",
    help="Provide the name of a specific model group to run all models in that group and skip the rest.",
    default=None,
)
def run(config, validate, model_name, model_group):
    """
    Runs the models you have created.
    """

    if model_name is not None and model_group is not None:
        raise EztInputException(
            "Do not provide values for both --model-group and --model-name in the same run."
        )

    if validate:

        validation_results_failed = config.validation_result["failed"]

        if validation_results_failed:
            # validation results exist
            for res in validation_results_failed:
                res.print_result()
            raise SystemExit(0)

    runner = Runner(config)
    result = runner.Execute(model_name, model_group)
    result.print_result()


if __name__ == "__main__":
    run(["--model-group", "products"])
