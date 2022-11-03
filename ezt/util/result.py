from abc import ABC
from dataclasses import dataclass, field
from itertools import zip_longest
from typing import List

from rich.console import Console
from rich.table import Table


@dataclass
class EztResult(ABC):

    code: int
    msg: str


@dataclass
class ModelResult:

    model_name: str = field(default_factory=str)
    completion_status: str = field(default_factory=str)
    duration: str = field(default_factory=str)


@dataclass
class ExecutionResult:

    processed_models: List[ModelResult] = field(default_factory=list)

    def append_model(self, model: ModelResult):
        self.processed_models.append(model)

    def print_result(self):

        console = Console()

        table = Table(title="Execution result")
        table.add_column("Model name", style="bold blue")
        table.add_column("Status", style="bold")
        table.add_column("Duration (seconds)", style="bold")

        for res in self.processed_models:
            table.add_row(res.model_name, res.completion_status, res.duration)

        console.print("\n")
        console.print(table)
