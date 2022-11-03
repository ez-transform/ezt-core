import traceback
from multiprocessing import Process, Queue

from ezt.build.process_model import process_model
from ezt.util.config import Config
from ezt.util.logger import EztLogger
from ezt.util.result import ExecutionResult, ModelResult
from rich.console import Console


class Runner:
    def __init__(self, config: Config):
        self.config = config
        self.logger = EztLogger(logs_destination=self.config.project["logs_destination"])

    @property
    def model_order(self):
        return list(self.config.execution_order.static_order())

    def Execute(self, model_name, model_group) -> ExecutionResult:

        execution_result = ExecutionResult()
        console = Console()

        # log models to be processed
        try:
            self.logger.log_info(
                f"Found {len(self.model_order)} models to be processed: {self.model_order}"
            )
        except Exception:
            console.print_exception()
            raise SystemExit()

        self.logger.log_info("Starting processing of models.")

        ts = self.config.execution_order
        q = Queue()
        finalized_task_queue = Queue()

        with console.status("[bold green]Processing models...") as status:
            # process models in the correct order
            ts.prepare()
            while ts.is_active():
                procs = []
                for name in ts.get_ready():

                    model_dict = self.config.get_model(name)
                    # check if model_name or model_group argument was passed and only process that model/group
                    if model_name:
                        if model_name != name:
                            console.log(f"{name} skipped.")
                            finalized_task_queue.put(
                                {
                                    "model_name": name,
                                    "status": "skipped",
                                    "duration": "0.00",
                                }
                            )
                            continue

                    if model_group:
                        if "group" not in model_dict:
                            console.log(f"{name} skipped.")
                            finalized_task_queue.put(
                                {
                                    "model_name": name,
                                    "status": "skipped",
                                    "duration": "0.00",
                                }
                            )
                            continue
                        elif "group" in model_dict:
                            if model_group != model_dict["group"]:
                                console.log(f"{name} skipped.")
                                finalized_task_queue.put(
                                    {
                                        "model_name": name,
                                        "status": "skipped",
                                        "duration": "0.00",
                                    }
                                )
                                continue
                            else:
                                pass

                    self.logger.log_info(f"Processing model {name}...")

                    q.put(model_dict)
                    output = f"Processing [bold blue]{name}[/]..."
                    console.log(output, log_locals=False)

                    # TODO: likely requires different logic for sql models
                    try:
                        model_module = self.config.import_model(name)
                    except Exception:
                        tb = traceback.format_exc()
                        finalized_task_queue.put(
                            {
                                "model_name": name,
                                "status": "failed",
                                "duration": "0.00",
                                "traceback": tb,
                            }
                        )
                        continue
                    p = Process(
                        target=process_model,
                        args=(q.get(), model_module, finalized_task_queue),
                    )
                    procs.append(p)
                    p.start()

                for p in procs:
                    p.join()

                result = finalized_task_queue.get()
                if not set(("model_name", "status", "duration")).issubset(result.keys()):
                    console.print(
                        "[bold red]Internal issue occured when processing models. Subprocess not returning correct keys to finalized_task_queue.[/]"
                    )
                    raise SystemExit()
                result_name = result["model_name"]

                # process result
                if result["status"] == "success":
                    self.logger.log_info(f"Processed successfully: {result_name}")
                    output = f"Model [bold green]{result_name}[/] processed successfully."
                    console.log(output)
                    execution_result.append_model(
                        ModelResult(result_name, result["status"], result["duration"])
                    )
                elif result["status"] == "failed":
                    self.logger.log_error(f"Processing failed for model {result_name}.")
                    output = f"[red]Model [bold]{result_name}[/] NOT processed successfully.[/]"
                    console.log(output)
                    console.log(f'\n{result["traceback"]}')
                    execution_result.append_model(
                        ModelResult(result_name, result["status"], result["duration"])
                    )
                elif result["status"] == "skipped":
                    execution_result.append_model(
                        ModelResult(result_name, result["status"], result["duration"])
                    )
                else:
                    self.logger.log_error(
                        f"Processing failed for model {result_name}.\n{result['status']}"
                    )
                    output = f"[red]Model [bold]{result_name}[/] NOT processed successfully.[/]"
                    console.log(output)
                    console.log(f'\n{result["traceback"]}')
                    execution_result.append_model(
                        ModelResult(result_name, result["status"], result["duration"])
                    )

                ts.done(result_name)

        return execution_result
