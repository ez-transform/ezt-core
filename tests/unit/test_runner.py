import os

from ezt.build.run import Runner
from ezt.util.result import ExecutionResult, ModelResult
from tests.fixtures.project_fixture import conf_fixture


def test_run(conf_fixture):

    os.chdir(path="./my_project")

    runner = Runner(conf_fixture)

    results = runner.Execute()
    assert isinstance(results, ExecutionResult)

    for result in results.processed_models:
        assert isinstance(result, ModelResult)
        assert result.completion_status == "success"
