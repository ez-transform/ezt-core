import io
import sys
from operator import mod

from ezt.util.result import ExecutionResult, EztResult, ModelResult


def test_ezt_result():

    my_res = EztResult(code=1, msg="some string")

    assert isinstance(my_res, EztResult)
    assert my_res.code == 1
    assert my_res.msg == "some string"


def test_model_result():

    my_res = ModelResult(model_name="my_model", completion_status="success", duration="5")

    assert my_res.model_name == "my_model"
    assert my_res.completion_status == "success"
    assert my_res.duration == "5"


def test_execution_result():

    results = [
        ModelResult(model_name="my_model1", completion_status="success", duration="5"),
        ModelResult(model_name="my_model2", completion_status="failed", duration="0"),
        ModelResult(model_name="my_model3", completion_status="skipped", duration="2"),
        ModelResult(model_name="my_model4", completion_status="success", duration="5"),
    ]

    my_res = ExecutionResult()

    for m in results:
        my_res.append_model(m)

    capturedOutput = io.StringIO()
    sys.stdout = capturedOutput
    my_res.print_result()
    sys.stdout = sys.__stdout__

    assert "Execution result" in capturedOutput.getvalue()
