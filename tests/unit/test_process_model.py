# from multiprocessing import Process, Queue
from queue import Queue
from types import FunctionType

import pyexpat
import pytest

import ezt.build.process_model
from ezt.build.process_model import (
    _df_local_delta_processor,
    _df_local_parquet_processor,
    _df_s3_parquet_processor,
    _get_processor,
    process_model,
)
from ezt.util.exceptions import EztConfigException


def test_process_model_local_parq(mocker, model_dict_local_parq):

    mock_module = ""
    finalized_task_queue = Queue()

    mocker.patch("ezt.build.process_model._df_local_parquet_processor")

    process_model(model_dict_local_parq, mock_module, finalized_task_queue)

    ezt.build.process_model._df_local_parquet_processor.assert_called_once_with(
        model_dict_local_parq, mock_module
    )


# def test_process_model_s3_delta(model_dict_s3_delta):

#     mock_module = ""
#     finalized_task_queue = Queue()

#     with pytest.raises(Exception) as e:
#         process_model(model_dict_s3_delta, mock_module, finalized_task_queue)

#     ezt.build.process_model._get_processor.assert_called_once_with(model_dict_s3_delta, mock_module)


# def test_process_model_invalid_model_dict(invalid_model_dict):

#     mock_module = ""
#     finalized_task_queue = Queue()

#     with pytest.raises(Exception) as e:
#         process_model(invalid_model_dict, mock_module, finalized_task_queue)


def test_get_processor_s3_sql(sql_s3_model):

    with pytest.raises(NotImplementedError):
        _get_processor(sql_s3_model)

def test_get_processor_local_sql(sql_local_model):

    processor = _get_processor(sql_local_model)

    assert isinstance(processor, FunctionType)
    assert processor.__name__ == '_sql_local_parquet'



def test_get_processor_df_local_delta(model_dict_local_delta_overwrite):

    processor = _get_processor(model_dict_local_delta_overwrite)

    assert isinstance(processor, FunctionType)


def test_get_processor_df_s3_delta(model_dict_s3_delta):

    processor = _get_processor(model_dict_s3_delta)

    assert isinstance(processor, FunctionType)


def test_get_processor_df_local_parq(model_dict_local_parq):

    processor = _get_processor(model_dict_local_parq)

    assert isinstance(processor, FunctionType)


def test_get_processor_df_s3_parq(model_dict_s3_parq):

    processor = _get_processor(model_dict_s3_parq)

    assert isinstance(processor, FunctionType)


def test_get_processor_df_s3_parq(model_dict_adls_parq):

    processor = _get_processor(model_dict_adls_parq)

    assert isinstance(processor, FunctionType)


def test_get_processor_df_s3_parq(model_dict_adls_delta):

    processor = _get_processor(model_dict_adls_delta)

    assert isinstance(processor, FunctionType)


def test_get_processor_invalid_type(invalid_model_type_dict):

    with pytest.raises(EztConfigException):
        _get_processor(invalid_model_type_dict)
