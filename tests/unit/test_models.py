import os
import types

import deltalake as dl
import pyarrow.parquet as pq
import pytest
from s3fs import S3FileSystem

import ezt.util.config
import ezt.util.helpers
from ezt.build.dfmodel.models import (
    _get_local_delta_model,
    _get_local_parq_model,
    _get_model_getter_func,
    _get_s3_delta_model,
    _get_s3_parq_model,
    get_model,
    pl,
)
from ezt.util.exceptions import EztAuthenticationException


def test_get_model(monkeypatch, conf_fixture, pyarrow_table):
    class mock_Config:
        def __init__(self, cwd):
            self.cwd = cwd

        def get_model(self, name):
            return conf_fixture.get_model("my_model")

    def mock_read_table(source=None):
        return pyarrow_table

    monkeypatch.setattr(ezt.build.dfmodel.models, "Config", mock_Config)
    monkeypatch.setattr(pq, "read_table", mock_read_table)

    model = get_model("my_model")
    model_df = model.collect()

    assert isinstance(model, pl.LazyFrame)
    assert model_df.is_empty() is False


def test_get_model_getter(
    model_dict_local_delta_overwrite,
    model_dict_s3_delta,
    model_dict_local_parq,
    model_dict_s3_parq,
):

    local_delta_getter = _get_model_getter_func(model_dict_local_delta_overwrite)
    s3_delta_getter = _get_model_getter_func(model_dict_s3_delta)
    local_parq_getter = _get_model_getter_func(model_dict_local_parq)
    s3_parq_getter = _get_model_getter_func(model_dict_s3_parq)

    assert isinstance(local_delta_getter, types.FunctionType)
    assert local_delta_getter.__name__ == "_get_local_delta_model"

    assert isinstance(s3_delta_getter, types.FunctionType)
    assert s3_delta_getter.__name__ == "_get_s3_delta_model"

    assert isinstance(local_parq_getter, types.FunctionType)
    assert local_parq_getter.__name__ == "_get_local_parq_model"

    assert isinstance(s3_parq_getter, types.FunctionType)
    assert s3_parq_getter.__name__ == "_get_s3_parq_model"


@pytest.mark.timeout(10)
def test_get_s3_delta_model(monkeypatch, pyarrow_table, model_dict_s3_delta):

    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)

    with pytest.raises(EztAuthenticationException) as e:
        _get_s3_delta_model(model_dict_s3_delta)

    class mock_DeltaTable:
        def __init__(self, table_uri):
            self.table_uri = table_uri

        def to_pyarrow_table(self):
            return pyarrow_table

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "foo")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "bar")

    monkeypatch.setattr(dl, "DeltaTable", mock_DeltaTable)

    result = _get_s3_delta_model(model_dict_s3_delta)
    model_df = result.collect()

    assert os.getenv("AWS_S3_ALLOW_UNSAFE_RENAME") == "true"
    assert isinstance(result, pl.LazyFrame)
    assert model_df.is_empty() is False


def test_get_s3_parq_model(monkeypatch, pyarrow_dataset, model_dict_s3_parq):

    s3 = S3FileSystem()

    def mock_ParquetDataset_func(path_or_paths=None, filesystem=None):
        return pyarrow_dataset

    monkeypatch.setattr(ezt.util.helpers, "get_s3_filesystem", s3)
    monkeypatch.setattr(pq, "ParquetDataset", mock_ParquetDataset_func)

    model = _get_s3_parq_model(model_dict_s3_parq)
    model_df = model.collect()

    assert isinstance(model, pl.LazyFrame)
    assert model_df.is_empty() is False


def test_get_local_parq_model(monkeypatch, pyarrow_table, model_dict_local_parq):
    def mock_read_table(source=None):
        return pyarrow_table

    monkeypatch.setattr(pq, "read_table", mock_read_table)

    model = _get_local_parq_model(model_dict_local_parq)
    model_df = model.collect()

    assert isinstance(model, pl.LazyFrame)
    assert model_df.is_empty() is False


def test_get_local_delta_model(monkeypatch, pyarrow_table, model_dict_local_delta_overwrite):
    class mock_DeltaTable:
        def __init__(self, table_uri):
            self.table_uri = table_uri

        def to_pyarrow_table(self):
            return pyarrow_table

    monkeypatch.setattr(dl, "DeltaTable", mock_DeltaTable)

    model = _get_local_delta_model(model_dict_local_delta_overwrite)
    model_df = model.collect()

    assert isinstance(model, pl.LazyFrame)
    assert model_df.is_empty() is False
