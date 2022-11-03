import types

import ezt.build.dfmodel.models
import polars as pl
import pyarrow.parquet as pq
import pytest
from ezt.build.dfmodel.sources import (
    _get_csv_local_source,
    _get_parq_local_source,
    _get_s3_csv_source,
    _get_s3_delta_source,
    _get_s3_parq_source,
    _get_source_getter_func,
    get_source,
)
from s3fs import S3FileSystem
from tests.fixtures.yml_fixtures import source_dict_local_parq


def test_get_source(monkeypatch, conf_fixture, pyarrow_table):
    class mock_Config:
        def __init__(self, cwd):
            self.cwd = cwd

        def get_source(self, name):
            return conf_fixture.get_source("raw_customers")

    def mock_scan_parquet(source=None):
        return pyarrow_table

    monkeypatch.setattr(ezt.build.dfmodel.sources, "Config", mock_Config)
    monkeypatch.setattr(pl, "scan_parquet", mock_scan_parquet)

    model = get_source("raw_customers")
    model_df = model.collect()

    assert isinstance(model, pl.LazyFrame)
    assert model_df.is_empty() is False


def test_get_source_getter(
    source_dict_local_parq,
    source_dict_local_csv_with_prop,
    source_dict_s3_parq_folder,
    source_dict_s3_csv,
    source_dict_s3_delta,
):

    local_parq_getter = _get_source_getter_func(source_dict_local_parq)
    local_csv_getter = _get_source_getter_func(source_dict_local_csv_with_prop)
    s3_delta_getter = _get_source_getter_func(source_dict_s3_delta)
    s3_parq_getter = _get_source_getter_func(source_dict_s3_parq_folder)
    s3_csv_getter = _get_source_getter_func(source_dict_s3_csv)

    assert isinstance(local_parq_getter, types.FunctionType)
    assert local_parq_getter.__name__ == "_get_parq_local_source"

    assert isinstance(local_csv_getter, types.FunctionType)
    assert local_csv_getter.__name__ == "_get_csv_local_source"

    assert isinstance(s3_delta_getter, types.FunctionType)
    assert s3_delta_getter.__name__ == "_get_s3_delta_source"

    assert isinstance(s3_parq_getter, types.FunctionType)
    assert s3_parq_getter.__name__ == "_get_s3_parq_source"

    assert isinstance(s3_csv_getter, types.FunctionType)
    assert s3_csv_getter.__name__ == "_get_s3_csv_source"


def test_get_s3_parq_source_folder(monkeypatch, pyarrow_dataset, source_dict_s3_parq_folder):

    s3 = S3FileSystem()

    def mock_ParquetDataset_func(path_or_paths=None, use_legacy_dataset=None):
        return pyarrow_dataset

    monkeypatch.setattr(ezt.util.helpers, "get_s3_filesystem", s3)
    monkeypatch.setattr(pq, "ParquetDataset", mock_ParquetDataset_func)

    model = _get_s3_parq_source(source_dict_s3_parq_folder)
    model_df = model.collect()

    assert isinstance(model, pl.LazyFrame)
    assert model_df.is_empty() is False


def test_get_s3_parq_source_file(monkeypatch, pyarrow_table, source_dict_s3_parq_file):

    s3 = S3FileSystem()

    def mock_pq_read_table_func(source=None, filesystem=None):
        return pyarrow_table

    monkeypatch.setattr(ezt.util.helpers, "get_s3_filesystem", s3)
    monkeypatch.setattr(pq, "read_table", mock_pq_read_table_func)

    model = _get_s3_parq_source(source_dict_s3_parq_file)
    model_df = model.collect()

    assert isinstance(model, pl.LazyFrame)
    assert model_df.is_empty() is False


def test_get_s3_parq_source_invalid(monkeypatch, pyarrow_table, source_dict_s3_parq_file):

    s3 = S3FileSystem()

    source_dict_s3_parq_file["path_type"] = "not_valid_path_type"

    monkeypatch.setattr(ezt.util.helpers, "get_s3_filesystem", s3)

    with pytest.raises(Exception) as e:
        model = _get_s3_parq_source(source_dict_s3_parq_file)


def test_get_s3_parq_source_no_folder(monkeypatch, pyarrow_table, source_dict_s3_parq_file):

    s3 = S3FileSystem()

    del source_dict_s3_parq_file["folder"]

    def mock_pq_read_table_func(source=None, filesystem=None):
        return pyarrow_table

    monkeypatch.setattr(ezt.util.helpers, "get_s3_filesystem", s3)
    monkeypatch.setattr(pq, "read_table", mock_pq_read_table_func)

    model = _get_s3_parq_source(source_dict_s3_parq_file)
    model_df = model.collect()

    assert isinstance(model, pl.LazyFrame)
    assert model_df.is_empty() is False


def test_get_csv_local_source(
    monkeypatch,
    polars_lazyframe,
    source_dict_local_csv_with_prop,
    source_dict_local_csv_no_prop,
):
    def mock_scan_csv(file, **kwargs):
        return polars_lazyframe

    monkeypatch.setattr(pl, "scan_csv", mock_scan_csv)

    model1 = _get_csv_local_source(source_dict_local_csv_with_prop)
    model1_df = model1.collect()
    model2 = _get_csv_local_source(source_dict_local_csv_no_prop)
    model2_df = model2.collect()

    assert isinstance(model1, pl.LazyFrame)
    assert model1_df.is_empty() is False

    assert isinstance(model2, pl.LazyFrame)
    assert model2_df.is_empty() is False


def test_get_parq_local_source(monkeypatch, polars_lazyframe, source_dict_local_parq):
    def mock_scan_parquet(file=None):
        return polars_lazyframe

    monkeypatch.setattr(pl, "scan_parquet", mock_scan_parquet)

    model = _get_parq_local_source(source_dict_local_parq)
    model_df = model.collect()

    assert isinstance(model, pl.LazyFrame)
    assert model_df.is_empty() is False


def test_get_s3_delta_source(source_dict_local_parq):

    with pytest.raises(NotImplementedError) as e:
        _get_s3_delta_source(source_dict_local_parq)


def test_get_s3_csv_source(source_dict_local_parq):

    with pytest.raises(NotImplementedError) as e:
        _get_s3_csv_source(source_dict_local_parq)
