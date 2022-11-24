import os

import deltalake as dl
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest


@pytest.fixture
def new_rows():
    file_path = os.path.dirname(os.path.realpath(__file__))
    data = pl.scan_csv(f"{file_path}/data/raw_customers.csv", sep=",", has_header=True)
    source_data = data[75:]
    target_data = data[:75]
    return (source_data, target_data)


@pytest.fixture
def overlapping_rows():
    file_path = os.path.dirname(os.path.realpath(__file__))
    data = pl.scan_csv(f"{file_path}/data/raw_customers.csv", sep=",", has_header=True)
    source_data = data[:75]
    target_data = data[25:100]
    return (source_data, target_data)


@pytest.fixture
def data_update():
    file_path = os.path.dirname(os.path.realpath(__file__))
    data = pl.scan_csv(f"{file_path}/data/raw_customers.csv", sep=",", has_header=True)
    source_data = data[:75].with_column(
        pl.when(pl.col("id") == 10)
        .then("UPDATED_NAME")
        .otherwise(pl.col("first_name"))
        .alias("first_name")
    )
    target_data = data[25:100]
    return (source_data, target_data)


@pytest.fixture
def sample_frame():
    file_path = os.path.dirname(os.path.realpath(__file__))
    data = pl.scan_csv(f"{file_path}/data/raw_customers.csv", sep=",", has_header=True)
    return data.collect()


@pytest.fixture
def pyarrow_table():
    table = pa.table(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "year": [2020, 2022, 2021, 2022, 2019, 2021],
            "n_legs": [2, 2, 4, 4, 5, 100],
            "animal": ["Flamingo", "Parrot", "Dog", "Horse", "Brittle stars", "Centipede"],
        }
    )

    return table


@pytest.fixture
def pyarrow_table_large_types():
    arr1 = pa.array(["a", "b", "a"], type=pa.large_string())
    arr2 = pa.array([1, 2, 3], type=pa.uint64())
    arr3 = pa.array([b"1", b"0", b"1"], type=pa.binary())

    schema = pa.schema(
        [
            pa.field("large_string", pa.large_string()),
            pa.field("large_int", pa.uint64()),
            pa.field("binary", pa.binary()),
        ]
    )

    table = pa.Table.from_arrays([arr1, arr2, arr3], schema=schema)
    return table


@pytest.fixture
def pyarrow_dataset(tmp_path, pyarrow_table):
    """Returns a pyarrow ParquetDataset"""
    temp_dest = tmp_path / "some_parquet_file.parquet"
    pq.write_table(pyarrow_table, temp_dest)
    dataset = pq.ParquetDataset(temp_dest)

    return dataset


@pytest.fixture
def arrow_dataset(tmp_path, pyarrow_table):
    """Returns a pyarrow Dataset"""
    temp_dest = tmp_path / "some_parquet_file.parquet"
    pq.write_table(pyarrow_table, temp_dest)
    dataset = ds.dataset(temp_dest, format="parquet")

    return dataset


@pytest.fixture
def polars_lazyframe(pyarrow_table):
    return pl.from_arrow(pyarrow_table).lazy()


@pytest.fixture
def polars_dataframe(pyarrow_table):
    return pl.from_arrow(pyarrow_table)


@pytest.fixture
def delta_table_path(tmp_path, pyarrow_table):

    delta_path = tmp_path / "my_delta_model"
    delta_path.mkdir()
    path = delta_path.as_posix()

    dl.write_deltalake(table_or_uri=path, data=pyarrow_table)

    return tmp_path
