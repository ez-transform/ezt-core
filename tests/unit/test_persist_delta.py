import os

import deltalake as dl
import pyarrow as pa

import ezt.build.dfmodel.persist_delta
from ezt.build.dfmodel.persist_delta import create_delta_table, update_types


def test_update_types(pyarrow_table_large_types):

    new_table = update_types(pyarrow_table_large_types)

    assert new_table.schema.field("large_string").type.equals(pa.string())
    assert new_table.schema.field("large_int").type.equals(pa.string())
    assert new_table.schema.field("binary").type.equals(pa.binary())


def test_create_delta_table_new(tmp_path, polars_dataframe, write_mode_settings):

    path = tmp_path / "some_folder"
    path = path.as_posix()
    name = "some_name"
    append = write_mode_settings["append"]

    create_delta_table(
        df=polars_dataframe, dest=path, name=name, write_mode_settings=append, storage_options=None
    )

    assert os.path.isdir(f"{path}/{name}")
    dl.DeltaTable(f"{path}/{name}")


def test_create_delta_table_overwrite(
    polars_dataframe,
    delta_table_path,
    write_mode_settings,
):

    name = "my_delta_model"
    overwrite = write_mode_settings["overwrite"]

    # create a new delta table
    create_delta_table(
        df=polars_dataframe,
        dest=delta_table_path,
        name=name,
        write_mode_settings=overwrite,
        storage_options=None,
    )

    assert os.path.isdir(f"{delta_table_path}/{name}")
    table = dl.DeltaTable(f"{delta_table_path}/{name}")


def test_create_delta_table_append(polars_dataframe, delta_table_path, write_mode_settings):

    name = "my_delta_model"
    append = write_mode_settings["append"]

    create_delta_table(
        df=polars_dataframe,
        dest=delta_table_path,
        name=name,
        write_mode_settings=append,
        storage_options=None,
    )

    assert os.path.isdir(f"{delta_table_path}/{name}")
    dl.DeltaTable(f"{delta_table_path}/{name}")


def test_create_delta_table_merge(polars_dataframe, delta_table_path, write_mode_settings):

    name = "my_delta_model"
    merge = write_mode_settings["merge"]

    # create a new delta table
    create_delta_table(
        df=polars_dataframe,
        dest=delta_table_path,
        name=name,
        write_mode_settings=merge,
        storage_options=None,
    )

    assert os.path.isdir(f"{delta_table_path}/{name}")
    dl.DeltaTable(f"{delta_table_path}/{name}")
