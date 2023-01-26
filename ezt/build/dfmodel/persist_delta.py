import os
import traceback

import deltalake as dl
import polars as pl
import pyarrow as pa

# from rich import Console
from ezt.build.dfmodel.merge import calculate_merge


def update_types(source_table: pa.Table) -> pa.Table:
    """
    Function that updates data types large_string and uint64 (bigint)
    to regular strings. Types large_string and uint64 are not supported
    by deltalake.
    """
    new_types = []
    arrays = []
    for col in source_table.schema:
        if col.type.equals(pa.large_string()):
            new_types.append(pa.field(col.name, pa.string()))
        elif col.type.equals(pa.uint64()):
            new_types.append(pa.field(col.name, pa.string()))
        else:
            new_types.append(col)

        arrays.append(source_table[col.name])

    new_schema = pa.schema(new_types)
    return pa.Table.from_arrays(arrays, schema=new_schema)


def create_delta_table(
    df: pl.DataFrame,
    dest: str,
    name: str,
    write_mode_settings: dict,
):
    """Writes delta table to destination."""
    try:
        dl.write_deltalake(data=update_types(df.to_arrow()), table_or_uri=f"{dest}/{name}")
    except AssertionError:
        # delta table already exists
        _write_to_delta_table(df, dest, name, write_mode_settings)


def _write_to_delta_table(
    df: pl.DataFrame,
    dest: str,
    name: str,
    write_mode_settings: dict,
):
    # delta table already exists

    if write_mode_settings["how"] in ("overwrite", "append"):
        dl.write_deltalake(
            data=update_types(df.to_arrow()),
            table_or_uri=f"{dest}/{name}",
            mode=write_mode_settings["how"],
        )
    elif write_mode_settings["how"] == "merge":
        dl.write_deltalake(
            data=update_types(
                calculate_merge(
                    source=df,
                    target=pl.from_arrow(
                        dl.DeltaTable(table_uri=f"{dest}/{name}").to_pyarrow_table()
                    ),
                    merge_col=write_mode_settings["merge_col"],
                )
            ),
            table_or_uri=f"{dest}/{name}",
            mode="overwrite",
        )
