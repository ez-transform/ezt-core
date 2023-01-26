import os
import time
import traceback
from multiprocessing import Queue
from types import ModuleType

import polars as pl
import pyarrow.parquet as pq
from ezt.build.dfmodel.merge import calculate_merge

# from ezt.build.dfmodel.register_df_sources import get_sources
from ezt.build.dfmodel.persist_delta import create_delta_table
from ezt.util.exceptions import EztAuthenticationException, EztConfigException
from ezt.util.fs import prepare_local
from ezt.util.helpers import get_s3_filesystem, prepare_s3_path


def process_model(
    model_dict: dict,
    ezt_module: ModuleType,
    finalized_task_queue: Queue,
):
    """
    Function that is responsible for processing a model and storing the resulting
    dataframe to disk. The processing results get put into finalized_task_queue.
    The result put into finalized_task_queue should be a dictionary with the keys model_name
    and status.
    """

    start_time = time.time()

    try:
        processor = _get_processor(model_dict)
    except Exception:
        tb = traceback.format_exc()
        finalized_task_queue.put(
            {
                "model_name": model_dict["name"],
                "status": "failed",
                "duration": f"{(time.time() - start_time):.2f}",
                "traceback": tb,
            }
        )
        return

    try:
        processor(model_dict, ezt_module)
        finalized_task_queue.put(
            {
                "model_name": model_dict["name"],
                "status": "success",
                "duration": f"{(time.time() - start_time):.2f}",
            }
        )
        return
    except Exception:
        tb = traceback.format_exc()
        finalized_task_queue.put(
            {
                "model_name": model_dict["name"],
                "status": "failed",
                "duration": f"{(time.time() - start_time):.2f}",
                "traceback": tb,
            }
        )
        return


def _get_processor(model_dict):
    """Function that determines how your model should be processed."""
    if model_dict["type"] == "sql":
        # TODO
        raise NotImplementedError("SQL models are not supported yet.")

    elif model_dict["type"] == "df":
        if (
            model_dict["write_settings"]["file_type"] == "delta"
            and model_dict["filesystem"] == "local"
        ):
            return _df_local_delta_processor

        elif (
            model_dict["write_settings"]["file_type"] == "delta"
            and model_dict["filesystem"] == "s3"
        ):
            return _df_s3_delta_processor

        elif (
            model_dict["write_settings"]["file_type"] == "parquet"
            and model_dict["filesystem"] == "local"
        ):
            return _df_local_parquet_processor

        elif (
            model_dict["write_settings"]["file_type"] == "parquet"
            and model_dict["filesystem"] == "s3"
        ):
            return _df_s3_parquet_processor
    else:
        raise EztConfigException()


def _df_local_parquet_processor(model_dict, ezt_module):
    """Function that processes DataFrame models that are to be stored as parquet on the local filesystem."""

    prepare_local(model_dict["destination"])

    df = ezt_module.df_model()

    df.write_parquet(
        file=f'{model_dict["destination"]}/{model_dict["name"]}.parquet',
        compression="snappy",
    )
    return


def _df_s3_parquet_processor(model_dict, ezt_module):
    """Function that processes DataFrame models that are to be stored as parquet in s3."""

    # set up s3 filesystem
    s3 = get_s3_filesystem()

    df = ezt_module.df_model()
    table = df.to_arrow()

    if model_dict["write_settings"]["mode"] == "overwrite":
        target_path = (
            f's3://{model_dict["destination"]}/{model_dict["name"]}/{model_dict["name"]}.parquet'
        )
        pq.write_table(table=table, filesystem=s3, where=target_path)
    elif model_dict["write_settings"]["mode"] == "append":
        target_path = f's3://{model_dict["destination"]}/{model_dict["name"]}'
        pq.write_to_dataset(table=table, root_path=target_path, filesystem=s3)
    elif model_dict["write_settings"]["mode"] == "merge":
        # if target does not exists, just write a parquet file to destination
        target_path = (
            f'{model_dict["destination"]}/{model_dict["name"]}/{model_dict["name"]}.parquet'
        )
        if s3.exists(target_path):
            table = calculate_merge(
                source=df,
                target=pl.from_arrow(pq.read_table(target_path, filesystem=s3)),
                merge_col=model_dict["write_settings"]["merge_col"],
            )
            pq.write_table(table=table, filesystem=s3, where=target_path)
        else:
            pq.write_table(table=table, filesystem=s3, where=target_path)
    else:
        raise EztConfigException()


def _df_local_delta_processor(model_dict, ezt_module):
    prepare_local(model_dict["destination"])

    result = create_delta_table(
        df=ezt_module.df_model(),
        dest=f'{model_dict["destination"]}',
        name=f'{model_dict["name"]}',
        write_mode_settings=model_dict["write_settings"],
    )


def _df_s3_delta_processor(model_dict, ezt_module):
    """Function that processes DataFrame models that are to be stored as delta-tables in s3."""

    if os.getenv("AWS_ACCESS_KEY_ID") is None or os.getenv("AWS_SECRET_ACCESS_KEY") is None:
        raise EztAuthenticationException(
            "Environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY need to be set to authenticate to S3."
        )

    # required for writing delta tables to s3 without LockClient. Opts out of concurrent writes.
    os.environ["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"

    path = prepare_s3_path(model_dict["destination"])

    result = create_delta_table(
        df=ezt_module.df_model(),
        dest=path,
        name=f'{model_dict["name"]}',
        write_mode_settings=model_dict["write_settings"],
    )
