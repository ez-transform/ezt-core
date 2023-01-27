import polars as pl
import pyarrow as pa
from ezt.util.exceptions import EztMergeException


# def calculate_merge(source, target, merge_col) -> pa.Table:

#     # config.log_info("Calculating merge...")

#     # preliminary checks
#     target_check = target.filter(pl.col(merge_col).is_duplicated())
#     if len(target_check) == 0:
#         pass
#     else:
#         raise EztMergeException("Key in target model is not unique.")

#     source_check = source.filter(pl.col(merge_col).is_duplicated())
#     if len(source_check) == 0:
#         pass
#     else:
#         raise EztMergeException("Key in source model is not unique.")

#     # sort source and target in same order
#     source = source.sort(merge_col, reverse=False)
#     target = target.sort(merge_col, reverse=False)

#     # new rows to append
#     append_mask = source[merge_col].is_in(target[merge_col])
#     df_append = source[append_mask.apply(lambda x: not x)]

#     # existing rows to leave as is
#     leave_mask = target[merge_col].is_in(source[merge_col])
#     df_leave = target[leave_mask.apply(lambda x: not x)]

#     # existing rows to update
#     df_source_existing = source[append_mask]
#     df_target_existing = target[leave_mask]

#     df_source_existing = df_source_existing.with_columns(
#         df_source_existing.hash_rows().alias("meta_source_hash")
#     )
#     df_target_existing = df_target_existing.with_columns(
#         df_target_existing.hash_rows().alias("meta_target_hash")
#     ).select([pl.col(merge_col), pl.col("meta_target_hash")])

#     df_existing_with_hash = df_source_existing.join(
#         df_target_existing, on=merge_col, how="outer"
#     ).with_columns(
#         (pl.col("meta_source_hash") == pl.col("meta_target_hash")).alias("meta_same_hash")
#     )

#     df_update = df_existing_with_hash.filter(pl.col("meta_same_hash") == False).select(
#         pl.exclude(["meta_source_hash", "meta_target_hash", "meta_same_hash"])
#     )
#     df_not_update = df_existing_with_hash.filter(pl.col("meta_same_hash") == True).select(
#         pl.exclude(["meta_source_hash", "meta_target_hash", "meta_same_hash"])
#     )

#     # concat all results

#     df_final = pl.concat([df_append, df_leave, df_update, df_not_update]).sort(
#         merge_col, reverse=False
#     )

#     final_check = df_final.filter(pl.col(merge_col).is_duplicated())
#     if len(final_check) == 0:
#         pass
#     else:
#         raise EztMergeException("Key in merger result is not unique.")

#     # TODO: store merge result somehow?

#     return df_final.to_arrow()


def calculate_merge(source, target, merge_col) -> pa.Table:

    # config.log_info("Calculating merge...")

    # preliminary checks
    target_check = target.filter(pl.col(merge_col).is_duplicated())
    if len(target_check) == 0:
        pass
    else:
        raise EztMergeException("Key in target model is not unique.")

    source_check = source.filter(pl.col(merge_col).is_duplicated())
    if len(source_check) == 0:
        pass
    else:
        raise EztMergeException("Key in target model is not unique.")

    # sort source and target in same order
    source = source.sort(merge_col, reverse=False)
    target = target.sort(merge_col, reverse=False)

    # new rows to append, i.e. rows in source but not in target
    df_append = (
        source.with_columns(
            source[merge_col].is_in(target[merge_col]).apply(lambda x: not x).alias("_append_rows")
        )
        .filter(pl.col("_append_rows") == True)
        .select(pl.exclude("_append_rows"))
    )

    # existing rows to leave, i.e. rows in target but not in source
    df_leave = (
        target.with_columns(
            target[merge_col].is_in(source[merge_col]).apply(lambda x: not x).alias("_leave_rows")
        )
        .filter(pl.col("_leave_rows") == True)
        .select(pl.exclude("_leave_rows"))
    )

    # if rows exist both in source and target, all rows should be updated and we can simply return source to replace target
    if df_leave.height == 0 and df_append.height == 0:
        return source.to_arrow()

    # get rows from source that will replace existing rows in target, i.e. rows in source that should not be appended to target
    df_update = (
        source.with_columns(
            source[merge_col]
            .is_in(df_append[merge_col])
            .apply(lambda x: not x)
            .alias("_update_rows")
        )
        .filter(pl.col("_update_rows") == True)
        .select(pl.exclude("_update_rows"))
    )

    df_final = pl.concat([df_append, df_leave, df_update]).sort(merge_col, reverse=False)

    final_check = df_final.filter(pl.col(merge_col).is_duplicated())
    if len(final_check) == 0:
        return df_final.to_arrow()
    else:
        raise EztMergeException("Key in merger result is not unique.")

    # TODO: store merge result somehow?
