### Uncomment all of the below and run 'ezt run' from your ezt project base directory and examine the results in the .target folder


# import polars as pl
# from ezt import py_model

# @py_model
# def df_model():

#     data = {"person": ["John", "Michael", "Angela", "Angela"], "points": ["3", "2", "1", "5"]}

#     q = (
#         pl.from_dict(data)
#         .lazy()
#         .with_column(pl.col("points").cast(pl.UInt32))
#         .groupby("person")
#         .agg(pl.sum("points").alias("points_summed"))
#     )

#     return q.collect()
