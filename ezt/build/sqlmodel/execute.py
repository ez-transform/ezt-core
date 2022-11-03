# import datafusion
# import pyarrow as pa
# import traceback
# from pyarrow import parquet as pq

# # def run_models(ctx: datafusion.ExecutionContext, models: dict):
# #     for model in models['models']:
# #         rendered_sql = render_model(model, ctx.tables())

# def exec_sql(context: datafusion.ExecutionContext, sql: str, destination: str):

#     ctx = context
#     try:
#         df = ctx.sql(sql)
#     except Exception as e:
#         traceback.print_exc()

#     try:
#         result = df.collect()[0]
#     except Exception as e:
#         traceback.print_exc()

#     try:
#         table = pa.Table.from_batches([result])
#         pq.write_table(table, destination)
#     except Exception as e:
#         traceback.print_exc()
