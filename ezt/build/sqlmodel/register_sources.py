# import datafusion
# import click
# import sys

# from typing import Union
# from os import path
# from yaml import load, dump, Loader


# def register_sources(sources: dict) -> Union[datafusion.ExecutionContext, str]:

#     fusion_ctx = datafusion.ExecutionContext()


#     for source in sources['sources']:
#         if not path.isfile(source['path']):
#             msg = f'Not able to find file: {source["path"]}.'
#             return msg

#         if source['type'].lower() == 'parquet':
#             fusion_ctx.register_parquet(
#                 name=source['name'], path=source['path'])
#             click.echo(f'Source table {source["name"]} registered.')
#         if source['type'].lower() == 'csv':
#             fusion_ctx.register_csv(
#                 name=source['name'], path=source['path'])
#             click.echo(f'Source table {source["name"]} registered.')

#     return fusion_ctx
