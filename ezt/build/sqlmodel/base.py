# import os

from ezt.include import MACROS_PATH
from jinja2 import DictLoader, Environment, Template
from ezt.build.dfmodel.models import get_model
from ezt.build.dfmodel.sources import get_source
import polars as pl


def render_sql(sql: str, sources, models) -> str:
    """Function that renders a sql model."""

    macros_location = f"{MACROS_PATH}/base_macros.sql"

    with open(macros_location, 'r') as f:
        macros = f.read()
    
    # loader = DictLoader({"macros": macros})
    env = Environment()

    env.globals['sources'] = sources
    env.globals['models'] = models

    macro_template = env.from_string(macros)
    global_macros = [macro for macro in dir(macro_template.module) if not macro.startswith('_')]
    for m in global_macros:
        macro = f"macro_template.module.{m}"
        env.globals[m] = eval(macro)

    template = env.from_string(sql)
    # print(template.module)
    rendered = template.render()

    return rendered


def execute_sql(sql: str, deps: dict):
    """Function that executes a rendered sql model."""
    frames = {}
    for source in deps["sources"]:
        frames[source] = get_source(source)

    for model in deps["models"]:
        frames[model] = get_model(model)
    
    ctx = pl.SQLContext(frames=frames)
    result = ctx.execute(sql)
    return result

if __name__ == '__main__':

    sql = "select * from {{ source('tango') }}"

    print(render_sql(sql, [{'name': 'tango'}], [{}]))
