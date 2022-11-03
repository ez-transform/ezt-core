# import os

# from ezt.include import MACROS_PATH
# from jinja2 import DictLoader, Environment, Template


# def render_sql(sql_file: str, sources: dict, models: dict):

#     macros = f"{MACROS_PATH}/base_macros.sql"

#     with open(sql_file, "r") as f, open(macros, "r") as e:
#         sql = f.read()
#         macro = e.read()

#     # template = Template(sql).render()

#     # print(template)

#     loader = DictLoader({"template": sql})
#     env = Environment(loader=loader)
#     macro_template = env.from_string(macro, globals=sources)
#     env.globals["source"] = macro_template.module.source
#     template = env.get_template("template")
#     rendered = template.render()

#     # print(rendered)
#     return rendered
