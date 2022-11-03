from graphlib import TopologicalSorter
from inspect import getmembers, isfunction

from ezt import py_model
from ezt.util.config import Config, unwrap


class TestConfig:
    def test_unwrap(self):
        @py_model
        def my_decorated_func():
            pass

        unwrapped_func = unwrap(my_decorated_func)

        assert hasattr(unwrapped_func, "__wrapped__") == False

    def test_config_init(self):

        my_conf = Config("my_project_dir")
        assert my_conf.project_dir == "my_project_dir"

    def test_config_project(self, conf_fixture):

        project = conf_fixture.project

        assert set(("name", "models", "logs_destination")).issubset(project.keys())

    def test_config_sources(self, conf_fixture):

        sources = conf_fixture.sources

        assert isinstance(sources, dict)
        assert {"sources"}.issubset(sources.keys())

    def test_config_models(self, conf_fixture):

        models = conf_fixture.models

        assert isinstance(models, dict)
        assert {"models"}.issubset(models.keys())

    def test_config_get_model(self, conf_fixture):

        model = conf_fixture.get_model("my_model")

        assert isinstance(model, dict)
        assert model["name"] == "my_model"
        assert set(("name", "type", "destination")).issubset(model.keys())

    def test_execution_order(self, conf_fixture):

        order = conf_fixture.execution_order

        assert isinstance(order, TopologicalSorter)
        assert list(order.static_order())[0] == "my_model"

    def test_validation_result(self, conf_fixture):

        validation_result = conf_fixture.validation_result

        assert isinstance(validation_result, dict)
        assert set(("failed", "success")).issubset(validation_result.keys())

    def test_import_model(self, conf_fixture):

        model_name = conf_fixture.models["models"][0]["name"]

        my_module = conf_fixture.import_model(model_name)
        funcs = getmembers(my_module, isfunction)

        assert len(funcs) > 0
