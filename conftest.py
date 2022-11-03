import pytest

pytest_plugins = [
    "tests.fixtures.data_fixtures",
    "tests.fixtures.yml_fixtures",
    "tests.fixtures.model_fixtures",
    "tests.fixtures.project_fixture",
]


# @pytest.fixture
# def testing_config():
#     class TestConf:
#         def __init__(self):
#             pass

#         def log_info(self, message):
#             pass

#         def log_error(self, message):
#             pass

#     my_conf = TestConf()
#     return my_conf
