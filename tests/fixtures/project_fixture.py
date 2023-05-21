import os
import shutil

import pytest
import yaml

from ezt.util.config import Config
from ezt.util.helpers import copy_starter


@pytest.fixture(scope="session")
def conf_fixture(tmp_path_factory):
    tmp_dir = tmp_path_factory.mktemp("starter")
    os.chdir(tmp_dir)

    copy_starter(tmp_dir, "my_project")

    # os.chdir(tmp_dir / "my_project")

    project_path = tmp_dir / "my_project"
    raw_customers_path = f"{os.path.dirname(__file__)}/data/raw_customers.csv"

    sources = {
        "sources": [
            {
                "name": "raw_customers",
                "filesystem": "local",
                "path_type": "file",
                "format": "csv",
                "path": raw_customers_path,
                "csv_properties": {
                    "has_header": True,
                    "separator": ",",
                },
            }
        ]
    }

    with open(project_path / "sources.yml", "w") as f:
        yaml.dump(sources, f)

    models = {
        "models": [
            {
                "name": "my_model",
                "type": "df",
                "destination": project_path / ".target",
                "filesystem": "local",
                "write_settings": {
                    "file_type": "parquet",
                    "how": "overwrite",
                },
            }
        ]
    }

    with open(project_path / "models" / "models.yml", "w") as f:
        yaml.dump(models, f)

    test_model_path = f"{os.path.dirname(__file__)}/models/my_model.py"
    shutil.copy(src=test_model_path, dst=f"{project_path}/models/")

    conf = Config(project_path)

    return conf


@pytest.fixture
def log_folder(tmp_path):

    path = tmp_path / "logs"
    os.mkdir(path)

    return path


@pytest.fixture
def log_folder_path(tmp_path):

    path = tmp_path / "logs_2"

    return path


@pytest.fixture
def change_cwd(request, path):
    os.chdir(path)
    yield
    os.chdir(request.fspath.invocation_dir)
