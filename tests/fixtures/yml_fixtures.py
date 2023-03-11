import pytest
import yaml


@pytest.fixture
def model_dict_local_delta_overwrite():

    model_dict = {
        "name": "my_model",
        "type": "df",
        "destination": "tests/fixtures/tmp_result",
        "filesystem": "local",
        "write_settings": {"file_type": "delta", "how": "overwrite"},
    }
    return model_dict


@pytest.fixture
def model_dict_s3_delta():

    model_dict = {
        "name": "my_model",
        "type": "df",
        "filesystem": "s3",
        "path_type": "folder",
        "destination": "some-bucket",
        "write_settings": {"file_type": "delta", "how": "overwrite"},
    }
    return model_dict


@pytest.fixture
def model_dict_local_parq():

    model_dict = {
        "name": "my_model",
        "type": "df",
        "filesystem": "local",
        "destination": "./some_folder",
        "write_settings": {"file_type": "parquet", "how": "overwrite"},
    }
    return model_dict


@pytest.fixture
def model_dict_s3_parq():

    model_dict = {
        "name": "my_model",
        "type": "df",
        "filesystem": "s3",
        "destination": "some-bucket",
        "write_settings": {"file_type": "parquet", "how": "overwrite"},
    }
    return model_dict


@pytest.fixture
def model_dict_adls_parq():

    model_dict = {
        "name": "my_model",
        "type": "df",
        "filesystem": "adls",
        "destination": "some-container",
        "write_settings": {"file_type": "parquet", "how": "overwrite"},
    }
    return model_dict


@pytest.fixture
def model_dict_adls_delta():

    model_dict = {
        "name": "my_model",
        "type": "df",
        "filesystem": "adls",
        "path_type": "folder",
        "destination": "some-container",
        "write_settings": {"file_type": "delta", "how": "overwrite"},
    }
    return model_dict


@pytest.fixture
def project_yml(tmp_path):
    """Fixtures that creates a temporary yml file representing a ezt_project.yml."""

    ezt_project = {
        "name": "my_project1",
        "models_folder": "models",
    }

    yml_path = tmp_path / "project_yaml"
    yml_path.mkdir()

    with open(yml_path / "ezt_project.yml", "w") as outfile:
        yaml.dump(ezt_project, outfile, default_flow_style=False)

    return yml_path / "ezt_project.yml"


@pytest.fixture
def write_mode_settings():
    """Returns a dictionary with different write_mode_settings (overwrite, append and merge)."""
    all_settings = {
        "overwrite": {
            "mode": "overwrite",
        },
        "append": {
            "mode": "append",
        },
        "merge": {
            "mode": "merge",
            "merge_col": "id",
        },
    }

    return all_settings


@pytest.fixture
def source_dict_s3_parq_folder():

    source_dict = {
        "name": "my_source",
        "filesystem": "s3",
        "path_type": "folder",
        "format": "parquet",
        "path": "some_bucket/some_folder",
    }
    return source_dict


@pytest.fixture
def source_dict_s3_parq_file():

    source_dict = {
        "name": "my_source",
        "filesystem": "s3",
        "path_type": "file",
        "format": "parquet",
        "path": "some_bucket/some_folder/my_source.parquet",
    }
    return source_dict


@pytest.fixture
def source_dict_s3_delta():

    source_dict = {
        "name": "my_source",
        "filesystem": "s3",
        "path_type": "folder",
        "format": "delta",
        "path": "some_bucket/some_folder",
    }
    return source_dict


@pytest.fixture
def source_dict_s3_csv():

    source_dict = {
        "name": "my_source",
        "filesystem": "s3",
        "path_type": "folder",
        "format": "csv",
        "bucket": "some_bucket",
        "folder": "some_folder",
        "csv_properties": {
            "some_property": "some_val",
        },
    }
    return source_dict


@pytest.fixture
def source_dict_local_csv_with_prop():

    source_dict = {
        "name": "my_source",
        "filesystem": "local",
        "path_type": "file",
        "format": "csv",
        "path": "my/path",
        "csv_properties": {
            "my_prop": "my_val",
        },
    }
    return source_dict


@pytest.fixture
def source_dict_local_csv_no_prop():

    source_dict = {
        "name": "my_source",
        "filesystem": "local",
        "path_type": "file",
        "format": "csv",
        "path": "my/path",
    }
    return source_dict


@pytest.fixture
def source_dict_local_parq():

    source_dict = {
        "name": "my_source",
        "filesystem": "local",
        "path_type": "file",
        "format": "parquet",
        "path": "my/path",
    }
    return source_dict


@pytest.fixture
def invalid_model_dict():

    model_dict = {
        "name": "invalid",
        "type": "df",
        "filesystem": "s3",
        "destination": "some_bucket",
        "write_settings": {"file_type": "parquet", "how": "invalid_mode"},
    }

    return model_dict


@pytest.fixture
def sql_model():

    model_dict = {
        "name": "my_name",
        "type": "sql",
        "filesystem": "local",
        "destination": "my_folder",
    }
    return model_dict


@pytest.fixture
def invalid_model_type_dict():

    model_dict = {
        "name": "invalid",
        "type": "invalid_type",
        "filesystem": "s3",
        "destination": "some_bucket",
        "write_settings": {"file_type": "parquet", "how": "invalid_mode"},
    }

    return model_dict
