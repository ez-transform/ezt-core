import os
from ast import parse

import polars as pl
from s3fs import S3FileSystem

from ezt.util.helpers import (
    copy_starter,
    get_model_dependencies,
    get_s3_filesystem,
    parse_yaml,
)


def test_copy_starter(tmp_path):
    """Test for creating the starter project."""
    tmp_dir = tmp_path / "starter"
    tmp_dir.mkdir()
    os.chdir(tmp_dir)

    copy_starter(tmp_dir, "my_project")

    assert os.path.isdir(tmp_dir / "my_project")
    assert os.path.isfile(tmp_dir / "my_project" / "ezt_project.yml")
    assert os.path.isfile(tmp_dir / "my_project" / "sources.yml")
    assert os.path.isfile(tmp_dir / "my_project" / "README.md")
    assert os.path.isfile(tmp_dir / "my_project" / ".gitignore")
    assert os.path.isfile(tmp_dir / "my_project" / "__init__.py")
    assert os.path.isdir(tmp_dir / "my_project" / "models")
    assert os.path.isfile(tmp_dir / "my_project" / "models" / "models.yml")
    assert os.path.isfile(tmp_dir / "my_project" / "models" / "__init__.py")
    assert os.path.isfile(tmp_dir / "my_project" / "models" / "my_example.py")


def test_parse_yaml(project_yml):
    """Test for parsing yml files and returning dict."""

    result = parse_yaml(project_yml)

    assert isinstance(result, dict)
    assert result["name"] == "my_project1"
    assert result["models_folder"] == "models"


def test_get_model_dependencies(example_model):
    deps = get_model_dependencies(example_model)
    expected = {"ingest1", "ingest2"}

    assert deps == expected


def test_get_model_dependencies_all(example_model):
    deps = get_model_dependencies(example_model)
    expected = {"ingest1", "ingest2"}

    assert deps == expected


def test_get_s3_filesystem(monkeypatch):
    # setup
    envs = {
        "AWS_ACCESS_KEY_ID": "test_key",
        "AWS_SECRET_ACCESS_KEY": "test_secret",
    }

    monkeypatch.setattr(os, "environ", envs)

    s3 = get_s3_filesystem()

    assert isinstance(s3, S3FileSystem)
    assert s3.key == "test_key"
    assert s3.secret == "test_secret"
