import io
import sys
import traceback

import pytest
from ezt.util.validator import (
    ValidationResultFailed,
    ValidationResultSuccess,
    Validator,
)
from jsonschema import ValidationError


@pytest.fixture
def valid_validator(conf_fixture):
    validator = Validator(
        project_yml=conf_fixture.project,
        sources_yml=conf_fixture.sources,
    )
    return validator


@pytest.fixture
def invalid_validator():
    validator = Validator(
        project_yml={"random_key": "random_val"},
        sources_yml={"random_key": "random_val"},
    )
    return validator


class TestValidator:
    def test_create_validationresultsuccess(self):
        msg = "testing validation passed"
        res = ValidationResultSuccess(code=1, msg=msg)
        capturedOutput = io.StringIO()
        sys.stdout = capturedOutput
        res.print_result()
        sys.stdout = sys.__stdout__

        assert msg in capturedOutput.getvalue()

    def test_create_validationresultfailed(self):
        msg = "testing validation failed"

        # get a traceback
        tb_msg = "raised exception"
        try:
            raise Exception(tb_msg)
        except:
            tb = traceback.format_exc()
        res = ValidationResultFailed(code=0, msg=msg, traceback_msg=tb)
        capturedOutput = io.StringIO()
        sys.stdout = capturedOutput
        res.print_result()
        sys.stdout = sys.__stdout__

        assert tb_msg in capturedOutput.getvalue()
        assert msg in capturedOutput.getvalue()

    def test_load_schema(self, valid_validator):

        project_schema = valid_validator._load_schema("project_schema")
        sources_schema = valid_validator._load_schema("sources_schema")
        # TODO: add models schema when ready

        assert isinstance(project_schema, dict)
        assert {"type", "properties", "required"}.issubset(project_schema.keys())
        assert isinstance(sources_schema, dict)
        assert {"type", "properties", "required"}.issubset(sources_schema.keys())

    def test_validate_yml(self, conf_fixture, valid_validator, invalid_validator):

        # test project schema
        valid_validator._validate_yml(valid_validator.project_yml, "project_schema")

        # test sources schema
        valid_validator._validate_yml(valid_validator.sources_yml, "sources_schema")

        # test failed validation
        with pytest.raises(ValidationError):
            invalid_validator._validate_yml(invalid_validator.project_yml, "project_schema")

    def test_create_validation_results(self, valid_validator, invalid_validator):

        valid_validator.create_validation_results()

        valid_failed = False
        for res_valid in valid_validator.results:
            if isinstance(res_valid, ValidationResultFailed):
                valid_failed = True
                break

        assert not valid_failed

        invalid_validator.create_validation_results()
        print(invalid_validator.results)
        invalid_failed = True
        for res_invalid in invalid_validator.results:
            if isinstance(res_invalid, ValidationResultSuccess):
                invalid_failed = False
                break

        assert invalid_failed
