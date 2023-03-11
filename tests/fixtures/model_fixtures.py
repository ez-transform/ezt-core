import pytest

from ezt import get_model, get_source, py_model


@pytest.fixture
def example_model():
    pass

    def df_model():

        # dummy
        df1 = get_model("ingest1")
        df2 = get_model(name="ingest2")

        return df1.collect()

    return df_model
