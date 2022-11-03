import polars as pl
import pyarrow as pa
import pytest
from ezt.build.dfmodel.merge import calculate_merge
from ezt.build.dfmodel.persist_delta import update_types


@pytest.fixture
def identical_frames():
    data = {"id": ["01", "02"], "a": [1, 2], "b": [3, 4]}
    source = pl.from_dict(data)
    target = pl.from_dict(data)
    return (source, target)


class TestMerge:

    # test types update
    def test_update_types(self):
        data = [{"large_string": "some_string", "unsigned_int_64": 64, "int32": 32}]
        my_schema = pa.schema(
            [
                pa.field("large_string", pa.large_string()),
                pa.field("unsigned_int_64", pa.uint64()),
                pa.field("int32", pa.int32()),
            ]
        )

        test_schema = pa.schema(
            [
                pa.field("large_string", pa.string()),
                pa.field("unsigned_int_64", pa.string()),
                pa.field("int32", pa.int32()),
            ]
        )

        table = pa.Table.from_pylist(data, schema=my_schema)
        new_table = update_types(table)

        assert new_table.schema.equals(test_schema)

    # test merge of identical dataframes
    def test_calculate_merge_identical(self, identical_frames):

        source, target = identical_frames

        result = pl.from_arrow(calculate_merge(source, target, "id"))

        assert source.frame_equal(result)

    # test merging of new rows to existing dataframe
    def test_calculate_merge_new_rows(self, new_rows):

        source, target = new_rows

        result = pl.from_arrow(calculate_merge(source.collect(), target.collect(), "id"))

        assert result.height == 100, f"Height of result: {result.height}"

    # test merge of new and overlapping rows
    def test_calculate_merge_overlapping_rows(self, overlapping_rows):

        source, target = overlapping_rows

        result = pl.from_arrow(calculate_merge(source.collect(), target.collect(), "id"))

        assert result.height == 100, f"Height of result: {result.height}"

    # test merge with data update of existing row
    def test_calculate_merge_with_updated_data(self, data_update):

        source, target = data_update

        result = pl.from_arrow(calculate_merge(source.collect(), target.collect(), "id"))

        assert (
            result.filter(pl.col("id") == 10)["first_name"][0] == "UPDATED_NAME"
        ), f"Value: {result.filter(pl.col('id') == 10)['first_name'][0]}"
