from contextlib import nullcontext as does_not_raise

import pytest

from src.spark_schemanator.main import create_data


@pytest.mark.parametrize(
    "schema_fixture_name, expected_context",
    [
        ("get_valid_schema", does_not_raise()),
    ],
)
def test_create_data(request, get_spark_session, schema_fixture_name, expected_context):
    schema = request.getfixturevalue(schema_fixture_name)
    with expected_context:
        assert create_data(get_spark_session, schema).schema == schema
