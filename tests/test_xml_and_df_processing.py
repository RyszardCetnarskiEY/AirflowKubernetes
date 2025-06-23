import json
import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

from dags.entsoe_ingest import parse_xml, store_raw_xml
from tasks.df_processing_tasks import add_timestamp_column, add_timestamp_elements


@pytest.fixture
def sample_parsed_df():
    filename = "sample_prices_df_pl.csv"
    path = os.path.join(os.path.dirname(__file__), "data", filename)
    parsed_df = pd.read_csv(path, index_col=0)
    return parsed_df


@pytest.fixture
def sample_parsed_df_with_timestamp():
    filename = "sample_prices_df_pl_with_timestamp.csv"
    path = os.path.join(os.path.dirname(__file__), "data", filename)
    parsed_df = pd.read_csv(path, index_col=0)
    return parsed_df


@pytest.fixture
def full_response():
    filename = "full_response_prices_pl.json"
    path = os.path.join(os.path.dirname(__file__), "data", filename)
    with open(path, "r") as f:
        loaded_dict = json.load(f)
    return loaded_dict


def test_parse_xml(full_response, sample_parsed_df):
    parsed_df = parse_xml.function(full_response)
    assert all(parsed_df == sample_parsed_df)


def test_add_timestamp_column(sample_parsed_df, sample_parsed_df_with_timestamp):
    parsed_timestamp_df = add_timestamp_column.function(sample_parsed_df)
    #    parsed_timestamp_elements_df = add_timestamp_elements.function(parsed_timestamp_df)

    assert parsed_timestamp_df is not None
    assert "timestamp" in parsed_timestamp_df.columns
    print(parsed_timestamp_df["timestamp"])
    print(sample_parsed_df_with_timestamp["timestamp"])

    assert all(
        parsed_timestamp_df["timestamp"] == sample_parsed_df_with_timestamp["timestamp"]
    )


def test_add_timestamp_elements(sample_parsed_df_with_timestamp):
    parsed_timestamp_elements_df = add_timestamp_elements.function(
        sample_parsed_df_with_timestamp
    )
    print(list(parsed_timestamp_elements_df["year"].unique()))
    assert list(parsed_timestamp_elements_df["year"].unique()) == [2024, 2025]
    assert list(parsed_timestamp_elements_df["month"].unique()) == [12, 1]
    assert list(parsed_timestamp_elements_df["quarter"].unique()) == [4, 1]
    assert list(parsed_timestamp_elements_df["day"].unique()) == [31, 1, 2]
    assert list(parsed_timestamp_elements_df["dayofweek"].unique()) == [
        1,
        2,
        3,
    ], parsed_timestamp_elements_df["dayofweek"].unique()

    assert parsed_timestamp_elements_df["hour"].min() == 0
    assert parsed_timestamp_elements_df["hour"].max() == 23


@patch("tasks.xml_processing_tasks.PostgresHook")
def test_store_raw_xml_success(mock_pg_hook_class, full_response):
    mock_pg_hook = MagicMock()
    mock_pg_hook.run.return_value = 42
    mock_pg_hook_class.return_value = mock_pg_hook

    result = store_raw_xml.function(full_response, "test_conn", "test_table")

    assert result == 42
    mock_pg_hook.run.assert_called_once()
    args, kwargs = mock_pg_hook.run.call_args
    assert "INSERT INTO airflow_data" in args[0]  # Check SQL string
    assert kwargs["parameters"] == (
        full_response["var_name"],
        full_response["country_name"],
        full_response["area_code"],
        full_response["status_code"],
        full_response["period_start"],
        full_response["period_end"],
        full_response["logical_date_processed"],
        full_response["xml_content"],
        full_response["request_params"],
        full_response["content_type"],
    )
