import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pendulum import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

from tasks.entsoe_api_tasks import _generate_run_parameters_logic
from tasks.sql_tasks import (
    _create_prod_table,
    _create_table_columns,
    cleanup_staging_tables,
    create_initial_tables,
    load_to_staging_table,
    merge_data_to_production,
)

HISTORICAL_START_DATE = datetime(2025, 1, 1, tz="UTC")
HISTORICAL_END_DATE = datetime(2025, 1, 2, tz="UTC")


@pytest.fixture
def df_and_params():
    filename = "sample_prices_df_pl_with_timestamp.csv"
    path = os.path.join(os.path.dirname(__file__), "data", filename)
    parsed_df = pd.read_csv(path, index_col=0)

    # todo - perfrom testing on all params, not just 0 - perhaps it is best to sace the test_params to json or pickle
    test_params = _generate_run_parameters_logic(
        HISTORICAL_START_DATE, HISTORICAL_END_DATE
    )

    return {"df": parsed_df, "params": test_params[0]}


@pytest.fixture
def staging_dict():
    import pickle

    with open(
        "/workspace/myairflowdags/tests/data/staging_result_prices_pl.pkl", "rb"
    ) as f:
        result = pickle.load(f)
    return result


@pytest.fixture
def mock_postgres_hook():
    hook = MagicMock()
    conn = MagicMock()
    cursor = MagicMock()
    hook.get_conn.return_value = conn
    conn.cursor.return_value = cursor
    return hook, conn, cursor


@pytest.fixture
def expected_columns():
    return [
        '"Resolution" TEXT',
        '"quantity" NUMERIC',
        '"variable" TEXT',
        '"area_code" TEXT',
        '"timestamp" TEXT',
        '"year" NUMERIC',
        '"quarter" NUMERIC',
        '"month" NUMERIC',
        '"day" NUMERIC',
        '"dayofweek" NUMERIC',
        '"hour" NUMERIC',
    ]


def test_create_table_columns(df_and_params, expected_columns):
    df = df_and_params["df"]
    df = df.drop("Position", axis=1)  # Todo, a little ugly but not sure how to fix now

    assert _create_table_columns(df) == expected_columns, repr(
        _create_table_columns(df)
    )


@patch("tasks.sql_tasks.random.randint", return_value=12345)
@patch("tasks.sql_tasks.PostgresHook")
def test_load_to_staging_table_with_data(
    mock_postgres_hook_class,
    mock_randint,
    df_and_params,
    mock_postgres_hook,
    expected_columns,
):
    hook, conn, cursor = mock_postgres_hook
    mock_postgres_hook_class.return_value = hook

    result = load_to_staging_table.function(df_and_params)

    expected_table = "stg_entsoe_PL_202501010000_12345"
    assert result["staging_table_name"].startswith(
        expected_table[:63]
    ), f"Expected table name {result['staging_table_name']}"
    assert (
        result["var_name"] == "Energy Prices fixing I"
    ), f"Expected var name {result['var_name']}"
    assert hook.run.call_count >= 2, hook.run.call_count

    hook.run.assert_any_call(
        f'CREATE TABLE airflow_data."{expected_table}" (id SERIAL PRIMARY KEY, {", ".join(expected_columns)}, processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP);'
    )
    hook.run.assert_any_call(f'DROP TABLE IF EXISTS airflow_data."{expected_table}";')
    assert hook.run.call_count == 2, hook.run.call_count

    # Save to file
    # import pickle
    # with open("/workspace/myairflowdags/tests/data/staging_result_prices_pl.pkl", "wb") as f:
    #     pickle.dump(result, f)

    assert cursor.copy_expert.called, cursor.copy_expert.called


@patch("tasks.sql_tasks._create_prod_table")
@patch("tasks.sql_tasks.PostgresHook")
def test_merge_executes_sql(mock_pg_hook_class, mock_create_prod_table, staging_dict):
    mock_pg_hook = MagicMock()
    mock_pg_hook_class.return_value = mock_pg_hook

    result = merge_data_to_production.function(staging_dict, db_conn_id="fake_conn")

    production_table = "energy_prices_fixing_i"
    staging_table = "stg_entsoe_PL_202501010000_12345"

    assert (
        result == f"Merged {staging_table}"
    ), f"expected {staging_table} actual {result}"
    mock_create_prod_table.assert_called_once_with(production_table)

    assert mock_pg_hook.run.call_count == 1
    actual_sql = mock_pg_hook.run.call_args[0][0]
    assert f'FROM airflow_data."{staging_table}"' in actual_sql
    assert f'INTO airflow_data."{production_table}"' in actual_sql, actual_sql


@patch("tasks.sql_tasks.PostgresHook")
@patch("tasks.sql_tasks.logger")
def test_create_initial_tables(mock_logger, mock_pg_hook_class):
    mock_pg_hook = MagicMock()
    mock_pg_hook_class.return_value = mock_pg_hook

    result = create_initial_tables.function("my_conn_id", "test_raw_table")

    expected_sql_snippet = 'CREATE TABLE IF NOT EXISTS airflow_data."test_raw_table"'
    actual_sql = mock_pg_hook.run.call_args[0][0]

    assert expected_sql_snippet in actual_sql
    mock_pg_hook.run.assert_called_once()
    assert result == {"raw_xml_table": "test_raw_table"}
    mock_logger.info.assert_called_once_with(
        'Ensured raw XML table airflow_data."test_raw_table" exists.'
    )


@patch("tasks.sql_tasks.PostgresHook")
@patch("tasks.sql_tasks.logger")
def test_create_prod_table(mock_logger, mock_pg_hook_class):
    mock_pg_hook = MagicMock()
    mock_pg_hook_class.return_value = mock_pg_hook

    _create_prod_table("my_var")

    expected_sql_snippet = 'CREATE TABLE IF NOT EXISTS airflow_data."my_var"'
    actual_sql = mock_pg_hook.run.call_args[0][0]

    assert expected_sql_snippet in actual_sql
    mock_pg_hook.run.assert_called_once()
    mock_logger.info.assert_called_once_with(
        'Ensured production table airflow_data."my_var" exists.'
    )


@patch("tasks.sql_tasks.PostgresHook")
@patch("tasks.sql_tasks.logger")
def test_cleanup_staging_tables_drops_table(mock_logger, mock_pg_hook_class):
    mock_pg_hook = MagicMock()
    mock_pg_hook_class.return_value = mock_pg_hook

    staging_dict = {"staging_table_name": "stg_table_x"}
    cleanup_staging_tables.function(staging_dict, "fake_conn")

    mock_pg_hook.run.assert_called_once_with(
        'DROP TABLE IF EXISTS airflow_data."stg_table_x";'
    )
    mock_logger.info.assert_called_once_with(
        'Dropped staging table: airflow_data."stg_table_x".'
    )
