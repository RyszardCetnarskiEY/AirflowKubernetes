import sys
import os
from pendulum import datetime
import pytest
import pickle
from unittest.mock import patch, MagicMock


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from tasks.entsoe_api_tasks import _generate_run_parameters_logic, _get_entsoe_response

from dags.entsoe_ingest import extract_from_api, store_raw_xml, parse_xml, add_timestamp_column, add_timestamp_elements, combine_df_and_params, load_to_staging_table, merge_data_to_production, cleanup_staging_tables

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook

HISTORICAL_START_DATE = datetime(2025, 1, 1, tz="UTC")
HISTORICAL_END_DATE = datetime(2025, 1, 2, tz="UTC")

POSTGRES_CONN_ID = "postgres_azure_vm"
RAW_XML_TABLE_NAME = "entsoe_raw_xml_landing" # Changed name for clarity

@pytest.fixture
def sample_xml_response():
    filename="sample_entsoe_response_prices_pl.xml"  
    path = os.path.join(os.path.dirname(__file__), "data", filename)

    with open(path, "r", encoding="utf-8") as f:
        return f.read()

@pytest.fixture
def test_param():
    #todo - perfrom testing on all params, not just 0
    test_params = _generate_run_parameters_logic(HISTORICAL_START_DATE, HISTORICAL_END_DATE)
    return test_params[0]

@pytest.fixture()
def airflow_context():   
    test_context = {}
    test_context['logical_date'] = HISTORICAL_START_DATE # or data_interval_start            
    test_context['data_interval_start'] = HISTORICAL_START_DATE # or data_interval_start            
    test_context['data_interval_end'] = HISTORICAL_END_DATE # or data_interval_start        
    test_context['dag_run'] =  type('MockDagRun', (), {'run_id': 'test_1234'})()
    return test_context


@patch("tasks.entsoe_api_tasks.HttpHook")
def test_http_connection(mock_http_hook_class):

    mock_http_hook = MagicMock()
    mock_conn = MagicMock()

    mock_conn.password = "dummy_token"
    mock_conn.host = "web-api.tp.entsoe.eu"

    mock_http_hook.get_connection.return_value = mock_conn
    mock_http_hook_class.return_value = mock_http_hook

    from tasks.entsoe_api_tasks import _entsoe_http_connection_setup
    base_url, api_key, conn, http_hook = _entsoe_http_connection_setup()

    assert api_key.strip() == api_key  # Should not raise!
    assert base_url == "https://web-api.tp.entsoe.eu/api"



# def test_entsoe_repsone(test_param, airflow_context):
#     #todo - mock http connection and return pre-determined files that should be returned by request
#     response = extract_from_api.function(test_param, **airflow_context)

#     # filename="sample_entsoe_response_prices_pl.xml"  
#     # path = os.path.join(os.path.dirname(__file__), "data", filename)
#     # os.makedirs(os.path.dirname(path), exist_ok=True)

#     # with open(path, "w", encoding="utf-8") as f:
#     #         f.write(response['xml_content'])

#     assert type(response)==dict
#     assert response['status_code'] == 200

@patch("tasks.entsoe_api_tasks.HttpHook")  # ✅ correct path
def test_extract_from_api_with_mocked_http(
    mock_http_hook_class, sample_xml_response, test_param, airflow_context
):
    # Arrange: Mock HttpHook and its connection
    mock_http_hook = MagicMock()
    mock_response = MagicMock()

    mock_response.status_code = 200
    mock_response.text = sample_xml_response
    mock_response.headers.get.return_value = "application/xml"

    mock_http_hook.get_connection.return_value = MagicMock(
        password="fake_token", host="web-api.tp.entsoe.eu/api"
    )
    mock_http_hook.get_conn.return_value.get.return_value = mock_response
    mock_http_hook_class.return_value = mock_http_hook

    # Act: call the task's .function interface with fixtures
    result = extract_from_api.function(test_param, **airflow_context)

    # Assert: validate response structure and content
    assert result["status_code"] == 200
    assert "xml_content" in result
    assert "<Publication_MarketDocument" in result["xml_content"]
    assert result["country_name"] == test_param["task_run_metadata"]["country_name"]
    assert result["var_name"] == test_param["task_run_metadata"]["var_name"]