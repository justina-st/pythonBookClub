import os
import pandas as pd
import json
import azure.functions as func

from typing import NamedTuple
from requests import Session, Response
from pathlib import Path
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector import SnowflakeConnection
from datetime import datetime
from dateutil.relativedelta import relativedelta
from utils import logging_decorator_factory, call_api
from split_locations_data import LOAD_ID

BASE_PATH = Path(__file__).parent
LOAD_NAME = "GET_DATA_FROM_AZURE_MAPS_API"
BASE_URL = "https://atlas.microsoft.com"
FUZZY_ENDPOINT = "/search/fuzzy/json"
TIMEZONES_ENDPOINT = "/timezone/byCoordinates/json"
CATEGORY_SET = "7383,9159,7308,7352,7380,9942,7347,9388,7391"
OUTPUT_DATA_COLUMNS = [
    "LOCATION_PK",
    "COUNTRY_CODE",
    "SOURCE",
    "LATITUDE_DD",
    "LONGITUDE_DD",
    "LOCATION_COORDINATE_DETAILS",
    "SCORE",
    "TIMEZONE_DETAILS",
]


@logging_decorator_factory(LOAD_ID, LOAD_NAME, step_name="Processing Storage Queue message")
def process_queue_message(msg: func.QueueMessage) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    message_payload = json.loads(msg.get_body().decode())
    df_data = pd.DataFrame.from_dict(message_payload.get("data"), orient="columns")
    _locations_logic_filter = df_data.LATITUDE_DD.isna() & df_data.LONGITUDE_DD.isna() & ~df_data.LOCATION.isna()
    df_location = df_data[_locations_logic_filter].copy()
    df_tmp = df_data[~_locations_logic_filter]
    df_coordinate = df_tmp[(~df_tmp.LATITUDE_DD.isna() & ~df_tmp.LONGITUDE_DD.isna())].copy()
    additional_info = json.dumps(
        {
            "PROCEDURE_OUTPUT_DATA": len(df_coordinate) + len(df_location),
            "COUNT_WITH_COORDINATES": len(df_coordinate),
            "COUNT_WITHOUT_COORDINATES": len(df_location),
            "COUNT_DISCARDED": len(df_tmp) - len(df_coordinate),
        }
    )
    return df_coordinate, df_location, additional_info


@logging_decorator_factory(LOAD_ID, LOAD_NAME, "Calling Azure Maps API for coordinates")
def get_coordinates(df_loc: pd.DataFrame, session: Session) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
    ldf = pd.DataFrame(
        columns=[
            "PK",
            "COUNTRY_CODE",
            "LATITUDE_DD",
            "LONGITUDE_DD",
            "LOCATION_COORDINATE_DETAILS",
            "SCORE",
            "COORDINATE_SOURCE",
        ]
    )
    missing_results = pd.DataFrame(columns=OUTPUT_DATA_COLUMNS)
    failed_response = pd.DataFrame(columns=OUTPUT_DATA_COLUMNS)
    url = f"{BASE_URL}{FUZZY_ENDPOINT}"
    for row in df_loc.itertuples():
        params = {
            "api-version": "1.0",
            "query": row.LOCATION,
            "subscription-key": os.getenv("AzureMapsSubscriptionKey"),
            "countrySet": row.COUNTRY_CODE,
            "CategorySet": CATEGORY_SET,
            "idxSet": "POI",
        }
        http_output = call_api(url, params, session)
        ldf, missing_results, failed_response = _process_coordinates(
            http_output, ldf, missing_results, failed_response, row
        )
    row_count = len(df_loc.index) + len(missing_results.index) + len(failed_response.index)
    additional_info = json.dumps(
        {
            "PROCEDURE_OUTPUT_DATA": row_count,
            "COUNT_WITH_COORDINATES": len(df_loc.index),
            "COUNT_WITHOUT_COORDINATES": len(missing_results.index),
            "COUNT_WITH_API_FAILURE": len(failed_response.index),
        }
    )

    return ldf, missing_results, failed_response, additional_info


@logging_decorator_factory(LOAD_ID, LOAD_NAME, step_name="Calling Azure Maps API for timezones")
def get_timezones(df_coord: pd.DataFrame, session: Session) -> tuple[pd.DataFrame, str]:
    rdf = pd.DataFrame(columns=OUTPUT_DATA_COLUMNS)
    url = f"{BASE_URL}{TIMEZONES_ENDPOINT}"
    current_day = datetime.now().replace(month=1, day=1, hour=0, second=0, minute=0, microsecond=0)
    transition_from = current_day - relativedelta(years=5)
    for row in df_coord.itertuples():
        params = {
            "api-version": "1.0",
            "query": f"{row.LATITUDE_DD},{row.LONGITUDE_DD}",
            "subscription-key": os.getenv("AzureMapsSubscriptionKey"),
            "options": "all",
            "transitionsFrom": f"{transition_from}Z",
            "transitionsYears": "10",
        }
        http_output = call_api(url, params, session)
        rdf = _process_timezones(http_output, rdf, row)
    additional_info = json.dumps({"PROCEDURE_OUTPUT_DATA": len(rdf.index)})
    return rdf, additional_info


@logging_decorator_factory(LOAD_ID, LOAD_NAME, step_name="Writing Data to Snowflake")
def write_timezones_to_snowflake(con: SnowflakeConnection, output: pd.DataFrame) -> tuple[None, str]:
    con.cursor().execute("USE SCHEMA STAGE;")
    n_rows = 0
    if len(output) > 0:
        success, n_chunks, n_rows, _ = write_pandas(
            con,
            output,
            table_name="AZURE_MAPS_TIMEZONES_BY_COORDINATES",
            database=os.getenv("ReferenceDb"),
            schema="STAGE",
        )
    additional_info = json.dumps({"PROCEDURE_OUTPUT_DATA": n_rows})
    return None, additional_info


def _process_coordinates(
    http_output: Response,
    data_frame: pd.DataFrame,
    missing_results_dataframe: pd.DataFrame,
    failed_response_dataframe: pd.DataFrame,
    data_row: NamedTuple,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if http_output is not None:

        loc = http_output.json()

        if http_output.status_code == 200:
            _results = loc.get("results", {})
            if len(_results) != 0:
                _position = _results[0].get("position", {})
                lat = _position.get("lat", None)
                long = _position.get("lon", None)
                score = _results[0].get("score")

                data_list = [
                    data_row.PK,
                    data_row.COUNTRY_CODE,
                    lat,
                    long,
                    loc,
                    score,
                    f"{data_row.COORDINATE_SOURCE}_ATLAS_API",
                ]
                data_columns = [
                    "PK",
                    "COUNTRY_CODE",
                    "LATITUDE_DD",
                    "LONGITUDE_DD",
                    "LOCATION_COORDINATE_DETAILS",
                    "SCORE",
                    "COORDINATE_SOURCE",
                ]

                data_frame = _prepare_dataframe(data_frame, data_list, data_columns)
            else:
                missing_data_list = [
                    data_row.PK,
                    data_row.COUNTRY_CODE,
                    f"{data_row.COORDINATE_SOURCE}_ATLAS_API",
                    None,
                    None,
                    loc,
                    None,
                    None,
                ]
                missing_data_columns = OUTPUT_DATA_COLUMNS
                missing_results_dataframe = _prepare_dataframe(
                    missing_results_dataframe, missing_data_list, missing_data_columns
                )
        else:
            failed_data_list = [
                data_row.PK,
                data_row.COUNTRY_CODE,
                f"{data_row.COORDINATE_SOURCE}_ATLAS_API",
                None,
                None,
                loc,
                None,
                None,
            ]
            failed_data_columns = OUTPUT_DATA_COLUMNS
            failed_response_dataframe = _prepare_dataframe(
                failed_response_dataframe, failed_data_list, failed_data_columns
            )
    else:
        failed_data_list = [
            data_row.PK,
            data_row.COUNTRY_CODE,
            f"{data_row.COORDINATE_SOURCE}_ATLAS_API",
            None,
            None,
            {"error": "All retries failed"},
            None,
            None,
        ]
        failed_data_columns = OUTPUT_DATA_COLUMNS
        failed_response_dataframe = _prepare_dataframe(failed_response_dataframe, failed_data_list, failed_data_columns)

    return data_frame, missing_results_dataframe, failed_response_dataframe


def _process_timezones(http_output: Response, data_frame: pd.DataFrame, data_row: NamedTuple):
    data_list = [
        data_row.PK,
        data_row.COUNTRY_CODE,
        data_row.COORDINATE_SOURCE,
        data_row.LATITUDE_DD,
        data_row.LONGITUDE_DD,
        data_row.LOCATION_COORDINATE_DETAILS,
        data_row.SCORE,
        http_output.json(),
    ]
    data_columns = OUTPUT_DATA_COLUMNS
    rdf = _prepare_dataframe(data_frame, data_list, data_columns)
    return rdf


def _prepare_dataframe(data_frame: pd.DataFrame, data_list: list, data_columns: list):
    rdf = pd.concat(
        [
            data_frame,
            pd.DataFrame(
                data=[data_list],
                columns=data_columns,
            ),
        ],
        ignore_index=True,
    )
    return rdf
