import os
import pysftp
import requests
import snowflake.connector

from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from storage_account_manager import StorageAccountManager
from dotenv import dotenv_values
from pathlib import Path
from furl import furl
from requests.auth import HTTPBasicAuth
from snowflake.connector.errors import DatabaseError
from steps.expected_structures import EXPECTED_STRUCTURES


def _prepare_env(envdir):
    env_values = dotenv_values(envdir)
    env_values.update({k.replace("_", "-"): v for k, v in env_values.items()})
    os.environ.update(env_values)


def _load_environment_variables():
    if not _prepare_env(Path(__file__).parents[2] / "local_setup" / ".env"):
        _prepare_env(".env")


def _create_storage_account_manager():
    credentials = ClientSecretCredential(
        tenant_id=os.getenv("TENANT-ID"),
        client_id=os.getenv("SERVICE-PRINCIPAL-APPLICATION-ID"),
        client_secret=os.getenv("SERVICE-PRINCIPAL-PASSWORD"),
    )

    subscription_id = os.getenv("SUBSCRIPTION-ID")
    resource_group = os.getenv("TRAFFICDATA-RESOURCE-GROUP")
    storage_account = f"st{os.getenv('APP-NAME')}dvweu"

    return StorageAccountManager(
        credentials=credentials,
        subscription_id=subscription_id,
        resource_group=resource_group,
        storage_account=storage_account,
    )


def before_all(context):
    _load_environment_variables()

    context.call_airflow = _create_base_airflow_request
    context.databases = {
        "TRAFFIC_DB": os.getenv("SNOWFLAKE-TRAFFIC-DATABASE"),
        "REFERENCE_DB": os.getenv("SNOWFLAKE-REFERENCE-DATABASE"),
        "SCHEDULES_DB": os.getenv("SNOWFLAKE-SCHEDULES-DATABASE"),
        "FLIGHT_STATUS_DB": os.getenv("SNOWFLAKE-FLIGHT-STATUS-DATABASE"),
    }
    context.snowflake_connection = _create_snowflake_connection(context.databases["TRAFFIC_DB"])
    context.container_client = _create_blob_storage_client()


def before_scenario(context, scenario):
    if scenario.feature.name in ["Traffic sftp data read"]:
        context.storage_account_manager = _create_storage_account_manager()
        context.storage_account_manager.enable_sftp()
        context.sftp_connection = _create_sftp_client()


def before_tag(context, tag):
    if "endtoend" == tag:
        _truncate_tables(context.snowflake_connection)


def after_scenario(context, scenario):
    if scenario.feature.name in ["Traffic data files", "Great Expectations for Traffic data"]:
        files_in_blob = context.container_client.list_blobs()
        for file in files_in_blob:
            context.container_client.delete_blobs(file.name)
    elif scenario.feature.name in ["Traffic sftp data read"]:
        context.storage_account_manager.disable_sftp()


def after_all(context):
    context.snowflake_connection.close()


def get_object_structure(context):
    database, schema, schema_object = context.interface_name.split(".")

    try:
        result = (
            context.snowflake_connection.cursor()
            .execute(
                """
                    SELECT COLUMN_NAME
                         , DATA_TYPE
                         , IS_NULLABLE
                    FROM IDENTIFIER(%(info_schema_table)s)
                    WHERE TABLE_SCHEMA = %(schema)s
                        AND TABLE_NAME = %(schema_object)s
                    ORDER BY ORDINAL_POSITION
                """,
                {
                    "info_schema_table": f"{database}.information_schema.columns",
                    "schema": schema,
                    "schema_object": schema_object,
                },
            )
            .fetch_pandas_all()
        )
    except DatabaseError as e:
        raise AssertionError(f"Column metadata of {database}.information_schema.columns cannot be accessed: {e}")

    return result


def get_expected_object_structure(object):
    return EXPECTED_STRUCTURES[object]


def _create_base_airflow_request(http_method, url_suffix, json=None):
    airflow_user = os.environ.get("AIRFLOW-TRAFFIC-USERNAME")
    airflow_password = os.environ.get("AIRFLOW-TRAFFIC-PASSWORD")
    airflow_url = os.environ.get("AIRFLOW-BASE-URL")
    headers = {"Content-Type": "application/json", "accept": "application/json"}
    basic = HTTPBasicAuth(airflow_user, airflow_password)

    furl_object = furl(airflow_url)
    furl_object /= f"/api/v1/{url_suffix}"

    return requests.request(http_method, url=furl_object.url, auth=basic, headers=headers, json=json)


def _create_snowflake_connection(database):
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE-TRAFFIC-USERNAME"),
        password=os.getenv("SNOWFLAKE-TRAFFIC-PASSWORD"),
        account=os.getenv("SNOWFLAKE-ACCOUNT"),
        database=database,
        warehouse=os.getenv("SNOWFLAKE-TRAFFIC-WAREHOUSE"),
    )


def _create_blob_storage_client():
    sas = os.getenv("TRAFFICDATA-STORAGE-ACCOUNT-SAS-TOKEN")
    traffic_container_url = os.getenv("TRAFFICDATA-CONTAINER-URL")

    container_url = furl(traffic_container_url)
    container_name = container_url.path.segments[0]

    blob_service_client = BlobServiceClient(container_url.origin, credential=sas)
    return blob_service_client.get_container_client(container=container_name)


def _create_sftp_client():
    sftp_url = os.getenv("SFTP-URL")
    sftp_username = os.getenv("SFTP-TRAFFIC-USERNAME")
    sftp_password = os.getenv("SFTP-TRAFFIC-PASSWORD")
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    sftp_connection = pysftp.Connection(
        host=sftp_url, port=22, username=sftp_username, password=sftp_password, cnopts=cnopts
    )
    return sftp_connection


def _truncate_tables(connection):
    connection.cursor().execute(
        """
        execute immediate
        $$
        begin
        truncate table staging.adjusted_preliminary;
        truncate table staging.adjusted_final;
        truncate table staging.unadjusted_current;
        truncate table staging.unadjusted_advanced;
        truncate table internal.adjusted_combined;
        truncate table internal.unadjusted_combined;
        end
        $$
        """
    )
