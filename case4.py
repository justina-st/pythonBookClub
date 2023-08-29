import pytest
import os

from pandas.testing import assert_frame_equal


@pytest.mark.database
def test_procedure_proc_load_seats_api_training_data(helper):
    helper.truncate_table("SEATS_DATA.SEATS_API_TRAINING_DATA")
    sub_folder = "test_data_for_proc_load_seats_api_training_data"
    helper.insert_into_table(sub_folder, "insert_into_seats_data_seats_api_training_data_procedure_test.sql")

    helper.mock_interface_table(
        source_database=os.getenv("BeakerDb"),
        source_schema="DBO",
        source_table="FLIGHTSUMMARY",
        target_schema="SEATS_DATA",
    )
    helper.insert_into_table(sub_folder, "insert_into_seats_data_flightsummary.sql")

    helper.mock_interface_table(
        source_database=os.getenv("ReferenceDb"),
        source_schema="INTERFACE",
        source_table="VIEW_CHAVIATION_CARRIER_CODES",
        target_schema="SEATS_DATA",
    )
    helper.insert_into_table(sub_folder, "insert_into_seats_data_view_chaviation_carrier_codes.sql")

    helper.mock_interface_table(
        source_database=os.getenv("ReferenceDb"),
        source_schema="INTERFACE",
        source_table="EQUIPMENT_MAIN_EFFECTIVITY",
        target_schema="SEATS_DATA",
    )
    helper.insert_into_table(sub_folder, "insert_into_seats_data_equipment_main_effectivity.sql")

    helper.mock_interface_table(
        source_database=os.getenv("ReferenceDb"),
        source_schema="INTERFACE",
        source_table="HISTORICAL_FLEET_DATA",
        target_schema="SEATS_DATA",
    )
    helper.insert_into_table(sub_folder, "insert_into_seats_data_historical_fleet_data.sql")

    helper.mock_interface_table(
        source_database=os.getenv("SchedulesDb"),
        source_schema="INTERFACE",
        source_table="VIEW_SCHEDULE_INSTANCES",
        target_schema="SEATS_DATA",
    )
    helper.insert_into_table(sub_folder, "insert_into_seats_data_view_schedule_instances.sql")

    helper.create_expected_table("SEATS_DATA.SEATS_API_TRAINING_DATA")
    helper.insert_into_table(sub_folder, "insert_into_seats_data_seats_api_training_data_expected.sql")

    helper.execute_procedure(
        procedure_name="SEATS_DATA.PROC_LOAD_SEATS_API_TRAINING_DATA",
        arguments=(
            "SEATS_DATA.FLIGHTSUMMARY",
            "SEATS_DATA.VIEW_SCHEDULE_INSTANCES",
            "SEATS_DATA.EQUIPMENT_MAIN_EFFECTIVITY",
            "SEATS_DATA.HISTORICAL_FLEET_DATA",
            "SEATS_DATA.VIEW_CHAVIATION_CARRIER_CODES",
            "2022-01-03",
        ),
    )

    expected_table = helper.select_from_table("SEATS_DATA.SEATS_API_TRAINING_DATA_EXPECTED").apply(
        lambda x: x.sort_values().values
    )
    actual_table = helper.select_from_table("SEATS_DATA.SEATS_API_TRAINING_DATA").apply(
        lambda x: x.sort_values().values
    )

    helper.drop_table("SEATS_DATA.FLIGHTSUMMARY")
    helper.drop_table("SEATS_DATA.VIEW_CHAVIATION_CARRIER_CODES")
    helper.drop_table("SEATS_DATA.EQUIPMENT_MAIN_EFFECTIVITY")
    helper.drop_table("SEATS_DATA.HISTORICAL_FLEET_DATA")
    helper.drop_table("SEATS_DATA.VIEW_SCHEDULE_INSTANCES")
    helper.drop_table("SEATS_DATA.SEATS_API_TRAINING_DATA_EXPECTED")

    assert_frame_equal(expected_table, actual_table)


def test_procedure_proc_factor_model_training_data(helper):

    sub_folder = "test_data_for_proc_factor_model_training_data"

    helper.insert_into_table(sub_folder, "insert_into_stage_actual_fuel_burn.sql")
    helper.insert_into_table(sub_folder, "create_table_factor_model_training_data_expected.sql")

    actual_table = helper.get_procedure_results(
        procedure_name="STAGE.PROC_FACTOR_MODEL_TRAINING_DATA").apply(
        lambda x: x.sort_values().values
    )

    expected_table = helper.select_from_table("STAGE.FACTOR_MODEL_TRAINING_DATA_EXPECTED").apply(
        lambda x: x.sort_values().values
    )

    assert_frame_equal(expected_table, actual_table)

    helper.drop_table("STAGE.FACTOR_MODEL_TRAINING_DATA_EXPECTED")


def test_procedure_proc_schedules_predictions_dataset(helper):

    sub_folder = "test_data_for_proc_schedules_predictions_dataset"

    helper.insert_into_table(sub_folder, "insert_into_stage_data_actual_fuel_burn.sql")

    helper.mock_interface_table(
        source_database=os.getenv("ReferenceDb"),
        source_schema="INTERFACE",
        source_table="LOCATIONS_MAIN_EFFECTIVITY",
        target_schema="INTERFACE",
    )
    helper.insert_into_table(sub_folder, "insert_into_interface_locations_main_effectivity.sql")

    helper.insert_into_table(sub_folder, "insert_into_stage_estimated_status.sql")
    helper.insert_into_table(sub_folder, "insert_into_stage_estimated_schedules.sql")
    helper.insert_into_table(sub_folder, "create_table_schedules_predictions_dataset_expected.sql")

    actual_table = helper.get_procedure_results(
        procedure_name="STAGE.PROC_SCHEDULES_PREDICTIONS_DATASET",
        arguments=(
            "INTERFACE.LOCATIONS_MAIN_EFFECTIVITY",
            "2023-01-01",
            "2023-09-01",
        )
    ).apply(
        lambda x: x.sort_values().values
    )

    expected_table = helper.select_from_table("STAGE.SCHEDULES_PREDICTIONS_DATASET_EXPECTED").apply(
        lambda x: x.sort_values().values
    )

    assert_frame_equal(expected_table, actual_table)

    helper.drop_table("STAGE.SCHEDULES_PREDICTIONS_DATASET_EXPECTED")


def test_procedure_proc_status_predictions_dataset(helper):

    sub_folder = "test_data_for_proc_status_predictions_dataset"

    helper.insert_into_table(sub_folder, "insert_into_stage_data_actual_fuel_burn.sql")

    helper.mock_interface_table(
        source_database=os.getenv("ReferenceDb"),
        source_schema="INTERFACE",
        source_table="LOCATIONS_MAIN_EFFECTIVITY",
        target_schema="INTERFACE",
    )
    helper.insert_into_table(sub_folder, "insert_into_interface_locations_main_effectivity.sql")

    helper.insert_into_table(sub_folder, "insert_into_stage_estimated_status.sql")
    helper.insert_into_table(sub_folder, "create_table_status_predictions_dataset_expected.sql")

    actual_table = helper.get_procedure_results(
        procedure_name="STAGE.PROC_STATUS_PREDICTIONS_DATASET",
        arguments=("INTERFACE.LOCATIONS_MAIN_EFFECTIVITY",)
    ).apply(
        lambda x: x.sort_values().values
    )

    expected_table = helper.select_from_table("STAGE.STATUS_PREDICTIONS_DATASET_EXPECTED").apply(
        lambda x: x.sort_values().values
    )

    assert_frame_equal(expected_table, actual_table)

    helper.drop_table("STAGE.STATUS_PREDICTIONS_DATASET_EXPECTED")


def test_procedure_proc_output_active_icao_codes(helper):

    sub_folder = "test_data_for_proc_output_active_icao_codes"

    helper.mock_interface_table(
        source_database=os.getenv("ReferenceDb"),
        source_schema="INTERFACE",
        source_table="EQUIPMENT_MAIN_EFFECTIVITY",
        target_schema="INTERFACE",
    )
    helper.insert_into_table(sub_folder, "insert_into_interface_equipment_main_effectivity.sql")

    helper.insert_into_table(sub_folder, "create_table_active_icao_codes_expected.sql")

    actual_table = helper.get_procedure_results(
        procedure_name="STAGE.PROC_OUTPUT_ACTIVE_ICAO_CODES",
        arguments=("INTERFACE.EQUIPMENT_MAIN_EFFECTIVITY",)
    ).apply(
        lambda x: x.sort_values().values
    )

    expected_table = helper.select_from_table("STAGE.ACTIVE_ICAO_CODES_EXPECTED").apply(
        lambda x: x.sort_values().values
    )

    assert_frame_equal(expected_table, actual_table)

    helper.drop_table("STAGE.ACTIVE_ICAO_CODES_EXPECTED")


def test_procedure_proc_output_active_ch_engines(helper):

    sub_folder = "test_data_for_proc_output_active_ch_engines"

    helper.mock_interface_table(
        source_database=os.getenv("ReferenceDb"),
        source_schema="INTERFACE",
        source_table="EQUIPMENT_MAIN_EFFECTIVITY",
        target_schema="INTERFACE",
    )
    helper.insert_into_table(sub_folder, "insert_into_interface_equipment_main_effectivity.sql")

    helper.mock_interface_table(
        source_database=os.getenv("ReferenceDb"),
        source_schema="INTERFACE",
        source_table="HISTORICAL_FLEET_DATA",
        target_schema="INTERFACE",
    )
    helper.insert_into_table(sub_folder, "insert_into_interface_historical_fleet_data.sql")

    helper.insert_into_table(sub_folder, "insert_into_seats_data_aircraft_av_codes.sql")
    helper.insert_into_table(sub_folder, "create_table_active_ch_engines_expected.sql")

    actual_table = helper.get_procedure_results(
        procedure_name="STAGE.PROC_OUTPUT_ACTIVE_CH_ENGINES",
        arguments=(
          "INTERFACE.EQUIPMENT_MAIN_EFFECTIVITY",
          "INTERFACE.HISTORICAL_FLEET_DATA"
        )
    ).apply(
        lambda x: x.sort_values().values
    )

    expected_table = helper.select_from_table("STAGE.ACTIVE_CH_ENGINES_EXPECTED").apply(
        lambda x: x.sort_values().values
    )

    assert_frame_equal(expected_table, actual_table)

    helper.drop_table("STAGE.ACTIVE_CH_ENGINES_EXPECTED")


def test_procedure_proc_insert_schedules_outputs(helper):

    sub_folder = "test_data_for_proc_insert_schedules_outputs"

    helper.insert_into_table(sub_folder, "insert_into_stage_estimated_schedules.sql")
    helper.insert_into_table(sub_folder, "insert_into_stage_schedules_adjustments.sql")

    helper.create_table("STAGE.SCHEDULES_OUTPUTS_EXPECTED", "INTERFACE.ESTIMATED_SCHEDULES")
    helper.insert_into_table(sub_folder, "insert_into_stage_schedules_outputs_expected.sql")

    procedure_output = helper.get_procedure_results(
        procedure_name="INTERFACE.PROC_INSERT_SCHEDULES_OUTPUTS",
        arguments=(2,)
    )

    actual_count = procedure_output.iloc[0]["PROC_INSERT_SCHEDULES_OUTPUTS"]

    actual_table = helper.select_from_table("INTERFACE.ESTIMATED_SCHEDULES").apply(
        lambda x: x.sort_values().values
    )

    expected_table = helper.select_from_table("STAGE.SCHEDULES_OUTPUTS_EXPECTED").apply(
        lambda x: x.sort_values().values
    )

    assert actual_count == 3
    assert_frame_equal(expected_table, actual_table)

    helper.drop_table("STAGE.SCHEDULES_OUTPUTS_EXPECTED")


def test_procedure_proc_insert_status_outputs(helper):

    sub_folder = "test_data_for_proc_insert_status_outputs"

    helper.insert_into_table(sub_folder, "insert_into_stage_estimated_status.sql")
    helper.insert_into_table(sub_folder, "insert_into_stage_status_adjustments.sql")

    helper.create_table("STAGE.STATUS_OUTPUTS_EXPECTED", "INTERFACE.ESTIMATED_STATUS")
    helper.insert_into_table(sub_folder, "insert_into_stage_status_outputs_expected.sql")

    procedure_output = helper.get_procedure_results(
        procedure_name="INTERFACE.PROC_INSERT_STATUS_OUTPUTS",
        arguments=(2,)
    )

    actual_count = procedure_output.iloc[0]["PROC_INSERT_STATUS_OUTPUTS"]

    actual_table = helper.select_from_table("INTERFACE.ESTIMATED_STATUS").apply(
        lambda x: x.sort_values().values
    )

    expected_table = helper.select_from_table("STAGE.STATUS_OUTPUTS_EXPECTED").apply(
        lambda x: x.sort_values().values
    )

    assert actual_count == 4
    assert_frame_equal(expected_table, actual_table)

    helper.drop_table("STAGE.STATUS_OUTPUTS_EXPECTED")
