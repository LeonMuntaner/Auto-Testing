import pytest
from datetime import datetime
from datetime import timedelta
from conftest import db_connect

def test_gender_values(db_connect):
    cursor = db_connect.cursor()
    cursor.execute("SELECT DISTINCT gender FROM staging.patient_leo;")
    
    genders = [row[0] for row in cursor.fetchall()]
    valid_genders = ['male', 'female']
    
    # Assert that the required values exist
    assert all(gender in valid_genders for gender in genders)

def test_managingorgID_values(db_connect):
    cursor = db_connect.cursor()
    cursor.execute("SELECT DISTINCT managingorganizationid FROM staging.patient_leo;")
    
    org_id = [row[0] for row in cursor.fetchall()]
    valid_org_ids = ['NCTBLD']
    
    assert all(manage_org_id in valid_org_ids for manage_org_id in org_id)

@pytest.mark.parametrize("table_name", ['staging.patient_leo', 'staging.specimen_leo', 'staging.microscopy_leo', 'staging.culture_leo', 'staging.condition_leo'])
def test_no_duplicates_in_identifier_col(db_connect, table_name):
    cursor = db_connect.cursor()
    cursor.execute(f"SELECT identifier, COUNT(*) FROM {table_name} GROUP BY identifier HAVING COUNT(*) > 1;")
    duplicated_values = cursor.fetchall()
    
    # Assert that there are no duplicates in the unique column
    assert not duplicated_values, f"Duplicate values found in identifier of {table_name}: {duplicated_values}"

@pytest.mark.parametrize("table_name", ['staging.specimen_leo', 'staging.microscopy_leo', 'staging.culture_leo', 'staging.condition_leo'])
def test_no_null_values_in_registrationdate_col(db_connect,table_name):
    cursor = db_connect.cursor()
    cursor.execute(f"SELECT registrationdate FROM {table_name} WHERE registrationdate IS NULL;")
    null_values = cursor.fetchall()
    
    # Assert that there are no null values in the unique column
    assert not null_values, f"Null values found in registrationdate of {table_name}: {null_values}"

@pytest.mark.parametrize("table_name, column_name, valid_values", [
    ("staging.specimen_leo", "bodysite", {"biopsy", "sputum", "other", "surgeryCaseousMasses", "surgeryCavityInternalWall", "surgeryCavityExternalWall", "surgeryModulu", "surgeryHealthy tissue",
                                        "bronchialLavage", "asciticFluid", "blood", "urine", "pleuralFluid", "cerebrospinalFluid", "paraffinEmbeddedTissue"}),
    ("staging.microscopy_leo", "value", {"-0.09", "1+", "2+", "3+", "4+", "saliva", "unknownData", "negative", "notDone"}),
    ("staging.microscopy_leo", "microscopytype", {"zn", "florescence", "notSpecified"}),
    ("staging.culture_leo", "value", {"singleColony", "1+", "2+", "3+", "positive", "negative", "unknownData", "unfinishedResult", "notDone", "contamination", "mott"}),
    ("staging.culture_leo", "culturetype", {"liquid", "solid", "notSpecified"})
])
def test_valid_values(db_connect, table_name, column_name, valid_values):
    cursor = db_connect.cursor()
    
    # Properly format the valid values for the query
    formatted_values = "', '".join(valid_values)
    
    # Use the except clause to find unexpected values
    cursor.execute(f"SELECT {column_name} FROM {table_name} WHERE {column_name} NOT IN ('{formatted_values}');")
    
    unexpected_values = {row[0] for row in cursor.fetchall()}
    
    # Assert that the required values exist
    assert not unexpected_values, f"Unexpected values found in {column_name} column of {table_name}: {unexpected_values}"

@pytest.mark.parametrize("table_name, column_name, expected_data_type", [
    ("specimen_leo", "bodysite", "character varying"),
    ("microscopy_leo", "microscopytype", "character varying"),
    ("culture_leo", "value", "character varying")
])
def test_col_data_type(db_connect, table_name, column_name, expected_data_type):
    # Check that the specified column has the expected data type
    cursor = db_connect.cursor()
    
    cursor.execute(f"SELECT data_type FROM information_schema.columns WHERE table_name = '{table_name}' AND column_name = '{column_name}' AND table_schema = 'staging';")
    result = cursor.fetchone()
    
    assert result is not None, f"Column {column_name} not found in {table_name}"
    
    actual_data_type = result[0]
    
    # Assert that the required values exist
    assert actual_data_type == expected_data_type, f"Unexpected data type found in {column_name} column of {table_name}. Expected: {expected_data_type}, Actual: {actual_data_type}"

@pytest.mark.parametrize("table_name, registration_column, issued_column", [
    ("staging.culture_leo", "registrationdate", "issued")
])
def test_dates_relationship(db_connect, table_name, registration_column, issued_column):
    # Check that the 'issued' dates are at least 3 days after the 'registrationdate' dates
    cursor = db_connect.cursor()
    
    cursor.execute(f"SELECT * FROM {table_name} WHERE CAST({issued_column} AS TIMESTAMP) < CAST({registration_column} AS TIMESTAMP) + INTERVAL '3 days';")
    mismatched_dates = cursor.fetchall()
    
    # Properly format the mismatched dates for the error message
    formatted_mismatches = [f"{registration_column}: {row[0]}, {issued_column}: {row[1]}" for row in mismatched_dates]
    
    # Assert that the required values exist
    assert not mismatched_dates, f"Dates mismatched in {issued_column} column for rows where {issued_column} is not at least 3 days after {registration_column}:\n{', '.join(formatted_mismatches)}"

# TODO: Add a test that compares the row count from the source and destination tables