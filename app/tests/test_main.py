import sys
sys.path.append('../../utils')
from utils.ETL import create_frame_from_catalog, normalize_column_names, repartition_frames, write_dynamic_frame, deduplicate_rows
import pytest
from unittest.mock import MagicMock, patch
#from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import DataFrame

# Mock GlueContext and SparkContext
glueContext = MagicMock()
spark = MagicMock()
glueContext.spark_session.return_value = spark

# Mock DynamicFrame and DataFrame
mock_dyf = MagicMock(spec=DynamicFrame)
mock_df = MagicMock(spec=DataFrame)

# Mock create_dynamic_frame.from_catalog method
def mock_create_dynamic_frame_from_catalog(database, table_name, transformation_ctx):
    return mock_dyf

# Mock fromDF method
def mock_fromDF(df, gluectx, name):
    return mock_dyf

# Mock dropDuplicates method
def mock_drop_duplicates():
    return mock_df

# Pytest fixtures
@pytest.fixture
def mock_glue_context():
    with patch('awsglue.context.GlueContext', return_value=glueContext):
        yield glueContext

@pytest.fixture
def mock_spark_context():
    with patch('pyspark.context.SparkContext', return_value=spark):
        yield spark

@pytest.fixture
def mock_create_dynamic_frame():
    with patch('awsglue.context.GlueContext.create_dynamic_frame.from_catalog', side_effect=mock_create_dynamic_frame_from_catalog):
        yield mock_dyf

@pytest.fixture
def mock_dynamic_frame():
    with patch('awsglue.dynamicframe.DynamicFrame', return_value=mock_dyf):
        yield mock_dyf

@pytest.fixture
def mock_dataframe():
    with patch('pyspark.sql.DataFrame', return_value=mock_df):
        yield mock_df

@pytest.fixture
def mock_drop_duplicates_method():
    with patch('pyspark.sql.DataFrame.dropDuplicates', side_effect=mock_drop_duplicates):
        yield mock_drop_duplicates

# Pytests
def test_create_frame_from_catalog(mock_create_dynamic_frame, mock_glue_context):
    parameters = {
        "prefix": "cmdb_ci_aws_accounts",
        "database": "db_sor_01",
        "table_name": "awc_sor",
        "mappings": [("year", "year"), ("month", "month"), ("day", "day")]
    }
    fields_to_keep = ["year", "month", "day"]

    dyf = create_frame_from_catalog(parameters, fields_to_keep, glueContext)

    assert dyf == mock_dyf

def test_normalize_column_names(mock_dynamic_frame):
    parameters = {
        "prefix": "cmdb_ci_aws_accounts",
        "mappings": [("year", "year"), ("month", "month"), ("day", "day")]
    }
    fields_to_keep = ["year", "month", "day"]

    normalized_dyf = normalize_column_names(mock_dyf, parameters, fields_to_keep)

    assert normalized_dyf == mock_dyf

# Similarly, you can write tests for other functions like repartition_frames, write_dynamic_frame, deduplicate_rows, etc.
