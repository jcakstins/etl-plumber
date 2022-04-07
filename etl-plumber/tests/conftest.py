import pytest
from base_operators import ExtractOperator, TransformOperator, LoadOperator
from typing import Union, List
from job_setup import SparkSetup
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

class DummyExtractor(ExtractOperator):

    def extract(self)-> DataFrame:
        spark = SparkSession.builder.getOrCreate()
        dummy_cols = ["col1","col2","col3"]
        dummy_data = [[1,2,3], [2,3,4], [3,4,5]]
        df = spark.createDataFrame(data=dummy_data, schema = dummy_cols)
        return df
    
class DummyTransformer(TransformOperator):
    
    def transform(self, input_data: Union[DataFrame, List[DataFrame]]) -> DataFrame:
        if isinstance(input_data, list):
            return input_data[0]
        return input_data

class DummyLoader(LoadOperator):
    
    def load(self, df: DataFrame) -> None:
        df.show()

@pytest.fixture
def spark_setup()-> SparkSetup:
    out = SparkSetup()
    return out

@pytest.fixture
def dummy_extractor():
    out = DummyExtractor()
    return out

@pytest.fixture
def dummy_transformer():
    out = DummyTransformer()
    return out

@pytest.fixture
def dummy_loader():
    out = DummyLoader()
    return out