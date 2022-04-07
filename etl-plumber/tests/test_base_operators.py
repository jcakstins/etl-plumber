import pytest
from base_operators import ExtractOperator, TransformOperator, LoadOperator
from pyspark.sql import DataFrame

class TestRightShift():
    
    def test_extract_transform(self, 
                               dummy_extractor: ExtractOperator, 
                               dummy_transformer: TransformOperator):
        dag = dummy_extractor >> dummy_transformer
        assert isinstance(dag, DataFrame)