import pytest
from base_operators import ExtractOperator, TransformOperator, LoadOperator
from pyspark.sql import DataFrame

class TestRightShift():
    
    def test_extract_transform(self, 
                               dummy_extractor: ExtractOperator, 
                               dummy_transformer: TransformOperator):
        dag_single = dummy_extractor >> dummy_transformer
        dag_multi = [dummy_extractor, dummy_extractor] >> dummy_transformer
        assert isinstance(dag_single, DataFrame)
        assert isinstance(dag_multi, DataFrame)