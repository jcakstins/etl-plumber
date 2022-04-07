from ast import Load
import pytest
from base_operators import ExtractOperator, TransformOperator, LoadOperator
from pyspark.sql.dataframe import DataFrame

class TestRightShift():
    
    def test_extract_transform(self, 
                               dummy_extractor: ExtractOperator, 
                               dummy_transformer: TransformOperator):
        dag_single = dummy_extractor >> dummy_transformer
        dag_multi_extr = [dummy_extractor, dummy_extractor] >> dummy_transformer
        dag_multi_tran = dummy_extractor >> dummy_transformer >> dummy_transformer
        dag_complex = [dummy_extractor >> dummy_transformer,
                       dummy_extractor >> dummy_transformer] >> dummy_transformer
        assert isinstance(dag_single, DataFrame)
        assert isinstance(dag_multi_extr, DataFrame)
        assert isinstance(dag_multi_tran, DataFrame)
        assert isinstance(dag_complex, DataFrame)
        
        with pytest.raises(TypeError):
            dummy_extractor >> dummy_extractor, "Extractor can only send data to transform or load operators"
        
    def test_extract_load(self, 
                          dummy_extractor: ExtractOperator, 
                          dummy_loader: LoadOperator):
        dag = dummy_extractor >> dummy_loader
        assert dag is None
        
    def test_extract_transform_load(self, 
                               dummy_extractor: ExtractOperator, 
                               dummy_transformer: TransformOperator,
                               dummy_loader: LoadOperator):
        dag = dummy_extractor >> dummy_transformer >> dummy_loader
        assert dag is None, "LoadOperator.apply() should always return None"