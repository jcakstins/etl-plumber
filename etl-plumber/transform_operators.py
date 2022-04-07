from base_operators import TransformOperator
from types import FunctionType
from typing import Union, List
from job_setup import JobSetup
from pyspark.sql import DataFrame

class FunctionTransformer(TransformOperator):
    
    def __init__(self,
                 func: FunctionType,
                 func_params: dict):
        super().__init__()
        self.func = func
        self.func_params = func_params
        
    def transform(self, 
                  input_data: Union[DataFrame, List[DataFrame]]
                  ) -> DataFrame:
        df: DataFrame = self.func(input_data, **self.func_params)
        return df
    