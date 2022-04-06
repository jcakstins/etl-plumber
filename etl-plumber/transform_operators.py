from base_operator import Operator
from abc import abstractmethod
from types import FunctionType
from job_logger import logger
from typing import Union, List, Dict
from job_setup import JobSetup
from pyspark.sql import DataFrame
from extract_operators import ExtractOperator

class TransformOperator(Operator):
    
    def __init__(self, job_setup: JobSetup):
        self.job_setup = job_setup
    
    @abstractmethod
    def transform(self, 
                  input_data: Union[DataFrame, List[DataFrame], Dict[str:DataFrame]]
                  ) -> DataFrame:
        pass
    
    def apply(self, input_data: Union[DataFrame, List[DataFrame], Dict[str:DataFrame]]) -> DataFrame:
        if isinstance(input_data, DataFrame):
            df: DataFrame = self.transform(input_data)
        elif isinstance(input_data, list):
            checked_input: list = self.__check_list_input_type_consistency(input_data)
            df: DataFrame = self.transform(checked_input)
        #TODO: what if error occurs in transform, or returns None, or returns empty
        return df
    
    def __rrshift__(self, other):
        check: bool = isinstance(other, DataFrame) or \
            isinstance(other, list)
        if check:
            return self.apply(other)
        raise TypeError(f"Can't perform pipeline execution,\
                on TransformOperator, expected input DataFrame but got {type(other)}")
        
    def __check_list_input_type_consistency(self, l: list) -> list:
        if self.__check_all_dataframes(l):
            return l
        elif self.__check_all_extractors(l):
            out = [*map(lambda x: x.apply(), l)]
            return out
        l_types = [*map(lambda x: type(x), l)]
        raise TypeError(f"All members of input list should contain\
            pyspark.sql.DataFrame type, instead got {l_types}")
        
    def __check_all_dataframes(self, l: list) -> bool:
        check = all(
            map(lambda x: isinstance(x, DataFrame), l)
            )
        return check
    
    def __check_all_extractors(self, l: list) -> bool:
        check = all(
            map(lambda x: isinstance(x, ExtractOperator), l)
            )
        return check

class FunctionTransformer(TransformOperator):
    
    def __init__(self, 
                 job_setup: JobSetup,
                 func: FunctionType,
                 func_params: dict):
        super().__init__(job_setup)
        self.func = func
        self.func_params = func_params
        
    def transform(self, 
                  input_data: Union[DataFrame, List[DataFrame]]
                  ) -> DataFrame:
        df: DataFrame = self.func(input_data, **self.func_params)
        return df
    