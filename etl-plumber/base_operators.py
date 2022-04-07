from abc import ABC, abstractmethod
from pyspark.sql.dataframe import DataFrame
from typing import Union, List
     
class Operator(ABC):
    
    @abstractmethod
    def apply(self):
        pass

class ExtractOperator(Operator):
    
    @abstractmethod
    def extract(self) -> DataFrame:
        pass
    
    def apply(self) -> DataFrame:
        df: DataFrame = self.extract()
        return df
    
    def __rshift__(self, other):
        if isinstance(other, TransformOperator) or isinstance(other, LoadOperator):
            df = other.apply(self.apply())
            return df
        raise TypeError(f"Can't perform pipeline execution,\
                expected TransformOperator or LoadOperator, but got {type(other)}")
        
class TransformOperator(Operator):
    
    @abstractmethod
    def transform(self, 
                  input_data: Union[DataFrame, List[DataFrame]]
                  ) -> DataFrame:
        pass
    
    def apply(self, input_data: Union[DataFrame, List[DataFrame]]) -> DataFrame:
        if isinstance(input_data, DataFrame):
            df: DataFrame = self.transform(input_data)
        elif isinstance(input_data, list):
            checked_input: list = self.__check_list_input_type_consistency(input_data)
            df: DataFrame = self.transform(checked_input)
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
    
class LoadOperator(Operator):
    
    @abstractmethod
    def load(self, df: DataFrame):
        pass
    
    def apply(self, df: DataFrame) -> None:
        self.load(df)
        
    def __rrshift__(self, other):
        if isinstance(other, DataFrame):
            return self.apply(other)
        raise TypeError(f"Can't perform pipeline execution,\
                on LoadOperator, expected input DataFrame but got {type(other)}")
