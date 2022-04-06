import boto3
from base_operator import Operator
from abc import abstractmethod
from job_logger import logger
from typing import Union
from job_setup import JobSetup
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from awsglue.dynamicframe import DynamicFrame
from load_operators import LoadOperator
from transform_operators import TransformOperator

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
  
class GlueCatalogExtractor(ExtractOperator):
    
    def __init__(self, 
                 job_setup: JobSetup, 
                 database: str, 
                 table_name: str,
                 optional_args: dict = {}):
        self.glue_context = job_setup.glue_context
        self.database = database
        self.table_name = table_name
        self.optional_args = optional_args
    
    def extract(self) -> DataFrame:
        """Creates spark dataframe from AWS glue catalog

        Returns:
            DataFrame
        """
        
        dynamic_df: DynamicFrame =self.glue_context.create_dynamic_frame_from_catalog(
            database=self.database,
            table_name= self.table_name,
            **self.optional_args
            )
        spark_df: DataFrame = dynamic_df.toDF()
        logger.info(f"Successfuly loaded {self.table_name} table from {self.database} database into a dataframe")
        return spark_df
           
class SparkExtractor(ExtractOperator):
    
    def __init__(self, 
                 job_setup: JobSetup, 
                 path: Union[str, list], 
                 format: str = "parquet",
                 schema: StructType= None,
                 optional_args: dict = {}):
        self.spark_session = job_setup.spark
        self.format = format
        self.path = path
        self.schema = schema
        self.optional_args = optional_args
        
    def extract(self) -> DataFrame:
        """Reads files into a spark dataframe

        Returns:
            DataFrame
        """
        logger.info(f"Reading {self.format} files from {self.path}")
        spark_df: DataFrame = self.spark_session.read.load(path = self.path, 
                                                           format = self.format,
                                                           schema = self.schema,
                                                           **self.optional_args)
        logger.info(f"Successfuly loaded files into a dataframe")
        return spark_df
    