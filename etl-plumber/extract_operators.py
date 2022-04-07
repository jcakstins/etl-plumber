from job_logger import logger
from typing import Union
from job_setup import JobSetup, GlueSetup
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from base_operator import ExtractOperator
  
class GlueCatalogExtractor(ExtractOperator):
    
    def __init__(self, 
                 job_setup: GlueSetup, 
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
        from awsglue.dynamicframe import DynamicFrame
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
    