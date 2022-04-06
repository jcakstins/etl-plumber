import boto3
from base_operator import Operator
from abc import abstractmethod
from job_logger import logger
from typing import Union
from job_setup import JobSetup
from pyspark.sql import DataFrame
from awsglue.dynamicframe import DynamicFrame
from awsglue.data_sink import DataSink

class LoadOperator(Operator):
    
    @abstractmethod
    def load(self):
        pass
    
    def apply(self, df: DataFrame) -> None:
        self.load(df)
        
    def __rrshift__(self, other):
        if isinstance(other, DataFrame):
            self.apply(other)
        raise TypeError(f"Can't perform pipeline execution,\
                on LoadOperator, expected input DataFrame but got {type(other)}")

class GlueSinkLoader(LoadOperator):
    
    def __init__(self, 
                 job_setup: JobSetup,
                 database: str, 
                 table_name: str,
                 path: str,
                 format: str = "parquet", 
                 partitions: Union[str, list, None] = None,
                 optional_args: dict = {}):
        self.glue_context = job_setup.glue_context
        self.database = database
        self.table_name = table_name
        self.path = path
        self.format = format
        self.optional_args = optional_args
        self.partitions = partitions
        
    def __create_sink(self) -> DataSink:
        
        sink = self.glue_context.getSink(
                    connection_type="s3", 
                    path=self.path,
                    partitionKeys=self.partitions,
                    **self.optional_args)
        self.__set_save_format(sink)
        sink.setCatalogInfo(catalogDatabase=self.database, 
                            catalogTableName=self.table_name)
        return sink
    
    def __set_save_format(self, sink: DataSink):
        if self.format == "parquet":
            sink.setFormat(self.format, useGlueParquetWriter=True)
        else:
            sink.setFormat(self.format)
        
    def load(self, df: DataFrame) -> None:
        dyf_output: DynamicFrame = DynamicFrame.fromDF(
            df, self.glue_context, "dyf_output")

        logger.info(f"Creating S3 DataSink Object")
        sink: DataSink = self.__create_sink()

        logger.info(
            f"Writing data to {self.path} using Glue Sink")
        sink.writeFrame(dyf_output)
    
class SparkLoader(LoadOperator):
    
    def __init__(self, 
                 table_name: str,
                 path: str,
                 format: str = "parquet", 
                 save_mode: str = "append", 
                 partitions: Union[str, list, None] = None,
                 crawler_name: str = None,
                 optional_args: dict = {}
                 ):
        self.table_name = table_name
        self.path = path
        self.format = format
        self.save_mode = save_mode
        self.partitions = partitions,
        self.crawler_name = crawler_name
        self.optional_args = optional_args
    
    def load(self, df: DataFrame) -> None:
        df.write.saveAsTable(name = self.table_name,
                                    format = self.format,
                                    mode = self.save_mode,
                                    partitionBy = self.partitions,
                                    path = self.path,
                                    **self.optional_args
                                    )
        if self.crawler_name:
            self.update_catalog()
        
    def update_catalog(self)-> None:
        logger.info(f"Starting crawler {self.crawler_name}")
        glue_client = boto3.client(service_name='glue', region_name='eu-west-1')
        glue_client.start_crawler(Name=self.crawler_name)
        