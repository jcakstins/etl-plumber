import sys
from abc import ABC, abstractmethod
from logging import Logger
from datetime import datetime
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

class JobSetup(ABC):
    
    @property
    @abstractmethod
    def logger(self) -> Logger:
        pass
    
    @property
    @abstractmethod
    def spark_context(self) -> SparkContext:
        pass
    
    @property
    def spark(self) -> SparkSession:
        pass
    
    @property
    def job_timestamp(self) -> datetime:
        pass
    
    def set_spark_runtime_config(self, spark_config: dict) -> None:
        if spark_config:
            self.__set_runtime_properties(spark_config)
            
    def __set_runtime_properties(self, spark_config: dict):
        for spark_conf_key, spark_conf_value in spark_config.items():
            self.__set_runtime_property(spark_conf_key, spark_conf_value)
                
    def __set_runtime_property(self, conf_key: str, conf_value: str):
        try:
            self.logger.info(f"Setting {conf_key} to {conf_value}")
            self.spark.conf.set(conf_key, conf_value)
        except Exception as err:
            self.logger.error(f"Failed to set {conf_key} to {conf_value}\n{err}")

class SparkSetup(JobSetup):
    
    def __init__(self,
                 app_name: str = None,
                 spark_conf: SparkConf = None,
                 logger: Logger = None
                 ):
        self._logger = logger
        self._spark_context: SparkContext = SparkContext.getOrCreate()
        self._spark: SparkSession = SparkSession.builder.\
            appName(app_name)\
                .config(conf=spark_conf)\
                    .getOrCreate()
        self._job_timestamp: datetime = datetime.utcnow()
        
    @property
    def logger(self):
        return self._logger
    
    @property
    def spark_context(self) -> SparkContext:
        return self._spark_context
    
    @property
    def spark(self) -> SparkSession:
        return self._spark
    
    @property
    def job_timestamp(self) -> datetime:
        return self._job_timestamp

class GlueSetup(object):

    def __init__(self, 
                 job_name_arg: str, 
                 glue_arg_list: list,
                 logger: Logger = None
                 ):
        from awsglue.context import GlueContext
        from awsglue.job import Job
        self._logger = logger
        self._glue_args: dict = self._get_glue_args(args=glue_arg_list)
        self._spark_context: SparkContext = SparkContext.getOrCreate()
        self._glue_context: GlueContext = GlueContext(self._spark_context)
        self._spark: SparkSession = self._glue_context.spark_session
        self._job: Job = Job(glue_context=self.glue_context)
        self._job_timestamp: datetime = datetime.utcnow()
        self._job.init(self._glue_args[job_name_arg], 
                       self._glue_args)
        
    @property
    def logger(self):
        return self._logger
    
    @property
    def spark_context(self) -> SparkContext:
        return self._spark_context
    
    @property
    def glue_context(self) -> "awsglue.context.GlueContext":
        return self._glue_context
    
    @property
    def spark(self) -> SparkSession:
        return self._spark
    
    @property
    def job(self) -> "awsglue.job.Job":
        return self._job
    
    @property
    def glue_args(self) -> dict:
        return self._glue_args
    
    @property
    def job_timestamp(self) -> datetime:
        return self._job_timestamp
    
    def _get_glue_args(self, args: list) -> dict:
        from awsglue.utils import getResolvedOptions

        glue_args: dict =  getResolvedOptions(args=sys.argv, options=args)
        if self.logger:
            self.logger.info(f"Glue Parsed Args {glue_args}")
        return glue_args
                