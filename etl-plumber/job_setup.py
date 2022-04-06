import sys
from job_logger import logger
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

class JobSetup(object):

    def __init__(self, 
                 job_name_arg: str, 
                 glue_arg_list: list
                 ):
        self._glue_args: dict = self._get_glue_args(args=glue_arg_list)
        self._spark_context: SparkContext = SparkContext.getOrCreate()
        self._glue_context: GlueContext = GlueContext(self._spark_context)
        self._spark: SparkSession = self._glue_context.spark_session
        self._job: Job = Job(glue_context=self.glue_context)
        self._job_timestamp: datetime = datetime.utcnow()
        self._job.init(self._glue_args[job_name_arg], 
                       self._glue_args)
        
    @property
    def spark_context(self) -> SparkContext:
        return self._spark_context
    
    @property
    def glue_context(self) -> GlueContext:
        return self._glue_context
    
    @property
    def spark(self) -> SparkSession:
        return self._spark
    
    @property
    def job(self) -> Job:
        return self._job
    
    @property
    def glue_args(self) -> dict:
        return self._glue_args
    
    @property
    def job_timestamp(self) -> datetime:
        return self._job_timestamp
    
    def _get_glue_args(self, args: list) -> dict:
        logger.info(f"Reading Glue Args {args}")
        glue_args: dict =  getResolvedOptions(args=sys.argv, options=args)
        logger.info(f"Glue Parsed Args {glue_args}")
        return glue_args
                
    def set_spark_runtime_config(self, spark_config: dict) -> None:
        if spark_config:
            self.__set_runtime_properties(spark_config)
            
    def __set_runtime_properties(self, spark_config: dict):
        for spark_conf_key, spark_conf_value in spark_config.items():
            self.__set_runtime_property(spark_conf_key, spark_conf_value)
                
    def __set_runtime_property(self, conf_key: str, conf_value: str):
        try:
            logger.info(f"Setting {conf_key} to {conf_value}")
            self._spark.conf.set(conf_key, conf_value)
        except Exception as err:
            logger.error(f"Failed to set {conf_key} to {conf_value}\n{err}")