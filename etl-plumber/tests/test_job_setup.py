import pytest
import logging
import sys
from logging import Logger
from job_setup import SparkSetup, GlueSetup
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from unittest import mock

test_logger: Logger = logging.getLogger()
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(message)s')

test_args = ["JOB_NAME", "BUCKET"]
test_glue_args = {"JOB_NAME": "TEST", "BUCKET": "raw123"}

class TestSparkSetup():
    
    @pytest.mark.parametrize("conf,logger", [(None, None),(None, test_logger),
                                             (SparkConf(), test_logger)])
    def test_creates_spark_setup(self,conf, logger):
        job_setup = SparkSetup(conf, logger)
        isinstance(job_setup.spark, SparkSession)
        isinstance(job_setup.spark_context, SparkContext)
        
    def test_fails_to_create_spark_setup_with_wrong_config_type(self):
        with pytest.raises(TypeError):
            SparkSetup("Config")
            
class TestGlueSetup():
    
    @mock.patch('job_setup.GlueSetup._initialise_job')
    @mock.patch('job_setup.GlueSetup._get_glue_args')
    def test_creates_glue_setup(self,
                                mocked_get_glue_args: mock.Mock,
                                mocked__initialise_job: mock.Mock):
        from awsglue.context import GlueContext
        from awsglue.job import Job
        mocked_get_glue_args.return_value = test_glue_args
        mocked__initialise_job.return_value = None
        job_setup = GlueSetup(test_args[0], test_args)
        isinstance(job_setup.glue_context, GlueContext)
        isinstance(job_setup.job, Job)