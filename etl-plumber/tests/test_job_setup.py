import pytest
import logging
from logging import Logger
import sys
from job_setup import SparkSetup
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf

test_logger: Logger = logging.getLogger()
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(message)s')

@pytest.fixture
def sparkconf():
    conf = SparkConf()
    return conf

class TestSparkSetup():
    
    @pytest.mark.parametrize("conf,logger", [(None, None),(None, test_logger),
                                             (SparkConf(), test_logger)])
    def test_creates_spark_setup(self,conf, logger):
        job_setup = SparkSetup(conf, logger)
        isinstance(job_setup, SparkSetup)
        isinstance(job_setup.spark, SparkSession)
        isinstance(job_setup.spark_context, SparkContext)
        
    def test_fails_to_create_spark_setup_with_wrong_config_type(self):
        with pytest.raises(TypeError):
            SparkSetup("Config")
        