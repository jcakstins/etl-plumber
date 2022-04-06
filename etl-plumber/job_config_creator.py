import json
from abc import ABC, abstractmethod
from typing import Any
from job_logger import logger
from datetime import datetime
import boto3

class ConfigCreator(ABC):
    
    @property
    @abstractmethod
    def config(self):
        pass
    
    @abstractmethod
    def get_job_config(self):
        pass
    
    @staticmethod
    def create_crawler_name(subtenant: str, target_table: str):
        crawler_name: str = f"{subtenant.lower()}-{target_table.lower()}-crawler"
        return crawler_name
    
class JsonConfigCreator(ConfigCreator):
    
    s3 = boto3.client('s3')
    
    def __init__(self, bucket: str, key: str):
        self.bucket = bucket
        self.key = key
        self._config: dict = self.get_job_config()
        
    @property
    def config(self):
        return self._config
        
    def get_job_config(self) -> dict:
        logger.info(
            f"Reading configuration file from {self.bucket}/{self.key}")
        object_body: str = self.s3.get_object(
            Bucket=self.bucket,
            Key=self.key)['Body'].read().decode('utf-8')
        config: Any = json.loads(object_body)
        logger.info(f"File contents: {object_body}")
        return config
    