from abc import ABC, abstractmethod
from job_logger import logger
from typing import Any
from datetime import datetime
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

class DynamoDBService(ABC):
    
    @property
    @abstractmethod
    def ddb_table(self):
        pass
    
    @property
    @abstractmethod
    def key(self):
        pass
    
    @property
    @abstractmethod
    def item_id(self):
        pass
    
    def get_item(self):
        logger.info(f"Getting item for id={self.item_id}")
        try:
            response = self.ddb_table.get_item(Key=self.key)
        except ClientError as err:
            print(err.response['Error']['Message'])
        else:
            item: dict = response['Item']
            logger.info(f"Fetched item: {item}")
            return item
    
    def put_item(self, item: dict) -> None:
        try:
            logger.info(f"Creating new item: {item}")
            self.ddb_table.put_item(
                Item=item
                )
        except ClientError as err:
            logger.info(f"Failed to create a new item")
            logger.info(err.response['Error']['Message'])
    
    def update_item(self, attributes_to_update: dict) -> None:
        for attribute_name, attribute_value in attributes_to_update.items():
            self.__update_attribute(attribute_name, attribute_value)
            
    def __update_attribute(self, attribute_name: str, attribute_value: Any) -> None:
        try:
            self.ddb_table.update_item(
                Key=self.key,
                UpdateExpression=f"set {attribute_name}=:v",
                ExpressionAttributeValues={
                    ':v': attribute_value
                },
                ReturnValues="UPDATED_NEW"
            )
        except ClientError as err:
            logger.info(f"Failed to update a record, key: {self.key} with {attribute_name}: {attribute_value}")
            logger.info(err.response['Error']['Message'])
    
    def delete_item(self) -> None:
        try:
            self.ddb_table.delete_item(
                Key=self.key
            )
        except ClientError as err:
            logger.info(f"Failed to delete a record, key: {self.key}")
            logger.info(err.response['Error']['Message'])
