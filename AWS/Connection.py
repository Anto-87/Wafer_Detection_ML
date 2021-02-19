"""
    The above co

"""

import json
from property_access.accessProperties import accessProperties
import boto3

class awsConnection:

    def __init__(self):
        self.config = accessProperties()

    def getAccessKeyID(self):
        return self.config.getProperty('ACCESS_KEY_ID')

    def getSecretAccessKey(self):
        return self.config.getProperty('SECRET_ACCESS_KEY')

    def createAWSResourceConnection(self):
        aws_resource = boto3.resource('s3', aws_access_key_id=self.getAccessKeyID(),aws_secret_access_key=self.getSecretAccessKey())
        return aws_resource

    def createAWSClientConnection(self):
        aws_client = boto3.client('s3', aws_access_key_id=self.getAccessKeyID(),
                                      aws_secret_access_key=self.getSecretAccessKey())
        return aws_client

