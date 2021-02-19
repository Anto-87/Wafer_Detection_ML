from AWS.Connection import awsConnection
from property_access.accessProperties import accessProperties


class getTrainingSchema:

    def __init__(self):
        self.accessConfig = accessProperties()
        self.client = awsConnection()
        self.con = self.client.createAWSClientConnection()

    def getTrainingSchemaJSON(self):
        result = self.con.get_object(Bucket=self.accessConfig.getProperty('BUCKET_NAME'),
                                     Key=self.accessConfig.getProperty('SCHEMA_TRAINING_FOLDER'))
        text = result["Body"].read().decode()
        return text
