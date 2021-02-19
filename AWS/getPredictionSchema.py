from AWS.Connection import awsConnection
from property_access.accessProperties import accessProperties


class getPredictionSchema:

    def __init__(self):
        self.accessConfig = accessProperties()
        self.client = awsConnection()
        self.con = self.client.createAWSClientConnection()

    def getPredictionSchemaJSON(self):
        result = self.con.get_object(Bucket=self.accessConfig.getProperty('BUCKET_NAME'),
                                     Key=self.accessConfig.getProperty('SCHEMA_PREDICTION_FOLDER'))
        text = result["Body"].read().decode()
        return text