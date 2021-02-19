from AWS.Connection import awsConnection
from AWS.getBatchFiles import getBatchFiles

import pandas as pd
import io

from property_access.accessProperties import accessProperties

class generateDataFrame:

    def __init__(self):
        self.accessConfig = accessProperties()
        self.con = awsConnection()
        self.client = self.con.createAWSClientConnection()

    def getDataFrameFromCSV(self, key):
        obj = self.client.get_object(Bucket=self.accessConfig.getProperty('BUCKET_NAME'), Key=key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8')
        return df