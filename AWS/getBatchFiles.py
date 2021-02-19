from AWS.Connection import awsConnection
from property_access.accessProperties import accessProperties


class getBatchFiles:

    def __init__(self):
        self.fileNames = []
        self.accessConfig = accessProperties()
        self.connection = awsConnection()
        self.con = self.connection.createAWSResourceConnection()

    def getFileNames(self, folderName):
        self.fileNames.clear()
        bucket = self.con.Bucket(self.accessConfig.getProperty('BUCKET_NAME'))
        for object_summary in bucket.objects.filter(Prefix=f"{folderName}/"):
            self.fileNames.append(object_summary.key)
        return self.fileNames

