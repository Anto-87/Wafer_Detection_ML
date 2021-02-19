from AWS.Connection import awsConnection
from property_access.accessProperties import accessProperties


class bucket_file_operation:

    def __init__(self):
        self.accessConfig = accessProperties()
        self.connection = awsConnection()
        self.con = self.connection.createAWSResourceConnection()
        self.client = self.connection.createAWSClientConnection()

    def move(self, source, key, foldername):
        fullsourcepath = self.accessConfig.getProperty('BUCKET_NAME')
        src = '{0}/{1}'.format(fullsourcepath,source)
        destinationFileName = "{0}/{1}".format(foldername, key)
        self.con.Object(self.accessConfig.getProperty('BUCKET_NAME'), destinationFileName).copy_from(CopySource=src)
        self.con.Object(self.accessConfig.getProperty('BUCKET_NAME'), source).delete()

    def upload(self, path, folderName, fileName):
        try:
           response = self.client.upload_file(path, self.accessConfig.getProperty('BUCKET_NAME'), "{0}/{1}".format(folderName, fileName))
        except Exception as e:
            print(e)

