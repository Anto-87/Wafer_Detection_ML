import pymongo
from pymongo import mongo_client
from pymongo.collation import Collation

from property_access.accessProperties import accessProperties


class DatabaseConnection:

    def __init__(self):
        self.config = accessProperties()

    def create(self):
        client = pymongo.MongoClient(
                 self.config.getMongoDBProperty('URL'))
        return client[self.config.getMongoDBProperty('DATABASE')]

    def getCollection(self, name):
        db = self.create()
        return db[name]

    def createCollection(self, collectionName):

        mongoClient = self.create()
        if collectionName in mongoClient.list_collection_names():
            mongoClient[collectionName].delete_many({})
        else:
            colla = Collation(
                locale="en_US",
                strength=2,
                numericOrdering=True,
                backwards=False
            )
            col = mongoClient.create_collection(
                name=collectionName,
                codec_options=None,
                read_preference=None,
                write_concern=None,
                read_concern=None,
                session=None,
                collation=colla
            )

