from mongoDB.Connection import DatabaseConnection


class Insert_DataFrame_To_Collection:

    def __init__(self):
        self.connection = DatabaseConnection()

    def insert(self, collectionName, dataFrame):
        collection = self.connection.getCollection(collectionName)
        collection.insert_many(dataFrame)

