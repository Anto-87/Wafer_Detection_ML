from mongoDB.Connection import DatabaseConnection


class Logging:

    def __init__(self):
        self.connection = DatabaseConnection()

    def write_Logs(self, tableName, mydict):
        collection = self.connection.getCollection(tableName)
        collection.insert_one(mydict)


