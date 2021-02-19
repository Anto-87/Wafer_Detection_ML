from jproperties import Properties

class accessProperties:

    def initializeProperty(self):
        config = Properties()
        with open('aws.properties', 'rb') as configFile:
            config.load(configFile)
        return config


    def getProperty(self, attribute):
        config_file = self.initializeProperty()
        value = config_file[attribute].data
        return value

    def initializeMongoDBProperty(self):
        config = Properties()
        with open('mongodb.properties', 'rb') as configFile:
            config.load(configFile)
        return config

    def getMongoDBProperty(self, attribute):
        config_file = self.initializeMongoDBProperty()
        value = config_file[attribute].data
        return value
