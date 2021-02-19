from datetime import datetime
from mongoDB.Log import Logging


class App_Logger:
    def __init__(self):
        self.writeLog = Logging()

    def setCollectionName(self, collectionName):
        self.collection = collectionName

    def getCollection(self):
        return self.collection

    def log(self,  log_message):
        str = self.generateJSON(log_message)
        #self.writeLog.write_Logs(self.getCollection(), str)

    def generateJSON(self, message):
        self.now = datetime.now()
        self.date = self.now.strftime("%m/%d/%Y")
        self.current_time = self.now.strftime("%H:%M:%S")
        data = {
            'date': self.date,
            'time':self.current_time,
            'message': message,
        }
        return data