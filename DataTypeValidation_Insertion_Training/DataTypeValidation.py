import shutil
import sqlite3
from datetime import datetime
from os import listdir
import os
import csv
import json

from AWS.generateDataFrame import generateDataFrame
from AWS.getBatchFiles import getBatchFiles
from application_logging.logger import App_Logger
from mongoDB.Connection import DatabaseConnection
from mongoDB.Insert_Dataframe_To_Collection import Insert_DataFrame_To_Collection
from property_access.accessProperties import accessProperties


class dBOperation:
    """
      This class shall be used for handling all the SQL operations.

      Written By: iNeuron Intelligence
      Version: 1.0
      Revisions: None

      """
    def __init__(self):
        self.path = 'Training_Database/'
        self.badFilePath = "Training_Raw_files_validated/Bad_Raw"
        self.goodFilePath = "Training_Raw_files_validated/Good_Raw"
        self.logger = App_Logger()
        self.connection = DatabaseConnection()
        self.config = accessProperties()
        self.trainingBatchFiles = getBatchFiles()
        self.insert_dataFrame = Insert_DataFrame_To_Collection()
        self.createDataframe = generateDataFrame()
        self.logger.setCollectionName(self.config.getMongoDBProperty('TRAINING_LOG'))

    def createTableDb(self,column_names):
        """
                        Method Name: createTableDb
                        Description: This method creates a table in the given database which will be used to insert the Good data after raw data validation.
                        Output: None
                        On Failure: Raise Exception

                         Written By: iNeuron Intelligence
                        Version: 1.0
                        Revisions: None

                    """
        try:
            self.connection.createCollection(self.config.getMongoDBProperty('TRAINING_GOOD_RAW_DATA'))
            self.logger.log("Created Good_Raw_Data table successfully!!")
        except Exception as e:
            exception = f"Error while creating table : {e}"
            self.logger.log(exception)
            

    def insertIntoTableGoodData(self,tableName):

        """
                               Method Name: insertIntoTableGoodData
                               Description: This method inserts the Good data files from the Good_Raw folder into the
                                            above created table.
                               Output: None
                               On Failure: Raise Exception

                                Written By: iNeuron Intelligence
                               Version: 1.0
                               Revisions: None

        """
        list_Of_Files = []

        list_Of_Files = self.trainingBatchFiles.getFileNames(self.config.getProperty('TRAINING_GOOD_FOLDER'))
        onlyfiles = [f for f in range(len(list_Of_Files))]

        for index in onlyfiles:
            try:
                if index == 0:
                    continue
                filename = list_Of_Files[index]
                fullpath = filename
                filename = filename.split('/')[1]
                csv = self.createDataframe.getDataFrameFromCSV(list_Of_Files[index])
                mongo_dict = json.loads(csv.T.to_json()).values()
                self.insert_dataFrame.insert(tableName, mongo_dict)
                self.logger.log("Inserted successfully")

            except Exception as e:
                self.logger.log("Error while creating table: %s " % e)



    def selectingDatafromtableintocsv(self,Database):

        """
                               Method Name: selectingDatafromtableintocsv
                               Description: This method exports the data in GoodData table as a CSV file. in a given location.
                                            above created .
                               Output: None
                               On Failure: Raise Exception

                                Written By: iNeuron Intelligence
                               Version: 1.0
                               Revisions: None

        """

        self.fileFromDb = 'Training_FileFromDB/'
        self.fileName = 'InputFile.csv'
        log_file = open("Training_Logs/ExportToCsv.txt", 'a+')
        try:
            conn = self.dataBaseConnection(Database)
            sqlSelect = "SELECT *  FROM Good_Raw_Data"
            cursor = conn.cursor()

            cursor.execute(sqlSelect)

            results = cursor.fetchall()
            # Get the headers of the csv file
            headers = [i[0] for i in cursor.description]

            #Make the CSV ouput directory
            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)

            # Open CSV file for writing.
            csvFile = csv.writer(open(self.fileFromDb + self.fileName, 'w', newline=''),delimiter=',', lineterminator='\r\n',quoting=csv.QUOTE_ALL, escapechar='\\')

            # Add the headers and data to the CSV file.
            csvFile.writerow(headers)
            csvFile.writerows(results)

            self.logger.log(log_file, "File exported successfully!!!")
            log_file.close()

        except Exception as e:
            self.logger.log(log_file, "File exporting failed. Error : %s" %e)
            log_file.close()





