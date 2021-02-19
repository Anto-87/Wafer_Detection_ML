from datetime import datetime
from Training_Raw_data_validation.rawValidation import Raw_Data_validation
from DataTypeValidation_Insertion_Training.DataTypeValidation import dBOperation
from DataTransform_Training.DataTransformation import dataTransform
from application_logging import logger
from mongoDB.Connection import DatabaseConnection
from property_access.accessProperties import accessProperties


class train_validation:
    def __init__(self):
        self.raw_data = Raw_Data_validation()
        self.dataTransform = dataTransform()
        self.dBOperation = dBOperation()
        self.log_writer = logger.App_Logger()
        self.con = DatabaseConnection()
        self.config = accessProperties()
        self.log_writer.setCollectionName(self.config.getMongoDBProperty('TRAINING_LOG'))

    def train_validation(self):
        try:
            self.con.createCollection(self.config.getMongoDBProperty('TRAINING_LOG'))
            self.log_writer.log( 'Start of Validation on files!!')
            # extracting values from prediction schema
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, noofcolumns = self.raw_data.valuesFromSchema()
            # getting the regex defined to validate filename
            regex = self.raw_data.manualRegexCreation()
            # validating filename of prediction files
            self.raw_data.validationFileNameRaw(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile)
            # validating column length in the file
            self.raw_data.validateColumnLength(noofcolumns)
            # validating if any column has all values missing
            self.raw_data.validateMissingValuesInWholeColumn()
            self.log_writer.log( "Raw Data Validation Complete!!")

            self.log_writer.log( "Starting Data Transforamtion!!")
            # replacing blanks in the csv file with "Null" values to insert in table
            self.dataTransform.replaceMissingWithNull()

            self.log_writer.log( "DataTransformation Completed!!!")

            self.log_writer.log("Creating Training_Database and tables on the basis of given schema!!!")
            # create database with given name, if present open the connection! Create table with columns given in schema
            self.dBOperation.createTableDb(column_names)
            self.log_writer.log( "Table creation Completed!!")
            self.log_writer.log( "Insertion of Data into Table started!!!!")
            # insert csv files in the table
            self.dBOperation.insertIntoTableGoodData(self.config.getMongoDBProperty('TRAINING_GOOD_RAW_DATA'))
            self.log_writer.log( "Insertion in Table completed!!!")
            #self.log_writer.log( "Deleting Good Data Folder!!!")
            # Delete the good data folder after loading files in table
            #self.raw_data.deleteExistingGoodDataTrainingFolder()
            #self.log_writer.log( "Good_Data folder deleted!!!")
            #self.log_writer.log( "Moving bad files to Archive and deleting Bad_Data folder!!!")
            # Move the bad files to archive folder
            #self.raw_data.moveBadFilesToArchiveBad()
            #self.log_writer.log( "Bad files moved to archive!! Bad folder Deleted!!")
            #self.log_writer.log( "Validation Operation completed!!")
            #self.log_writer.log( "Extracting csv file from table")
            # export data in table to csvfile
            #self.dBOperation.selectingDatafromtableintocsv('Training')
            #self.file_object.close()

        except Exception as e:
            raise e