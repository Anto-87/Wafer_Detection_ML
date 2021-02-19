from datetime import datetime
from Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation
from DataTypeValidation_Insertion_Prediction.DataTypeValidationPrediction import dBOperation
from DataTransformation_Prediction.DataTransformationPrediction import dataTransformPredict
from application_logging import logger
from mongoDB.Connection import DatabaseConnection
from property_access.accessProperties import accessProperties


class pred_validation:
    def __init__(self):
        self.raw_data = Prediction_Data_validation()
        self.dataTransform = dataTransformPredict()
        self.dBOperation = dBOperation()
        self.log_writer = logger.App_Logger()
        self.con = DatabaseConnection()
        self.accessConfig = accessProperties()
        self.log_writer.setCollectionName(self.accessConfig.getMongoDBProperty('PREDICTION_LOG'))

    def prediction_validation(self):

        try:




            self.con.createCollection(self.accessConfig.getMongoDBProperty(('PREDICTION_LOG')))
            self.log_writer.log('Start of Validation on files for prediction!!')
            #extracting values from prediction schema
            LengthOfDateStampInFile,LengthOfTimeStampInFile,column_names,noofcolumns = self.raw_data.valuesFromSchema()
            #getting the regex defined to validate filename
            regex = self.raw_data.manualRegexCreation()
            #validating filename of prediction files
            self.raw_data.validationFileNameRaw(regex,LengthOfDateStampInFile,LengthOfTimeStampInFile)
            #validating column length in the file
            self.raw_data.validateColumnLength(noofcolumns)
            #validating if any column has all values missing
            self.raw_data.validateMissingValuesInWholeColumn()
            self.log_writer.log("Raw Data Validation Complete!!")

            self.log_writer.log(("Starting Data Transforamtion!!"))
            #replacing blanks in the csv file with "Null" values to insert in table
            self.dataTransform.replaceMissingWithNull()

            self.log_writer.log("DataTransformation Completed!!!")

            self.log_writer.log("Creating Prediction_Database and tables on the basis of given schema!!!")
            #create database with given name, if present open the connection! Create table with columns given in schema
            self.dBOperation.createTableDb(column_names)
            self.log_writer.log("Table creation Completed!!")
            self.log_writer.log("Insertion of Data into Table started!!!!")
            #insert csv files in the table
            self.dBOperation.insertIntoTableGoodData(self.accessConfig.getMongoDBProperty('PREDICTION_GOOD_RAW_DATA'))
            self.log_writer.log("Insertion in Table completed!!!")
            # self.log_writer.log("Deleting Good Data Folder!!!")
            # #Delete the good data folder after loading files in table
            # self.raw_data.deleteExistingGoodDataTrainingFolder()
            # self.log_writer.log("Good_Data folder deleted!!!")
            # self.log_writer.log("Moving bad files to Archive and deleting Bad_Data folder!!!")
            # #Move the bad files to archive folder
            # self.raw_data.moveBadFilesToArchiveBad()
            # self.log_writer.log("Bad files moved to archive!! Bad folder Deleted!!")
            # self.log_writer.log("Validation Operation completed!!")
            # self.log_writer.log("Extracting csv file from table")
            # #export data in table to csvfile
            # self.dBOperation.selectingDatafromtableintocsv('Prediction')

        except Exception as e:
            raise e









