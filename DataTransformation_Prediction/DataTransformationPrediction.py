from datetime import datetime
from os import listdir
import pandas
import os

from AWS.bucketfileoperation import bucket_file_operation
from AWS.generateDataFrame import generateDataFrame
from AWS.getBatchFiles import getBatchFiles
from AWS.getTrainingSchema import getTrainingSchema
from application_logging.logger import App_Logger
from property_access.accessProperties import accessProperties


class dataTransformPredict:

     """
                  This class shall be used for transforming the Good Raw Training Data before loading it in Database!!.

                  Written By: iNeuron Intelligence
                  Version: 1.0
                  Revisions: None

                  """

     def __init__(self):
          self.goodDataPath = "Prediction_Raw_Files_Validated/Good_Raw"
          self.logger_object = App_Logger()
          self.connection = getTrainingSchema()
          self.accessConfig = accessProperties()
          self.batchFiles = getBatchFiles()
          self.fileOperation = bucket_file_operation()
          self.createDataframe = generateDataFrame()
          self.logger_object.setCollectionName(self.accessConfig.getMongoDBProperty('PREDICTION_LOG'))


     def replaceMissingWithNull(self):

          """
                                  Method Name: replaceMissingWithNull
                                  Description: This method replaces the missing values in columns with "NULL" to
                                               store in the table. We are using substring in the first column to
                                               keep only "Integer" data for ease up the loading.
                                               This column is anyways going to be removed during prediction.

                                   Written By: iNeuron Intelligence
                                  Version: 1.0
                                  Revisions: None

                                          """

          try:
               listoffiles = []
               listofFiles = self.batchFiles.getFileNames(self.accessConfig.getProperty('PREDICTION_GOOD_FOLDER'))
               onlyfiles = [f for f in range(len(listofFiles))]
               for index in onlyfiles:
                    if index == 0:
                         continue
                    filename = listofFiles[index]
                    fullpath = filename
                    filename = filename.split('/')[1]
                    csv = self.createDataframe.getDataFrameFromCSV(listofFiles[index])
                    csv.fillna('NULL',inplace=True)
                    # #csv.update("'"+ csv['Wafer'] +"'")
                    # csv.update(csv['Wafer'].astype(str))
                    csv['Wafer'] = csv['Wafer'].str[6:]
                    newFileName = f"Download_CSV/{filename}"
                    csv.to_csv(newFileName, index=None, header=True)
                    self.fileOperation.upload(newFileName, self.accessConfig.getProperty('PREDICTION_GOOD_FOLDER'),
                                              filename)
                    os.remove(newFileName)
                    self.logger_object.log(" %s: File Transformed successfully!!" % filename)
                     #log_file.write("Current Date :: %s" %date +"\t" + "Current time:: %s" % current_time + "\t \t" +  + "\n")

          except Exception as e:
               self.logger_object.log( "Data Transformation failed because:: %s" % e)
               #log_file.write("Current Date :: %s" %date +"\t" +"Current time:: %s" % current_time + "\t \t" + "Data Transformation failed because:: %s" % e + "\n")
               raise e