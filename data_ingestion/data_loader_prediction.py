import pandas as pd

from mongoDB.Connection import DatabaseConnection
from property_access.accessProperties import accessProperties


class Data_Getter_Pred:
    """
    This class shall  be used for obtaining the data from the source for prediction.

    Written By: iNeuron Intelligence
    Version: 1.0
    Revisions: None

    """
    def __init__(self, logger_object):
        self.prediction_file='Prediction_FileFromDB/InputFile.csv'
        self.logger_object=logger_object
        self.connection = DatabaseConnection()
        self.config = accessProperties()

    def get_data(self):
        """
        Method Name: get_data
        Description: This method reads the data from source.
        Output: A pandas DataFrame.
        On Failure: Raise Exception

         Written By: iNeuron Intelligence
        Version: 1.0
        Revisions: None

        """
        self.logger_object.log('Entered the get_data method of the Data_Getter class')
        try:
            collection = self.connection.getCollection(self.config.getMongoDBProperty('PREDICTION_GOOD_RAW_DATA'))
            self.data = pd.DataFrame(list(collection.find()))
            self.data.drop(['_id'], axis=1, inplace=True)
            # self.data= pd.read_csv(self.training_file) # reading the data file
            self.logger_object.log('Data Load Successful.Exited the get_data method of the Data_Getter class')
            return self.data
        except Exception as e:
            self.logger_object.log(
                'Exception occured in get_data method of the Data_Getter class. Exception message: ' + str(e))
            self.logger_object.log(
                'Data Load Unsuccessful.Exited the get_data method of the Data_Getter class')
            raise Exception()

