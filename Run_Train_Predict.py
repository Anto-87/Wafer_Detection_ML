from predictFromModel import prediction
from prediction_Validation_Insertion import pred_validation
from trainingModel import trainModel
from training_Validation_Insertion import train_validation


class RunTrainPredict:


    def train(self):
        try:

            train_valObj = train_validation()  # object initialization
            train_valObj.train_validation()  # calling the training_validation function

            trainModelObj = trainModel()  # object initialization
            trainModelObj.trainingModel()  # training the model for the files in the table

        except ValueError:
           print(ValueError)

        except KeyError:
            print(KeyError)

        except Exception as e:
            print(e)

    def predict(self):

        try:

            pred_val = pred_validation()  # object initialization
            pred_val.prediction_validation()  # calling the prediction_validation function

            pred = prediction()  # object initialization
            pred.predictionFromModel()

        except ValueError:
           print(ValueError)

        except KeyError:
            print(KeyError)

        except Exception as e:
            print(e)


obj = RunTrainPredict()
obj.train()
obj.predict()