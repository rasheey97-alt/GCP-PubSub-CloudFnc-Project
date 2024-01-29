import argparse
import logging
import os
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import f1_score 
import pickle
from google.cloud import storage
from sklearn import tree
from datetime import datetime

def read_from_bigquery(full_table_path, project_id=None, num_samples=None):
    """Read data from BigQuery and split into train and validation sets.
    Args:
      full_table_path: (string) full path of the table containing training data
        in the format of [project_id.dataset_name.table_name].
      project_id: (string, Optional) Google BigQuery Account project ID.
      num_samples: (int, Optional) Number of data samples to read.
    Returns:
      pandas.DataFrame
    """
    BASE_QUERY = '''
        SELECT
          *
        FROM
          `{table}`
      '''
    query = BASE_QUERY.format(table=full_table_path)
    limit = ' LIMIT {}'.format(num_samples) if num_samples else ''
    query += limit

    # Use "application default credentials"
    # Use SQL syntax dialect
    data_df = pd.read_gbq(query, project_id=project_id, dialect='standard')

    return data_df

def label_dataset(dataset):
    #Our logic to label the data
    dataset["Status"] = "Healthy"
    #If all the 5 sensors readings are more than 70 percentile of the distribution, we flag the event at a failure
    dataset.loc[(dataset["PRESSURE_1"] > np.percentile(dataset["PRESSURE_1"],70))
        & (dataset["PRESSURE_2"] > np.percentile(dataset["PRESSURE_2"],70))
        & (dataset["PRESSURE_3"] > np.percentile(dataset["PRESSURE_3"],70))
        & (dataset["PRESSURE_4"] > np.percentile(dataset["PRESSURE_4"],70))
        & (dataset["PRESSURE_5"] > np.percentile(dataset["PRESSURE_5"],70)),["Status"]] = "Failure"
    return dataset

def train_score_model(dataset):

    #Remove the Timestamp and Status coloumn as they are not out independent variables X
    X = dataset.drop(["Timestamp","Status"],axis=1)
    #Declare the dependent variable - we want to predict
    Y = dataset["Status"]
    #split the dataset into train and test 80:20
    X_train,X_test,Y_train,Y_test = train_test_split(X,Y,test_size=0.20, random_state=10)   

    classifier_DT = tree.DecisionTreeClassifier()
    classifier_DT = classifier_DT.fit(X_train,Y_train) 

    Y_pred_DT = classifier_DT.predict(X_test)
    score = f1_score(Y_test,Y_pred_DT, pos_label="Failure")
    logging.info('F1 Score: %s', score)

    return classifier_DT

def save_model_to_gcs(model, path):
    """Read data from BigQuery and split into train and validation sets.
    Args:
      path: (string) full path of the GCS bucket where out model would be stored
      model: The sklearn model we want to save
    """
    artifact_filename = 'model.pkl'

    #Dump the model locally
    pickle_out = pickle.dumps(model)
    
    model_directory = path
    storage_path = os.path.join(model_directory, artifact_filename)
    blob = storage.blob.Blob.from_string(storage_path, client=storage.Client())
    #Copy the local model.pkl to gcs
    blob.upload_from_string(pickle_out)

def _parse_args():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--log-level',
        help='Logging level.',
        choices=[
            'DEBUG',
            'ERROR',
            'FATAL',
            'INFO',
            'WARN',
        ],
        default='INFO',
    )

    parser.add_argument(
        '--input',
        help='''
              BigQuery table, specify as as PROJECT_ID.DATASET.TABLE_NAME.
            ''',
        required=True,
    )

    parser.add_argument(
        '--job-dir',
        help='Output directory for exporting model and other metadata.',
        required=True,
    )

    parser.add_argument(
        '--criterion',
        help='"gini" or "entropy"',
        default="gini",
        type=str,
    )

    parser.add_argument(
        '--max-depth',
        help='The maximum depth of the tree.',
        type=int,
        default=10,
    )

    return parser.parse_args()

def run_experiment(arguments):
    logging.info('Arguments: %s', arguments)
    dataset = read_from_bigquery(arguments.input)   
    dataset = label_dataset(dataset)    
    model = train_score_model(dataset)
    save_model_to_gcs(model, arguments.job_dir)

"""Entry point"""
def main():
    arguments = _parse_args()
    print(arguments)
    logging.basicConfig(level=arguments.log_level)
    # Run the train and evaluate experiment
    time_start = datetime.utcnow()
    run_experiment(arguments)
    time_end = datetime.utcnow()
    time_elapsed = time_end - time_start
    logging.info('Training elapsed time: {} seconds'.format(
        time_elapsed.total_seconds()))

if __name__ == '__main__':
    main()