from google.cloud import bigquery, exceptions
import logging
from datetime import datetime


class bigQueryLoad:

    def __init__(self, project, dataset, dataset_location='EU'):
        self.project = project
        self.dataset = dataset
        self.dataset_id = '.'.join([self.project, self.dataset])
        self.dataset_location = dataset_location
        self.bq = self.get_client()
    
    def get_client(self):
        logging.info('Attempting to authenticate BigQuery access with service account')
        return bigquery.Client(self.project)
    
    def check_dataset_exists(self):
        try:
            self.bq.get_dataset(self.dataset_id)
            return True
        except exceptions.NotFound:
            return False
    
    def create_dataset(self):
        logging.info(f'Attempting to create dataset `{self.dataset_id}`')
        dataset_obj = bigquery.Dataset(self.dataset_id)
        dataset_obj.location = self.dataset_location

        try:
            self.bq.create_dataset(dataset_obj, timeout=30)
            logging.info(f'Success - dataset created')
        except exceptions.Conflict:
            logging.info(f'Dataset already exists')

    def create_table_id(self, table):
        return '.'.join([self.dataset_id, table])
    
    def update_table_description(self, table_id, description):
        logging.info(f'Attempting to update the description for `{table_id}`')
        table = self.bq.get_table(table_id)
        table.description = description
        self.bq.update_table(table, ["description"])
        logging.info('Success - description updated')

    def load_table(self, data, table_id, add_loaded_ts=True, description=None):
        logging.info(f'Attempting to load records into `{table_id}`')
        job_config = bigquery.LoadJobConfig(
            autodetect=True, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
        
        # load_table_from_json needs a list of dicts
        if isinstance(data, dict):
            data = [data]
        
        if add_loaded_ts:
            loaded_ts = datetime.now().strftime('%Y-%m-%dT%XZ')
            for obj in data: obj["loaded_ts"] = loaded_ts

        job = self.bq.load_table_from_json(data, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete
        logging.info(f'Success - {len(data)} records loaded')

        if description:
            self.update_table_description(table_id, description)
