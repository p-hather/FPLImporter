import yaml
import requests
import os
import logging
from dotenv import load_dotenv
from python import bigquery


logging.basicConfig(level=logging.INFO, filename="FPLImporter.log", filemode="a+",
                    format="%(asctime)-15s %(levelname)-8s %(message)s")

env_path = 'secrets/.env'
load_dotenv(env_path)


class FPLImporter:
    def __init__(self, bq_project, bq_dataset, endpoint_fp='endpoints.yaml') -> None:
        self.base_url = 'https://fantasy.premierleague.com/api'
        self.endpoint_fp = endpoint_fp
        self.endpoints = self.get_endpoints(self.endpoint_fp)

        self.bq_project = bq_project
        self.bq_dataset = bq_dataset
        self.bq = bigquery.bigQueryLoad(self.bq_project, self.bq_dataset)

    def get_endpoints(self, fp) -> dict:
        with open(fp) as endpoints_file:
            return yaml.safe_load(endpoints_file)
    
    def import_data(self) -> None:
        for endpoint in self.endpoints:
            url = f'{self.base_url}/{endpoint["path"]}/'
            data = requests.get(url).json()

            table_id = self.bq.create_table_id(endpoint["name"])
            self.bq.load_table(data, table_id, description=endpoint["description"])
    
    def run_process(self) -> None:
        if not self.bq.check_dataset_exists():
            self.bq.create_dataset()
        
        self.import_data()


if __name__ == '__main__':
    bq_project = os.getenv('BQ_PROJECT')
    bq_dataset = 'fpl'
    fpl = FPLImporter(bq_project, bq_dataset)
    fpl.run_process()
