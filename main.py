import yaml
import requests
import logging
from python.bigquery import bigQueryLoad


logging.basicConfig(level=logging.INFO, filename="FPLImporter.log", filemode="a+",
                    format="%(asctime)-15s %(levelname)-8s %(message)s")


class FPLImporter:
    def __init__(self, config_fp='config.yaml', endpoints_fp='endpoints.yaml') -> None:
        self.base_url = 'https://fantasy.premierleague.com/api'
        self.config = self.read_yaml(config_fp)
        self.bq = bigQueryLoad(self.config["bigquery"]["project"], self.config["bigquery"]["dataset"])
        self.endpoints = self.read_yaml(endpoints_fp)

    def read_yaml(self, fp) -> dict:
        with open(fp) as file:
            return yaml.safe_load(file)
    
    def get_data(self, path, type) -> list:
        url = f'{self.base_url}/{path}/'
        if type == 'team':
            data = []
            for team_id in self.config["fpl"]["team_ids"]:
                team_data = requests.get(url.replace('%TEAM_ID%', str(team_id))).json()
                team_data["team_id"] = team_id
                data.append(team_data)
            return data
        return requests.get(url).json()

    def import_data(self) -> None:
        for name, endpoint in self.endpoints.items():
            data = self.get_data(endpoint["path"], endpoint["type"])
            table_id = self.bq.create_table_id(name)
            self.bq.load_table(data, table_id, description=endpoint["description"])
    
    def run_process(self) -> None:
        if not self.bq.check_dataset_exists():
            self.bq.create_dataset()
        
        self.import_data()


if __name__ == '__main__':
    fpl = FPLImporter()
    fpl.run_process()
