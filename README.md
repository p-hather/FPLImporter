# FPL Importer
## Import REST API data to BigQuery

### Overview

Import data from the Fantasy Premier League API into Google BigQuery, with just a few lines of config.

NB this repository is intended as a proof of concept - it's not a finished product and needs further development. That said - it does work and has the potential for adaptation to other use cases.

### Set up

Set up FPL Importer by cloning this repo, ensuring you have all the prerequisites listed below, and running `main.py`. This script will need to run on a regular schedule dependent on how fresh the data needs to be - a cloud platform or Raspberry Pi acting as a server is ideal.

FPL Importer will handle creating the dataset if it doesn't already exist.

### Prerequisites

* A GCP Service Account created with write access to your chosen BigQuery destination, and the JSON keyfile stored in `./secrets`.
* All packages in `requirements.txt` installed.
* A config.yaml file created in this directory, following the format of `example-config.yaml`.

### Adding additional endpoints

Endpoints can be added, changed, or removed by editing `endpoints.yaml` - following the format in the current examples.

### References
Thanks to Frenzel Timothy for his article [Fantasy Premier League API Endpoints: A Detailed Guide](https://medium.com/@frenzelts/fantasy-premier-league-api-endpoints-a-detailed-guide-acbd5598eb19), which was helpful for accessing the undocumented FPL API.
