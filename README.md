# Data Warehouse

Project 3 

## Installation

Make sure that boto3 is installed with its required dependencies. An AWS account will also have to be set up along with an IAM role. A file not included in the repo is the dwh.cfg file which is your configuration file to connect to Amazon Redshift.

** Warning: Do NOT upload any configuration file to the internet as this will allow other people to access your account. **


Clone the github repo and use pip install to install the required libraries

``
git clone https://github.com/Zingo3245/Udacity_Data_Warehouses
cd Udacity_Data_Warehouses
pip install -r requirements.txt
``

Also make sure that the cluster is created prior to running the scripts.

## Usage

This project uses python scripts that can be run from the terminal:

``

python create_tables.py

python etl.py

``



## License

This project is distributed under the GPL 3.0 license.