import configparser
import os

from azure.core.pipeline.transport._requests_basic import RequestsTransport
import kaggle
from azure.storage.blob import BlobServiceClient
from azure.core.pipeline.transport import RequestsTransport

api_file_path = "../../api_key.config"
containerName = "fodeprojectstorage"
directory_name = "AirlinesDelayKaggleDataset"
blob_name = "AirlinesDelay.csv"
local_dataset_file = "Data/downloads/Airline Dataset Updated - v2.csv"


def read_api_key(file_path):
    try:
        print("Current working directory:", os.getcwd())
        config = configparser.ConfigParser()
        config.read(file_path)

        # Check if the 'API' section exists in the file
        if 'CONNECTION-STRING' in config:
            # Check if the 'API_KEY' key exists within the 'API' section
            if 'CONNECTION-STRING' in config['CONNECTION-STRING']:
                return config['CONNECTION-STRING']['CONNECTION-STRING']

        raise KeyError('Connection string not found in the configuration file.')
    except FileNotFoundError:
        raise Exception('Connection string configuration file not found.')


# Azure Blob Storage connection String
connection_string = read_api_key(api_file_path)

world_population_dataset_name = "iamsouravbanerjee/airline-dataset"
csv_file_destination_path = "Data/downloads"


def dataset_upload(world_population_dataset_name, csv_file_destination_path):
    try:
        kaggle.api.authenticate()
        # Kaggle API call to download the csv file to the csv_destination_path
        kaggle.api.dataset_download_files(world_population_dataset_name, csv_file_destination_path, unzip=True)
        transport = RequestsTransport(timeout=600)  # 1 hour timeout (adjust as needed)
        blob_service_client = BlobServiceClient.from_connection_string(connection_string, transport=transport)
        # Create a container client
        container_client = blob_service_client.get_container_client(containerName)
        if blob_name:
            blob_client = container_client.get_blob_client(f"{directory_name}/{blob_name}")
        else:
            blob_client = container_client.get_blob_client(local_dataset_file)
        # Upload the Kaggle dataset file to Azure Blob Storage
        with open(local_dataset_file, "rb") as data:
            blob_client.upload_blob(
                data,
                blob_type="BlockBlob", overwrite=True
            )
        print("Dataset successfully downloaded from Kaggle and uploaded to Azure Blob Storage.")
    except kaggle.api.KaggleApiError as e:
        print(f"Kaggle API error: {e}")
    except FileNotFoundError as e:
        print(f"File not found error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


dataset_upload(world_population_dataset_name, csv_file_destination_path)
