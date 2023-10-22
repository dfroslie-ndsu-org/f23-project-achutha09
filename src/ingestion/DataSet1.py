import os
import configparser
import requests
from azure.storage.blob import BlobServiceClient

# The api key is mentioned in api_key.config file
file_path = "../../api_key.config"
# Azure container name
container_name = 'fodeprojectstorage'
directory_name = "AirlinesDataAviationStack"
blob_file_name = "AviationData.csv"


def read_connection_string(filepath):
    try:
        print("Current working directory:", os.getcwd())
        config = configparser.ConfigParser()
        config.read(filepath)
        # Check if the 'CONNECTION-STRING' section exists in the file
        if 'CONNECTION-STRING' in config:
            # Check if the 'CONNECTION-STRING' key exists within the 'CONNECTION-STRING' section
            if 'CONNECTION-STRING' in config['CONNECTION-STRING']:
                return config['CONNECTION-STRING']['CONNECTION-STRING']
            else:
                raise KeyError('CONNECTION-STRING key not found in the configuration file.')
        else:
            raise KeyError('CONNECTION-STRING section not found in the configuration file.')
    except FileNotFoundError:
        raise Exception('CONNECTION-STRING configuration file not found.')


def read_access_key(filepath):
    try:
        print("Current working directory:", os.getcwd())
        config = configparser.ConfigParser()
        config.read(filepath)
        # Check if the 'CONNECTION-STRING' section exists in the file
        if 'Access-Key' in config:
            # Check if the 'CONNECTION-STRING' key exists within the 'CONNECTION-STRING' section
            if 'Access-Key' in config['Access-Key']:
                return config['Access-Key']['Access-Key']
            else:
                raise KeyError('Access-Key key not found in the configuration file.')
        else:
            raise KeyError('Access-Key section not found in the configuration file.')
    except FileNotFoundError:
        raise Exception('Access-Key configuration file not found.')


api_url = "http://api.aviationstack.com/v1/airlines?access_key=" + read_access_key(file_path)


def download_json_and_upload_to_azure_storage(api_url):
    try:
        # Download JSON data from the API
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an exception if the request fails

        json_data = response.json()

        # Define the column names
        fieldnames = [
            "id",
            "fleet_average_age",
            "airline_id",
            "callsign",
            "hub_code",
            "iata_code",
            "icao_code",
            "country_iso2",
            "date_founded",
            "iata_prefix_accounting",
            "airline_name",
            "country_name",
            "fleet_size",
            "status",
            "type"
        ]

        # Convert JSON data to CSV
        csv_data = json_to_csv(json_data, fieldnames)
        return csv_data
    except requests.exceptions.RequestException as e:
        raise Exception(f'Failed to retrieve data from the API: {e}')
    except ValueError:
        raise Exception('Failed to parse JSON data from the API response.')


def json_to_csv(json_data, fieldnames):
    # Convert JSON data to a CSV string
    csv_data = [fieldnames]

    for item in json_data['data']:
        row = [str(item.get(field, '')) for field in fieldnames]
        csv_data.append(row)

    return '\n'.join([','.join(row) for row in csv_data])


def upload_to_azure_blob_storage(container_name, csv_data, connection_string, directory_name, blob_file_name):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        if not container_client.exists():
            # Creating a container
            container_client.create_container()

        blob_client = container_client.get_blob_client(f"{directory_name}/{blob_file_name}")
        blob_client.upload_blob(csv_data, overwrite=True)
    except Exception as e:
        raise Exception(f'Failed to upload data to Azure Blob Storage: {e}')


try:
    data = download_json_and_upload_to_azure_storage(api_url)
    connection_string = read_connection_string(file_path)
    upload_to_azure_blob_storage(container_name, data, connection_string, directory_name, blob_file_name)
    print("Data successfully downloaded and uploaded to Azure Blob Storage.")
except Exception as e:
    print(f"An error occurred: {e}")
