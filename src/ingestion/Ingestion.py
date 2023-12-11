import configparser
import io
import logging
import os
import random
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from io import StringIO

import kaggle
import pandas as pd
import pyodbc
import requests
from azure.core.pipeline.transport import RequestsTransport
from azure.storage.blob import BlobServiceClient

# Configure the logging settings
logging.basicConfig(filename='../logs/logfile.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# defining variables
container_name = 'fodeprojectstorage'
directory_name = "AirlinesDataAviationStack"
blob_file_name = "AviationData.csv"
api_file_path = "../../api_key.config"
containerName = "fodeprojectstorage"
directory_name_DS2 = "AirlinesDelayKaggleDataset"
transformed_directory = "TransformedData"
processed_directory = "ProcessedData"
data_downloads="Data/downloads"
blob_name = "AirlinesDelay.csv"
local_dataset_file = "Data/downloads/Airline Dataset Updated - v2.csv"
modified_dataset_file = "Data/downloads/Modified_Airline_Dataset.csv"
blob_file_modified = "Modified_Airline_Dataset.csv"
filtered_dataset_file = "Data/downloads/Modified__Airline_Dataset.csv"
validated_file_modified = "Validated_Airline_Dataset.csv"


# method to fetch the connection-string
def read_connection_string(api_file_path):
    try:
        logging.info("Trying to fetch the connection string")
        config = configparser.ConfigParser()
        config.read(api_file_path)
        # Check if the 'CONNECTION-STRING' section exists in the file
        if 'CONNECTION-STRING' in config:
            # Check if the 'CONNECTION-STRING' key exists within the 'CONNECTION-STRING' section
            if 'CONNECTION-STRING' in config['CONNECTION-STRING']:
                logging.info("connection string found")
                return config['CONNECTION-STRING']['CONNECTION-STRING']
            else:
                logging.info("Connection string missing in the file")
                raise KeyError('CONNECTION-STRING key not found in the configuration file.')
        else:
            logging.info("Connection string missing in the file")
            raise KeyError('CONNECTION-STRING section not found in the configuration file.')
    except FileNotFoundError:
        logging.info("File not found for connection string")
        raise Exception('CONNECTION-STRING configuration file not found.')

# method to fetch the access-key
def read_access_key(filepath):
    try:
        logging.info("Trying to fetch the access key")
        config = configparser.ConfigParser()
        config.read(filepath)
        # Check if the 'Access-Key' section exists in the file
        if 'Access-Key' in config:
            # Check if the 'Access-Key' key exists within the 'Access-Key' section
            if 'Access-Key' in config['Access-Key']:
                logging.info("Access key found")
                return config['Access-Key']['Access-Key']
            else:
                logging.info("Access key missing in the file")
                raise KeyError('Access-Key key not found in the configuration file.')
        else:
            logging.info("Access key missing in the file")
            raise KeyError('Access-Key section not found in the configuration file.')
    except FileNotFoundError:
        logging.info("File not found for connection string")
        raise Exception('Access-Key configuration file not found.')


api_url = "http://api.aviationstack.com/v1/airlines?access_key=" + read_access_key(api_file_path)


# method to call the aviationstack data using the api_url and access key
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
        df = pd.DataFrame(json_data['data'], columns=fieldnames)
        # Trim the values in the "type" column to a maximum length of 9 characters
        logging.info("Data validation starts on the aviationstack dataset")
        df['type'] = df['type'].str[:9]
        df = df.dropna(subset=df.columns[df.isin(['', None]).any()])
        alpha_pattern = '^[a-zA-Z\s]+$'
        alpha_pattern_with_backslash = '^[a-zA-Z/]+$'
        decimal_pattern = r'^\d+(\.\d+)?$'
        df = df[df['callsign'].str.match(alpha_pattern, na=False)]
        df = df[df['hub_code'].str.match(alpha_pattern, na=False)]
        df = df[df['status'].str.match(alpha_pattern_with_backslash, na=False)]
        df = df[df['type'].str.match(alpha_pattern, na=False)]
        df = df[df['fleet_average_age'].str.match(decimal_pattern, na=False)]
        logging.info("Data validation completed on the aviationstack dataset")
        csv_data = df.to_csv(index=False)
        return csv_data
    except requests.exceptions.RequestException as e:
        raise Exception(f'Failed to retrieve data from the API: {e}')
    except ValueError:
        raise Exception('Failed to parse JSON data from the API response.')

#method to convert json to csv
def json_to_csv(json_data, fieldnames):
    # Convert JSON data to a CSV string
    csv_data = [fieldnames]

    for item in json_data['data']:
        row = [str(item.get(field, '')) for field in fieldnames]
        csv_data.append(row)
    logging.info("Converted AviationStack Json data to CSV")
    return '\n'.join([','.join(row) for row in csv_data])


#uploading the file to azure storage account
def upload_to_azure_blob_storage(container_name, csv_data, connection_string,blob_file_name ,directory_name):
    try:
        logging.info("Trying to upload the file to azure storage")
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        if not container_client.exists():
            logging.info("Container does not exists, creating a new container. ")
            container_client.create_container()
        blob_client = container_client.get_blob_client(f"{directory_name}/{blob_file_name}")
        current_date = datetime.now()
        min_days_in_future = 1
        max_days_in_future = 30
        csv_file = StringIO(csv_data)
        df = pd.read_csv(csv_file)
        df['Journey Date'] = [
            (current_date + timedelta(days=random.randint(min_days_in_future, max_days_in_future))).strftime('%Y-%m-%d')
            for _ in range(len(df))
        ]
        date_pattern='^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$'
        df = df[df['Journey Date'].str.match(date_pattern, na=False)]
        logging.info("Trying to upload file to Azure Storage")
        blob_client.upload_blob(df.to_csv(), overwrite=True)
        logging.info("Successfully uploaded the file to Azure Storage")
        return df.to_csv()
    except Exception as e:
        logging.info("Failed to upload the file to Azure Storage")
        raise Exception(f'Failed to upload data to Azure Blob Storage: {e}')


# Azure Blob Storage connection String
connection_string = read_connection_string(api_file_path)

airlines_dataset_name = "iamsouravbanerjee/airline-dataset"
csv_file_destination_path = "Data/downloads"

# Kaggle dataset download and upload to azure storage account
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
            blob_client = container_client.get_blob_client(f"{directory_name_DS2}/{blob_name}")
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


# uploading the transformed data to azure storage account
def upload_transformed_dataset(source):
    try:

        logging.info("Trying to upload transformed file to azure")
        transport = RequestsTransport(timeout=600)  # 1 hour timeout (adjust as needed)
        blob_service_client = BlobServiceClient.from_connection_string(connection_string, transport=transport)
        container_client = blob_service_client.get_container_client(containerName)
        blob_client = container_client.get_blob_client(f"{transformed_directory}/{transformed_airlines}")

        with open(source, "rb") as data:
            blob_client.upload_blob(
                data,
                blob_type="BlockBlob", overwrite=True
            )
        logging.info(
            "Dataset successfully transformed and uploaded to Azure Blob Storage, directory name: " + transformed_directory + " file name: " + blob_file_modified)
    except kaggle.api.KaggleApiError as e:
        print(f"Kaggle API error: {e}")
    except FileNotFoundError as e:
        print(f"File not found error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

# method to perform data validation using regex
def validating_Airlines_delay_data():
    global df2
    file_path = 'Data/downloads/Validated_Airline_Dataset.csv'
    alpha_pattern = '^[a-zA-Z]+$'
    continent_pattern = '^[a-zA-Z\s]+$'
    datePattern = '^(0?[1-9]|1[0-2])[-](0?[1-9]|[12][0-9]|3[01])[-]\d{4}$'
    df2 = df2[df2["Airport Country Code"].str.match(alpha_pattern, na=False)]
    df2 = df2[df2["Airport Continent"].str.match(alpha_pattern, na=False)]
    df2 = df2[df2["Continents"].str.match(continent_pattern, na=False)]
    df2 = df2[df2["Flight Status"].str.match(continent_pattern, na=False)]
    df2["Departure Date"] = df2["Departure Date"].str.replace('/', '-')
    df2 = df2[df2["Departure Date"].str.match(datePattern, na=False)]
    df2.to_csv(modified_dataset_file, index=False)
    df2.to_csv(file_path, index=False)
    upload_transformed_dataset(file_path)


def read_sql_query_from_file(file_path):
    with open(file_path, 'r') as file:
        sql_query = file.read()
    return sql_query


# method to perform database operations
def create_table():
    try:
        table_creation_query = read_sql_query_from_file('../Transformation/query.sql')

        connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:fode-sql-achutha.database.windows.net,1433;Database=fode-db-achutha;Uid=AdminServer-achutha;Pwd={Zeb170cr@ndsucs};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
            cursor.fast_executemany = True  # Enable fast_executemany for improved performance
            cursor.execute(table_creation_query)
            conn.commit()
            cursor.commit()
    except Exception as e:
        print(f"Error during table creation: {e}")

def insert_chunk(chunk):
    try:
        connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:fode-sql-achutha.database.windows.net,1433;Database=fode-db-achutha;Uid=AdminServer-achutha;Pwd={Zeb170cr@ndsucs};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

        insert_query = "INSERT INTO databaseSchema.FlightInformation (airportName, airportCountryCode, countryName, airportContinent, continents, departureDate, arrivalAirport, flightStatus, nationality) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"

        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
            cursor.fast_executemany = True  # Enable fast_executemany for improved performance
            cursor.executemany(insert_query, chunk)
            conn.commit()
            cursor.commit()
    except Exception as e:
        print(f"Error during data insertion: {e}")




def insert_chunk2(chunk):
    try:
        connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:fode-sql-achutha.database.windows.net,1433;Database=fode-db-achutha;Uid=AdminServer-achutha;Pwd={Zeb170cr@ndsucs};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

        insert_query = (
            "INSERT INTO databaseSchema.AirlineInformation "
            "(fleet_average_age, airline_id, callsign, hub_code,iata_code, icao_code, "
            "country_iso2,iata_prefix_accounting,airline_name, country_name,fleet_size,status, type, journey_date) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
        )

        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
            cursor.fast_executemany = True  # Enable fast_executemany for improved performance
            cursor.executemany(insert_query, chunk)
            conn.commit()
            cursor.commit()
    except Exception as e:
        print(f"Error during data insertion: {e}")

# method to perform insertion of data into database usinf threadpoolexecutor
def connect_to_azure_sql():

    try:
        create_table()
        df_av =read_csv_from_azure(container_name, "AviationData.csv")
        values_to_insert_table2 = [tuple(row) for row in df_av[['fleet_average_age','airline_id','callsign','hub_code','iata_code','icao_code','country_iso2', 'iata_prefix_accounting','airline_name', 'country_name','fleet_size','status','type','Journey Date']].values]

        csv_file_path = 'Data/downloads/Validated_Airline_Dataset.csv'
        df = pd.read_csv(csv_file_path)
        df['Departure Date'] = df['Departure Date'].apply(lambda x: datetime.strptime(x, '%m-%d-%Y').strftime('%Y-%m-%d'))
        values_to_insert_table = [tuple(row) for row in df[['Airport Name', 'Airport Country Code', 'Country Name', 'Airport Continent', 'Continents', 'Departure Date', 'Arrival Airport', 'Flight Status', 'Nationality']].values]

        with ThreadPoolExecutor(max_workers=4) as executor:
            # Split values into chunks and submit tasks to the executor
            chunk_size = len(values_to_insert_table) // 4
            chunks = [values_to_insert_table[i:i + chunk_size] for i in range(0, len(values_to_insert_table), chunk_size)]
            chunks_av = [values_to_insert_table2[i:i + len(values_to_insert_table2)] for i in range(0, len(values_to_insert_table2), len(values_to_insert_table2))]
            # Submit tasks to the executor
            futures_av = [executor.submit(insert_chunk2, chunk) for chunk in chunks_av]
            # Wait for all tasks to complete
            for future in futures_av:
                future.result()

            logging.info("Data insertion completed for dataset one")
        # Submit tasks to the executor
            futures = [executor.submit(insert_chunk, chunk) for chunk in chunks]
            # Wait for all tasks to complete
            for future in futures:
                future.result()
        logging.info("Data insertion completed for dataset two")

    except Exception as e:
        print(f"Error during data insertion: {e}")


def read_csv_from_azure(container_name, blob_name):

    blob_service_client = BlobServiceClient.from_connection_string(read_connection_string(api_file_path))
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=directory_name+"/"+blob_name)
    # Download the blob content
    blob_data = blob_client.download_blob()
    content = blob_data.readall()
    file_path = os.path.expanduser('Data/downloads/AviationData.csv')
    # Convert the content to a DataFrame
    df = pd.read_csv(io.StringIO(content.decode('utf-8')))
    df = df.drop(columns=df.columns[df.columns.str.contains('Unnamed:')])
    df.to_csv(file_path, index=False)
    return df


try:
    data = download_json_and_upload_to_azure_storage(api_url)
    csv_filename = f'Aviation_stack.csv'
    connection_string = read_connection_string(api_file_path)
    upload_to_azure_blob_storage(container_name,data,connection_string,blob_file_name,directory_name)
    azure_df = read_csv_from_azure(container_name, "AviationData.csv")
    dataset_upload(airlines_dataset_name, csv_file_destination_path)
    filename = 'Airline Dataset Updated - v2.csv'
    df2 = pd.read_csv('Data/downloads/Airline Dataset Updated - v2.csv')
    columns_to_drop = ['Passenger ID', 'First Name', 'Last Name', 'Gender', 'Age', 'Pilot Name']
    df2 = df2.drop(columns=columns_to_drop)
    df2.drop(df2[(df2['Arrival Airport'] == '0') | (df2['Arrival Airport'] == '-')].index, inplace=True)
    df2.to_csv('Data/downloads/Modified_Airline_Dataset.csv', index=False)
    transformed_airlines="transformed_airlines_delay.csv"
    modified_dataset_file="Data/downloads/Modified_Airline_Dataset.csv"
    upload_transformed_dataset(modified_dataset_file)
    validating_Airlines_delay_data()
    connect_to_azure_sql()

except Exception as e:
    print(f"An error occurred: {e}")
