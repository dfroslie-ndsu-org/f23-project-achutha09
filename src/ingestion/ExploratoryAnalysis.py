import csv
import io

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
import configparser
from azure.storage.blob import BlobServiceClient

# Load the first dataset for analysis
data = pd.read_csv('Data/downloads/Airline Dataset Updated - v2.csv')

print("Dataset size:", data.shape)

numeric_columns = data.select_dtypes(include=[np.number])

def read_api_key(filepath):
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

print("Column names:", numeric_columns.columns)
print("Data types:\n", numeric_columns.dtypes)
print("Missing values:\n", numeric_columns.isnull().sum())
print("Summary statistics for 'Age':")
print(numeric_columns['Age'].describe())
plt.figure(figsize=(8, 6))
plt.hist(numeric_columns['Age'], bins=20, color='skyblue')
plt.title('Age Distribution')
plt.xlabel('Age')
plt.ylabel('Count')
plt.show()

gender_counts = data['Gender'].value_counts()
print("Gender distribution:\n", gender_counts)
gender_counts = data['Gender'].value_counts()
plt.figure(figsize=(8, 6))
gender_counts.plot(kind='bar', color='skyblue')
plt.title('Gender Distribution')
plt.xlabel('Gender')
plt.ylabel('Count')
plt.show()

data['Departure Date'] = pd.to_datetime(data['Departure Date'], errors='coerce')

data['Year'] = data['Departure Date'].dt.year
yearly_counts = data['Year'].value_counts().sort_index()
print("Yearly flight counts:\n", yearly_counts)

country_counts = data['Country Name'].value_counts()
print("Top 10 countries by flight counts:\n", country_counts.head(10))

df = pd.DataFrame(country_counts)
df = df.sort_values(by='count', ascending=True)
correlation_matrix = numeric_columns.corr()
print("Correlation matrix:\n", correlation_matrix)
plt.figure(figsize=(8, 6))
plt.boxplot(numeric_columns['Age'], vert=False)
plt.title('Age Boxplot')
plt.show()

on_time_flights = data[data['Flight Status'] == 'On Time']
cancelled_flights = data[data['Flight Status'] == 'Cancelled']
delayed_flights = data[data['Flight Status'] == 'Delayed']
on_time_count = on_time_flights.shape[0]
cancelled_count = cancelled_flights.shape[0]
delayed_count = delayed_flights.shape[0]
flight_statuses = ['On Time', 'Cancelled', 'Delayed']
count_data = [on_time_count, cancelled_count, delayed_count]
plt.bar(flight_statuses, count_data, color=['green', 'red', 'orange'])
plt.xlabel('Flight Status')
plt.ylabel('Count')
plt.title('Number of Flights by Status')
plt.show()


country_code_counts = data['Airport Country Code'].value_counts()
top_10_country_code_counts = country_code_counts.head(10)
plt.figure(figsize=(12, 6))
plt.bar(top_10_country_code_counts.index, top_10_country_code_counts.values, color='skyblue')
plt.xlabel('Airport Country Code')
plt.ylabel('Count')
plt.title('Top 10 Airport Country Codes by Number of Flights')
plt.xticks(rotation=90)
plt.show()




filtered_data = data[data['Arrival Airport'].apply(lambda x: x not in ['0', '-'])]
arrival_airport_counts = filtered_data['Arrival Airport'].value_counts()
top_10_arrival_airport_counts = arrival_airport_counts.head(10)
plt.figure(figsize=(12, 6))
plt.bar(top_10_arrival_airport_counts.index, top_10_arrival_airport_counts.values, color='skyblue')
plt.xlabel('Arrival Airport')
plt.ylabel('Count')
plt.title('Top 10 Arrival Airports by Number of Flights')
plt.xticks(rotation=90)  # Rotate x-axis labels for better visibility
plt.show()

# Analysis on the first dataset completed


# Load the second dataset for analysis
datasettwo = pd.read_csv('Data/downloads/AviationData (2).csv')

# Step 1: Data Inspection
print("Dataset size:", datasettwo.shape)
print("Column names:", datasettwo.columns)
print("Data types:\n", datasettwo.dtypes)
print("Missing values:\n", datasettwo.isnull().sum())

# Step 2: Descriptive Statistics
print("Summary statistics for 'fleet_average_age':")
print(datasettwo['fleet_average_age'].describe())

# Step 3: Data Visualization
plt.figure(figsize=(8, 6))
datasettwo['fleet_average_age'] = pd.to_numeric(datasettwo['fleet_average_age'], errors='coerce')
plt.title('Fleet Average Age Distribution')
plt.xlabel('Fleet Average Age')
plt.ylabel('Count')
plt.show()

# Step 4: Data Distribution
status_counts = datasettwo['status'].value_counts()
print("Status distribution:\n", status_counts)

# Step 5: Temporal Analysis
# Correct the date format for parsing
datasettwo['date_founded'] = pd.to_datetime(datasettwo['date_founded'], format="%Y")
datasettwo['year_founded'] = datasettwo['date_founded'].dt.year
yearly_counts = datasettwo['year_founded'].value_counts().sort_index()
print("Yearly founded counts:\n", yearly_counts)


non_numeric_values = ['AMERICAN','Gol Linhas Aéreas Inteligentes', 'Other non-numeric value']

# Exclude rows with non-numeric values in 'fleet_average_age'
datasettwo = datasettwo[~datasettwo['fleet_average_age'].isin(non_numeric_values)]
datasettwo['fleet_average_age'] = pd.to_numeric(datasettwo['fleet_average_age'], errors='coerce')
datasettwo = datasettwo.dropna(subset=['fleet_average_age'])
print(datasettwo)
# Plot the histogram for 'fleet_average_age'
plt.figure(figsize=(8, 6))
plt.hist(datasettwo['fleet_average_age'], bins=20, color='skyblue')
plt.title('Fleet Average Age Distribution')
plt.xlabel('Fleet Average Age')
plt.ylabel('Count')
plt.show()

