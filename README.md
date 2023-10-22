# Data Engineering and Machine Learning Project

Welcome to the 
Fundamentals of Data Engineering Project! This project aims to provide a comprehensive data engineering solution for predicting the flight delays in future based on two dynamic datasets, 
along with machine learning techniques for predictive analysis. Below, you'll find an overview of the project, the datasets used, and the ingestion process.

## Project Overview
The name of the project is A Sky of flight analysis using machine learning
The project focuses on ingesting, processing, and cleaning two distinct dynamic datasets, making them ready for serving, and applying machine learning for predictive analysis. It is designed to showcase a complete data engineering and machine learning. The project follows a structured approach with the following main phases:

1. **Data Ingestion:** Fetching data from multiple sources and preparing it for processing.

2. **Data Preprocessing:** Cleaning and transforming the data, which includes handling missing values.

3. **Modeling and Machine Learning:** Applying machine learning techniques to the prepared data for predictive analysis.

## Datasets

### Dataset 1

- **Airlines delay dataset from Kaggle:** The first dataset is from kaggle which has data records of past flights journey conclusion which can be utilized to make the prediction using machine learning.
- **Data Format:** The data is in CSV format and accessed through the kaggle API.
- **Content:** This dataset contains details like source, destination, (delayed,On-time,cancelled), passenger details, date of journey these details can be used to predict the future filght delays with the help of the source and destination.

### Dataset 2

- **Source:** This is the second dataset of the project which has data related to the future scheduled flights from place to place these are the records on which the prediction is going to be targeted.
- **Data Format:** The data is in CSV format and accessed through API of aviationstack.com
- **Content:** This dataset contains the details of  the details like airline company name, status of flight, fleet size, type of flight. These are going to be the records on which the prediction will be done comparing the previous data records of flights from the kaggle dataset.   

## Data Ingestion

The data ingestion process involves collecting data from the specified sources. Custom Python scripts have been developed to facilitate this step, catering to the specific data source types and formats. The primary goal is to fetch the data required for further processing. You can find the data ingestion scripts in the `src/ingestion/Data/DataSet1.py, src/ingestion/Data/DataSet2.py)` directory. The data ingestion is being done in the cloud azure storage account, container name fodeprojectstorage.

## Getting Started

To get started with this project, follow these steps:

1. Clone this repository to your local machine running command:
 git clone https://github.com/dfroslie-ndsu-org/f23-project-achutha09.git
```bash

