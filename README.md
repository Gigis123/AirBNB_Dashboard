# AirBNB_Dashboard
Develop a comprehensive dashboard using Kibana to analyze Airbnb property listings. The project involves automating data extraction, cleaning, and transformation processes using Apache Airflow, with daily updates scheduled at 6:30 UTC. The cleaned data will be imported into Elasticsearch for visualization in Kibana, aiding companies in making informed decisions about property rentals and evaluating the suitability of Airbnb as a rental platform.

# Project Objective
create a dashboard using kibana in order to analyze the properties listed in Airbnb by using an airflow to update the data everyday at 6.30. the result will then imported into elasticsearch and visualized using kibana. This visualization is done so that companies that are interested in renting their property could make a more informed decision on the best location to rent their properties and whether renting their property to Airbnb is the best decision

# Methods Used
- pandas
- psycopg2
- elasticsearch
- airflow
- great expectation

# Project Description
This project involves designing and implementing an automated data pipeline to facilitate comprehensive analysis of Airbnb property listings. The pipeline is constructed using Apache Airflow to manage and schedule a series of tasks that ensure the data is accurately fetched, cleaned, transformed, and visualized.

### Airflow Process:

Data Extraction: The first step of the pipeline involves extracting raw data from a PostgreSQL database. This data contains detailed information about Airbnb property listings, including features such as location, price, and availability.

Data Cleaning: Once the raw data is fetched, it undergoes a rigorous cleaning process using Pandas. This includes handling missing values, correcting data inconsistencies, and transforming data types to ensure the dataset is accurate and ready for analysis. The cleaning process involves removing unnecessary columns, filling in missing values, and standardizing data formats.

Data Transformation: After cleaning, the data is transformed from a SQL format to a NoSQL format using Elasticsearch. This transformation allows for efficient indexing and querying of the data, making it suitable for advanced analysis and visualization.

Data Validation: To ensure the accuracy and quality of the data, Great Expectations is utilized. This tool helps in validating the data against predefined expectations, identifying any anomalies or issues that could affect the reliability of the analysis.

Data Visualization: The cleaned and validated data is then imported into Elasticsearch, where it is indexed and made available for visualization. Kibana is used to create interactive dashboards and visualizations, providing valuable insights into the Airbnb property market. These visualizations help companies and stakeholders make informed decisions about property rentals and evaluate the benefits of listing their properties on Airbnb.

## Featured Links
* [Original Data](https://www.kaggle.com/datasets/arianazmoudeh/airbnbopendata)
