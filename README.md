## Objective
Build a robust data pipeline to extract Zillow real estate data, load it into a local PostgreSQL database, and transform it to analyze quarterly house price trends across various U.S. states.

## Pipeline Overview
Extract: Gather real estate house prices from Zillow using selenium and beautiful soap for scrapping data from the web (CSV/JSON APIs).
Load: Load the extracted data into a local storage.
Transform: Process and clean the data, compute quarterly trends.
Load: Loading the transform and process data into a database (Postres).

## Pipeline Design
1. Tools and Technologies
    Data Extraction: Python (selenium, pandas)
    Data Storage: Database (PostgreSQL)
    Data Processing and Transformation: Pandas
    Workflow Tool: Apache Airflow
    Orchestration: Docker-Compose
    Containerization: Docker
    Local File Storage: For raw and intermediate files

 ## Requirements packages
     Apache-Airflow
     Selenium
     psycopg2-binary
     pandas
     docker

## Getting Started
1. Create a python environment.
2. clone the project using (https://github.com/cisco00/realestate-data-pipeline.git)
3. cd realestate-data-pipeline
4. install the requirements using (pip install requirement.txt)
5. Setup your postgres database
6. Build and start service but, ensure Docker and Docker Compose are installed on your system. Run (docker-compose up --build)

7. Pls Note the zillow.com website is being configure every month so be sure to check your extract scripts to effect the changes being made.
     
                                                                                    ## Zillow Real Estate Data Pipeline
## Objective
Build a robust data pipeline to extract Zillow real estate data, load it into a local PostgreSQL database, and transform it to analyze quarterly house price trends across various U.S. states.

## Pipeline Overview
    Extract: Gather real estate house prices from Zillow using Selenium and Beautiful Soup for web scraping (CSV/JSON APIs).
    Load: Save the extracted data into local storage.
    Transform: Process and clean the data to compute quarterly trends.
    Load to Database: Store the processed data into a PostgreSQL database for further analysis.
    
## Pipeline Design
# 1. Tools and Technologies
      Data Extraction: Python (Selenium, pandas)
      Data Storage: Database (PostgreSQL)
      Data Processing and Transformation: pandas
      Workflow Management: Apache Airflow
      Orchestration: Docker-Compose
      Containerization: Docker
      Local File Storage: For raw and intermediate data files
  
##  Required Packages
      Ensure the following dependencies are installed in your environment:
      apache-airflow
      selenium
      psycopg2-binary
      pandas
      docker
      
## Getting Started
1. Set Up Python Environment
Create a Python virtual environment and activate it:

 2. Clone the Project
 git clone https://github.com/cisco00/realestate-data-pipeline.git
 cd realestate-data-pipeline

3. Install Requirements
pip install -r requirements.txt

5. Set Up PostgreSQL Database
Ensure PostgreSQL is installed locally or running via Docker. Configure your database credentials to match the project settings in docker-compose.yml or .env.

6. Build and Start Services
Ensure Docker and Docker Compose are installed on your system. Then, run:
docker-compose up --build

6. Verify the Extraction Script
The Zillow website frequently updates its structure on the 12th of every month.

Regularly check the data extraction scripts to handle any structural changes.
Update Selenium selectors or Beautiful Soup parsers as required.
