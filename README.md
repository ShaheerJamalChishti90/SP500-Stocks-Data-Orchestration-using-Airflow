# S&P 500 Stock Data Orchestration Pipeline

## Project Overview

This project implements an end-to-end data engineering pipeline to extract, transform, and load (ETL) S&P 500 stock market data. The pipeline is orchestrated using Apache Airflow running on an AWS EC2 instance. It extracts raw data from a financial API, processes it using Python (Pandas), stages the data in Amazon S3, and loads the final dataset into Snowflake for analytical querying.

## Architecture

The pipeline follows a standard ETL workflow:

1. **Extraction:** Python scripts query the Stock Market API to fetch real-time or historical data for S&P 500 companies.
2. **Transformation:** Data is cleaned, formatted, and validated using Pandas.
3. **Loading (Staging):** Processed CSV files are uploaded to an AWS S3 bucket for durable storage.
4. **Loading (Warehouse):** The data is copied from S3 into Snowflake tables for downstream analysis.
5. **Orchestration:** Apache Airflow manages the dependency management, scheduling, and error handling of these tasks.

## Tech Stack

* **Cloud Provider:** AWS (EC2, S3)
* **Orchestration:** Apache Airflow
* **Data Warehouse:** Snowflake
* **Language:** Python 3.9+
* **Libraries:** Pandas, Boto3, Snowflake-Connector-Python, Apache-Airflow
* **Infrastructure:** Linux (Ubuntu) on AWS EC2

## Project Structure

```text
sp500-data-pipeline/
├── dags/
│   └── sp500_pipeline_dag.py      # Airflow DAG definition
├── etl_scripts/
│   ├── extract.py                 # API data extraction logic
│   ├── transform.py               # Data cleaning and transformation
│   ├── load.py                    # Snowflake loading logic
│   └── utils/
│       └── s3_uploader.py         # Helper for S3 uploads
├── data/
│   ├── raw/                       # Local storage for raw extracts
│   └── processed/                 # Local storage for transformed data
├── config/
│   └── settings.yaml              # Configuration file (non-sensitive)
├── requirements.txt               # Python dependencies
└── README.md                      # Project documentation

```

## Prerequisites

* AWS Account with access to S3 and EC2.
* Snowflake Account.
* API Key for the stock data provider (e.g., Alpha Vantage, YFinance).
* Python 3.9 installed locally.

## Setup & Installation

**1. Clone the Repository**

```bash
git clone https://github.com/yourusername/sp500-data-pipeline.git
cd sp500-data-pipeline

```

**2. Environment Setup**
Create a virtual environment and install dependencies.

```bash
python3 -m venv sp500env
source sp500env/bin/activate
pip install -r requirements.txt

```

**3. Configuration**

* Create a `.env` file in the root directory to store your credentials (do not commit this file).
* Add the following variables:
```text
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
API_KEY=your_api_key

```



**4. Airflow Setup**

* Ensure Airflow is running on your EC2 instance.
* Copy the contents of the `dags/` folder to your Airflow `dags_folder` location.
* Restart the Airflow scheduler to detect the new DAG.

## Usage

1. Access the Airflow UI (typically at `http://<ec2-public-ip>:8080`).
2. Locate the DAG named `sp500_data_pipeline`.
3. Toggle the DAG to "On" to run on the defined schedule, or trigger it manually using the "Play" button.
4. Monitor the "Graph View" to ensure all tasks (Extract -> Transform -> Load_S3 -> Load_Snowflake) complete successfully.
5. Verify the data availability in your Snowflake table.
