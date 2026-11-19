# Crypto Market Data Pipeline

An end-to-end data engineering project that extracts real-time cryptocurrency market data from the CoinGecko API, orchestrates the workflow using Apache Airflow, stores the extracted data in Snowflake, and transforms it into analytics-ready datasets using dbt.

## Project Overview

This project demonstrates an automated data pipeline for collecting and transforming cryptocurrency market information.

The pipeline retrieves cryptocurrency market data through the CoinGecko API, processes the response using Python, and saves the extracted records into a CSV file. Apache Airflow orchestrates the workflow, while dbt performs data transformations in Snowflake using a layered data architecture.

The objective is to demonstrate practical data engineering concepts, including API integration, data extraction, workflow orchestration, cloud data warehousing, and ELT transformations.

## Architecture

```text
       CoinGecko API
             |
             v
        Python Script
       (API Extraction)
             |
             v
         CSV File
      (crypto_data.csv)
             |
             v
       Apache Airflow
       (Workflow DAG)
             |
             v
          Snowflake
             |
             v
          dbt Models
             |
             v
     +------------------+
     |  Bronze Layer    |
     |  Raw Data        |
     +------------------+
             |
             v
     +------------------+
     |  Silver Layer    |
     |  Cleaned Data    |
     +------------------+
             |
             v
     +------------------+
     |   Gold Layer     |
     |  Final Dataset   |
     +------------------+
```

## Technologies Used

| Technology | Purpose |
|---|---|
| Python | API integration and data extraction |
| CoinGecko API | Cryptocurrency market data source |
| Pandas | Data manipulation and CSV handling |
| Apache Airflow | Workflow orchestration and task scheduling |
| Snowflake | Cloud data warehouse |
| dbt | Data transformation and modeling |
| SQL | Data transformation and querying |
| Git & GitHub | Version control and project management |

## Data Source

**CoinGecko API**

Endpoint:

`/api/v3/coins/markets`

API parameters:

- `vs_currency`: USD
- `per_page`: 50

The pipeline retrieves market information for 50 cryptocurrencies.

### Selected Data Fields

| Column | Description |
|---|---|
| id | Unique cryptocurrency identifier |
| symbol | Cryptocurrency symbol |
| name | Cryptocurrency name |
| current_price | Current market price in USD |
| market_cap | Cryptocurrency market capitalization |

## Project Workflow

### 1. Data Extraction Using Python

- Integrated the CoinGecko API using Python and the Requests library.
- Retrieved cryptocurrency market data in JSON format.
- Configured the API request to fetch 50 cryptocurrency records in USD.
- Selected relevant fields, including cryptocurrency name, symbol, current price, and market capitalization.
- Converted the extracted data into a Pandas DataFrame.
- Exported the data into `crypto_data.csv`.

### 2. Workflow Orchestration Using Apache Airflow

Created an Airflow DAG to automate and manage the pipeline execution.

The DAG contains two tasks:

**Task 1: fetch_crypto_data**
- Executes the Python extraction script.
- Fetches data from the CoinGecko API.
- Generates the CSV file containing cryptocurrency market data.

**Task 2: run_dbt**
- Executes dbt transformations.
- Processes the data models in Snowflake.
- Runs after the successful completion of the extraction task.

The tasks are connected through Airflow dependencies to maintain the correct execution sequence.

### 3. Data Warehousing Using Snowflake

Used Snowflake as the cloud data warehouse for storing and transforming cryptocurrency data.

The extracted data is loaded into Snowflake and used as the source for dbt transformations.

### 4. Data Transformation Using dbt

Implemented a layered data transformation approach using dbt.

**Bronze Layer – Raw Data**
- Stores the initial cryptocurrency dataset.
- Maintains the source data for downstream transformations.

**Silver Layer – Cleaned Data**
- Applies data cleaning and transformation logic.
- Prepares structured data for further analysis.

**Gold Layer – Final Data**
- Contains the final transformed dataset.
- Provides a structured output for downstream analytics.

### dbt Models Created

| Model | Layer | Purpose |
|---|---|---|
| bronze_crypto | Bronze | Raw cryptocurrency data |
| silver_crypto | Silver | Cleaned and transformed data |
| gold_crypto | Gold | Final analytics-ready data |

## Project Execution Results

Successfully executed the pipeline components.

- CoinGecko API returned HTTP status code `200`.
- Successfully extracted 50 cryptocurrency records.
- Generated the `crypto_data.csv` file.
- Created and executed the Apache Airflow DAG with two tasks.
- Successfully executed dbt using dbt Core 1.11.8 and the Snowflake adapter 1.11.4.
- Successfully created all three dbt models in Snowflake.

**dbt execution result:**

- PASS: 3
- ERROR: 0

## Skills Demonstrated

- REST API Integration
- Python Programming
- Data Extraction and Processing
- Pandas Data Manipulation
- Apache Airflow DAG Development
- Workflow Orchestration
- Snowflake Data Warehousing
- dbt Data Transformation
- SQL
- Layered Data Architecture
- ETL/ELT Pipeline Development

## How to Run the Project

### 1. Clone the Repository

```bash
git clone https://github.com/faijanahamed11/crypto-data-pipeline.git
cd crypto-data-pipeline
```

### 2. Install Python Dependencies

Create a virtual environment:

```bash
python -m venv venv
```

Activate it on macOS/Linux:

```bash
source venv/bin/activate
```

Install the required packages:

```bash
pip install requests pandas apache-airflow
```

### 3. Configure CoinGecko API

Configure the API endpoint and request parameters in the Python extraction script.

### 4. Configure Snowflake

Set up your Snowflake connection through dbt profiles.

Configure the following details securely:

- Account
- User
- Password or authentication method
- Role
- Warehouse
- Database
- Schema

Do not commit passwords, API keys, or other credentials to GitHub.

### 5. Run Apache Airflow

Start your Airflow environment and configure the project DAG.

Trigger the DAG to execute the extraction and dbt transformation tasks.

### 6. Execute dbt Models

Navigate to the dbt project directory and execute:

```bash
dbt debug
```

Run the transformations:

```bash
dbt run
```

## Future Improvements

- Integrate AWS S3 for cloud-based raw data storage.
- Automate incremental cryptocurrency data ingestion.
- Implement data quality tests using dbt.
- Add logging and error-handling mechanisms.
- Build a Power BI dashboard for cryptocurrency market analysis.
- Implement historical price tracking and trend analysis.

## Author

**Faijan Ahamed**

Data Engineering | Python | SQL | Snowflake | Apache Airflow

- GitHub: [Faijan Ahamed](https://github.com/faijanahamed11)
- LinkedIn: [Faijan Ahamed](https://www.linkedin.com/in/faijanahamed/)

---

If you find this project useful, feel free to explore the repository and share your feedback!
