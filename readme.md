# Tone Config Analyzer

## Overview

The Tone Config Analyzer is a Python-based tool designed to process ZIP files containing T-ONE config exports. Cleans the JSON data, and store the results in an SQLite database. It then generates a comprehensive HTML report with visualizations and data summaries.

## Features

- **ZIP File Extraction**: Recursively extracts ZIP files.
- **Excel to JSON Conversion**: Converts `.xlsx` files to `.json` format using multithreading.
- **JSON Cleaning**: Removes unwanted fields and empty values from JSON files.
- **SQLite Database Creation**: Creates and populates SQLite tables with the cleaned JSON data.
- **Data Analysis**: Counts various entities and generates a summary report.
- **HTML Report Generation**: Creates an HTML report with tables and visualizations.

## How It Works

1. **Setup and Initialization**:
    - Configures logging and sets up directories.
    - Defines a decorator to measure execution time for functions.

2. **ZIP File Processing**:
    - `process_zip_files`: Processes ZIP files in the specified directory.
    - `recursive_extract`: Recursively extracts nested ZIP files.

3. **Excel to JSON Conversion**:
    - `convert_xlsx_to_json`: Converts Excel files to JSON format using multithreading.
    - `traverse_and_convert`: Traverses directories and converts all `.xlsx` files to `.json`.

4. **JSON Cleaning**:
    - `remove_empty_fields`: Recursively removes empty fields from JSON data.
    - `clean_json_fields`: Cleans specific fields from JSON files.
    - `clean_jsons`: Cleans all JSON files in the specified directory.

5. **SQLite Database Creation**:
    - `create_sqlite_tables`: Creates and populates SQLite tables with the cleaned JSON data.
    - `flatten_if_contains_keys`: Flattens nested JSON data if specific keys are present.

6. **Data Analysis and Report Generation**:
    - `fetch_all_counts`: Fetches counts of various entities from the database.
    - `transform_to_dataframe`: Transforms the results into a Pandas DataFrame.
    - `calculate_usage`: Calculates usage percentages for the entities.
    - `create_plots`: Generates visualizations using Plotly.
    - `generate_html_report`: Generates an HTML report with tables and visualizations.

7. **Execution and Logging**:
    - `main`: Orchestrates the entire process from ZIP extraction to report generation.
    - `print_top_functions`: Logs the top functions by execution time.

## Usage

1. Place the ZIP files to be processed in the `import` directory.
2. Run the script using the command:
    ```bash
    python app.py
    ```
3. The script will generate an HTML report (`report.html`) in the current directory.

## Dependencies

- Python 3.x
- pandas
- plotly
- jinja2
- orjson
- rich

Install the dependencies using:
```bash
pip install -r requirements.txt
```

## Creating a virtual environment is strongly advised.
## Creating a Virtual Environment

1. Create a virtual environment:
    ```bash
    python -m venv .venv
    ```
2. Activate the virtual environment:
    - On Windows:
        ```bash
        .venv\Scripts\activate
        ```
    - On macOS and Linux:
        ```bash
        source .venv/bin/activate
        ```

3. Install the dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## Logging

Logs are stored in `file_log.log` and include information about the processing steps and any errors encountered.
