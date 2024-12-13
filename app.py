import os
import zipfile
import pandas as pd
import logging
import json
import sqlite3
import glob
import tempfile
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from jinja2 import Environment, FileSystemLoader
import time
from functools import wraps
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import orjson
import threading
from rich.console import Console
from rich.progress import Progress
from rich.spinner import Spinner
from rich.text import Text


# Dictionary to store execution times
execution_times = defaultdict(float)

# Configure logging
logging.basicConfig(
    filename='file_log.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

zip_dir = 'import'
temp_dir = tempfile.TemporaryDirectory()
output_dir = temp_dir.name

def timeit(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_times[func.__name__] += end_time - start_time
        return result
    return wrapper

@timeit
def extract_zip(zip_path, extract_to):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

@timeit
def process_zip_files(zip_dir, output_dir):
    for zip_file in os.listdir(zip_dir):
        if zip_file.endswith('.zip'):
            logging.info(f'Processing: {zip_file}')
            folder_name = os.path.splitext(zip_file)[0]
            extract_path = os.path.join(output_dir, folder_name)
            os.makedirs(extract_path, exist_ok=True)
            extract_zip(os.path.join(zip_dir, zip_file), extract_path)

@timeit
def recursive_extract(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.zip'):
                zip_path = os.path.join(root, file)
                folder_name = os.path.splitext(file)[0]
                extract_path = os.path.join(root, folder_name)
                os.makedirs(extract_path, exist_ok=True)
                extract_zip(zip_path, extract_path)
                os.remove(zip_path)
                recursive_extract(extract_path)

@timeit
def convert_xlsx_to_json(xlsx_path, json_path):
    """
    Convert a single .xlsx file to .json using multithreading to handle I/O operations.
    """
    try:
        df = pd.read_excel(xlsx_path, engine='openpyxl', usecols=None, dtype=str, na_filter=False)
        
        # Define a function to serialize and write JSON
        def serialize_and_write():
            json_bytes = orjson.dumps(df.to_dict(orient='records'), option=orjson.OPT_APPEND_NEWLINE)
            with open(json_path, 'wb') as f:
                f.write(json_bytes)
        
        # Create a thread for serialization and writing
        thread = threading.Thread(target=serialize_and_write)
        thread.start()
        thread.join()
        
    except Exception as e:
        logging.error(f"Failed to convert {xlsx_path} to JSON: {e}")
        raise

@timeit
def traverse_and_convert(directory, max_workers=4):
    def process_file(file_path):
        try:
            json_filename = os.path.splitext(file_path)[0] + '.json'
            convert_xlsx_to_json(file_path, json_filename)
            os.remove(file_path)
            logging.info(f'Removed: {file_path}')
        except Exception as e:
            logging.error(f'Error processing {file_path}: {e}')
            
    if os.cpu_count() > max_workers:
        max_workers = os.cpu_count()
        
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.endswith('.xlsx'):
                    xlsx_path = os.path.join(root, file)
                    futures.append(executor.submit(process_file, xlsx_path))
        
        for future in as_completed(futures):
            future.result()  # To raise exceptions if any

@timeit
def remove_empty_fields(data):
    if isinstance(data, dict):
        return {k: remove_empty_fields(v) for k, v in data.items() if v not in [None, {}, [], ""]}
    elif isinstance(data, list):
        cleaned_list = [remove_empty_fields(item) for item in data]
        return [item for item in cleaned_list if item not in [None, {}, [], ""]]
    else:
        return data

@timeit
def clean_json_fields(input_file, output_file, fields_to_remove):
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError:
        return False
    except Exception:
        return False

    if isinstance(data, list):
        if not data or all(not item for item in data):
            return False
        cleaned_data = []
        for item in data:
            if isinstance(item, dict):
                for field in fields_to_remove:
                    item.pop(field, None)
                cleaned_item = remove_empty_fields(item)
                if cleaned_item:
                    cleaned_data.append(cleaned_item)
            else:
                cleaned_item = remove_empty_fields(item)
                if cleaned_item:
                    cleaned_data.append(cleaned_item)
    elif isinstance(data, dict):
        for field in fields_to_remove:
            data.pop(field, None)
        cleaned_data = remove_empty_fields(data)
    else:
        cleaned_data = remove_empty_fields(data)

    if not cleaned_data:
        logging.info(f'{input_file} has no meaningful data after cleaning. Skipping creating cleaned file.')
        return False

    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(cleaned_data, f, indent=4)
        return True
    except Exception:
        return False

@timeit
def clean_jsons(directory):
    fields_to_remove = ['css', 'html', 'js']
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.json'):
                json_path = os.path.join(root, file)
                clean_json_fields(json_path, json_path, fields_to_remove)

@timeit
def create_sqlite_tables():
    db_path = 'database.db'
    unwanted_substrings = ['tmhls', 'configuration']

    if not os.path.exists(db_path):
        open(db_path, 'w').close()

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_name TEXT UNIQUE
        )
    ''')

    project_names = [
        name for name in os.listdir(output_dir)
        if os.path.isdir(os.path.join(output_dir, name))
    ]

    for project in project_names:
        try:
            cursor.execute('INSERT INTO projects (project_name) VALUES (?)', (project,))
        except sqlite3.IntegrityError:
            pass

    json_files = glob.glob(os.path.join(output_dir, '**', '*.json'), recursive=True)

    for file in json_files:
        with open(file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        keylist = ['SsioList']
        if isinstance(data, dict) and any(key in data for key in keylist):
            data = flatten_if_contains_keys(data)        
        
        if isinstance(data, list) and len(data) > 0:
            table_name = os.path.splitext(os.path.basename(file))[0]
            for substr in unwanted_substrings:
                table_name = table_name.replace(substr, '').strip()
            
            relative_path = os.path.relpath(file, output_dir)
            parts = relative_path.split(os.sep)
            project_name = parts[0] if len(parts) > 1 else 'Unknown'

            desired_columns = set(list(data[0].keys()) + ['project_name'])

            cursor.execute(f'PRAGMA table_info("{table_name}")')
            existing_columns = set([info[1] for info in cursor.fetchall()])

            missing_columns = desired_columns - existing_columns

            for col in missing_columns:
                try:
                    cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" TEXT')
                except sqlite3.OperationalError:
                    columns_def = ', '.join([f'"{c}" TEXT' for c in desired_columns])
                    cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                    cursor.execute(f'CREATE TABLE "{table_name}" ({columns_def});')
                    break

            cursor.execute(f'PRAGMA table_info("{table_name}")')
            updated_columns = [info[1] for info in cursor.fetchall()]

            columns_formatted = ', '.join([f'"{col}"' for col in updated_columns])
            placeholders = ', '.join(['?'] * len(updated_columns))

            for item in data:
                values = [str(item.get(col, '')) if col != 'project_name' else project_name for col in updated_columns]
                cursor.execute(
                    f'INSERT INTO "{table_name}" ({columns_formatted}) VALUES ({placeholders})',
                    values
                )

    conn.commit()
    conn.close()

@timeit
def flatten_if_contains_keys(data):    
    flattened_list = []
    if isinstance(data, dict):
        for main_key, items in data.items():
            if isinstance(items, list):
                for item in items:
                    item['MainKey'] = main_key
                    flattened_list.append(item)
    return flattened_list

@timeit
def table_exists(cursor, table_name):
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name=?
    """, (table_name,))
    return cursor.fetchone() is not None

@timeit
def execute_query(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()

@timeit
def fetch_all_counts(cursor, queries):
    results = {}
    for key, query in queries.items():
        table_name = query.split('FROM')[1].split('\n')[0].strip().strip('"')
        if table_exists(cursor, table_name):
            project_names = [row[0] for row in execute_query(cursor, f'SELECT DISTINCT project_name FROM "{table_name}"')]
            for project in project_names:
                if project not in results:
                    results[project] = {}
            for row in execute_query(cursor, query):
                project_name, count = row
                results[project_name][key] = int(count)
    return results

@timeit
def transform_to_dataframe(results):
    df = pd.DataFrame.from_dict(results, orient='index').fillna(0)
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'Project Name'}, inplace=True)
    for col in df.columns:
        if col != 'Project Name':
            df[col] = df[col].astype(int).replace(0, 'N/A')
    return df

@timeit
def calculate_usage(df):
    row_count = len(df)
    x_counts = (df == 'N/A').sum()
    usage_percent = ((row_count - x_counts) / row_count) * 100
    x_counts_formatted = (row_count - x_counts).astype(str) + f"/{row_count}" + " (" + usage_percent.round(2).astype(str) + "%)"
    x_counts_formatted['Project Name'] = 'Usage'
    return x_counts_formatted

@timeit
def create_plots(df):
    df['Total PickUp and DropOff'] = df['PickUp Locations'].replace('N/A', 0).astype(int) + df['DropOff Locations'].replace('N/A', 0).astype(int)
    df_sorted = df.sort_values(by='Total PickUp and DropOff', ascending=True)

    fig = make_subplots(
        rows=1, cols=2,
        column_widths=[0.5, 0.5],
        specs=[[{"type": "bar"}, {"type": "scatter"}]]
    )

    fig.add_trace(go.Bar(
        x=df_sorted['Project Name'],
        y=df_sorted['PickUp Locations'].replace('N/A', 0).astype(int),
        name='PickUp Locations',
        marker_color='rgb(55, 83, 109)'
    ), row=1, col=1)

    fig.add_trace(go.Bar(
        x=df_sorted['Project Name'],
        y=df_sorted['DropOff Locations'].replace('N/A', 0).astype(int),
        name='DropOff Locations',
        marker_color='rgb(26, 118, 255)'
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=df_sorted['Project Name'],
        y=df_sorted['Total PickUp and DropOff'],
        mode='markers',
        marker=dict(size=15, color='rgb(255, 0, 0)'),
        name='Total PickUp and DropOff'
    ), row=1, col=2)

    fig.update_layout(
        title='Total PickUp and DropOff Locations',
        yaxis_title='Count',
        barmode='stack',
        height=400,
        margin=dict(l=50, r=50, t=50, b=50),
        showlegend=True
    )

    return fig

@timeit
def generate_html_report(df_table, x_counts_json, plot_html):
    env = Environment(loader=FileSystemLoader('.'))
    template = env.get_template('template.html')
    
    html_content = template.render(
        table_html=df_table.to_html(classes='table table-striped table-hover', index=False),
        x_counts_json=x_counts_json,
        plot_html=plot_html
    )
    
    html_content = html_content.replace('</thead>', '</thead><tfoot><tr>' + ''.join(['<th></th>' for _ in df_table.columns]) + '</tr></tfoot>')
    
    return generate_report(html_content)
 
def generate_report(html_content):
    report_filename = 'report.html'
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    # Get the absolute path of the report file
    report_path = os.path.abspath(report_filename)
    
    # Return the path of the report file
    return report_path 
 
@timeit       
def get_all_counts():
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    
        # Define query configurations
    query_definitions = [
        {'key': 'Vehicles', 'table': 'Vehicles', 'condition': None},
        {'key': 'Vehicle Types', 'table': 'Tmhls.VehicleType.Configuration', 'condition': None},
        {'key': 'Shuttles', 'table': 'Vehicles', 'condition': 'metadata LIKE "%ip%"'},
        {'key': 'PickUp Locations', 'table': 'Tmhls.Layout.Configuration', 'condition': "Action = 'PickUp'"},
        {'key': 'DropOff Locations', 'table': 'Tmhls.Layout.Configuration', 'condition': "Action = 'DropOff'"},
        {'key': 'Screens', 'table': 'Tmhls.Screen.Configuration', 'condition': "template = 'False'"},
        {'key': 'IO Signals', 'table': 'Tmhls.IO.Configuration', 'condition': None},
        {'key': 'Storage Locations', 'table': 'Tmhls.StorageLayout.StorageLocations.Configuration', 'condition': "length != 'Integer'"},
        {'key': 'Reservation Strategies', 'table': 'Tmhls.StorageReservation.ReservationStrategies.Configuration', 'condition': None},
        {'key': 'Location Metadata', 'table': 'Tmhls.StorageLayout.LocationDataSchemas.Configuration', 'condition': None},
        {'key': 'Load Metadata', 'table': 'Tmhls.Inventory.LoadDataSchemas.Configuration', 'condition': None},
        {'key': 'Storage Areas', 'table': 'Tmhls.StorageLayout.StorageAreas.Configuration', 'condition': None},
        {'key': 'Scanning Configs', 'table': 'Tmhls.Scanning.Configuration', 'condition': None},
        {'key': 'OPCua Configs', 'table': 'Tmhls.OPCUA.Configuration', 'condition': None},
        {'key': 'Event Definitions', 'table': 'Tmhls.Workflow.EventDefinitions.Configuration', 'condition': None},
        {'key': 'Tables', 'table': 'Tables', 'condition': None},
    ]
    
    # Dynamically generate the queries dictionary
    queries = {}
    for q in query_definitions:
        query = f'''
            SELECT project_name, COUNT(*) as count
            FROM "{q['table']}"
        '''
        if q['condition']:
            query += f" WHERE {q['condition']}\n"
        query += 'GROUP BY project_name'
        queries[q['key']] = query
    
    results = fetch_all_counts(cursor, queries)
    conn.close()
    
    df = transform_to_dataframe(results)
    x_counts_formatted = calculate_usage(df)
    
    logging.info(df)
    
    fig = create_plots(df)
    plot_html = fig.to_html(full_html=False, include_plotlyjs='cdn')
    
    x_counts_json = x_counts_formatted.to_json()
    
    return generate_html_report(df, x_counts_json, plot_html)

console = Console()

def print_top_functions(n=5):
    sorted_funcs = sorted(execution_times.items(), key=lambda x: x[1], reverse=True)
    console.log("[green]Execution time spent on:")
    for func, t in sorted_funcs[:n]:
        print(f"{func}: {t:.4f} seconds")
        
@timeit
def main():
    # Step 1: Remove existing database.db if it exists
    if os.path.exists('database.db'):
        with console.status("[bold red]Removing existing `database.db` file...", spinner="dots"):
            logging.info('Removing existing database.db file.')
            os.remove('database.db')
            console.log("[green]`database.db` removed successfully.")

    # Step 2: Create output directory if it does not exist
    with console.status("[bold blue]Creating output directory...", spinner="dots"):
        os.makedirs(output_dir, exist_ok=True)
        console.log("[green]Output directory is ready.")

    # Step 3: Extract files recursively
    with console.status("[bold yellow]Extracting ZIP files...", spinner="dots"):
        process_zip_files(zip_dir, output_dir)
        recursive_extract(output_dir)
        console.log("[green]ZIP files extracted successfully.")

    # Step 4: Traverse and convert files
    with console.status("[bold magenta]Converting `.xlsx` files to `.json`...", spinner="dots"):
        traverse_and_convert(output_dir)
        console.log("[green]File conversion completed.")

    # Step 5: Clean JSON files
    with console.status("[bold cyan]Cleaning JSON files...", spinner="dots"):
        clean_jsons(output_dir)
        console.log("[green]JSON files cleaned successfully.")

    # Step 6: Create SQLite tables
    with console.status("[bold green]Creating structured data...", spinner="dots"):
        create_sqlite_tables()
        console.log("[green]Data created successfully.")

    # Step 7: Get all counts
    with console.status("[bold white]Counting cats and dogs (and also aliens) to create the report...", spinner="dots"):
        report_path = get_all_counts()
        console.log("[green]Report created successfully here: " + report_path)

if __name__ == "__main__":
    try:
        main()
    finally:
        print_top_functions()