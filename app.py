import os
import zipfile
import pandas as pd
import logging
import json
import sqlite3
import os
import json
import glob

# Configure logging
logging.basicConfig(
    filename='file_log.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

zip_dir = 'import'
output_dir = 'extracted'

def extract_zip(zip_path, extract_to):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)


for zip_file in os.listdir(zip_dir):
    if zip_file.endswith('.zip'):
        folder_name = os.path.splitext(zip_file)[0]
        extract_path = os.path.join(output_dir, folder_name)
        os.makedirs(extract_path, exist_ok=True)
        extract_zip(os.path.join(zip_dir, zip_file), extract_path)

# Recursive extraction
def recursive_extract(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.zip'):
                zip_path = os.path.join(root, file)
                folder_name = os.path.splitext(file)[0]
                extract_path = os.path.join(root, folder_name)
                os.makedirs(extract_path, exist_ok=True)
                extract_zip(zip_path, extract_path)
                os.remove(zip_path)  # Remove zip after extraction
                recursive_extract(extract_path)

def convert_xlsx_to_json(xlsx_path, json_path):
    df = pd.read_excel(xlsx_path)
    df.to_json(json_path, orient='records', indent=4)

def traverse_and_convert(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.xlsx'):
                xlsx_path = os.path.join(root, file)
                json_filename = os.path.splitext(file)[0] + '.json'
                json_path = os.path.join(root, json_filename)
                convert_xlsx_to_json(xlsx_path, json_path)
                os.remove(xlsx_path)
                logging.info(f'Removed: {xlsx_path}')

def remove_empty_fields(data):
    """
    Recursively removes empty fields from JSON data.

    Args:
        data (dict or list): The JSON data to clean.

    Returns:
        The cleaned JSON data.
    """
    if isinstance(data, dict):
        return {k: remove_empty_fields(v) for k, v in data.items() if v not in [None, {}, [], ""]}
    elif isinstance(data, list):
        cleaned_list = [remove_empty_fields(item) for item in data]
        return [item for item in cleaned_list if item not in [None, {}, [], ""]]
    else:
        return data


def clean_json_fields(input_file, output_file, fields_to_remove):
    """
    Removes specified bulky fields from a JSON file, cleans empty fields recursively,
    and saves the cleaned data.

    Args:
        input_file (str): The path to the original JSON file.
        output_file (str): The path where the cleaned JSON file will be saved.
        fields_to_remove (list): List of field names to remove from each JSON object.

    Returns:
        bool: True if cleaned file was created, False otherwise.
    """
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        return False
    except Exception as e:
        return False

    # Remove specified fields
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
        print(f'ℹ️ {input_file} has no meaningful data after cleaning. Skipping creating cleaned file.')
        return False

    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(cleaned_data, f, indent=4)
        return True
    except Exception as e:
        return False

def clean_jsons(directory):
    fields_to_remove = ['css', 'html', 'js']
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.json'):
                json_path = os.path.join(root, file)
                clean_json_fields(json_path, json_path, fields_to_remove)

def create_sqlite_tables():
    db_path = 'database.db'
    extracted_dir = os.path.join('extracted')
    unwanted_substrings = ['tmhls', 'configuration']

    # Create the database file if it does not exist
    if not os.path.exists(db_path):
        open(db_path, 'w').close()

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create the projects table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_name TEXT UNIQUE
        )
    ''')

    # Get all first-level subdirectories as project names
    project_names = [
        name for name in os.listdir(extracted_dir)
        if os.path.isdir(os.path.join(extracted_dir, name))
    ]

    # Insert project names into the projects table
    for project in project_names:
        try:
            cursor.execute('INSERT INTO projects (project_name) VALUES (?)', (project,))
        except sqlite3.IntegrityError:
            pass  # Ignore duplicates

    # Process JSON files
    json_files = glob.glob(os.path.join(extracted_dir, '**', '*.json'), recursive=True)

    for file in json_files:
        with open(file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if isinstance(data, list) and len(data) > 0:
            table_name = os.path.splitext(os.path.basename(file))[0]
            for substr in unwanted_substrings:
                table_name = table_name.replace(substr, '').strip()
            
            # Extract the first child folder as project_name
            relative_path = os.path.relpath(file, extracted_dir)
            parts = relative_path.split(os.sep)
            if len(parts) > 1:
                project_name = parts[0]
            else:
                project_name = 'Unknown'

            # Define desired columns
            desired_columns = set(list(data[0].keys()) + ['project_name'])

            # Get existing columns in the table
            cursor.execute(f'PRAGMA table_info("{table_name}")')
            existing_columns = set([info[1] for info in cursor.fetchall()])

            # Determine missing columns
            missing_columns = desired_columns - existing_columns

            # Add missing columns
            for col in missing_columns:
                try:
                    cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" TEXT')
                except sqlite3.OperationalError:
                    # If ALTER TABLE fails, create the table
                    columns_def = ', '.join([f'"{c}" TEXT' for c in desired_columns])
                    cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                    cursor.execute(f'CREATE TABLE "{table_name}" ({columns_def});')
                    break  # Exit the loop after recreating the table

            # Get the updated columns after possible ALTER or CREATE
            cursor.execute(f'PRAGMA table_info("{table_name}")')
            updated_columns = [info[1] for info in cursor.fetchall()]

            # Prepare columns and placeholders for insertion
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

def get_all_counts():
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    
    queries = {
        'Vehicles': '''
            SELECT project_name, COUNT(*) as count
            FROM "Vehicles"
            GROUP BY project_name
        ''',
        'Vehicle Types': '''
            SELECT project_name, COUNT(*) as count
            FROM "Tmhls.VehicleType.Configuration"
            GROUP BY project_name
        ''',
        'Kollmorgen Vehicles': '''
            SELECT project_name, count(*) 
            FROM "Vehicles" 
            WHERE metadata like "%kollmorgen%" 
            GROUP BY project_name
        ''',
        'Shuttles': '''
            SELECT project_name, count(*) 
            FROM "Vehicles" 
            WHERE metadata like "%EAB%" 
            GROUP BY project_name
        ''',
        'PickUp Locations': '''
            SELECT project_name, COUNT(*) as pickup_count
            FROM "Tmhls.Layout.Configuration"
            WHERE Action = 'PickUp'
            GROUP BY project_name
        ''',
        'DropOff Locations': '''
            SELECT project_name, COUNT(*) as dropoff_count
            FROM "Tmhls.Layout.Configuration"
            WHERE Action = 'DropOff'
            GROUP BY project_name
        ''',
        'Screens': '''
            SELECT project_name, COUNT(*) as count
            FROM "Tmhls.Screen.Configuration"
            WHERE template = "False"
            GROUP BY project_name
        ''',
        'IO Signals': '''
            SELECT project_name, COUNT(*) as count
            FROM "Tmhls.IO.Configuration"
            GROUP BY project_name
        ''',
        'Storage Locations': '''
            SELECT project_name, COUNT(*) as count
            FROM "Tmhls.StorageLayout.StorageLocations.Configuration"
			WHERE length != "Integer"
            GROUP BY project_name
        ''',
        'Reservation Strategies': '''
            SELECT project_name, COUNT(*) as count
            FROM "Tmhls.StorageReservation.ReservationStrategies.Configuration"
            GROUP BY project_name
        ''',
        'Location Metadata': '''
            SELECT project_name, COUNT(*) as count
            FROM "Tmhls.StorageLayout.LocationDataSchemas.Configuration"
            GROUP BY project_name
        ''',
        'Load Metadata': '''
            SELECT project_name, COUNT(*) as count
            FROM "Tmhls.Inventory.LoadDataSchemas.Configuration"
            GROUP BY project_name
        ''',
        'Storage Areas': '''
            SELECT project_name, COUNT(*) as count
            FROM "Tmhls.StorageLayout.StorageAreas.Configuration"
            GROUP BY project_name
        ''',
        'Scanning Configs': '''
            SELECT project_name, COUNT(*) as count
            FROM "Tmhls.Scanning.Configuration"
            GROUP BY project_name
        ''',
        'OPCua Configs': '''
            SELECT project_name, COUNT(*) as count
            FROM "Tmhls.OPCUA.Configuration"
            GROUP BY project_name
        ''',
        'Event Definitions': '''
            SELECT project_name, COUNT(*) as count
            FROM "Tmhls.Workflow.EventDefinitions.Configuration"
            GROUP BY project_name
        ''',
        'Tables': '''
            SELECT project_name, COUNT(*) as count
            FROM "Tables"
            GROUP BY project_name
        '''
    }
    
    results = {}
    for key, query in queries.items():
        # Check if the table exists
        table_name = query.split('FROM')[1].split('\n')[0].strip()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name={table_name}")
        if cursor.fetchone():
            # Fetch all distinct project names
            cursor.execute(f"SELECT DISTINCT project_name FROM {table_name}")
            all_project_names = [row[0] for row in cursor.fetchall()]
        
            # Initialize results with 0 for each project_name
            for project_name in all_project_names:
                if project_name not in results:
                    results[project_name] = {}
                results[project_name][key] = 0
            
            cursor.execute(query)
            for row in cursor.fetchall():
                project_name = row[0]
                count = int(row[1])
                if project_name not in results:
                    results[project_name] = {}
                results[project_name][key] = int(count)
    
    conn.close()
    
    # Convert results to a DataFrame
    df = pd.DataFrame.from_dict(results, orient='index').fillna(0)
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'project_name'}, inplace=True)

    # Convert all number fields to int and replace 0 with 'X'
    for col in df.columns:
        if col != 'project_name':
            df[col] = df[col].astype(int).replace(0, 'X')

    # Calculate the count of 'X' values in each column
    row_count = len(df)
    x_counts = (df == 'X').sum()
    x_counts_formatted = (row_count - x_counts).astype(str) + f"/{row_count}"
    x_counts_formatted['project_name'] = 'Feature usage'

    # Print the DataFrame to the console
    print(df)

    # Create a fancy HTML table
    html = df.to_html(classes='table table-striped table-hover', index=False)
    html = html.replace('</thead>', '</thead><tfoot><tr>' + ''.join(['<th></th>' for _ in df.columns]) + '</tr></tfoot>')

    x_counts_json = x_counts_formatted.to_json()

    html = f"""
    <html>
        <head>
            <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
            <link rel="stylesheet" href="https://cdn.datatables.net/1.11.5/css/dataTables.bootstrap5.min.css">
            <link rel="stylesheet" href="https://cdn.datatables.net/buttons/2.0.1/css/buttons.bootstrap5.min.css">
            <style>
                .dataTables_wrapper {{
                    width: 100%;
                }}
                .table {{
                    width: 100%;
                    margin-bottom: 1rem;
                    color: #212529;
                }}
                
                .table th,
                .table td {{
                    padding: 0.75rem;
                    vertical-align: top;
                    border-top: 1px solid #dee2e6;
                }}
                .table thead th {{
                    vertical-align: bottom;
                    border-bottom: 2px solid #dee2e6;
                }}
                .table tbody + tbody {{
                    border-top: 2px solid #dee2e6;
                }}
                .red-cross {{
                    color: red;
                    font-weight: bold;
                }}
                .container {{
                    margin-left: 0px;
                }}
            </style>
            <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
            <script src="https://cdn.datatables.net/1.11.5/js/jquery.dataTables.min.js"></script>
            <script src="https://cdn.datatables.net/1.11.5/js/dataTables.bootstrap5.min.js"></script>
            <script src="https://cdn.datatables.net/buttons/2.0.1/js/dataTables.buttons.min.js"></script>
            <script src="https://cdn.datatables.net/buttons/2.0.1/js/buttons.bootstrap5.min.js"></script>
            <script src="https://cdnjs.cloudflare.com/ajax/libs/jszip/3.1.3/jszip.min.js"></script>
            <script src="https://cdn.datatables.net/buttons/2.0.1/js/buttons.html5.min.js"></script>
            <script src="https://cdn.datatables.net/buttons/2.0.1/js/buttons.print.min.js"></script>
            <script src="https://cdn.datatables.net/buttons/2.0.1/js/buttons.colVis.min.js"></script>
            <script>
                $(document).ready(function() {{
                    var table = $('.table').DataTable({{
                        "paging": false,
                        "searching": true,
                        "ordering": true,
                        "info": true,
                        "responsive": true,
                        "dom": 'Bfrtip',
                        "buttons": [
                            'copy', 'csv', 'excel', 'pdf', 'print'
                        ],
                        "footerCallback": function (row, data, start, end, display) {{
                            var api = this.api();
                            var xCounts = {x_counts_json};
                            var rowData = [];
                            for (var key in xCounts) {{
                                rowData.push(xCounts[key]);
                            }}
                            $(api.column(0).footer()).html('Feature usage');
                            for (var i = 1; i < rowData.length; i++) {{
                                $(api.column(i).footer()).html(rowData[i]);
                            }}
                        }}
                    }});
                    table.draw(false); // Trigger a full redraw
                }});
            </script>
        </head>
        <body>
            <div class="container">
                <h2 class="my-4">Project Counts</h2>
                {html}
            </div>
        </body>
    </html>
    """

    # Save the HTML to a file or serve it in your web application
    with open('project_counts.html', 'w') as f:
        f.write(html)

# Initial extraction
# delete the db file if it exists
if os.path.exists('database.db'):
    os.remove('database.db')
os.makedirs(output_dir, exist_ok=True)
recursive_extract(output_dir)
traverse_and_convert(output_dir)
clean_jsons(output_dir)
create_sqlite_tables()
get_all_counts()