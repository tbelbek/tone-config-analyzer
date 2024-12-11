import os
import json
import pandas as pd
import glob
import zipfile

def convert_excel_to_json(excel_path, json_path):
    """
    Converts an Excel file to JSON format.

    Args:
        excel_path (str): The path to the Excel file.
        json_path (str): The path where the JSON file will be saved.
    """
    try:
        df = pd.read_excel(excel_path)
        df.to_json(json_path, orient='records', indent=4)
        print(f'✅ Converted {excel_path} to {json_path}')
    except Exception as e:
        print(f'❌ Failed to convert {excel_path}: {e}')

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
        print(f'❌ Error decoding JSON from {input_file}: {e}')
        return False
    except Exception as e:
        print(f'❌ Failed to read {input_file}: {e}')
        return False

    # Remove specified fields
    if isinstance(data, list):
        if not data or all(not item for item in data):
            print(f'ℹ️ {input_file} is empty after initial check. Skipping cleaning.')
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
        print(f'✅ Cleaned data from {input_file} and saved to {output_file}')
        return True
    except Exception as e:
        print(f'❌ Failed to write cleaned data to {output_file}: {e}')
        return False

def extract_zips(import_dir='import', extract_dir='extracted'):
    """
    Recursively extracts all ZIP files from the import directory into the extracted directory,
    maintaining folder structure based on ZIP filenames.

    Args:
        import_dir (str): Directory to search for ZIP files.
        extract_dir (str): Directory where extracted files will be stored.
    """
    for root, dirs, files in os.walk(import_dir):
        for file in files:
            if file.lower().endswith('.zip'):
                zip_path = os.path.join(root, file)
                zip_name = os.path.splitext(file)[0]
                destination = os.path.join(extract_dir, zip_name)
                os.makedirs(destination, exist_ok=True)
                try:
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        zip_ref.extractall(destination)
                    print(f'✅ Extracted {zip_path} to {destination}')
                except zipfile.BadZipFile:
                    print(f'❌ Failed to extract {zip_path}: Bad ZIP file.')
                except Exception as e:
                    print(f'❌ Failed to extract {zip_path}: {e}')

def generate_features_html(output_dir):
    """
    Generates a main HTML file listing all features with entry counts and creates individual HTML files for each feature
    with column-based filtering using DataTables.
    
    Args:
        output_dir (str): The directory where JSON files are located and HTML will be saved.
    """
    html_file = os.path.join(output_dir, 'features.html')

    # Define patterns for both cleaned and original configuration files
    patterns = [
        os.path.join(output_dir, 'Cleaned_Tmhls.*.Configuration.json'),
        os.path.join(output_dir, 'Tmhls.*.Configuration.json')
    ]
    files = []
    for pattern in patterns:
        files.extend(glob.glob(pattern))

    # Extract feature names and counts
    features = []
    feature_counts = {}
    for file in files:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Check if data is not empty
            is_empty = False
            if isinstance(data, list):
                if not data or all(not item for item in data):
                    is_empty = True
            elif isinstance(data, dict):
                if not data:
                    is_empty = True
            else:
                if not data:
                    is_empty = True

            if is_empty:
                print(f'ℹ️ Skipping {file} as it contains empty data.')
                continue

            # Count entries if data is a list
            count = len(data) if isinstance(data, list) else 1

            basename = os.path.basename(file)
            parts = basename.split('.')
            if len(parts) >= 3:
                feature_name = parts[1]
                features.append(feature_name)
                feature_counts[feature_name] = count

                # Create individual HTML for the feature
                feature_html = os.path.join(output_dir, f'{feature_name}.html')
                with open(feature_html, 'w', encoding='utf-8') as fh:
                    fh.write(f"""<!DOCTYPE html>
<html>
<head>
    <title>{feature_name} Details</title>
    <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.0/css/bootstrap.min.css">
    <!-- DataTables CSS -->
    <link rel="stylesheet" href="https://cdn.datatables.net/1.13.4/css/jquery.dataTables.min.css">
    <style>
        /* Optional: Add custom styles for the filter inputs */
        tfoot input {{
            width: 100%;
            padding: 3px;
            box-sizing: border-box;
        }}
    </style>
</head>
<body>
    <h1>{feature_name} Details</h1>
    <table id="datatable" class="table table-bordered">
        <thead>
            <tr>
""")
                    # Assuming all entries have the same keys
                    if isinstance(data, list) and len(data) > 0:
                        keys = data[0].keys()
                        for key in keys:
                            fh.write(f'                <th>{key}</th>\n')
                        fh.write('            </tr>\n        </thead>\n        <tfoot>\n            <tr>\n')
                        for key in keys:
                            fh.write(f'                <th><input type="text" placeholder="Search {key}" /></th>\n')
                        fh.write('            </tr>\n        </tfoot>\n        <tbody>\n')
                        for entry in data:
                            fh.write('            <tr>\n')
                            for key in keys:
                                fh.write(f'                <td>{entry.get(key, "")}</td>\n')
                            fh.write('            </tr>\n')
                        fh.write('        </tbody>\n    </table>\n')
                    elif isinstance(data, dict):
                        fh.write('            <tr>')
                        for key in data.keys():
                            fh.write(f'<th>{key}</th>')
                        fh.write('</tr>\n        </thead>\n        <tfoot>\n            <tr>\n')
                        for key in data.keys():
                            fh.write(f'                <th><input type="text" placeholder="Search {key}" /></th>\n')
                        fh.write('            </tr>\n        </tfoot>\n        <tbody>\n')
                        fh.write('            <tr>')
                        for value in data.values():
                            fh.write(f'<td>{value}</td>')
                        fh.write('</tr>\n        </tbody>\n    </table>\n')

                    # Initialize DataTables with column-based filtering
                    fh.write("""
    <!-- jQuery -->
    <script src="https://code.jquery.com/jquery-3.5.1.js"></script>
    <!-- DataTables JS -->
    <script src="https://cdn.datatables.net/1.13.4/js/jquery.dataTables.min.js"></script>
    <script>
        $(document).ready(function() {
            // Setup - add a text input to each footer cell
            $('#datatable tfoot th').each(function() {
                var title = $(this).text();
                $(this).find('input').on('keyup change clear', function() {
                    if (table.column($(this).parent().index()).search() !== this.value) {
                        table
                            .column($(this).parent().index())
                            .search(this.value)
                            .draw();
                    }
                });
            });

            var table = $('#datatable').DataTable({
                "paging": true,
                "searching": true,
                "ordering": true,
                "info": true
            });
        });
    </script>
</body>
</html>
""")
        except json.JSONDecodeError:
            print(f'❌ Failed to decode JSON from {file}. Skipping.')
            continue
        except Exception as e:
            print(f'❌ Error processing {file}: {e}. Skipping.')
            continue

    # Remove duplicates and sort
    features = sorted(list(set(features)))

    if not features:
        print('ℹ️ No features to include in HTML. Skipping HTML creation.')
        return

    # Generate main HTML content
    html_content = """<!DOCTYPE html>
<html>
<head>
    <title>Features List</title>
    <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.0/css/bootstrap.min.css">
    <style>
        body {
            padding: 20px;
            background-color: #f8f9fa;
        }
        h1 {
            margin-bottom: 30px;
            text-align: center;
        }
        table {
            width: 80%;
            margin: 0 auto;
        }
        th {
            background-color: #343a40;
            color: white;
        }
        tr:nth-child(even) {
            background-color: #e9ecef;
        }
        tr:hover {
            background-color: #dee2e6;
        }
        .checkmark {
            font-size: 1.2em;
            color: green;
        }
    </style>
</head>
<body>
    <h1>Features</h1>
    <div class="table-responsive">
        <table id="featuresTable" class="table table-bordered table-hover">
            <thead class="thead-dark">
                <tr>
                    <th>Feature</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
"""

    for feature in features:
        count = feature_counts.get(feature, 0)
        feature_html = f"{feature}.html"
        html_content += f"""                <tr>
                    <td><a href="{feature_html}">{feature}</a></td>
                    <td class="checkmark">&#10003; {count}</td>
                </tr>
"""

    html_content += """            </tbody>
        </table>
    </div>

    <!-- jQuery -->
    <script src="https://code.jquery.com/jquery-3.5.1.js"></script>
    <!-- DataTables JS -->
    <script src="https://cdn.datatables.net/1.13.4/js/jquery.dataTables.min.js"></script>
    <script>
        $(document).ready(function() {
            $('#featuresTable').DataTable({
                "paging": true,
                "searching": true,
                "ordering": true,
                "info": true
            });
        });
    </script>
</body>
</html>
"""

    # Write main HTML file
    try:
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f'✅ features.html has been created in the {output_dir} folder.')
    except Exception as e:
        print(f'❌ Failed to write features.html: {e}')
        
def process_workspace():
    """
    Processes the 'ServiceConfigurations' directory to convert Excel files to JSON,
    clean existing JSON files by removing bulky data fields and empty properties,
    extract ZIP files, and generate HTML summaries. All output files are saved to
    the 'extracted' folder within 'ServiceConfigurations'.
    """
    workspace_dir = 'ServiceConfigurations'
    import_dir = os.path.join(workspace_dir, 'import')  # Define the import directory
    extract_dir = os.path.join(workspace_dir, 'extracted')  # Define the extract directory
    output_dir = extract_dir  # Use 'extracted' as the output directory
    fields_to_remove = ['css', 'html', 'script']

    # Create the output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f'🛠️ Created output directory at {output_dir}')

    # Extract ZIP files
    extract_zips(import_dir, extract_dir)

    converted_count = 0
    cleaned_count = 0

    for root, dirs, files in os.walk(workspace_dir):
        # Skip the extracted directory to prevent processing output files
        if os.path.abspath(root) == os.path.abspath(extract_dir):
            continue

        for file in files:
            file_path = os.path.join(root, file)

            # Convert Excel files to JSON
            if file.lower().endswith('.xlsx'):
                json_file = os.path.splitext(file)[0] + '.json'
                json_path = os.path.join(output_dir, json_file)
                convert_excel_to_json(file_path, json_path)
                converted_count += 1

            # Clean JSON files, excluding already cleaned files
            elif file.lower().endswith('.json') and not file.startswith('Cleaned_'):
                cleaned_json_file = 'Cleaned_' + file
                cleaned_json_path = os.path.join(output_dir, cleaned_json_file)
                cleaned = clean_json_fields(file_path, cleaned_json_path, fields_to_remove)
                if cleaned:
                    cleaned_count += 1

    # Generate features.html
    generate_features_html(output_dir)

    print(f'\n📊 Summary:')
    print(f' - Extracted ZIP files to "extracted" folder.')
    print(f' - Converted Excel files to JSON: {converted_count}')
    print(f' - Cleaned JSON files: {cleaned_count}')
    print(f' - All output files are saved in the "extracted" folder at {output_dir}')

if __name__ == "__main__":
    process_workspace()