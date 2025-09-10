from flask import Flask, render_template, request, jsonify
import os
import json
import tempfile
from werkzeug.utils import secure_filename
import openpyxl
import xlrd
from datetime import datetime
import re

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'your-secret-key-change-this')

# Configure upload settings
UPLOAD_FOLDER = tempfile.gettempdir()
ALLOWED_EXTENSIONS = {'xls', 'xlsx'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Driver data file path
DRIVER_DATA_FILE = 'driver_data.json'

def load_driver_data_from_file():
    """Load driver data from JSON file"""
    if os.path.exists(DRIVER_DATA_FILE):
        try:
            with open(DRIVER_DATA_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading driver data: {e}")
            return {}
    return {}

def save_driver_data_to_file(driver_data):
    """Save driver data to JSON file"""
    try:
        with open(DRIVER_DATA_FILE, 'w') as f:
            json.dump(driver_data, f, indent=2)
        return True
    except Exception as e:
        print(f"Error saving driver data: {e}")
        return False

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def extract_station_code(station_info, filename):
    """Extract station code from station info or filename"""
    patterns = [r'([A-Z]{3,4}\/\d+\/\d+)', r'([A-Z]{2,5}\d{2,5})', 
                r'([A-Z]{2,5}[-\s]\d{2,5})', r'([A-Z]{2,10})']
    
    # Try station info first
    for pattern in patterns:
        match = re.search(pattern, station_info)
        if match:
            return match.group(1)
    
    # Fall back to filename
    for pattern in patterns:
        match = re.search(pattern, filename)
        if match:
            return match.group(1)
    
    return filename.replace('.xlsx', '').replace('.xls', '') or 'Unknown Station'

def is_valid_driver_name(text):
    """Check if text looks like a valid driver name"""
    if not text or not isinstance(text, str):
        return False
    
    text = str(text).strip()
    
    # Basic validation
    if not text or ',' not in text or len(text) > 50 or len(text) < 3:
        return False
    
    # Exclude common non-driver keywords
    exclude_keywords = [
        'attachment', 'addendum', 'schedule', 'stop rate', 'variability',
        'settlement', 'density', 'threshold', 'printed', 'materials',
        'reference', 'however', 'purposes', 'activity', 'totaled',
        'represent', 'counted', 'towards', 'shall', 'due to', 'total',
        'summary', 'report', 'page', 'date', 'station'
    ]
    
    if any(keyword in text.lower() for keyword in exclude_keywords):
        return False
    
    # Should only contain valid characters
    if not re.match(r"^[A-Za-z\s,.''-]+$", text):
        return False
    
    # Should have lastname, firstname format
    parts = text.split(',')
    return len(parts) >= 2 and re.search(r'[A-Za-z]', parts[1].strip())

def parse_hours(value):
    """Parse hours from various time formats"""
    if not value:
        return 0
    
    str_val = str(value).strip()
    
    # Handle HH:MM format
    if ':' in str_val and str_val not in ["00:00", "0:00"]:
        parts = str_val.split(':')
        if len(parts) == 2:
            try:
                hours = int(parts[0])
                minutes = int(parts[1])
                return round((hours + (minutes / 60)) * 2) / 2  # Round to nearest 0.5
            except ValueError:
                pass
    
    # Handle decimal/whole numbers
    try:
        num = float(str_val)
        if 0 < num < 24:
            return round(num * 2) / 2  # Round to nearest 0.5
    except ValueError:
        pass
    
    return 0

def process_excel_file(file_path, filename):
    """Process an Excel file and extract driver data"""
    try:
        # Handle both .xls and .xlsx files
        print(f"File extension check: {filename.lower()}")
        if filename.lower().endswith('.xlsx'):
            workbook = openpyxl.load_workbook(file_path)
            sheet = workbook.active
            # Convert to list of lists for processing
            data = []
            for row in sheet.iter_rows(values_only=True):
                data.append(list(row) if row else [])
        else:
            # Handle .xls files with xlrd
            print(f"Opening .xls file with xlrd: {filename}")
            workbook = xlrd.open_workbook(file_path)
            sheet = workbook.sheet_by_index(0)
            data = []
            for row_idx in range(sheet.nrows):
                row = []
                for col_idx in range(sheet.ncols):
                    cell_value = sheet.cell_value(row_idx, col_idx)
                    # Convert xlrd cell types to appropriate Python types
                    if sheet.cell_type(row_idx, col_idx) == xlrd.XL_CELL_EMPTY:
                        row.append(None)
                    elif sheet.cell_type(row_idx, col_idx) == xlrd.XL_CELL_TEXT:
                        row.append(str(cell_value))
                    elif sheet.cell_type(row_idx, col_idx) == xlrd.XL_CELL_NUMBER:
                        # Check if it's an integer or float
                        if cell_value == int(cell_value):
                            row.append(int(cell_value))
                        else:
                            row.append(cell_value)
                    elif sheet.cell_type(row_idx, col_idx) == xlrd.XL_CELL_DATE:
                        row.append(xlrd.xldate_as_datetime(cell_value, workbook.datemode))
                    else:
                        row.append(cell_value)
                data.append(row)
            print(f"Successfully read {len(data)} rows from {filename}")
        
        # Extract station info from first few rows
        station_info = ''
        for i in range(min(5, len(data))):
            if data[i] and len(data[i]) > 0 and data[i][0]:
                row_content = str(data[i][0])
                if len(row_content) > len(station_info):
                    station_info = row_content
        
        # Extract station code
        station_code = extract_station_code(station_info, filename)
        
        # Extract date
        date = datetime.now().strftime('%m/%d/%Y')
        date_patterns = [r'(\d{2}\/\d{2}\/\d{4})', r'(\d{1,2}\/\d{1,2}\/\d{4})', 
                        r'(\d{2}-\d{2}-\d{4})', r'(\d{4}-\d{2}-\d{2})']
        
        for pattern in date_patterns:
            match = re.search(pattern, station_info) or re.search(pattern, filename)
            if match:
                try:
                    parsed_date = datetime.strptime(match.group(1), '%m/%d/%Y' if '/' in match.group(1) else '%Y-%m-%d')
                    date = parsed_date.strftime('%m/%d/%Y')
                    break
                except ValueError:
                    continue
        
        # Find all driver name occurrences in the worksheet
        driver_occurrences = {}
        
        # Scan through rows to find driver names (limit scan to first 200 rows for performance)
        for i in range(1, min(len(data), 200)):
            row = data[i]
            if not row:
                continue
            
            # Check first 15 columns for driver names
            for col in range(min(len(row), 15)):
                if row[col] and isinstance(row[col], str) and is_valid_driver_name(row[col]):
                    driver_name = str(row[col]).strip()
                    
                    if driver_name not in driver_occurrences:
                        driver_occurrences[driver_name] = []
                    
                    driver_occurrences[driver_name].append({
                        'row_index': i,
                        'name_column': col,
                        'row': row
                    })
                    
                    break  # Only take first occurrence per row

        print(f"Found {len(driver_occurrences)} unique drivers in {filename}")
        
        # Process each found driver to extract their stop and hour data
        drivers = []
        for driver_name, occurrences in driver_occurrences.items():
            total_stops = 0
            on_duty_hours = 0
            
            print(f"Processing driver: {driver_name} ({len(occurrences)} occurrences)")
            
            if len(occurrences) == 1:
                # Single occurrence - extract data from same row
                occurrence = occurrences[0]
                row = occurrence['row']
                name_col = occurrence['name_column']
                
                print(f"  Single occurrence at column {name_col}, row has {len(row)} columns")
                
                # Extract data based on column position - ONLY delivery stops, not pickup
                if name_col == 3 and len(row) > 26:
                    total_stops = 0
                    # Only count delivery stops (column 9), ignore pickup stops (column 11)
                    if len(row) > 9 and row[9] and str(row[9]).replace('.0', '').replace('.', '').isdigit():
                        total_stops = int(float(row[9]))  # Only delivery stops
                    
                    on_duty_hours = parse_hours(row[26])
                    if on_duty_hours == 0:
                        # Try alternative columns
                        for col in [25, 24, 27, 23, 28, 22, 29, 21, 30]:
                            if len(row) > col and row[col]:
                                on_duty_hours = parse_hours(row[col])
                                if on_duty_hours > 0:
                                    break
                
                elif name_col == 2 and len(row) > 25:
                    total_stops = 0
                    # Only count delivery stops (column 8), ignore pickup stops (column 10)
                    if len(row) > 8 and row[8] and str(row[8]).replace('.0', '').replace('.', '').isdigit():
                        total_stops = int(float(row[8]))  # Only delivery stops
                    
                    for col in [25, 24, 26]:
                        if len(row) > col and row[col]:
                            on_duty_hours = parse_hours(row[col])
                            if on_duty_hours > 0:
                                break
                
                else:
                    # Handle other column positions with more flexible extraction
                    print(f"  Trying flexible extraction for column {name_col}")
                    # Look for reasonable stop counts in nearby columns
                    for col in range(max(0, name_col + 1), min(len(row), name_col + 20)):
                        if row[col] and str(row[col]).replace('.0', '').replace('.', '').isdigit():
                            val = int(float(row[col]))
                            if 1 <= val <= 200:  # Reasonable stop count range
                                total_stops += val
                                if total_stops > 0:  # Take first reasonable value
                                    break
                    
                    # Look for hours in later columns
                    for col in range(max(0, name_col + 10), min(len(row), name_col + 30)):
                        if row[col]:
                            hours = parse_hours(row[col])
                            if hours > 0:
                                on_duty_hours = hours
                                break
                
                print(f"  Extracted: {total_stops} stops, {on_duty_hours} hours")
            
            else:
                # Multiple occurrences - try to get data from different rows
                print(f"  Multiple occurrences ({len(occurrences)})")
                first_occurrence = occurrences[0]
                first_row = first_occurrence['row']
                first_name_col = first_occurrence['name_column']
                
                # Try to get stops from first occurrence
                if len(first_row) > max(9, 11) and first_name_col == 3:
                    if len(first_row) > 9 and first_row[9] and str(first_row[9]).replace('.0', '').replace('.', '').isdigit():
                        total_stops += int(float(first_row[9]))
                    if len(first_row) > 11 and first_row[11] and str(first_row[11]).replace('.0', '').replace('.', '').isdigit():
                        total_stops += int(float(first_row[11]))
                
                # Try second occurrence for hours if available
                if len(occurrences) > 1:
                    second_occurrence = occurrences[1]
                    second_row = second_occurrence['row']
                    # Try to get hours from second occurrence
                    for col in range(20, min(len(second_row), 35)):
                        if second_row[col]:
                            hours = parse_hours(second_row[col])
                            if hours > 0:
                                on_duty_hours = hours
                                break
                
                print(f"  Multi-occurrence extracted: {total_stops} stops, {on_duty_hours} hours")
            
            # Only add driver if they have meaningful data
            if total_stops > 0 or on_duty_hours > 0:
                drivers.append({
                    'driverName': driver_name,
                    'totalStops': total_stops,
                    'onDutyHours': on_duty_hours
                })
                print(f"  Added driver: {driver_name} - {total_stops} stops, {on_duty_hours} hours")
            else:
                print(f"  Skipped driver {driver_name} - no meaningful data found")

        print(f"Final result: {len(drivers)} drivers with data")
        return {
            'stationInfo': station_info,
            'stationCode': station_code,
            'date': date,
            'drivers': drivers
        }
        
    except Exception as e:
        raise Exception(f"Failed to process {filename}: {str(e)}")

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/upload', methods=['POST'])
def upload_files():
    try:
        print("Upload request received")  # Debug log
        
        if 'files' not in request.files:
            print("No files in request")
            return jsonify({'error': 'No files uploaded'}), 400
        
        files = request.files.getlist('files')
        print(f"Received {len(files)} files")  # Debug log
        
        processed_files = []
        
        for file in files:
            if file and file.filename and allowed_file(file.filename):
                print(f"Processing file: {file.filename}")  # Debug log
                filename = secure_filename(file.filename)
                file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                file.save(file_path)
                
                try:
                    file_data = process_excel_file(file_path, filename)
                    processed_files.append(file_data)
                    print(f"Successfully processed {filename}")  # Debug log
                except Exception as e:
                    print(f"Error processing {filename}: {str(e)}")  # Debug log
                    # Continue with other files instead of failing completely
                    continue
                finally:
                    # Clean up uploaded file
                    if os.path.exists(file_path):
                        os.remove(file_path)
        
        if not processed_files:
            return jsonify({'error': 'No valid Excel files could be processed'}), 400
        
        # Group by station and create weekly summaries
        stations = {}
        for file_data in processed_files:
            station_code = file_data['stationCode']
            if station_code not in stations:
                stations[station_code] = {
                    'stationInfo': file_data['stationInfo'],
                    'stationCode': station_code,
                    'files': [],
                    'dates': set(),
                    'drivers': {}
                }
            
            stations[station_code]['files'].append(file_data)
            stations[station_code]['dates'].add(file_data['date'])
            
            # Organize driver data by date
            for driver in file_data['drivers']:
                driver_name = driver['driverName']
                if driver_name not in stations[station_code]['drivers']:
                    stations[station_code]['drivers'][driver_name] = {}
                
                stations[station_code]['drivers'][driver_name][file_data['date']] = {
                    'totalStops': driver['totalStops'],
                    'hours': driver['onDutyHours']
                }
        
        # Convert to final format
        result = {}
        for station_code, station_data in stations.items():
            dates = sorted(list(station_data['dates']))
            weekly_data = []
            
            for driver_name, driver_dates in station_data['drivers'].items():
                driver_row = {
                    'driver': driver_name,
                    'dates': {}
                }
                
                for date in dates:
                    driver_row['dates'][date] = driver_dates.get(date, {'totalStops': 0, 'hours': 0})
                
                weekly_data.append(driver_row)
            
            weekly_data.sort(key=lambda x: x['driver'])
            
            result[station_code] = {
                'stationInfo': station_data['stationInfo'],
                'stationCode': station_code,
                'files': [f['stationInfo'] for f in station_data['files']],
                'dates': dates,
                'weeklyData': weekly_data
            }
        
        return jsonify(result)
        
    except Exception as e:
        print(f"Server error in upload_files: {str(e)}")  # Debug log
        import traceback
        traceback.print_exc()  # Print full error traceback
        return jsonify({'error': f'Server processing error: {str(e)}'}), 500

@app.route('/api/drivers', methods=['GET', 'POST', 'DELETE'])
def manage_drivers():
    # Load from file instead of session
    driver_data = load_driver_data_from_file()
    
    if request.method == 'GET':
        return jsonify(driver_data)
    
    elif request.method == 'POST':
        data = request.get_json()
        driver_name = data.get('name')
        driver_config = data.get('config')
        
        if not driver_name or not driver_config:
            return jsonify({'error': 'Missing driver name or configuration'}), 400
        
        driver_data[driver_name] = driver_config
        if save_driver_data_to_file(driver_data):
            return jsonify({'message': 'Driver saved successfully'})
        else:
            return jsonify({'error': 'Failed to save driver data'}), 500
    
    elif request.method == 'DELETE':
        driver_name = request.args.get('name')
        if driver_name in driver_data:
            del driver_data[driver_name]
            if save_driver_data_to_file(driver_data):
                return jsonify({'message': 'Driver deleted successfully'})
            else:
                return jsonify({'error': 'Failed to delete driver data'}), 500
        
        return jsonify({'error': 'Driver not found'}), 404

if __name__ == '__main__':
    app.run(debug=True)

if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
