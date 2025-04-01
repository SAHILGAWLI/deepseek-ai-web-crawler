import os
import sys
import uuid
import json
import pandas as pd
import numpy as np
import re
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables
load_dotenv()

# Configure Supabase connection
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
# For admin operations that need to bypass RLS policies
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# Source crawler detection
CRAWLER_DEVPOST = "devpost"
CRAWLER_UNSTOP = "unstop"
CRAWLER_MLH = "mlh"
CRAWLER_HACKEREARTH = "hackerearth"
CRAWLER_HACKATHON_FAST = "hackathon_fast"  # Added for hackathon_crawler_fast.py
CRAWLER_KAGGLE = "kaggle"  # Added for kaggle_crawler.py

def connect_to_supabase() -> Client:
    """Connect to Supabase client"""
    if not SUPABASE_URL:
        raise ValueError("Missing Supabase URL. Add SUPABASE_URL to your .env file.")
    
    # Use service key if available (bypasses RLS), otherwise use anon key
    key_to_use = SUPABASE_SERVICE_KEY if SUPABASE_SERVICE_KEY else SUPABASE_KEY
    
    if not key_to_use:
        raise ValueError("Missing Supabase API key. Add SUPABASE_KEY or SUPABASE_SERVICE_KEY to your .env file.")
    
    # Check for placeholder credentials
    if "your-project-id" in SUPABASE_URL or "your-supabase-anon-key" in key_to_use:
        raise ValueError(
            "You're using placeholder Supabase credentials. Please update your .env file with your actual Supabase URL and API key.\n"
            "1. Go to your Supabase dashboard (https://app.supabase.com)\n"
            "2. Open your project and go to Project Settings > API\n"
            "3. Copy the URL and proper key\n"
            "4. Update the .env file with these values"
        )
    
    # Check if service key is in the correct format (should be JWT token)
    if SUPABASE_SERVICE_KEY:
        if not SUPABASE_SERVICE_KEY.startswith("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"):
            print("WARNING: Your SUPABASE_SERVICE_KEY does not appear to be in the correct JWT format.")
            print("It should start with 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9' like the anon key.")
            print("Please get the correct service_role key from Supabase dashboard (Project Settings > API).")
            print("Falling back to anon key...")
            key_to_use = SUPABASE_KEY
        else:
            print("Using service key to bypass Row Level Security (RLS) policies")
    else:
        print("WARNING: Using anonymous key which may be restricted by Row Level Security (RLS) policies")
    
    try:
        return create_client(SUPABASE_URL, key_to_use)
    except Exception as e:
        error_msg = str(e).lower()
        if "invalid api key" in error_msg:
            print("\nERROR: Supabase rejected your API key. Please check the following:")
            print("1. Make sure your SUPABASE_URL is correct")
            print("2. For SUPABASE_SERVICE_KEY, use the service_role JWT from Project Settings > API")
            print("3. The key should start with 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9'")
            print("4. Do not use the 'Reference ID' starting with 'sbp_'")
            print("5. Make sure you're using the key from the correct project")
        raise

def get_existing_hackathons(supabase: Client):
    """Get URLs of existing hackathons to avoid duplicates"""
    try:
        response = supabase.table('hackathons').select('url').execute()
        data = response.data
        return set(item['url'] for item in data if 'url' in item)
    except Exception as e:
        print(f"Error fetching existing hackathons: {e}")
        # Return empty set if we can't fetch existing hackathons
        return set()

def filter_out_duplicates(df, existing_urls):
    """Filter out hackathons that are already in the database"""
    # Filter the dataset to only include hackathons not already in the database
    if 'url' not in df.columns:
        print("Warning: DataFrame doesn't have a 'url' column for deduplication")
        return df
    
    # Count before filtering
    original_count = len(df)
    
    # Filter out rows where url is in existing_urls
    df_new = df[~df['url'].isin(existing_urls)]
    
    # Count after filtering
    filtered_count = len(df_new)
    duplicate_count = original_count - filtered_count
    
    print(f"Found {duplicate_count} duplicate hackathons that are already in the database")
    print(f"Processing {filtered_count} new hackathons")
    
    return df_new

def detect_crawler_type(csv_file):
    """Detects which crawler produced the CSV file based on filename or column patterns"""
    filename = os.path.basename(csv_file).lower()
    
    # First, try to detect from filename
    if "devpost" in filename:
        return CRAWLER_DEVPOST
    elif "unstop" in filename:
        return CRAWLER_UNSTOP
    elif "mlh" in filename:
        return CRAWLER_MLH
    elif "hackerearth" in filename:
        return CRAWLER_HACKEREARTH
    elif "kaggle" in filename:
        return CRAWLER_KAGGLE
    elif "hackathon" in filename:
        return CRAWLER_HACKATHON_FAST
    
    # If can't determine from filename, read the first few rows and check columns
    try:
        df = pd.read_csv(csv_file, nrows=1)
        cols = set(df.columns.str.lower())
        
        # Check for source_platform column first
        if 'source_platform' in cols:
            if any(df['source_platform'].str.contains('devpost', case=False)):
                return CRAWLER_DEVPOST
            elif any(df['source_platform'].str.contains('unstop', case=False)):
                return CRAWLER_UNSTOP
            elif any(df['source_platform'].str.contains('mlh', case=False)):
                return CRAWLER_MLH
            elif any(df['source_platform'].str.contains('hackerearth', case=False)):
                return CRAWLER_HACKEREARTH
            elif any(df['source_platform'].str.contains('kaggle', case=False)):
                return CRAWLER_KAGGLE
        
        # Check for distinctive column patterns
        if {'title', 'url', 'logo_url', 'banner_url'}.issubset(cols):
            if 'prize_pool' in cols:
                # Could be either Devpost or Kaggle, check for Kaggle-specific fields
                if 'abstract' in cols or 'timeline' in cols or 'participation_stats' in cols:
                    return CRAWLER_KAGGLE
                return CRAWLER_DEVPOST
            elif 'prize_money' in cols:
                return CRAWLER_UNSTOP
            
        if {'mode', 'tags', 'banner_image_url'}.issubset(cols):
            return CRAWLER_MLH
        
        # Check for hackathon_crawler_fast.py specific columns
        if {'runs_from_text', 'happening_text', 'num_participants'}.issubset(cols):
            return CRAWLER_HACKATHON_FAST
            
    except Exception as e:
        print(f"Error detecting crawler type: {e}")
    
    # Default to devpost if can't determine
    print("Could not determine crawler type from filename or columns, defaulting to devpost")
    return CRAWLER_DEVPOST

def serialize_value(value):
    """Helper function to serialize a single value"""
    # Handle None
    if value is None:
        return None
    
    # Handle pandas NaT specifically
    elif pd.isna(value) or (hasattr(pd, 'NaT') and value is pd.NaT):
        return None
    
    # Handle NaN and infinity
    elif isinstance(value, (float, int)) and (pd.isna(value) or not np.isfinite(value)):
        return None
    
    # Handle timestamps
    elif isinstance(value, (pd.Timestamp, datetime)):
        # Double-check for NaT before converting to string
        if pd.isna(value):
            return None
        return value.isoformat()
    
    # Handle pandas/numpy arrays and series
    elif isinstance(value, (pd.Series, np.ndarray)):
        return [serialize_value(x) for x in value]
    
    # Handle nested dictionaries
    elif isinstance(value, dict):
        return {k: serialize_value(v) for k, v in value.items()}
    
    # Handle nested lists
    elif isinstance(value, list):
        return [serialize_value(item) for item in value]
    
    # Return other values as is
    else:
        return value

def json_serializable_record(record):
    """Convert record to JSON serializable format"""
    serialized = {}
    for key, value in record.items():
        # Special handling for tags field to ensure it's always a valid list
        if key == 'tags':
            # Fix: Check if value is None or empty using proper methods, avoiding boolean context
            if value is None:
                serialized[key] = []
            elif isinstance(value, (list, pd.Series, np.ndarray)):
                # Check if empty using length/size without boolean context
                if isinstance(value, (np.ndarray, pd.Series)) and value.size == 0:
                    serialized[key] = []
                elif isinstance(value, list) and len(value) == 0:
                    serialized[key] = []
                else:
                    # Clean up the tags - remove duplicates and empty strings
                    clean_tags = []
                    for tag in value:
                        if tag is not None and pd.notna(tag) and str(tag).strip():
                            clean_tags.append(str(tag).strip())
                    # Use a set to remove duplicates then convert back to list
                    unique_tags = list(set(clean_tags)) if len(clean_tags) > 0 else []
                    serialized[key] = unique_tags
            elif isinstance(value, str):
                # Handle tag string - might be comma-separated, JSON array, or single tag
                if value.strip():
                    if value.startswith('[') and value.endswith(']'):
                        try:
                            # Try to parse as JSON
                            parsed = json.loads(value)
                            if isinstance(parsed, list):
                                serialized[key] = [t.strip() for t in parsed if t.strip()]
                            else:
                                serialized[key] = [value.strip()]
                        except:
                            # Split by comma if JSON parsing fails
                            serialized[key] = [t.strip() for t in value.strip('[]').split(',') if t.strip()]
                    else:
                        # Split by comma for plain comma-separated string
                        serialized[key] = [t.strip() for t in value.split(',') if t.strip()]
                else:
                    serialized[key] = []
            else:
                serialized[key] = []
                
        # Special handling for prizes_details and schedule_details to ensure they're always valid JSON
        elif key in ['prizes_details', 'schedule_details', 'images']:
            if value is None:
                serialized[key] = {}
            elif isinstance(value, (dict, list)):
                serialized[key] = value  # Keep dict/list as is
            elif isinstance(value, str):
                # Try to parse as JSON
                try:
                    parsed = json.loads(value)
                    serialized[key] = parsed
                except:
                    # If parsing fails, use empty dict
                    serialized[key] = {}
            elif isinstance(value, (np.ndarray, pd.Series)):
                # Convert array to list
                serialized[key] = list(value) if value.size > 0 else {}
            else:
                serialized[key] = {}
        else:
            # Use the helper function for other values
            serialized[key] = serialize_value(value)
            
    return serialized

def extract_tags(skills_row):
    """Extract tags from skills_required, tags, or similar fields and clean them up"""
    # Handle None/NaN values
    if pd.isna(skills_row) or skills_row is None:
        return []
        
    # If already a list, process each item
    if isinstance(skills_row, (list, np.ndarray, pd.Series)):
        # Process each item to ensure they're clean strings
        clean_tags = []
        for item in skills_row:
            if item is not None and pd.notna(item) and str(item).strip():
                clean_tags.append(str(item).strip())
        return clean_tags
    
    # If it's a string that looks like a list representation
    elif isinstance(skills_row, str):
        if skills_row.startswith('[') and skills_row.endswith(']'):
            try:
                # Try to parse as JSON
                items = json.loads(skills_row)
                if isinstance(items, list):
                    return [str(item).strip() for item in items if item and str(item).strip()]
                return []
            except:
                # Remove brackets and split by comma
                items = skills_row[1:-1].split(',')
                # Clean up each item
                return [item.strip().strip("'\"") for item in items if item.strip()]
        else:
            # Split by comma for regular comma-separated string
            items = skills_row.split(',')
            return [item.strip() for item in items if item.strip()]
    
    # Return empty list for any other case
    return []

def safe_json_loads(json_str):
    """Safely parse JSON string, returning empty dict on error"""
    if not isinstance(json_str, str) or not json_str.strip():
        return {}
    
    try:
        return json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return {}

def create_images_json(row):
    """Create a structured JSON object for images"""
    images = {}
    
    # Add banner image if exists
    banner_fields = ['banner_url', 'banner_image_url', 'header_image', 'cover_image']
    for field in banner_fields:
        if field in row and pd.notna(row.get(field)) and isinstance(row.get(field), str) and row.get(field).strip():
            images['banner'] = row.get(field)
            break
    
    # Add logo/header image if exists
    logo_fields = ['logo_url', 'logo_image_url', 'thumbnail', 'icon']
    for field in logo_fields:
        if field in row and pd.notna(row.get(field)) and isinstance(row.get(field), str) and row.get(field).strip():
            images['logo'] = row.get(field)
            break
        
    return images

def clean_and_transform_data(df, crawler_type):
    """Clean and transform the CSV data to match the database schema"""
    print(f"Transforming data from {crawler_type} crawler to match database schema...")
    
    # Create a new DataFrame with the expected column structure
    transformed_df = pd.DataFrame()
    
    # Fill missing URLs with placeholder to avoid None/NaN issues
    if 'url' in df.columns:
        df['url'] = df['url'].fillna('').astype(str)
    
    # Map CSV columns to database columns based on crawler type
    # Handle the 'name' field (may be called 'title' in crawlers)
    if 'name' in df.columns:
        transformed_df['name'] = df['name'].fillna('Unnamed Hackathon')
    elif 'title' in df.columns:
        transformed_df['name'] = df['title'].fillna('Unnamed Hackathon')
    else:
        transformed_df['name'] = 'Unnamed Hackathon'
    
    # Map description field
    if crawler_type == CRAWLER_KAGGLE:
        # For Kaggle, combine abstract and description if both exist
        if 'abstract' in df.columns and 'description' in df.columns:
            transformed_df['description'] = df.apply(
                lambda row: f"{row['abstract']}\n\n{row['description']}" 
                if pd.notna(row.get('abstract')) and pd.notna(row.get('description'))
                else row.get('description', '') if pd.notna(row.get('description'))
                else row.get('abstract', '') if pd.notna(row.get('abstract'))
                else '',
                axis=1
            )
        elif 'description' in df.columns:
            transformed_df['description'] = df['description'].fillna('')
        elif 'abstract' in df.columns:
            transformed_df['description'] = df['abstract'].fillna('')
        else:
            transformed_df['description'] = ''
    else:
        transformed_df['description'] = df['description'].fillna('') if 'description' in df.columns else ''
    
    # URL is required
    transformed_df['url'] = df['url'] if 'url' in df.columns else ''
    
    # Map location field
    transformed_df['location'] = df['location'].fillna('') if 'location' in df.columns else ''
    
    # Map mode field (online, offline, hybrid)
    if 'mode' in df.columns:
        transformed_df['mode'] = df['mode'].fillna('')
    else:
        # Try to infer mode from location or other fields
        transformed_df['mode'] = 'online'  # Default to online if not specified
    
    # Map prize amount field - crawler specific
    if crawler_type in [CRAWLER_DEVPOST, CRAWLER_HACKATHON_FAST, CRAWLER_KAGGLE]:
        transformed_df['prize_amount'] = df['prize_pool'].fillna('') if 'prize_pool' in df.columns else ''
    elif crawler_type == CRAWLER_UNSTOP:
        transformed_df['prize_amount'] = df['prize_money'].fillna('') if 'prize_money' in df.columns else ''
    elif crawler_type in [CRAWLER_MLH, CRAWLER_HACKEREARTH]:
        transformed_df['prize_amount'] = df['prize_amount'].fillna('') if 'prize_amount' in df.columns else ''
    
    # Map organizer field
    transformed_df['organizer'] = df['organizer'].fillna('') if 'organizer' in df.columns else ''
    
    # Process and store the text fields for times and location
    if 'runs_from_text' in df.columns:
        transformed_df['runs_from_text'] = df['runs_from_text'].fillna('')
    else:
        # Try to construct runs_from_text from start_date and end_date
        transformed_df['runs_from_text'] = ''
    
    if 'happening_text' in df.columns:
        transformed_df['happening_text'] = df['happening_text'].fillna('')
    else:
        # Default empty
        transformed_df['happening_text'] = ''
    
    # Process num_participants - convert to integer or null
    if 'num_participants' in df.columns:
        transformed_df['num_participants'] = pd.to_numeric(df['num_participants'], errors='coerce')
    elif 'participants' in df.columns:
        transformed_df['num_participants'] = pd.to_numeric(df['participants'], errors='coerce')
    elif crawler_type == CRAWLER_KAGGLE and 'participation_stats' in df.columns:
        # Extract participants count from participation_stats JSON
        def extract_participants(stats):
            if pd.isna(stats):
                return np.nan
            try:
                if isinstance(stats, dict):
                    # Try to get any of these fields
                    for field in ['participants', 'entrants', 'teams']:
                        if field in stats:
                            # Convert string with commas to number
                            val = stats[field]
                            if isinstance(val, str):
                                val = val.replace(',', '')
                            return pd.to_numeric(val, errors='coerce')
                elif isinstance(stats, str):
                    # Try to parse JSON string
                    stats_dict = json.loads(stats)
                    return extract_participants(stats_dict)
            except:
                pass
            return np.nan
        
        transformed_df['num_participants'] = df['participation_stats'].apply(extract_participants)
    
    # IMAGES - Handle all image URLs
    # Banner image
    banner_fields = ['banner_url', 'banner_image_url', 'header_image']
    for field in banner_fields:
        if field in df.columns:
            transformed_df['banner_image_url'] = df[field].fillna('')
            break
    else:
        transformed_df['banner_image_url'] = ''
    
    # Logo image
    logo_fields = ['logo_url', 'logo_image_url', 'thumbnail']
    for field in logo_fields:
        if field in df.columns:
            transformed_df['logo_image_url'] = df[field].fillna('')
            break
    else:
        transformed_df['logo_image_url'] = ''
    
    # Apply the image JSON creation function row by row
    transformed_df['images'] = df.apply(create_images_json, axis=1)
    
    # SOURCE TRACKING - Track which platform this hackathon came from
    if 'source_platform' in df.columns:
        transformed_df['source_platform'] = df['source_platform']
    elif crawler_type == CRAWLER_HACKATHON_FAST:
        # For hackathon_crawler_fast.py, map to appropriate platform if available
        if 'base_url' in df.columns:
            # Try to determine source platform from base URL
            base_urls = df['base_url']
            def map_url_to_platform(url):
                if pd.isna(url):
                    return 'unknown'
                url = str(url).lower()
                if 'devfolio' in url:
                    return 'devfolio'
                elif 'devpost' in url:
                    return 'devpost'
                elif 'unstop' in url:
                    return 'unstop'
                elif 'hackerearth' in url:
                    return 'hackerearth'
                return 'unknown'
            
            transformed_df['source_platform'] = base_urls.apply(map_url_to_platform)
        else:
            transformed_df['source_platform'] = 'devfolio'  # Default for hackathon_crawler_fast.py
    else:
        transformed_df['source_platform'] = crawler_type
    
    # Add any additional schedule details as JSON
    if 'schedule_details' in df.columns:
        transformed_df['schedule_details'] = df['schedule_details'].apply(
            lambda x: x if isinstance(x, dict) else {} if pd.isna(x) else safe_json_loads(x) if isinstance(x, str) else {}
        )
    elif crawler_type == CRAWLER_KAGGLE and 'timeline' in df.columns:
        # For Kaggle, use timeline data for schedule details
        transformed_df['schedule_details'] = df['timeline'].apply(
            lambda x: x if isinstance(x, (dict, list)) else 
                      safe_json_loads(x) if isinstance(x, str) else {}
        )
    else:
        transformed_df['schedule_details'] = [{}] * len(df)
    
    # Process prize details as JSON
    prize_fields = ['prizes_details', 'prize_details', 'prizes', 'prize_breakdown']
    for field in prize_fields:
        if field in df.columns:
            transformed_df['prizes_details'] = df[field].apply(
                lambda x: x if isinstance(x, dict) else [] if isinstance(x, list) else {} if pd.isna(x) else safe_json_loads(x) if isinstance(x, str) else {}
            )
            break
    else:
        transformed_df['prizes_details'] = [{}] * len(df)
    
    # Process dates - handle various formats and null values
    date_fields = {
        'start_date': ['start_date', 'startDate', 'start'],
        'end_date': ['end_date', 'endDate', 'end'],
        'registration_deadline': ['registration_deadline', 'registrationDeadline', 'reg_deadline', 'deadline', 'submission_deadline']
    }
    
    for target_field, source_fields in date_fields.items():
        # Find the first matching field
        found = False
        for field in source_fields:
            if field in df.columns:
                # Try to convert to datetime, set invalid dates to None
                transformed_df[target_field] = pd.to_datetime(df[field], errors='coerce')
                found = True
                break
        
        # If no matching field was found, set to None
        if not found:
            transformed_df[target_field] = None
    
    # Process tags from various sources
    tag_fields = ['tags', 'skills_required', 'categories', 'themes']
    for field in tag_fields:
        if field in df.columns:
            transformed_df['tags'] = df[field].apply(extract_tags)
            break
    else:
        transformed_df['tags'] = [[]] * len(df)
    
    # Add status field if present, otherwise leave blank
    if 'status' in df.columns:
        transformed_df['status'] = df['status'].fillna('')
    else:
        transformed_df['status'] = ''
    
    # Add timestamps
    now = datetime.now()
    transformed_df['last_updated'] = now
    transformed_df['created_at'] = now
    
    # Add UUID primary key
    transformed_df['id'] = [str(uuid.uuid4()) for _ in range(len(df))]
    
    # Print field stats to verify
    print("\nTransformed data field statistics:")
    print(f"  Total rows: {len(transformed_df)}")
    
    # Count rows with banner and logo images
    banner_count = transformed_df['banner_image_url'].notna().sum()
    logo_count = transformed_df['logo_image_url'].notna().sum()
    print(f"  Rows with banner images: {banner_count}")
    print(f"  Rows with logo images: {logo_count}")
    
    return transformed_df

def validate_record(record, index):
    """Validate a record before inserting into Supabase"""
    # Check for invalid dates
    for date_field in ['start_date', 'end_date', 'registration_deadline']:
        if date_field in record:
            if record[date_field] == "NaT" or str(record[date_field]).strip() == "NaT":
                print(f"  Warning: Record {index} has invalid {date_field} value: {record[date_field]}")
                record[date_field] = None
    
    # Validate required fields
    if not isinstance(record.get('url'), str) or not record.get('url', '').strip():
        print(f"  Warning: Record {index} is missing a URL, which is required")
        return False
    
    if not isinstance(record.get('name'), str) or not record.get('name', '').strip():
        print(f"  Warning: Record {index} is missing a name, which is required")
        return False
        
    if not isinstance(record.get('source_platform'), str) or not record.get('source_platform', '').strip():
        print(f"  Warning: Record {index} is missing source_platform, which is required")
        record['source_platform'] = 'unknown'
        
    # Validate mode value
    if (isinstance(record.get('mode'), str) and 
        record['mode'].strip() and 
        record['mode'] not in ('online', 'offline', 'hybrid', '')):
        print(f"  Warning: Record {index} has invalid mode value: {record['mode']}")
        # Try to normalize the mode
        mode = record['mode'].lower().strip()
        if 'online' in mode or 'virtual' in mode:
            record['mode'] = 'online'
        elif 'offline' in mode or 'in-person' in mode or 'person' in mode or 'onsite' in mode:
            record['mode'] = 'offline'
        elif 'hybrid' in mode:
            record['mode'] = 'hybrid'
        else:
            record['mode'] = 'online'  # Default to online
            
    # Ensure that lists are handled properly        
    for field in ['tags', 'prizes_details']:
        if field in record:
            # Handle numpy arrays and pandas series
            if isinstance(record[field], (np.ndarray, pd.Series)):
                record[field] = list(record[field]) if record[field].size > 0 else []
            # Handle lists
            elif isinstance(record[field], list):
                # Already a list, just ensure it's not None
                pass
            # Handle None or other types
            else:
                record[field] = []
            
    # Return True if record is valid
    return True

def insert_data_to_supabase(supabase: Client, data_df, existing_urls=None):
    """Insert data into Supabase, skipping duplicates"""
    # Safety check - ensure dataframe isn't empty
    if data_df.empty:
        print("Error: No data to process. The input dataframe is empty.")
        return
        
    # If we have existing URLs, filter out duplicates
    if existing_urls is not None:
        data_df = filter_out_duplicates(data_df, existing_urls)
    
    # If no new hackathons to add, return early
    if len(data_df) == 0:
        print("No new hackathons to insert. All are already in the database.")
        return
    
    # Explicitly handle NaT values in date columns before serialization
    print("Fixing NaT values in date columns...")
    for date_col in ['start_date', 'end_date', 'registration_deadline']:
        if date_col in data_df.columns:
            # Replace NaT with None (will become NULL in JSON)
            nat_count = data_df[date_col].isna().sum()
            if nat_count > 0:
                print(f"  Found {nat_count} NaT values in {date_col}")
            data_df[date_col] = data_df[date_col].where(~pd.isna(data_df[date_col]), None)
    
    # Convert DataFrame to records safely
    try:
        print("Converting data to JSON-serializable format...")
        records = data_df.to_dict(orient='records')
        json_records = []
        
        # Process each record individually to catch and handle any errors
        for i, record in enumerate(records):
            try:
                # Fix handling of empty arrays or lists
                for field in ['tags', 'prizes_details']:
                    if field in record:
                        # Convert numpy arrays and pandas series to lists
                        if isinstance(record[field], (np.ndarray, pd.Series)):
                            record[field] = list(record[field])
                        # Ensure empty arrays are properly handled
                        elif record[field] is None or (hasattr(record[field], '__len__') and len(record[field]) == 0):
                            record[field] = []
                
                json_record = json_serializable_record(record)
                
                # Final check for date fields to ensure no "NaT" strings
                for date_field in ['start_date', 'end_date', 'registration_deadline']:
                    if date_field in json_record and (json_record[date_field] == "NaT" or str(json_record[date_field]).strip() == "NaT"):
                        print(f"  Fixing {date_field} with value 'NaT' in record {i}")
                        json_record[date_field] = None
                
                # Validate the record
                if not validate_record(json_record, i):
                    continue
                
                json_records.append(json_record)
            except Exception as e:
                print(f"Error processing record {i}: {str(e)}")
                print(f"Problematic record: {record}")
                # Continue with other records
        
        print(f"Successfully converted {len(json_records)} out of {len(records)} records")
        
        if len(json_records) == 0:
            print("No valid records to insert after conversion. Aborting.")
            return
    
    except Exception as e:
        print(f"Error during data conversion: {str(e)}")
        import traceback
        traceback.print_exc()
        return
    
    # Insert data in batches to avoid timeout issues
    BATCH_SIZE = 10
    total_records = len(json_records)
    successful = 0
    
    for i in range(0, total_records, BATCH_SIZE):
        batch = json_records[i:i+BATCH_SIZE]
        try:
            # Insert data into the hackathons table
            response = supabase.table('hackathons').insert(batch).execute()
            
            # Check for errors
            if hasattr(response, 'error') and response.error:
                if '42501' in str(response.error):
                    print(f"Row Level Security Error: You don't have permission to insert records.")
                    print("To fix this, either:")
                    print("1. Add SUPABASE_SERVICE_KEY to your .env file (get it from Project Settings > API > service_role key)")
                    print("2. Modify RLS policies in Supabase dashboard to allow INSERT operations for your user")
                    print("Aborting remaining inserts.")
                    break
                else:
                    print(f"Error inserting batch {i//BATCH_SIZE + 1}: {response.error}")
            else:
                successful += len(batch)
                print(f"Successfully inserted batch {i//BATCH_SIZE + 1} ({len(batch)} records)")
        except Exception as e:
            error_msg = str(e)
            if "row-level security" in error_msg.lower() or "42501" in error_msg:
                print(f"Row Level Security Error: You don't have permission to insert records.")
                print("To fix this, either:")
                print("1. Add SUPABASE_SERVICE_KEY to your .env file (get it from Project Settings > API > service_role key)")
                print("2. Modify RLS policies in Supabase dashboard to allow INSERT operations for your user")
                print("Aborting remaining inserts.")
                break
            else:
                print(f"Exception in batch {i//BATCH_SIZE + 1}: {error_msg}")
                # Print the first record that caused the error for debugging
                if batch:
                    try:
                        # Safely convert to JSON string with fallback
                        record_str = json.dumps(batch[0], indent=2, default=str)[:500]
                        print(f"First record in batch: {record_str}...")
                    except:
                        print("Could not serialize record for display")
    
    print(f"Import complete. Successfully inserted {successful} out of {total_records} new records.")

def main():
    try:
        # Check if a CSV file was provided as an argument
        if len(sys.argv) > 1:
            csv_file = sys.argv[1]
        else:
            # Look for most recent CSV files from each crawler
            crawler_files = {
                CRAWLER_DEVPOST: None,
                CRAWLER_UNSTOP: None,
                CRAWLER_MLH: None,
                CRAWLER_HACKEREARTH: None
            }
            
            # Find the most recent file for each crawler type
            for file in os.listdir('.'):
                if file.endswith('.csv'):
                    for crawler in crawler_files.keys():
                        if crawler in file.lower():
                            # Get file creation time
                            creation_time = os.path.getctime(file)
                            # If this is the first file for this crawler or newer than existing
                            if crawler_files[crawler] is None or creation_time > os.path.getctime(crawler_files[crawler]):
                                crawler_files[crawler] = file
            
            # List found files and let user choose
            valid_files = [f for f in crawler_files.values() if f]
            if not valid_files:
                print("Error: No CSV files found from any crawler. Please specify a CSV file as an argument.")
                return
                
            # If only one file, use it automatically
            if len(valid_files) == 1:
                csv_file = valid_files[0]
                print(f"Using the only found CSV file: {csv_file}")
            else:
                print("Found multiple CSV files. Please choose one:")
                for i, file in enumerate(valid_files):
                    print(f"{i+1}. {file}")
                
                choice = input("Enter the number of the file to import: ")
                try:
                    choice = int(choice) - 1
                    if 0 <= choice < len(valid_files):
                        csv_file = valid_files[choice]
                    else:
                        print("Invalid choice. Exiting.")
                        return
                except:
                    print("Invalid input. Exiting.")
                    return
        
        # Confirm the file exists
        if not os.path.exists(csv_file):
            print(f"Error: The CSV file '{csv_file}' does not exist")
            return
            
        # Detect which crawler produced the CSV
        crawler_type = detect_crawler_type(csv_file)
        print(f"Detected crawler type: {crawler_type}")
            
        # Read the CSV file
        print(f"Reading data from {csv_file}...")
        df = pd.read_csv(csv_file)
        print(f"Found {len(df)} records in CSV file")
        
        # Early validation check
        if df.empty:
            print("Error: The CSV file is empty")
            return
            
        if 'url' not in df.columns:
            print("Warning: CSV is missing the 'url' column needed for deduplication")
        
        # Clean and transform the data based on crawler type
        print("Transforming data to match database schema...")
        transformed_df = clean_and_transform_data(df, crawler_type)
        
        # Connect to Supabase
        print("Connecting to Supabase...")
        supabase = connect_to_supabase()
        
        # Get existing hackathons to avoid duplicates
        print("Fetching existing hackathons to prevent duplicates...")
        existing_urls = get_existing_hackathons(supabase)
        
        # Insert data, skipping duplicates
        print("Inserting new hackathons into Supabase...")
        insert_data_to_supabase(supabase, transformed_df, existing_urls)
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 