import os
import sys
import uuid
import json
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables
load_dotenv()

# CSV file to import - you can change this to the file you want to import
CSV_FILE = "hackathons_20250323_032151.csv"

# Configure Supabase connection
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
# For admin operations that need to bypass RLS policies
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

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

def serialize_value(value):
    """Helper function to serialize a single value"""
    # Handle None
    if value is None:
        return None
    
    # Handle pandas NaT specifically
    elif pd.isna(value) or (hasattr(pd, 'NaT') and value is pd.NaT):
        return None
    
    # Handle NaN and infinity
    elif isinstance(value, (float, int)) and (pd.isna(value) or not pd.isfinite(value)):
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
                        if tag is not None and pd.notna(tag) and isinstance(tag, str) and tag.strip():
                            clean_tags.append(tag.strip())
                    # Use a set to remove duplicates then convert back to list, never evaluate array in boolean context
                    unique_tags = list(set(clean_tags)) if len(clean_tags) > 0 else []
                    serialized[key] = unique_tags
            else:
                serialized[key] = []
        # Special handling for prizes_details to ensure it's always a valid list or dict
        elif key == 'prizes_details':
            if value is None:
                serialized[key] = []
            elif isinstance(value, list):
                serialized[key] = value  # Keep list as is
            elif isinstance(value, dict):
                serialized[key] = value  # Keep dict as is
            elif isinstance(value, (np.ndarray, pd.Series)):
                # Convert to list without boolean evaluation
                serialized[key] = list(value) if value.size > 0 else []
            else:
                serialized[key] = []
        else:
            # Use the helper function for non-tag values
            serialized[key] = serialize_value(value)
            
    return serialized

def extract_tags(skills_row):
    """Extract tags from skills_required field and clean them up"""
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
                # Remove brackets and split by comma
                items = skills_row[1:-1].split(',')
                # Clean up each item
                return [item.strip().strip("'\"") for item in items if item.strip()]
            except:
                return []
        else:
            # Single tag as string
            return [skills_row.strip()] if skills_row.strip() else []
    
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
    # Add banner image if exists - avoid boolean context with arrays/series
    if 'banner_url' in row and pd.notna(row.get('banner_url')) and isinstance(row.get('banner_url'), str) and row.get('banner_url').strip():
        images['banner'] = row.get('banner_url')
    
    # Add logo/header image if exists - avoid boolean context with arrays/series
    if 'logo_url' in row and pd.notna(row.get('logo_url')) and isinstance(row.get('logo_url'), str) and row.get('logo_url').strip():
        images['logo'] = row.get('logo_url')
        
    # Add header image if it exists - avoid boolean context with arrays/series
    if 'header_url' in row and pd.notna(row.get('header_url')) and isinstance(row.get('header_url'), str) and row.get('header_url').strip():
        images['header'] = row.get('header_url')
        
    return images

def clean_and_transform_data(df):
    """Clean and transform the CSV data to match the database schema"""
    
    # Create a new DataFrame with the expected column structure
    transformed_df = pd.DataFrame()
    
    # Fill missing URLs with placeholder to avoid None/NaN issues
    if 'url' in df.columns:
        df['url'] = df['url'].fillna('').astype(str)
    
    # Map CSV columns to database columns
    transformed_df['name'] = df['title'].fillna('Unnamed Hackathon')
    transformed_df['description'] = df['description'].fillna('')
    transformed_df['url'] = df['url']
    transformed_df['location'] = df['location'].fillna('')
    transformed_df['mode'] = df['mode'].fillna('')
    transformed_df['prize_amount'] = df['prize_pool'].fillna('')
    transformed_df['organizer'] = df['organizer'].fillna('')
    
    # Process and store the text fields for times and location
    transformed_df['runs_from_text'] = df['runs_from_text'].fillna('')
    transformed_df['happening_text'] = df['happening_text'].fillna('')
    
    # Process num_participants - convert to integer or null
    if 'num_participants' in df.columns:
        transformed_df['num_participants'] = pd.to_numeric(df['num_participants'], errors='coerce')
    
    # IMAGES - Handle all image URLs in a clear, structured way
    # Banner image stored both in its own field and in the images JSONB
    transformed_df['banner_image_url'] = df['banner_url'].fillna('')
    transformed_df['logo_image_url'] = df['logo_url'].fillna('')
    
    # Apply the image JSON creation function row by row
    transformed_df['images'] = df.apply(create_images_json, axis=1)
    
    # SOURCE TRACKING - Track which platform this hackathon came from
    transformed_df['source_platform'] = 'devfolio'  # Default to devfolio since that's what we're currently scraping
    
    # Add any additional schedule details as JSON
    # Handle schedule_details safely
    if 'schedule_details' in df.columns:
        # Use safer approach with the new helper function
        transformed_df['schedule_details'] = df['schedule_details'].apply(
            lambda x: x if isinstance(x, dict) else {} if pd.isna(x) else safe_json_loads(x) if isinstance(x, str) else {}
        )
    else:
        print("Note: 'schedule_details' column not found in CSV. Using empty JSON objects.")
        transformed_df['schedule_details'] = [{} for _ in range(len(df))]
    
    # Process prize details as JSON - also handle safely
    if 'prizes_details' in df.columns:
        transformed_df['prizes_details'] = df['prizes_details'].apply(
            lambda x: x if isinstance(x, dict) else [] if isinstance(x, list) else {} if pd.isna(x) else safe_json_loads(x) if isinstance(x, str) else {}
        )
    else:
        print("Note: 'prizes_details' column not found in CSV. Using empty JSON objects.")
        transformed_df['prizes_details'] = [{} for _ in range(len(df))]
    
    # Process dates - handle various formats and null values
    for date_field in ['start_date', 'end_date', 'registration_deadline']:
        # Try to convert to datetime, but set invalid dates to None
        transformed_df[date_field] = pd.to_datetime(df[date_field], errors='coerce')
    
    # Process tags from skills_required
    skills_col = df.get('skills_required', pd.Series([]))
    transformed_df['tags'] = skills_col.apply(extract_tags)
    
    # Add timestamps
    now = datetime.now()
    transformed_df['last_updated'] = now
    transformed_df['created_at'] = now
    
    # Add UUID primary key
    transformed_df['id'] = [str(uuid.uuid4()) for _ in range(len(df))]
    
    # Print field stats to verify
    print("\nTransformed data field statistics:")
    print(f"  Total rows: {len(transformed_df)}")
    banner_count = (~pd.isna(df['banner_url']) if 'banner_url' in df.columns else pd.Series([False] * len(df))).sum()
    logo_count = (~pd.isna(df['logo_url']) if 'logo_url' in df.columns else pd.Series([False] * len(df))).sum()
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
    
    # Validate required fields - avoid boolean context with arrays
    if not isinstance(record.get('url'), str) or not record.get('url', '').strip():
        print(f"  Warning: Record {index} is missing a URL, which is required")
        return False
    
    if not isinstance(record.get('name'), str) or not record.get('name', '').strip():
        print(f"  Warning: Record {index} is missing a name, which is required")
        return False
        
    if not isinstance(record.get('source_platform'), str) or not record.get('source_platform', '').strip():
        print(f"  Warning: Record {index} is missing source_platform, which is required")
        record['source_platform'] = 'devfolio'  # Default to devfolio
        
    # Validate mode value - avoid boolean context with arrays
    if (isinstance(record.get('mode'), str) and 
        record['mode'].strip() and 
        record['mode'] not in ('online', 'offline', 'hybrid', '')):
        print(f"  Warning: Record {index} has invalid mode value: {record['mode']}")
        # Try to normalize the mode
        mode = record['mode'].lower().strip()
        if 'online' in mode:
            record['mode'] = 'online'
        elif 'offline' in mode or 'in-person' in mode or 'person' in mode:
            record['mode'] = 'offline'
        elif 'hybrid' in mode:
            record['mode'] = 'hybrid'
        else:
            record['mode'] = None
            
    # Ensure that lists are handled properly        
    for field in ['tags', 'prizes_details']:
        if field in record:
            # Handle numpy arrays and pandas series
            if isinstance(record[field], (np.ndarray, pd.Series)):
                # Check size without using boolean context
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
            # Count NaT values before replacement
            nat_count = data_df[date_col].isna().sum()
            if nat_count > 0:
                print(f"  Found {nat_count} NaT values in {date_col}")
            
            # Replace NaT with None (will become NULL in JSON)
            data_df[date_col] = data_df[date_col].where(~pd.isna(data_df[date_col]), None)
    
    # Convert DataFrame to records safely
    try:
        print("Converting data to JSON-serializable format...")
        records = data_df.to_dict(orient='records')
        json_records = []
        
        # Process each record individually to catch and handle any errors
        for i, record in enumerate(records):
            try:
                # Check for NaT strings directly in the record before serialization
                for date_field in ['start_date', 'end_date', 'registration_deadline']:
                    if date_field in record and record[date_field] is pd.NaT:
                        record[date_field] = None
                
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
        # Read the CSV file
        print(f"Reading data from {CSV_FILE}...")
        df = pd.read_csv(CSV_FILE)
        print(f"Found {len(df)} records in CSV file")
        
        # Early validation check
        if df.empty:
            print("Error: The CSV file is empty")
            return
            
        if 'url' not in df.columns:
            print("Warning: CSV is missing the 'url' column needed for deduplication")
        
        # Clean and transform the data
        print("Transforming data to match database schema...")
        transformed_df = clean_and_transform_data(df)
        
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