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

# CSV file to import
CSV_FILE = "hackathons_20250323_032151.csv"

# Configure Supabase connection
# You need to add these to your .env file
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
# For admin operations that need to bypass RLS policies
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

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
    
    # BANNER IMAGE - Use banner as the main image_url (this is the primary image)
    transformed_df['image_url'] = df['banner_url'].fillna('')
    
    # BOTH IMAGES - Create a JSON field to store both banner and header urls with clear labels
    def create_images_json(row):
        images = {}
        # Add banner_url if exists - clearly labeled as "banner"
        if 'banner_url' in df.columns and not pd.isna(row.get('banner_url')) and row.get('banner_url'):
            images['banner'] = row.get('banner_url')
        
        # Add header_url if exists - clearly labeled as "header"
        if 'header_url' in df.columns and not pd.isna(row.get('header_url')) and row.get('header_url'):
            images['header'] = row.get('header_url')
            
        return json.dumps(images)
    
    # Apply the image JSON creation function row by row
    transformed_df['images'] = df.apply(create_images_json, axis=1)
    
    transformed_df['source_site'] = 'devfolio'  # Hardcoded source
    
    # Process dates - handle various formats and null values
    for date_field in ['start_date', 'end_date', 'registration_deadline']:
        # Try to convert to datetime, but set invalid dates to None
        transformed_df[date_field] = pd.to_datetime(df[date_field], errors='coerce')
    
    # Create tags from skills_required (convert list-like string to actual list)
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
    
    # Apply the extraction function, handling potential None values
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
    header_count = (~pd.isna(df['header_url']) if 'header_url' in df.columns else pd.Series([False] * len(df))).sum()
    print(f"  Rows with banner images (main image_url): {banner_count}")
    print(f"  Rows with header images: {header_count}")
    
    return transformed_df

def serialize_value(value):
    """Helper function to serialize a single value"""
    # Handle None, NaN, etc.
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
            if value is None or (isinstance(value, (list, pd.Series, np.ndarray)) and len(value) == 0):
                serialized[key] = []
            else:
                # Convert tags to a clean list with no duplicates
                if isinstance(value, (list, pd.Series, np.ndarray)):
                    # Clean up the tags - remove duplicates and empty strings
                    clean_tags = []
                    for tag in value:
                        if tag and isinstance(tag, str) and tag.strip():
                            clean_tags.append(tag.strip())
                    serialized[key] = list(set(clean_tags)) if clean_tags else []
                else:
                    serialized[key] = []
        else:
            # Use the helper function for non-tag values
            serialized[key] = serialize_value(value)
            
    return serialized

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
            "3. Copy the URL and anon/public key\n"
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
    """Get all existing hackathons URLs from Supabase"""
    try:
        # Only fetch URLs to save bandwidth and memory
        response = supabase.table('hackathons').select('url').execute()
        
        if hasattr(response, 'data') and response.data:
            # Create a set of existing URLs for faster lookup, filtering out None/empty values
            existing_urls = {item['url'] for item in response.data if item.get('url')}
            print(f"Found {len(existing_urls)} existing hackathons in database")
            return existing_urls
        else:
            print("No data returned when fetching existing hackathons")
            return set()
    except Exception as e:
        print(f"Error fetching existing hackathons: {str(e)}")
        return set()

def filter_out_duplicates(data_df, existing_urls):
    """Filter out hackathons that already exist in database"""
    # First ensure no URL is NaN or None
    data_df['url'] = data_df['url'].fillna('').astype(str)
    
    # If existing_urls is empty, return all records
    if not existing_urls:
        return data_df
    
    # Keep only records where URL is not in existing_urls and URL is not empty
    mask = (~data_df['url'].isin(existing_urls)) & (data_df['url'] != '')
    new_records = data_df[mask].copy()
    
    dupe_count = len(data_df) - len(new_records)
    print(f"Found {dupe_count} duplicate or invalid hackathons that will be skipped")
    print(f"Preparing to insert {len(new_records)} new hackathons")
    
    return new_records

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

def validate_record(record, index):
    """Validate a record before inserting into Supabase"""
    # Check for invalid dates
    for date_field in ['start_date', 'end_date', 'registration_deadline']:
        if date_field in record:
            if record[date_field] == "NaT" or str(record[date_field]).strip() == "NaT":
                print(f"  Warning: Record {index} has invalid {date_field} value: {record[date_field]}")
                record[date_field] = None
    
    # Return True if record is valid
    return True

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