import os
import sys
import uuid
import json
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client
import glob

# Load environment variables
load_dotenv()

# Configure Supabase connection
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

def connect_to_supabase() -> Client:
    """Connect to Supabase client"""
    if not SUPABASE_URL:
        raise ValueError("Missing Supabase URL. Add SUPABASE_URL to your .env file.")
    
    # Use service key if available (bypasses RLS), otherwise use anon key
    key_to_use = SUPABASE_SERVICE_KEY if SUPABASE_SERVICE_KEY else SUPABASE_KEY
    
    if not key_to_use:
        raise ValueError("Missing Supabase API key. Add SUPABASE_KEY or SUPABASE_SERVICE_KEY to your .env file.")
    
    try:
        return create_client(SUPABASE_URL, key_to_use)
    except Exception as e:
        raise

def get_existing_hackathons(supabase: Client):
    """Get URLs of existing hackathons to avoid duplicates"""
    try:
        response = supabase.table('hackathons').select('url').execute()
        data = response.data
        return set(item['url'] for item in data if 'url' in item)
    except Exception as e:
        print(f"Error fetching existing hackathons: {e}")
        return set()

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
                    # Use a set to remove duplicates then convert back to list
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
        # Special handling for schedule_details to ensure it's always a valid dict
        elif key == 'schedule_details':
            if value is None:
                serialized[key] = {}
            elif isinstance(value, dict):
                serialized[key] = value  # Keep dict as is
            elif isinstance(value, str):
                try:
                    serialized[key] = json.loads(value)
                except:
                    serialized[key] = {}
            else:
                serialized[key] = {}
        # Special handling for images to ensure it's always a valid dict
        elif key == 'images':
            if value is None:
                serialized[key] = {}
            elif isinstance(value, dict):
                serialized[key] = value  # Keep dict as is
            elif isinstance(value, str):
                try:
                    serialized[key] = json.loads(value)
                except:
                    serialized[key] = {}
            else:
                serialized[key] = {}
        else:
            # Use the helper function for non-tag values
            if pd.isna(value):
                serialized[key] = None
            else:
                serialized[key] = value
            
    return serialized

def map_source_fields(df, source_platform):
    """Map source-specific fields to our standardized schema"""
    print(f"Mapping fields for source: {source_platform}")
    
    # Create a new DataFrame with the expected column structure
    transformed_df = pd.DataFrame()
    
    # Common mappings for all platforms
    transformed_df['id'] = [str(uuid.uuid4()) for _ in range(len(df))]
    transformed_df['source_platform'] = source_platform
    transformed_df['created_at'] = datetime.now()
    transformed_df['last_updated'] = datetime.now()
    
    # Fill URL field first - crucial for deduplication
    if 'url' in df.columns:
        transformed_df['url'] = df['url'].fillna('').astype(str)
    
    # Add source-specific mappings
    if source_platform == 'devfolio':
        # Devfolio mapping (already handled in original script)
        transformed_df['name'] = df['title'].fillna('Unnamed Hackathon')
        transformed_df['description'] = df['description'].fillna('')
        transformed_df['location'] = df['location'].fillna('')
        transformed_df['mode'] = df['mode'].fillna('')
        transformed_df['prize_amount'] = df['prize_pool'].fillna('')
        transformed_df['organizer'] = df['organizer'].fillna('')
        transformed_df['runs_from_text'] = df['runs_from_text'].fillna('')
        transformed_df['happening_text'] = df['happening_text'].fillna('')
        transformed_df['status'] = 'active'  # Default status
        
        # Set original_id if it exists
        transformed_df['original_id'] = df['id'] if 'id' in df.columns else None
        
        # Process dates
        for date_field in ['start_date', 'end_date', 'registration_deadline']:
            transformed_df[date_field] = pd.to_datetime(df[date_field], errors='coerce')
        
        # Images
        transformed_df['banner_image_url'] = df['banner_url'].fillna('')
        transformed_df['logo_image_url'] = df['logo_url'].fillna('')
        
        # Handle JSON fields
        if 'schedule_details' in df.columns:
            transformed_df['schedule_details'] = df['schedule_details'].apply(
                lambda x: x if isinstance(x, dict) else {} if pd.isna(x) else json.loads(x) if isinstance(x, str) else {}
            )
        else:
            transformed_df['schedule_details'] = [{} for _ in range(len(df))]
        
        if 'prizes_details' in df.columns:
            transformed_df['prizes_details'] = df['prizes_details'].apply(
                lambda x: x if isinstance(x, dict) or isinstance(x, list) else 
                         [] if isinstance(x, list) else 
                         {} if pd.isna(x) else 
                         json.loads(x) if isinstance(x, str) else {}
            )
        else:
            transformed_df['prizes_details'] = [[] for _ in range(len(df))]
        
        # Tags
        if 'skills_required' in df.columns:
            transformed_df['tags'] = df['skills_required'].apply(
                lambda x: x if isinstance(x, list) else 
                         json.loads(x) if isinstance(x, str) and x.startswith('[') else 
                         x.split(',') if isinstance(x, str) else []
            )
        else:
            transformed_df['tags'] = [[] for _ in range(len(df))]
        
        # Participants count
        if 'num_participants' in df.columns:
            transformed_df['num_participants'] = pd.to_numeric(df['num_participants'], errors='coerce')
        
    elif source_platform == 'devpost':
        # Devpost mapping
        transformed_df['name'] = df['title'].fillna('Unnamed Hackathon')
        transformed_df['description'] = df['description'].fillna('')
        transformed_df['location'] = df['location'].fillna('')
        transformed_df['mode'] = df['mode'].fillna('')
        transformed_df['prize_amount'] = df['prize_pool'].fillna('')
        transformed_df['organizer'] = df['organizer'].fillna('')
        transformed_df['status'] = df['status'].fillna('active') if 'status' in df.columns else 'active'
        
        # Set original_id if it exists
        transformed_df['original_id'] = df['id'] if 'id' in df.columns else None
        
        # Process dates
        for date_field in ['start_date', 'end_date', 'registration_deadline']:
            if date_field in df.columns:
                transformed_df[date_field] = pd.to_datetime(df[date_field], errors='coerce')
            else:
                transformed_df[date_field] = None
        
        # Images
        transformed_df['banner_image_url'] = df['banner_url'].fillna('')
        transformed_df['logo_image_url'] = df['logo_url'].fillna('')
        
        # Handle JSON fields
        if 'schedule_details' in df.columns:
            transformed_df['schedule_details'] = df['schedule_details'].apply(
                lambda x: x if isinstance(x, dict) else {} if pd.isna(x) else json.loads(x) if isinstance(x, str) else {}
            )
        else:
            # Create schedule details from dates if available
            transformed_df['schedule_details'] = df.apply(
                lambda row: {'schedule': f"{row.get('start_date', '')} to {row.get('end_date', '')}"} 
                if not pd.isna(row.get('start_date')) and not pd.isna(row.get('end_date')) 
                else {}, axis=1
            )
        
        if 'prizes_details' in df.columns:
            transformed_df['prizes_details'] = df['prizes_details'].apply(
                lambda x: x if isinstance(x, dict) or isinstance(x, list) else 
                         [] if isinstance(x, list) else 
                         {} if pd.isna(x) else 
                         json.loads(x) if isinstance(x, str) else {}
            )
        else:
            transformed_df['prizes_details'] = [[] for _ in range(len(df))]
        
        # Tags
        if 'tags' in df.columns:
            transformed_df['tags'] = df['tags'].apply(
                lambda x: x if isinstance(x, list) else 
                         json.loads(x) if isinstance(x, str) and x.startswith('[') else 
                         x.split(',') if isinstance(x, str) else []
            )
        else:
            transformed_df['tags'] = [[] for _ in range(len(df))]
        
        # Participants count
        if 'num_participants' in df.columns:
            transformed_df['num_participants'] = pd.to_numeric(df['num_participants'], errors='coerce')
    
    elif source_platform == 'mlh':
        # MLH mapping
        transformed_df['name'] = df['title'].fillna('Unnamed Hackathon')
        transformed_df['description'] = df['description'].fillna('')
        transformed_df['location'] = df['location'].fillna('')
        transformed_df['mode'] = df['mode'].fillna('')
        transformed_df['organizer'] = df['organizer'].fillna('MLH')
        transformed_df['status'] = 'active'  # MLH mainly lists active hackathons
        
        # Set original_id if it exists
        transformed_df['original_id'] = df['id'] if 'id' in df.columns else None
        
        # MLH usually doesn't have prize info, but add it if available
        if 'prize_pool' in df.columns:
            transformed_df['prize_amount'] = df['prize_pool'].fillna('')
        else:
            transformed_df['prize_amount'] = ''
        
        # Process dates
        for date_field in ['start_date', 'end_date']:
            if date_field in df.columns:
                transformed_df[date_field] = pd.to_datetime(df[date_field], errors='coerce')
            else:
                transformed_df[date_field] = None
        
        # Registration deadline often not available for MLH
        transformed_df['registration_deadline'] = None
        
        # Images
        transformed_df['banner_image_url'] = df['banner_url'].fillna('')
        transformed_df['logo_image_url'] = df['logo_url'].fillna('')
        
        # Create JSON fields
        transformed_df['schedule_details'] = df.apply(
            lambda row: {'schedule': f"{row.get('start_date', '')} to {row.get('end_date', '')}"} 
            if 'start_date' in row and 'end_date' in row and not pd.isna(row.get('start_date')) and not pd.isna(row.get('end_date')) 
            else {}, axis=1
        )
        
        transformed_df['prizes_details'] = [[] for _ in range(len(df))]
        
        # Tags
        if 'tags' in df.columns:
            transformed_df['tags'] = df['tags'].apply(
                lambda x: x if isinstance(x, list) else 
                         x.split(',') if isinstance(x, str) else []
            )
        else:
            transformed_df['tags'] = [[] for _ in range(len(df))]

    elif source_platform == 'hackerearth':
        # HackerEarth mapping
        transformed_df['name'] = df['title'].fillna('Unnamed Hackathon')
        transformed_df['description'] = df['description'].fillna('')
        transformed_df['location'] = df['location'].fillna('')
        transformed_df['mode'] = df['mode'].fillna('')
        transformed_df['prize_amount'] = df['prize_pool'].fillna('')
        transformed_df['organizer'] = df['organizer'].fillna('HackerEarth')
        
        # Status from HackerEarth data if available
        if 'status' in df.columns:
            transformed_df['status'] = df['status'].apply(
                lambda x: 'active' if x == 'LIVE' else 
                         'upcoming' if x == 'UPCOMING' else 
                         'completed' if x == 'PREVIOUS' else 'active'
            )
        else:
            transformed_df['status'] = 'active'
        
        # Set original_id if it exists
        transformed_df['original_id'] = df['id'] if 'id' in df.columns else None
        
        # Process dates
        for date_field in ['start_date', 'end_date', 'registration_deadline']:
            if date_field in df.columns:
                transformed_df[date_field] = pd.to_datetime(df[date_field], errors='coerce')
            else:
                transformed_df[date_field] = None
        
        # Images
        transformed_df['banner_image_url'] = df['banner_url'].fillna('')
        transformed_df['logo_image_url'] = df['logo_url'].fillna('')
        
        # Handle JSON fields
        if 'prizes' in df.columns:
            transformed_df['prizes_details'] = df['prizes'].apply(
                lambda x: x if isinstance(x, dict) or isinstance(x, list) else 
                         json.loads(x) if isinstance(x, str) else []
            )
        else:
            transformed_df['prizes_details'] = [[] for _ in range(len(df))]
        
        # Create schedule details from dates and any other time info
        transformed_df['schedule_details'] = df.apply(
            lambda row: {
                'schedule': f"{row.get('start_date', '')} to {row.get('end_date', '')}",
                'phase': row.get('phase', ''),
                'status': row.get('status', '')
            } if 'start_date' in row and 'end_date' in row and not pd.isna(row.get('start_date')) and not pd.isna(row.get('end_date')) 
            else {}, axis=1
        )
        
        # Tags
        if 'tags' in df.columns:
            transformed_df['tags'] = df['tags'].apply(
                lambda x: x if isinstance(x, list) else 
                         x.split(',') if isinstance(x, str) else []
            )
        elif 'themes_summary' in df.columns:
            transformed_df['tags'] = df['themes_summary'].apply(
                lambda x: x.split('|') if isinstance(x, str) else []
            )
        else:
            transformed_df['tags'] = [[] for _ in range(len(df))]
        
        # Participants count
        if 'registered_participants' in df.columns:
            transformed_df['num_participants'] = pd.to_numeric(df['registered_participants'], errors='coerce')
        elif 'num_participants' in df.columns:
            transformed_df['num_participants'] = pd.to_numeric(df['num_participants'], errors='coerce')

    elif source_platform == 'kaggle':
        # Kaggle mapping
        transformed_df['name'] = df['title'].fillna('Unnamed Competition')
        transformed_df['description'] = df['description'].fillna('')
        transformed_df['location'] = 'Online'  # Kaggle competitions are online
        transformed_df['mode'] = 'online'      # Kaggle competitions are online
        transformed_df['prize_amount'] = df['prize_pool'].fillna('')
        transformed_df['organizer'] = df['organizer'].fillna('Kaggle')
        
        # Status from Kaggle data if available
        if 'status' in df.columns:
            transformed_df['status'] = df['status']
        else:
            transformed_df['status'] = 'active'
        
        # Set original_id if it exists
        transformed_df['original_id'] = df['id'] if 'id' in df.columns else None
        
        # Process dates
        for date_field in ['start_date', 'end_date', 'registration_deadline']:
            if date_field in df.columns:
                transformed_df[date_field] = pd.to_datetime(df[date_field], errors='coerce')
            else:
                transformed_df[date_field] = None
        
        # Images - Kaggle often doesn't have these
        transformed_df['banner_image_url'] = df['banner_url'].fillna('') if 'banner_url' in df.columns else ''
        transformed_df['logo_image_url'] = df['logo_url'].fillna('') if 'logo_url' in df.columns else ''
        
        # Handle JSON fields
        if 'prizes_details' in df.columns:
            transformed_df['prizes_details'] = df['prizes_details'].apply(
                lambda x: x if isinstance(x, dict) or isinstance(x, list) else 
                         json.loads(x) if isinstance(x, str) else []
            )
        else:
            transformed_df['prizes_details'] = [[] for _ in range(len(df))]
        
        # Create schedule details
        transformed_df['schedule_details'] = df.apply(
            lambda row: {'schedule': f"{row.get('start_date', '')} to {row.get('end_date', '')}"} 
            if 'start_date' in row and 'end_date' in row and not pd.isna(row.get('start_date')) and not pd.isna(row.get('end_date')) 
            else {}, axis=1
        )
        
        # Tags - competitions often have categories or tags
        if 'categories' in df.columns:
            transformed_df['tags'] = df['categories'].apply(
                lambda x: x if isinstance(x, list) else 
                         x.split(',') if isinstance(x, str) else []
            )
        elif 'tags' in df.columns:
            transformed_df['tags'] = df['tags'].apply(
                lambda x: x if isinstance(x, list) else 
                         x.split(',') if isinstance(x, str) else []
            )
        else:
            transformed_df['tags'] = [[] for _ in range(len(df))]
        
        # Participants count
        if 'num_participants' in df.columns:
            transformed_df['num_participants'] = pd.to_numeric(df['num_participants'], errors='coerce')
        elif 'participants_count' in df.columns:
            transformed_df['num_participants'] = pd.to_numeric(df['participants_count'], errors='coerce')
    
    # Create the images JSON object for all platforms
    transformed_df['images'] = df.apply(
        lambda row: {
            'banner': row.get('banner_url', '') if 'banner_url' in row and not pd.isna(row.get('banner_url', '')) else '',
            'logo': row.get('logo_url', '') if 'logo_url' in row and not pd.isna(row.get('logo_url', '')) else '',
        }, axis=1
    )
    
    print(f"Field mapping complete for {source_platform}. Total rows: {len(transformed_df)}")
    
    return transformed_df

def import_from_csv(csv_file, source_platform=None):
    """Import data from CSV file with source platform detection"""
    print(f"Reading data from {csv_file}...")
    
    # Try to detect source platform if not specified
    if not source_platform:
        if 'devfolio' in csv_file.lower():
            source_platform = 'devfolio'
        elif 'devpost' in csv_file.lower():
            source_platform = 'devpost'
        elif 'mlh' in csv_file.lower():
            source_platform = 'mlh'
        elif 'hackerearth' in csv_file.lower():
            source_platform = 'hackerearth'
        elif 'kaggle' in csv_file.lower():
            source_platform = 'kaggle'
        else:
            raise ValueError(f"Could not detect source platform from filename: {csv_file}. Please specify source_platform.")
    
    print(f"Detected source platform: {source_platform}")
    
    # Read CSV
    df = pd.read_csv(csv_file)
    print(f"Found {len(df)} records in CSV file")
    
    # Map fields based on source platform
    transformed_df = map_source_fields(df, source_platform)
    
    return transformed_df

def validate_record(record):
    """Validate record before insertion"""
    # Required fields
    if not isinstance(record.get('url'), str) or not record.get('url', '').strip():
        print(f"Warning: Record is missing a URL, which is required")
        return False
    
    if not isinstance(record.get('name'), str) or not record.get('name', '').strip():
        print(f"Warning: Record is missing a name, which is required")
        return False
    
    if not isinstance(record.get('source_platform'), str) or not record.get('source_platform', '').strip():
        print(f"Warning: Record is missing source_platform, which is required")
        return False
    
    # JSON fields should be valid
    for field in ['images', 'schedule_details', 'prizes_details']:
        if field in record:
            value = record[field]
            if not (value is None or isinstance(value, dict) or isinstance(value, list)):
                print(f"Warning: Record has invalid {field} value type: {type(value)}")
                return False
    
    # Date fields should be valid dates or None
    for field in ['start_date', 'end_date', 'registration_deadline']:
        if field in record:
            value = record[field]
            if value and value != "NaT" and not pd.isna(value):
                if not (isinstance(value, str) or isinstance(value, datetime)):
                    print(f"Warning: Record has invalid {field} value type: {type(value)}")
                    return False
    
    return True

def prepare_records_for_insert(df):
    """Prepare records for insertion into Supabase"""
    records = []
    
    for _, row in df.iterrows():
        record = row.to_dict()
        
        # Clean up NaN values
        for key, value in list(record.items()):
            if pd.isna(value):
                record[key] = None
        
        # Convert all dates to ISO format strings
        for date_field in ['start_date', 'end_date', 'registration_deadline', 'created_at', 'last_updated']:
            if date_field in record and record[date_field] is not None:
                if isinstance(record[date_field], (pd.Timestamp, datetime)):
                    record[date_field] = record[date_field].isoformat()
                elif record[date_field] == 'NaT':
                    record[date_field] = None
        
        # Make JSON serializable
        record = json_serializable_record(record)
        
        # Validate record
        if validate_record(record):
            records.append(record)
    
    return records

def main():
    try:
        # Connect to Supabase
        print("Connecting to Supabase...")
        supabase = connect_to_supabase()
        
        # Get existing hackathons to avoid duplicates
        print("Fetching existing hackathons to prevent duplicates...")
        existing_urls = get_existing_hackathons(supabase)
        print(f"Found {len(existing_urls)} existing hackathons in the database")
        
        # Process all CSV files in the current directory with hackathon data
        csv_files = []
        sources = ['devfolio', 'devpost', 'mlh', 'hackerearth', 'kaggle']
        
        for source in sources:
            # Look for CSV files for this source
            source_files = glob.glob(f"*{source}*.csv")
            if source_files:
                print(f"Found {len(source_files)} CSV files for {source}")
                csv_files.extend([(file, source) for file in source_files])
        
        if not csv_files:
            print("No CSV files found. Please run crawlers first.")
            return
        
        print(f"Found {len(csv_files)} total CSV files to process")
        
        # Process each CSV file
        for csv_file, source in csv_files:
            print(f"\nProcessing {csv_file} (source: {source})...")
            
            # Import and transform data
            transformed_df = import_from_csv(csv_file, source)
            
            # Skip empty dataframes
            if transformed_df.empty:
                print(f"No data to import from {csv_file}")
                continue
            
            # Filter out duplicates
            print(f"Checking for duplicates among {len(transformed_df)} records...")
            if 'url' in transformed_df.columns:
                duplicates = transformed_df['url'].isin(existing_urls)
                new_records = transformed_df[~duplicates]
                
                print(f"Found {duplicates.sum()} duplicates, {len(new_records)} new records")
                
                if new_records.empty:
                    print(f"No new records to import from {csv_file}")
                    continue
                
                # Prepare records for insert
                records = prepare_records_for_insert(new_records)
                
                # Insert data in batches
                BATCH_SIZE = 10
                total_inserted = 0
                
                for i in range(0, len(records), BATCH_SIZE):
                    batch = records[i:i+BATCH_SIZE]
                    try:
                        response = supabase.table('hackathons').insert(batch).execute()
                        
                        if hasattr(response, 'data'):
                            inserted_count = len(response.data)
                            total_inserted += inserted_count
                            print(f"Inserted batch {i//BATCH_SIZE + 1} ({inserted_count} records)")
                            
                            # Update existing_urls with new URLs
                            for record in batch:
                                if 'url' in record and record['url']:
                                    existing_urls.add(record['url'])
                        else:
                            print(f"Warning: Unexpected response format from batch {i//BATCH_SIZE + 1}")
                        
                    except Exception as e:
                        print(f"Error inserting batch {i//BATCH_SIZE + 1}: {e}")
                
                print(f"Successfully inserted {total_inserted} out of {len(records)} records from {csv_file}")
            else:
                print(f"CSV file {csv_file} is missing the 'url' column required for deduplication")
                
        print("\nImport complete!")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 