# Importing Hackathon Data to Supabase

This document explains how to import the harvested hackathon data into your Supabase database.

## Prerequisites

1. A Supabase account and project set up with the proper database schema:
   ```sql
   CREATE TABLE hackathons (
       id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
       name TEXT NOT NULL,
       description TEXT,
       url TEXT,
       start_date TIMESTAMP WITH TIME ZONE,
       end_date TIMESTAMP WITH TIME ZONE,
       location TEXT,
       mode TEXT,
       registration_deadline TIMESTAMP WITH TIME ZONE,
       prize_amount TEXT,
       organizer TEXT,
       tags TEXT[],
       image_url TEXT,
       source_site TEXT,
       last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
       created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
   );

   -- Create indexes for faster querying
   CREATE INDEX idx_hackathons_start_date ON hackathons (start_date);
   CREATE INDEX idx_hackathons_location ON hackathons (location);
   ```

2. Python 3.7+ installed on your machine
3. Required packages installed: `pip install -r requirements.txt`

## Setup

1. Open the `.env` file and add your Supabase credentials:
   ```
   SUPABASE_URL=https://your-project-id.supabase.co
   SUPABASE_KEY=your-anon-key
   ```

   You can find these credentials in your Supabase project dashboard under Settings > API.

## Running the Import

1. Make sure your CSV file is in the project directory (the script is configured to use `hackathons_20250323_032151.csv`)

2. Run the import script:
   ```
   python import_to_supabase.py
   ```

3. The script will:
   - Read data from the CSV file
   - Transform it to match the database schema
   - Connect to your Supabase instance
   - Import the data in batches
   - Display progress information

## Troubleshooting

1. **Connection Issues**: Make sure your Supabase credentials are correct and your network allows connections to Supabase.

2. **Data Format Issues**: The script attempts to clean and format the data, but if you encounter errors, you may need to examine the CSV data and adjust the transformation logic in `clean_and_transform_data()`.

3. **Duplicate Records**: If you run the import multiple times, you might get duplicate records. To prevent this, consider adding a unique constraint on URL or other identifying fields.

## Modifications

- If your CSV file has a different name or structure, you'll need to update the `CSV_FILE` constant and possibly the mapping logic in the script.
- You can adjust the `BATCH_SIZE` constant to import more or fewer records at once (larger batches are faster but might hit timeouts).

## Accessing Data from Frontend

Once the data is imported, you can access it from your frontend application using the Supabase client. Example:

```javascript
// In your React/Next.js/etc. frontend
import { createClient } from '@supabase/supabase-js';

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL,
  process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY
);

// Get all upcoming hackathons
const getUpcomingHackathons = async () => {
  const { data, error } = await supabase
    .from('hackathons')
    .select('*')
    .gt('start_date', new Date().toISOString())
    .order('start_date', { ascending: true });
    
  if (error) console.error('Error fetching hackathons:', error);
  return data;
};
``` 