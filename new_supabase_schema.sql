-- Hackathons Database Schema for Supabase
-- This schema is optimized for storing hackathon data from various platforms

-- Create hackathons table with comprehensive structure
CREATE TABLE IF NOT EXISTS public.hackathons (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Basic Information
    name TEXT NOT NULL,                    -- Name/title of the hackathon
    description TEXT,                      -- Full description
    url TEXT NOT NULL UNIQUE,              -- Unique identifier - original URL from the source
    
    -- Location & Mode Information
    location TEXT,                         -- Physical location (if applicable)
    mode TEXT CHECK (mode IN ('online', 'offline', 'hybrid', NULL)),  -- Online, offline, hybrid
    
    -- Date Information (with timezone support)
    start_date TIMESTAMPTZ,                -- When the hackathon starts
    end_date TIMESTAMPTZ,                  -- When the hackathon ends
    registration_deadline TIMESTAMPTZ,     -- Deadline for registration
    
    -- Original time/date text (helpful context)
    runs_from_text TEXT,                   -- Raw text of when hackathon runs
    happening_text TEXT,                   -- Raw text of when hackathon is happening
    
    -- Prize Information
    prize_amount TEXT,                     -- Prize pool amount (as text to preserve currency symbols)
    prizes_details JSONB,                  -- Structured details about prizes
    
    -- Participation Details
    num_participants INTEGER,              -- Number of participants
    
    -- Skills and Tags
    tags TEXT[],                           -- Skills/tags as a searchable array
    
    -- Organizer Information
    organizer TEXT,                        -- Organizer name
    
    -- Source Tracking (where this hackathon was scraped from)
    source_platform TEXT NOT NULL,         -- Platform source (devfolio, devpost, etc.)
    
    -- Images
    banner_image_url TEXT,                 -- Primary/banner image URL
    logo_image_url TEXT,                   -- Logo image URL
    images JSONB,                          -- Structured JSON of all image URLs with labels
    
    -- Additional structured data
    schedule_details JSONB,                -- Schedule information in structured format
    
    -- Metadata and timestamps
    created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    last_updated TIMESTAMPTZ DEFAULT now() NOT NULL
);

-- Add comments to the table and columns for documentation
COMMENT ON TABLE public.hackathons IS 'Table storing hackathon events from various platforms';
COMMENT ON COLUMN public.hackathons.source_platform IS 'The platform this hackathon was scraped from (e.g., devfolio, devpost)';
COMMENT ON COLUMN public.hackathons.images IS 'JSON structure with labeled images (banner, logo, header, etc.)';
COMMENT ON COLUMN public.hackathons.prizes_details IS 'Structured information about hackathon prizes';

-- Create indexes for common search and filtering operations
CREATE INDEX IF NOT EXISTS hackathons_name_idx ON public.hackathons (name);
CREATE INDEX IF NOT EXISTS hackathons_start_date_idx ON public.hackathons (start_date);
CREATE INDEX IF NOT EXISTS hackathons_source_platform_idx ON public.hackathons (source_platform);
CREATE INDEX IF NOT EXISTS hackathons_tags_idx ON public.hackathons USING GIN (tags);

-- Add a full-text search index for name and description
CREATE INDEX IF NOT EXISTS hackathons_fts_idx ON public.hackathons 
USING GIN (to_tsvector('english', coalesce(name, '') || ' ' || coalesce(description, '')));

-- Enable Row Level Security
ALTER TABLE public.hackathons ENABLE ROW LEVEL SECURITY;

-- Create a policy for read access to all users
CREATE POLICY "Allow read access for all users" 
ON public.hackathons FOR SELECT USING (true);

-- Create a policy for insert and update access to authenticated users
CREATE POLICY "Allow insert for authenticated users" 
ON public.hackathons FOR INSERT TO authenticated WITH CHECK (true);

-- Create a policy for update access to authenticated users
CREATE POLICY "Allow update for authenticated users" 
ON public.hackathons FOR UPDATE TO authenticated USING (true);

-- Create policy for full access for service roles
CREATE POLICY "Allow all access for service role" 
ON public.hackathons FOR ALL TO service_role USING (true) WITH CHECK (true);

-- Grant access to public and authenticated users
GRANT SELECT ON public.hackathons TO public;
GRANT INSERT, UPDATE, DELETE ON public.hackathons TO authenticated;

-- Create RLS trigger to automatically update the last_updated timestamp
CREATE OR REPLACE FUNCTION public.handle_updated_at() 
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_updated = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_updated_at
BEFORE UPDATE ON public.hackathons
FOR EACH ROW EXECUTE FUNCTION public.handle_updated_at(); 