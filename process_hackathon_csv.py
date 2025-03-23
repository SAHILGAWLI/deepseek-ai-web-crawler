import os
import csv
import json
import argparse
from dotenv import load_dotenv
from groq import Groq

# Load environment variables
load_dotenv()

# Initialize Groq client
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

def process_hackathon_data(file_path):
    """
    Process the hackathon CSV file, extract prize and schedule data,
    and use Groq API to return just the extracted information.
    """
    # Initialize a list to store processed hackathons
    processed_hackathons = []
    
    # Read the CSV file
    with open(file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        
        for row in reader:
            # Extract the name, prizes_details, and schedule_details
            name = row.get('name', 'Unknown Hackathon')
            prizes_details = row.get('prizes_details', '')
            schedule_details = row.get('schedule_details', '')
            
            # Skip if no prize or schedule details
            if not prizes_details and not schedule_details:
                print(f"Skipping {name}: No prize or schedule details found")
                continue
            
            # Truncate details to avoid token limit issues
            # An approximate heuristic to avoid exceeding the token limit
            MAX_CHARS = 2000  # Conservative estimate
            if len(prizes_details) > MAX_CHARS:
                print(f"Truncating prize details for {name} from {len(prizes_details)} to {MAX_CHARS} characters")
                prizes_details = prizes_details[:MAX_CHARS] + "..."
            
            if len(schedule_details) > MAX_CHARS:
                print(f"Truncating schedule details for {name} from {len(schedule_details)} to {MAX_CHARS} characters")
                schedule_details = schedule_details[:MAX_CHARS] + "..."
            
            # Prepare the data to send to Groq
            hackathon_data = {
                'name': name,
                'prizes_details': prizes_details,
                'schedule_details': schedule_details
            }
            
            # Process with Groq API
            processed_data = extract_info_with_groq(hackathon_data)
            processed_hackathons.append(processed_data)
    
    # Save processed data to new CSV
    output_file = file_path.replace('.csv', '_processed.csv')
    save_to_csv(processed_hackathons, output_file)
    
    return processed_hackathons

def extract_info_with_groq(hackathon_data):
    """
    Use Groq API to extract just the prize and schedule information from the hackathon data.
    """
    prompt = f"""
    You are a data extraction assistant. Extract the precise prize and schedule details from the following hackathon information.
    
    Hackathon: {hackathon_data['name']}
    
    Prize Details: {hackathon_data['prizes_details']}
    
    Schedule Details: {hackathon_data['schedule_details']}
    
    Output Format:
    {{
        "name": "{hackathon_data['name']}",
        "prize_summary": "A clear, concise summary of all prizes, including total prize pool and breakdown of individual prizes",
        "schedule_summary": "A concise summary of the key dates and schedule information"
    }}
    
    Only include factual information that is explicitly mentioned in the provided text. Do not add any additional information or assumptions.
    """
    
    try:
        # Call Groq API with the prompt
        response = client.chat.completions.create(
            model="llama3-8b-8192",  # Using a currently supported model
            messages=[
                {"role": "system", "content": "You are a precise data extraction assistant that extracts exact prize details and schedule information from hackathon data."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,  # Low temperature for more consistent output
            max_tokens=1024
        )
        
        # Extract response content
        content = response.choices[0].message.content
        print(f"Response for {hackathon_data['name']}: {content}")
        
        # Parse JSON from the response
        try:
            # Try to extract JSON from the response
            if '{' in content and '}' in content:
                json_str = content[content.find('{'):content.rfind('}')+1]
                data = json.loads(json_str)
            else:
                # If no JSON format is found, create a structured response
                data = {
                    "name": hackathon_data['name'],
                    "prize_summary": content.strip() if content else "Could not extract prize information",
                    "schedule_summary": "Could not extract schedule information"
                }
        except json.JSONDecodeError:
            print(f"Error parsing JSON for {hackathon_data['name']}, using raw response")
            data = {
                "name": hackathon_data['name'],
                "prize_summary": content.strip() if content else "Error parsing response",
                "schedule_summary": "Error parsing schedule information"
            }
        
        return data
    
    except Exception as e:
        print(f"Error processing {hackathon_data['name']} with Groq API: {str(e)}")
        return {
            "name": hackathon_data['name'],
            "prize_summary": "Error processing with API",
            "schedule_summary": "Error processing with API"
        }

def save_to_csv(hackathons, output_file):
    """
    Save the processed hackathon data to a new CSV file.
    """
    if not hackathons:
        print("No hackathons to save")
        return
    
    # Define fieldnames based on the data structure
    fieldnames = ['name', 'prize_summary', 'schedule_summary']
    
    # Write to CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(hackathons)
    
    print(f"Saved {len(hackathons)} processed hackathons to '{output_file}'")

def main():
    parser = argparse.ArgumentParser(description='Process hackathon CSV data with Groq API')
    parser.add_argument('--input', type=str, default='hackathons_20250322_204832.csv',
                        help='Path to the input CSV file')
    args = parser.parse_args()
    
    if not os.path.exists(args.input):
        print(f"Error: Input file '{args.input}' not found")
        return
    
    print(f"Processing hackathon data from: {args.input}")
    processed_data = process_hackathon_data(args.input)
    print(f"Processed {len(processed_data)} hackathons")

if __name__ == "__main__":
    main() 