import os
import csv
import json
import time
from datetime import datetime
from typing import List, Dict, Any
from dotenv import load_dotenv
import pandas as pd
from crawl4ai.browser import Browser

# Load environment variables
load_dotenv()

# Configuration
BASE_URL = "https://devpost.com/hackathons"
DEFAULT_PARAMS = "?open_to[]=public&status[]=open"
MAX_PAGES = 5  # Number of pagination pages to crawl
MAX_HACKATHONS = 100  # Maximum number of hackathons to crawl
DOWNLOAD_FOLDER = "screenshots"

# Ensure screenshot directory exists
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

class DevpostCrawler:
    """
    A crawler for extracting hackathon data from Devpost.com using craw4ai
    """
    
    def __init__(self):
        self.browser = None
        self.hackathons = []
    
    async def initialize(self):
        """Initialize the browser"""
        self.browser = Browser(
            headless=True,
            default_timeout=30000,
            downloads_folder=DOWNLOAD_FOLDER
        )
        await self.browser.start()
    
    async def take_screenshot(self, filename):
        """Take a screenshot for debugging purposes"""
        await self.browser.screenshot(path=os.path.join(DOWNLOAD_FOLDER, filename))
        print(f"Screenshot saved as {filename}")
    
    async def extract_hackathon_links(self):
        """Extract links to individual hackathon pages from the current page"""
        links = await self.browser.evaluate("""
            () => {
                // First try to get hackathon cards
                const cards = document.querySelectorAll('a.hackathon-card');
                if (cards.length > 0) {
                    console.log(`Found ${cards.length} hackathon cards`);
                    return Array.from(cards).map(card => card.href);
                }
                
                // Fallback: try alternative selectors for hackathon links
                const allLinks = Array.from(document.querySelectorAll('a'))
                    .filter(a => a.href && a.href.includes('/hackathons/'))
                    .map(a => a.href);
                
                // Filter out navigation links and keep only hackathon detail links
                return allLinks.filter(link => {
                    // Exclude links to category pages or listings
                    return !link.includes('?') && 
                           !link.includes('/hackathons/filter/') && 
                           link.match(/[a-z0-9-]+$/);
                });
            }
        """)
        
        print(f"Found {len(links)} hackathon links on current page")
        return links
    
    async def extract_hackathon_details(self, url):
        """Extract detailed information from a hackathon page"""
        try:
            print(f"Navigating to {url}")
            await self.browser.goto(url, wait_until="domcontentloaded")
            await self.browser.wait(2000)  # Wait for JS to execute
            
            # Get the hackathon slug for the screenshot filename
            hackathon_slug = url.split('/')[-1]
            await self.take_screenshot(f"hackathon_{hackathon_slug}.png")
            
            # Extract basic information
            basic_info = await self.browser.evaluate("""
                () => {
                    const titleElement = document.querySelector('h1.title');
                    const title = titleElement ? titleElement.textContent.trim() : '';
                    
                    // Extract description
                    const descriptionElement = document.querySelector('div.description');
                    const description = descriptionElement ? descriptionElement.textContent.trim() : '';
                    
                    // Extract organizer
                    const organizerElement = document.querySelector('.organizer-info h5 a');
                    const organizer = organizerElement ? organizerElement.textContent.trim() : '';
                    
                    // Extract logo
                    const logoElement = document.querySelector('.hackathon-logo img');
                    const logo_url = logoElement ? logoElement.src : '';
                    
                    // Extract banner
                    const bannerElement = document.querySelector('.cover-image img, .header-image img');
                    const banner_url = bannerElement ? bannerElement.src : '';
                    
                    // Extract participant count
                    const statElements = document.querySelectorAll('.stats .stat');
                    let participantCount = '';
                    for (const stat of statElements) {
                        if (stat.textContent.includes('participant')) {
                            const countElement = stat.querySelector('.count');
                            if (countElement) {
                                participantCount = countElement.textContent.trim();
                            }
                            break;
                        }
                    }
                    
                    // Extract prize info
                    const prizeElement = document.querySelector('.prize .value, .prizes .value');
                    const prizeText = prizeElement ? prizeElement.textContent.trim() : '';
                    
                    // Extract location/mode
                    const locationElement = document.querySelector('.location');
                    const locationText = locationElement ? locationElement.textContent.trim() : '';
                    
                    // Determine mode (online/offline/hybrid)
                    let mode = '';
                    if (locationText) {
                        if (locationText.toLowerCase().includes('online')) {
                            mode = 'online';
                        } else if (locationText.toLowerCase().includes('hybrid')) {
                            mode = 'hybrid';
                        } else {
                            mode = 'offline';
                        }
                    }
                    
                    // Extract dates from sidebar
                    const dateElements = document.querySelectorAll('.side-bar .date-range, .side-bar .dates');
                    let startDate = '';
                    let endDate = '';
                    let deadlineDate = '';
                    
                    // Process date elements
                    for (const dateEl of dateElements) {
                        const dateText = dateEl.textContent.trim();
                        
                        // Check if this is the timeline section
                        if (dateText.includes('Timeline') || dateEl.closest('.timeline')) {
                            continue;
                        }
                        
                        // Try to identify what kind of date this is
                        if (dateText.toLowerCase().includes('registration') || 
                            dateText.toLowerCase().includes('deadline')) {
                            
                            const dateMatch = dateText.match(/(\\d{1,2}\\/\\d{1,2}\\/\\d{4}|\\w+ \\d{1,2},? \\d{4})/);
                            if (dateMatch) {
                                deadlineDate = dateMatch[0];
                            }
                        }
                        else if (dateText.includes('-') || dateText.includes('to')) {
                            // This is likely a date range
                            const dates = dateText.split(/[-–—]|to/).map(d => d.trim());
                            if (dates.length >= 2) {
                                // Check if the first date has month and year
                                const firstDateMatch = dates[0].match(/(\\d{1,2}\\/\\d{1,2}\\/\\d{4}|\\w+ \\d{1,2},? \\d{4})/);
                                const secondDateMatch = dates[1].match(/(\\d{1,2}\\/\\d{1,2}\\/\\d{4}|\\w+ \\d{1,2},? \\d{4})/);
                                
                                startDate = firstDateMatch ? firstDateMatch[0] : dates[0];
                                endDate = secondDateMatch ? secondDateMatch[0] : dates[1];
                            }
                        }
                    }
                    
                    // Look for "Runs from" and "Happening" in the sidebar
                    const sidebarTextElements = document.querySelectorAll('.side-bar .item');
                    let runsFromText = '';
                    let happeningText = '';
                    
                    for (const element of sidebarTextElements) {
                        const titleElement = element.querySelector('.title');
                        if (!titleElement) continue;
                        
                        const title = titleElement.textContent.trim();
                        const valueElement = element.querySelector('.value');
                        const value = valueElement ? valueElement.textContent.trim() : '';
                        
                        if (title.includes('RUNS FROM')) {
                            runsFromText = value;
                            
                            // Try to parse start and end date from this text
                            if (!startDate || !endDate) {
                                const dates = value.split(/[-–—]|to/).map(d => d.trim());
                                if (dates.length >= 2) {
                                    startDate = dates[0];
                                    endDate = dates[1];
                                }
                            }
                        } 
                        else if (title.includes('HAPPENING')) {
                            happeningText = value;
                        }
                    }
                    
                    // Extract skills required
                    const skillElements = document.querySelectorAll('.topics .topic, .topics .tag, .tag-container .tag');
                    const skills = Array.from(skillElements).map(el => el.textContent.trim());
                    
                    return {
                        title,
                        description,
                        organizer,
                        logo_url,
                        banner_url,
                        participant_count: participantCount,
                        prize_pool: prizeText,
                        location: locationText,
                        mode,
                        start_date: startDate,
                        end_date: endDate,
                        registration_deadline: deadlineDate,
                        skills_required: skills,
                        runs_from_text: runsFromText,
                        happening_text: happeningText
                    };
                }
            """)
            
            # Extract prizes details
            prizes_details = await self.browser.evaluate("""
                () => {
                    const prizesSection = document.querySelector('.prizes');
                    if (!prizesSection) return {};
                    
                    const prizeItems = prizesSection.querySelectorAll('.prize-item, .prize');
                    const prizes = {};
                    
                    Array.from(prizeItems).forEach((item, index) => {
                        const titleElement = item.querySelector('.prize-title, .title');
                        const valueElement = item.querySelector('.prize-value, .value');
                        
                        const title = titleElement ? titleElement.textContent.trim() : `Prize ${index + 1}`;
                        const value = valueElement ? valueElement.textContent.trim() : '';
                        
                        prizes[title] = value;
                    });
                    
                    return prizes;
                }
            """)
            
            # Extract schedule details
            schedule_details = await self.browser.evaluate("""
                () => {
                    const scheduleSection = document.querySelector('.schedule, .timeline');
                    if (!scheduleSection) return {};
                    
                    const scheduleItems = scheduleSection.querySelectorAll('.schedule-item, .item');
                    const schedule = {};
                    
                    Array.from(scheduleItems).forEach((item, index) => {
                        const titleElement = item.querySelector('.schedule-title, .title');
                        const dateElement = item.querySelector('.schedule-date, .date');
                        
                        const title = titleElement ? titleElement.textContent.trim() : `Event ${index + 1}`;
                        const date = dateElement ? dateElement.textContent.trim() : '';
                        
                        schedule[title] = date;
                    });
                    
                    return schedule;
                }
            """)
            
            # Combine all extracted information
            details = {
                'title': basic_info.get('title', ''),
                'organizer': basic_info.get('organizer', ''),
                'description': basic_info.get('description', ''),
                'start_date': basic_info.get('start_date', ''),
                'end_date': basic_info.get('end_date', ''),
                'location': basic_info.get('location', ''),
                'mode': basic_info.get('mode', ''),
                'registration_deadline': basic_info.get('registration_deadline', ''),
                'prize_pool': basic_info.get('prize_pool', ''),
                'url': url,
                'num_participants': basic_info.get('participant_count', ''),
                'skills_required': basic_info.get('skills_required', []),
                'logo_url': basic_info.get('logo_url', ''),
                'banner_url': basic_info.get('banner_url', ''),
                'prizes_details': prizes_details or {},
                'schedule_details': schedule_details or {},
                'runs_from_text': basic_info.get('runs_from_text', ''),
                'happening_text': basic_info.get('happening_text', ''),
                'source_platform': 'devpost'
            }
            
            print(f"Successfully extracted details for: {details['title']}")
            return details
            
        except Exception as e:
            print(f"Error extracting hackathon details: {e}")
            await self.take_screenshot(f"error_{url.split('/')[-1]}.png")
            return None
    
    def save_hackathons_to_csv(self, filename: str):
        """
        Saves hackathons to a CSV file.
        """
        if not self.hackathons:
            print("No hackathons to save")
            return
        
        # Prepare data for CSV
        cleaned_hackathons = []
        for hackathon in self.hackathons:
            # Convert any dict/list fields to JSON strings for CSV compatibility
            cleaned_hackathon = {}
            for key, value in hackathon.items():
                if isinstance(value, (dict, list)):
                    cleaned_hackathon[key] = json.dumps(value)
                else:
                    cleaned_hackathon[key] = value
            cleaned_hackathons.append(cleaned_hackathon)
        
        # Collect all fields from all hackathons
        fieldnames = set()
        for hackathon in cleaned_hackathons:
            fieldnames.update(hackathon.keys())
        
        # Write to CSV
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=sorted(fieldnames))
            writer.writeheader()
            writer.writerows(cleaned_hackathons)
        
        print(f"Saved {len(self.hackathons)} hackathons to '{filename}'")
        
        # Also save to pandas DataFrame
        df = pd.DataFrame(self.hackathons)
        df.to_csv(f"{filename.replace('.csv', '')}_df.csv", index=False)
    
    async def crawl(self):
        """Main crawling method"""
        try:
            await self.initialize()
            all_hackathon_links = []
            
            # Crawl multiple pages of hackathon listings
            for page_num in range(1, MAX_PAGES + 1):
                # Calculate the page parameter
                page_url = f"{BASE_URL}{DEFAULT_PARAMS}&page={page_num}"
                print(f"Navigating to page {page_num}: {page_url}")
                
                # Navigate to the listings page
                await self.browser.goto(page_url, wait_until="domcontentloaded")
                await self.browser.wait(2000)  # Wait for JS to execute
                
                # Take screenshot for debugging
                await self.take_screenshot(f"listings_page_{page_num}.png")
                
                # Extract hackathon links
                page_links = await self.extract_hackathon_links()
                
                all_hackathon_links.extend(page_links)
                
                # Stop if we have more than MAX_HACKATHONS links
                if len(all_hackathon_links) >= MAX_HACKATHONS:
                    print(f"Reached maximum number of hackathons ({MAX_HACKATHONS})")
                    break
            
            # Remove duplicates and limit to MAX_HACKATHONS
            all_hackathon_links = list(dict.fromkeys(all_hackathon_links))[:MAX_HACKATHONS]
            print(f"Total unique hackathon links: {len(all_hackathon_links)}")
            
            # Process each hackathon
            for i, link in enumerate(all_hackathon_links):
                print(f"Processing hackathon {i+1}/{len(all_hackathon_links)}: {link}")
                details = await self.extract_hackathon_details(link)
                
                if details:
                    self.hackathons.append(details)
                
                # Short delay between requests to be respectful
                if i < len(all_hackathon_links) - 1:
                    await self.browser.wait(1000)
            
            # Generate timestamp for the filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"devpost_hackathons_{timestamp}.csv"
            
            # Save to CSV
            self.save_hackathons_to_csv(filename)
            
            print(f"Crawl completed. Extracted {len(self.hackathons)} hackathons.")
            
        except Exception as e:
            print(f"Error during crawling: {e}")
            await self.take_screenshot("error_state.png")
        
        finally:
            # Close the browser
            if self.browser:
                await self.browser.close()

async def main():
    """Main function to start the crawler"""
    print("Starting Devpost Hackathon Crawler with craw4ai...")
    crawler = DevpostCrawler()
    await crawler.crawl()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main()) 