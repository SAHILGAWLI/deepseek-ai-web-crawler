import asyncio
import csv
import json
import os
import time
import re
from datetime import datetime, timedelta
from typing import List, Dict, Any
from playwright.async_api import async_playwright, Error as PlaywrightError
import pandas as pd
from dotenv import load_dotenv
import traceback

# Load environment variables
load_dotenv()

# Configuration
BASE_URL = "https://mlh.io/seasons/2025/events"
OUTPUT_CSV = f"mlh_hackathons_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
REQUIRED_FIELDS = ["title", "start_date", "end_date", "mode", "location"]
MAX_HACKATHONS = 100
DEBUG_MODE = True
MAX_RETRIES = 3
MIN_RATE_LIMIT_DELAY = 1
MAX_RATE_LIMIT_DELAY = 3

# Create directories for debugging
os.makedirs("screenshots", exist_ok=True)
os.makedirs("debug", exist_ok=True)

# Get current date for reference only
CURRENT_DATE = datetime.now()
print(f"Current date: {CURRENT_DATE}")

def is_upcoming_event(end_date_str):
    """Check if an event is upcoming based on its end date."""
    if not end_date_str or end_date_str == 'See event website':
        # If no valid end date, we need to be more permissive to avoid filtering out valid events
        print(f"No specific end date: '{end_date_str}' - marking as upcoming by default")
        return True
        
    try:
        # Try various date formats
        date_formats = [
            "%B %d, %Y",       # March 15, 2025
            "%b %d, %Y",       # Mar 15, 2025
            "%d %B %Y",        # 15 March 2025
            "%d %b %Y",        # 15 Mar 2025
            "%Y-%m-%d",        # 2025-03-15
            "%m/%d/%Y"         # 03/15/2025
        ]
        
        for date_format in date_formats:
            try:
                end_date = datetime.strptime(end_date_str, date_format)
                # Event is upcoming if end date is in the future
                is_upcoming = end_date >= CURRENT_DATE
                print(f"Successfully parsed date '{end_date_str}' as {end_date}, upcoming: {is_upcoming}")
                return is_upcoming
            except ValueError:
                continue
                
        # If we've tried all formats and couldn't parse the date
        # Check if the string contains a year that's this year or later
        current_year = CURRENT_DATE.year
        year_match = re.search(r'20\d\d', end_date_str)
        if year_match:
            year = int(year_match.group(0))
            if year < current_year:
                print(f"Date '{end_date_str}' contains past year {year}, marking as not upcoming")
                return False
            elif year > current_year:
                print(f"Date '{end_date_str}' contains future year {year}, marking as upcoming")
                return True
                
        # As a last resort, check if the string has year indicators
        if '2023' in end_date_str or '2024' in end_date_str or 'last year' in end_date_str.lower():
            print(f"Date '{end_date_str}' appears to be from a past year, marking as not upcoming")
            return False
            
        print(f"Could not parse date format '{end_date_str}' - marking as not upcoming")
        return False  # Changed to be conservative - if we can't parse it, don't include it
    except Exception as e:
        print(f"Error parsing date '{end_date_str}': {e} - marking as not upcoming")
        return False  # Changed to be conservative

async def smart_wait(min_delay=MIN_RATE_LIMIT_DELAY, max_delay=MAX_RATE_LIMIT_DELAY):
    """Wait for a random amount of time to avoid rate limiting."""
    import random
    delay = random.uniform(min_delay, max_delay)
    print(f"Waiting for {delay:.2f} seconds to avoid rate limiting...")
    await asyncio.sleep(delay)

async def retry_with_backoff(coroutine, max_retries=MAX_RETRIES, start_delay=5):
    """Retry a coroutine with exponential backoff for rate limiting"""
    retries = 0
    delay = start_delay
    
    while retries < max_retries:
        try:
            return await coroutine
        except PlaywrightError as e:
            if "429" in str(e) or "Too Many Requests" in str(e) or "timeout" in str(e).lower():
                retries += 1
                if retries >= max_retries:
                    print(f"Max retries ({max_retries}) reached, giving up.")
                    raise
                
                wait_time = delay * (2 ** (retries - 1))  # Exponential backoff
                print(f"Rate limited or timeout (attempt {retries}/{max_retries}). Waiting {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            else:
                raise

async def take_screenshot(page, filename):
    """Take a screenshot for debugging purposes"""
    try:
        await page.screenshot(path=f"screenshots/{filename}")
        print(f"Screenshot saved to screenshots/{filename}")
    except Exception as e:
        print(f"Error taking screenshot: {e}")

async def extract_hackathon_links(page):
    """Extract links to individual hackathon pages from the listing page."""
    print(f"Extracting hackathon links from: {page.url}")
    
    # Take a screenshot for debugging
    if DEBUG_MODE:
        await take_screenshot(page, "mlh_listing.png")
        
        # Save HTML for debugging
        html_content = await page.content()
        with open("debug/mlh_listing.html", "w", encoding="utf-8") as f:
            f.write(html_content)
        print("Saved HTML content for debugging")
    
    try:
        # Wait for the page to load completely
        await page.wait_for_load_state("networkidle", timeout=30000)
        
        # Extract hackathon cards specifically from the "Upcoming Events" section
        hackathon_data = await page.evaluate("""() => {
            const hackathons = [];
            
            // Look for the "Upcoming Events" heading
            const upcomingHeading = Array.from(document.querySelectorAll('h3')).find(
                h => h.textContent.trim().includes('Upcoming Events')
            );
            
            if (!upcomingHeading) {
                console.log('Could not find "Upcoming Events" heading');
                return hackathons;
            }
            
            console.log('Found "Upcoming Events" heading');
            
            // Get all event cards that come after the "Upcoming Events" heading
            // We'll collect all .event divs until we hit another h3 (which would be "Past Events")
            let currentElement = upcomingHeading.nextElementSibling;
            while (currentElement && !currentElement.matches('h3')) {
                if (currentElement.classList.contains('event')) {
                    try {
                        // Extract the event data
                        const eventCard = currentElement;
                        
                        // Extract title
                        let title = '';
                        const titleElement = eventCard.querySelector('.event-name');
                        if (titleElement) {
                            title = titleElement.textContent.trim();
                        }
                        
                        // Extract URL
                        let url = '';
                        const linkElement = eventCard.querySelector('a.event-link');
                        if (linkElement) {
                            url = linkElement.href;
                        }
                        
                        // Extract logo
                        let logo_url = '';
                        const logoElement = eventCard.querySelector('.event-logo img');
                        if (logoElement && logoElement.src) {
                            logo_url = logoElement.src;
                        }
                        
                        // Extract banner/splash image
                        let banner_url = '';
                        const bannerElement = eventCard.querySelector('.image-wrap img');
                        if (bannerElement && bannerElement.src) {
                            banner_url = bannerElement.src;
                        }
                        
                        // Extract dates from meta tags when available (most accurate)
                        let start_date = '';
                        let end_date = '';
                        const startDateMeta = eventCard.querySelector('meta[itemprop="startDate"]');
                        const endDateMeta = eventCard.querySelector('meta[itemprop="endDate"]');
                        
                        if (startDateMeta && startDateMeta.content) {
                            // Convert ISO format to more readable format
                            const startDate = new Date(startDateMeta.content);
                            start_date = startDate.toLocaleDateString('en-US', {
                                month: 'long',
                                day: 'numeric',
                                year: 'numeric'
                            });
                        }
                        
                        if (endDateMeta && endDateMeta.content) {
                            const endDate = new Date(endDateMeta.content);
                            end_date = endDate.toLocaleDateString('en-US', {
                                month: 'long',
                                day: 'numeric',
                                year: 'numeric'
                            });
                        }
                        
                        // Fallback to text date if meta not available
                        if (!start_date || !end_date) {
                            const dateElement = eventCard.querySelector('.event-date');
                            if (dateElement) {
                                const dateText = dateElement.textContent.trim();
                                
                                // Try to parse the date text
                                const monthNames = [
                                    'January', 'February', 'March', 'April', 'May', 'June', 'July',
                                    'August', 'September', 'October', 'November', 'December',
                                    'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
                                ];
                                
                                // Date format on MLH is typically "Apr 26th - 27th"
                                const dateRegex = /([A-Za-z]{3,}) (\d{1,2})(?:st|nd|rd|th)? *- *(\d{1,2})(?:st|nd|rd|th)?/i;
                                const match = dateText.match(dateRegex);
                                
                                if (match) {
                                    const month = match[1];
                                    const startDay = match[2];
                                    const endDay = match[3];
                                    const year = new Date().getFullYear(); // Default to current year if not specified
                                    
                                    if (!start_date) {
                                        start_date = `${month} ${startDay}, ${year}`;
                                    }
                                    
                                    if (!end_date) {
                                        end_date = `${month} ${endDay}, ${year}`;
                                    }
                                } else {
                                    // Just use the date text as is
                                    if (!start_date && !end_date) {
                                        start_date = dateText;
                                        end_date = dateText;
                                    }
                                }
                            }
                        }
                        
                        // Extract location
                        let location = '';
                        let city = '';
                        let state = '';
                        const cityElement = eventCard.querySelector('[itemprop="city"]');
                        const stateElement = eventCard.querySelector('[itemprop="state"]');
                        
                        if (cityElement) {
                            city = cityElement.textContent.trim();
                        }
                        
                        if (stateElement) {
                            state = stateElement.textContent.trim();
                        }
                        
                        if (city && state) {
                            location = `${city}, ${state}`;
                        } else if (city) {
                            location = city;
                        } else if (state) {
                            location = state;
                        }
                        
                        // Extract event mode (in-person, digital, hybrid)
                        let mode = 'N/A';
                        const modeElement = eventCard.querySelector('.event-hybrid-notes span');
                        if (modeElement) {
                            const modeText = modeElement.textContent.trim().toLowerCase();
                            if (modeText.includes('in-person')) {
                                mode = 'offline';
                            } else if (modeText.includes('digital')) {
                                mode = 'online';
                            } else if (modeText.includes('hybrid')) {
                                mode = 'hybrid';
                            }
                        }
                        
                        // Check for any special tags such as "HIGH SCHOOL" or "DIVERSITY"
                        let tags = [];
                        const ribbonElement = eventCard.querySelector('.ribbon');
                        if (ribbonElement) {
                            const ribbonText = ribbonElement.textContent.trim();
                            if (ribbonText.includes('HIGH SCHOOL')) {
                                tags.push('high school');
                            }
                            if (ribbonText.includes('DIVERSITY')) {
                                tags.push('diversity');
                            }
                        }
                        
                        // Only add if we have at least a title or URL
                        if (title || url) {
                            hackathons.push({
                                title,
                                url,
                                logo_url,
                                banner_url,
                                start_date,
                                end_date,
                                location,
                                mode,
                                tags: tags.join(', '),
                                source_platform: 'mlh'
                            });
                        }
                    } catch (e) {
                        console.error('Error processing event card:', e);
                    }
                }
                
                // Move to the next element
                currentElement = currentElement.nextElementSibling;
            }
            
            return hackathons;
        }""")
        
        if not hackathon_data:
            print("No hackathon data found in the 'Upcoming Events' section. Trying alternate approach...")
            
            # If the targeted approach didn't work, try a more general approach to find event cards
            hackathon_data = await page.evaluate("""() => {
                const hackathons = [];
                
                // Get all event cards on the page
                const eventCards = document.querySelectorAll('.event');
                
                eventCards.forEach(eventCard => {
                    try {
                        // Extract title
                        let title = '';
                        const titleElement = eventCard.querySelector('.event-name');
                        if (titleElement) {
                            title = titleElement.textContent.trim();
                        }
                        
                        // Extract URL
                        let url = '';
                        const linkElement = eventCard.querySelector('a.event-link');
                        if (linkElement) {
                            url = linkElement.href;
                        }
                        
                        // Extract logo
                        let logo_url = '';
                        const logoElement = eventCard.querySelector('.event-logo img');
                        if (logoElement && logoElement.src) {
                            logo_url = logoElement.src;
                        }
                        
                        // Extract banner/splash image
                        let banner_url = '';
                        const bannerElement = eventCard.querySelector('.image-wrap img');
                        if (bannerElement && bannerElement.src) {
                            banner_url = bannerElement.src;
                        }
                        
                        // Extract dates from meta tags when available
                        let start_date = '';
                        let end_date = '';
                        const startDateMeta = eventCard.querySelector('meta[itemprop="startDate"]');
                        const endDateMeta = eventCard.querySelector('meta[itemprop="endDate"]');
                        
                        if (startDateMeta && startDateMeta.content) {
                            const startDate = new Date(startDateMeta.content);
                            start_date = startDate.toLocaleDateString('en-US', {
                                month: 'long',
                                day: 'numeric',
                                year: 'numeric'
                            });
                        }
                        
                        if (endDateMeta && endDateMeta.content) {
                            const endDate = new Date(endDateMeta.content);
                            end_date = endDate.toLocaleDateString('en-US', {
                                month: 'long',
                                day: 'numeric',
                                year: 'numeric'
                            });
                        }
                        
                        // Fallback to text date
                        if (!start_date || !end_date) {
                            const dateElement = eventCard.querySelector('.event-date');
                            if (dateElement) {
                                const dateText = dateElement.textContent.trim();
                                if (!start_date && !end_date) {
                                    start_date = dateText;
                                    end_date = dateText;
                                }
                            }
                        }
                        
                        // Extract location
                        let location = '';
                        let city = '';
                        let state = '';
                        const cityElement = eventCard.querySelector('[itemprop="city"]');
                        const stateElement = eventCard.querySelector('[itemprop="state"]');
                        
                        if (cityElement) {
                            city = cityElement.textContent.trim();
                        }
                        
                        if (stateElement) {
                            state = stateElement.textContent.trim();
                        }
                        
                        if (city && state) {
                            location = `${city}, ${state}`;
                        } else if (city) {
                            location = city;
                        } else if (state) {
                            location = state;
                        }
                        
                        // Extract event mode
                        let mode = 'N/A';
                        const modeElement = eventCard.querySelector('.event-hybrid-notes span');
                        if (modeElement) {
                            const modeText = modeElement.textContent.trim().toLowerCase();
                            if (modeText.includes('in-person')) {
                                mode = 'offline';
                            } else if (modeText.includes('digital')) {
                                mode = 'online';
                            } else if (modeText.includes('hybrid')) {
                                mode = 'hybrid';
                            }
                        }
                        
                        // Check for any special tags
                        let tags = [];
                        const ribbonElement = eventCard.querySelector('.ribbon');
                        if (ribbonElement) {
                            const ribbonText = ribbonElement.textContent.trim();
                            if (ribbonText.includes('HIGH SCHOOL')) {
                                tags.push('high school');
                            }
                            if (ribbonText.includes('DIVERSITY')) {
                                tags.push('diversity');
                            }
                        }
                        
                        // Only add if we have at least a title or URL
                        if (title || url) {
                            hackathons.push({
                                title,
                                url,
                                logo_url,
                                banner_url,
                                start_date,
                                end_date,
                                location,
                                mode,
                                tags: tags.join(', '),
                                source_platform: 'mlh'
                            });
                        }
                    } catch (e) {
                        console.error('Error processing event card:', e);
                    }
                });
                
                return hackathons;
            }""")
        
        # Log the results
        print(f"Found {len(hackathon_data)} hackathons from the MLH website")
        
        # Get just the URLs for simplified return
        hackathon_links = [hack.get("url") for hack in hackathon_data if hack.get("url")]
        
        # Check if we should extract directly from the listing page or scrape individual pages
        has_sufficient_data = all(
            hack.get("title") and hack.get("start_date") and hack.get("end_date") 
            for hack in hackathon_data if hack
        )
        
        if has_sufficient_data:
            print("All required data already extracted from listing page")
            # We have all the data we need directly from the listing page
            # Just pass back the data without visiting individual pages
            return hackathon_links, hackathon_data
        else:
            print("Need to visit individual pages for more details")
            # We need to visit individual pages to get more details
            return hackathon_links, hackathon_data
            
    except Exception as e:
        print(f"Error extracting hackathon links: {e}")
        traceback.print_exc()
        
        # Try a last-resort approach to find event links
        try:
            print("Attempting last-resort extraction of event links...")
            event_links_raw = await page.evaluate("""() => {
                // Get all links on the page
                return Array.from(document.querySelectorAll('a[href]'))
                    .map(a => a.href)
                    .filter(href => 
                        href.includes('mlh.io') && 
                        (href.includes('hackathon') || href.includes('event') || href.includes('hack'))
                    );
            }""")
            
            event_links = [{"url": url, "title": url.split('/')[-1].replace('-', ' ').title(), "source_platform": "mlh"} 
                           for url in event_links_raw]
            
            print(f"Last-resort extraction found {len(event_links)} links")
            return event_links_raw, event_links
            
        except Exception as fallback_error:
            print(f"Last-resort extraction also failed: {fallback_error}")
            return [], []

async def extract_hackathon_details(page, url, listing_data=None):
    """Extract detailed information from a hackathon page."""
    print(f"Extracting details from: {url}")
    
    # Get event ID for debugging purposes (last part of URL)
    event_id = url.split('/')[-1] if '/' in url else 'unknown'
    
    # Use listing data if available
    event_details = {}
    if listing_data:
        # Find matching listing data for this URL
        for item in listing_data:
            if item.get("url") == url:
                event_details = item.copy()
                print(f"Found listing data for {url}")
                break
    
    try:
        # Navigate to the event page
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_load_state("networkidle", timeout=30000)
        except Exception as e:
            print(f"Navigation error: {e}")
            # If navigation fails but we have listing data, we can still return that
            if event_details:
                print("Using listing data despite navigation error")
                return event_details
            else:
                raise  # Re-raise if we don't have listing data
        
        # Take a screenshot of the event page
        if DEBUG_MODE:
            await take_screenshot(page, f"mlh_event_{event_id}.png")
            
            # Save HTML for debugging
            html_content = await page.content()
            os.makedirs("debug/events", exist_ok=True)
            with open(f"debug/events/{event_id}.html", "w", encoding="utf-8") as f:
                f.write(html_content)
        
        # Extract event details from the page
        page_details = await page.evaluate("""() => {
            const data = {};
            
            try {
                // Title - try different selectors
                const titleElement = document.querySelector('h1, .event-title, .title');
                if (titleElement) {
                    data.title = titleElement.textContent.trim();
                }
                
                // Logo
                const logoElement = document.querySelector('.event-logo, img[alt*="logo"], header img');
                if (logoElement && logoElement.src) {
                    data.logo_url = logoElement.src;
                }
                
                // Banner image
                const bannerElement = document.querySelector('.event-banner, .banner-image, .hero-image, header img[class*="banner"]');
                if (bannerElement && bannerElement.src) {
                    data.banner_url = bannerElement.src;
                } else if (!data.banner_url) {
                    // Try to find any large image that might be a banner
                    const images = document.querySelectorAll('img');
                    for (const img of images) {
                        if (img.width > 600 || img.height > 300) {
                            data.banner_url = img.src;
                            break;
                        }
                    }
                }
                
                // Dates - look for date information
                const dateElement = document.querySelector('.event-date, .date, time, [datetime]');
                if (dateElement) {
                    // Try to get directly from datetime attribute
                    const datetime = dateElement.getAttribute('datetime');
                    if (datetime) {
                        const date = new Date(datetime);
                        data.start_date = date.toLocaleDateString('en-US', {month: 'long', day: 'numeric', year: 'numeric'});
                    } else {
                        // Extract from text content
                        const dateText = dateElement.textContent.trim();
                        // Look for patterns in the text
                        const datePattern1 = /([A-Za-z]+)\\s+(\\d{1,2})\\s*-\\s*(\\d{1,2}),?\\s*(\\d{4})/i; // Month Day-Day, Year
                        const datePattern2 = /([A-Za-z]+)\\s+(\\d{1,2})\\s*-\\s*([A-Za-z]+)\\s+(\\d{1,2}),?\\s*(\\d{4})/i; // Month Day - Month Day, Year
                        
                        const match1 = dateText.match(datePattern1);
                        const match2 = dateText.match(datePattern2);
                        
                        if (match1) {
                            const month = match1[1];
                            const startDay = match1[2];
                            const endDay = match1[3];
                            const year = match1[4];
                            
                            data.start_date = `${month} ${startDay}, ${year}`;
                            data.end_date = `${month} ${endDay}, ${year}`;
                        } else if (match2) {
                            const startMonth = match2[1];
                            const startDay = match2[2];
                            const endMonth = match2[3];
                            const endDay = match2[4];
                            const year = match2[5];
                            
                            data.start_date = `${startMonth} ${startDay}, ${year}`;
                            data.end_date = `${endMonth} ${endDay}, ${year}`;
                        }
                    }
                }
                
                // Location and mode
                const locationElement = document.querySelector('.event-location, .location, [class*="location"]');
                if (locationElement) {
                    data.location = locationElement.textContent.trim();
                    
                    // Determine mode based on location text
                    const locationText = data.location.toLowerCase();
                    if (locationText.includes('online') || locationText.includes('virtual')) {
                        data.mode = 'online';
                    } else if (locationText.includes('hybrid')) {
                        data.mode = 'hybrid';
                    } else {
                        data.mode = 'offline';
                    }
                }
                
                // Description
                const descriptionElement = document.querySelector('.event-description, .description, section p');
                if (descriptionElement) {
                    data.description = descriptionElement.textContent.trim();
                } else {
                    // Try to find any paragraph that might be a description
                    const paragraphs = document.querySelectorAll('main p, article p, section p');
                    if (paragraphs.length > 0) {
                        // Take the longest paragraph as the description
                        let longestText = '';
                        paragraphs.forEach(p => {
                            const text = p.textContent.trim();
                            if (text.length > longestText.length) {
                                longestText = text;
                            }
                        });
                        
                        if (longestText) {
                            data.description = longestText;
                        }
                    }
                }
                
                // Check for registration link
                const registrationElement = document.querySelector('a[href*="register"], a.register-button, a[class*="register"]');
                if (registrationElement) {
                    data.registration_url = registrationElement.href;
                }
                
                // Organizer information
                const organizerElement = document.querySelector('.organizer, .host, .university');
                if (organizerElement) {
                    data.organizer = organizerElement.textContent.trim();
                }
                
                // Try to extract other useful information
                // Prizes
                const prizeElement = document.querySelector('[class*="prize"], [id*="prize"], h2:contains("Prize"), h3:contains("Prize")');
                if (prizeElement) {
                    data.prize_pool = prizeElement.textContent.trim();
                }
                
                // Tags
                const tagElements = document.querySelectorAll('.tag, .badge, [class*="tag"]');
                if (tagElements.length > 0) {
                    data.tags = Array.from(tagElements).map(tag => tag.textContent.trim()).join(', ');
                }
            } catch (e) {
                console.error('Error extracting event details:', e);
            }
            
            return data;
        }""")
        
        # Merge page details with listing data, prioritizing page details where available
        if page_details:
            for key, value in page_details.items():
                if value:  # Only update if the value is not empty
                    event_details[key] = value
        
        # Ensure required fields have values
        if not event_details.get('title'):
            # Use the URL to generate a title if missing
            event_details['title'] = url.split('/')[-1].replace('-', ' ').title()
        
        # Set defaults for required fields if missing
        if not event_details.get('start_date'):
            event_details['start_date'] = 'See event website'
        
        if not event_details.get('end_date'):
            event_details['end_date'] = 'See event website'
        
        if not event_details.get('mode'):
            event_details['mode'] = 'offline'  # Default to offline
        
        if not event_details.get('location'):
            event_details['location'] = 'See event website'
        
        # Always set the URL and source platform
        event_details['url'] = url
        event_details['source_platform'] = 'mlh'
        
        print(f"Extracted details for: {event_details.get('title', 'Unknown event')}")
        return event_details
        
    except Exception as e:
        print(f"Error extracting event details: {e}")
        traceback.print_exc()
        
        # If we have listing data, return that rather than failing completely
        if event_details:
            print("Returning listing data due to detail extraction error")
            return event_details
        
        # Return minimal data with error information
        return {
            "title": event_id.replace('-', ' ').title(),
            "url": url,
            "source_platform": "mlh",
            "start_date": "See event website",
            "end_date": "See event website",
            "mode": "offline",
            "location": "See event website",
            "error": str(e)
        }

def is_complete_hackathon(hackathon: Dict[str, Any], required_fields: List[str]) -> bool:
    """Check if a hackathon has all required fields."""
    for field in required_fields:
        if field not in hackathon or not hackathon[field]:
            return False
    return True

def save_hackathons_to_csv(hackathons: List[Dict[str, Any]], filename: str) -> None:
    """Save hackathons to a CSV file."""
    if not hackathons:
        print("No hackathons to save")
        return
    
    # Prepare data for CSV
    cleaned_hackathons = []
    for hackathon in hackathons:
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
    
    # Ensure required fields are at the beginning
    ordered_fieldnames = []
    for field in REQUIRED_FIELDS:
        if field in fieldnames:
            ordered_fieldnames.append(field)
            fieldnames.remove(field)
    
    # Add remaining fields
    ordered_fieldnames.extend(sorted(fieldnames))
    
    # Write to CSV
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=ordered_fieldnames)
        writer.writeheader()
        writer.writerows(cleaned_hackathons)
    
    print(f"Saved {len(hackathons)} hackathons to '{filename}'")

async def crawl_mlh_hackathons():
    """Main function to crawl MLH hackathons."""
    print("Starting MLH Hackathon Crawler...")
    print(f"Targeting upcoming events from {BASE_URL}")
    
    async with async_playwright() as p:
        # Launch browser
        browser = await p.chromium.launch(headless=not DEBUG_MODE)
        context = await browser.new_context(
            viewport={'width': 1280, 'height': 800},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        
        # Set default timeout
        context.set_default_timeout(60000)  # 60 seconds
        
        page = await context.new_page()
        
        # Initialize pages list with the main page to fix UnboundLocalError in finally block
        pages = [page]
        
        try:
            # Navigate to the MLH events page
            print(f"Navigating to: {BASE_URL}")
            await page.goto(BASE_URL, wait_until="domcontentloaded")
            await page.wait_for_load_state("networkidle")
            
            # Extract hackathon links and listing data
            hackathon_links, listing_data = await extract_hackathon_links(page)
            
            # Check if we found any hackathons
            if not hackathon_links:
                print("No upcoming hackathon links found. Exiting.")
                return
                
            print(f"Found {len(hackathon_links)} upcoming hackathon links")
            
            # Limit the total number of hackathons if needed
            if MAX_HACKATHONS and len(hackathon_links) > MAX_HACKATHONS:
                print(f"Limiting to {MAX_HACKATHONS} hackathons out of {len(hackathon_links)}")
                hackathon_links = hackathon_links[:MAX_HACKATHONS]
                # Also limit the listing data to match
                if isinstance(listing_data, list):
                    listing_data = [data for data in listing_data 
                                  if data.get("url") in hackathon_links]
            
            # Check if we already have all the data from the listing page
            all_data_available = all(
                item.get("title") and item.get("start_date") and item.get("end_date") and item.get("mode")
                for item in listing_data if isinstance(item, dict)
            )
            
            if all_data_available and len(listing_data) == len(hackathon_links):
                print("All required data already extracted from listing page, skipping individual page visits")
                all_hackathons = listing_data
            else:
                # Process each hackathon page
                print(f"Processing {len(hackathon_links)} individual hackathon pages...")
                
                # Create a pool of pages for parallel processing
                concurrency = min(5, len(hackathon_links))
                # Ensure concurrency is at least 1 to avoid division by zero
                concurrency = max(1, concurrency)
                
                # pages list already initialized at the beginning with the main page
                
                for _ in range(concurrency - 1):
                    pages.append(await context.new_page())
                
                # Process hackathons in batches
                all_hackathons = []
                for i in range(0, len(hackathon_links), concurrency):
                    batch = hackathon_links[i:i+concurrency]
                    tasks = []
                    
                    for j, url in enumerate(batch):
                        # Use the appropriate page from the pool
                        hackathon_page = pages[j % len(pages)]
                        
                        # Create the task with listing data
                        tasks.append(extract_hackathon_details(hackathon_page, url, listing_data))
                    
                    # Wait for all tasks in this batch to complete
                    batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Process results
                    for result in batch_results:
                        if isinstance(result, Exception):
                            print(f"Error during extraction: {result}")
                        elif result:  # If not None
                            all_hackathons.append(result)
                    
                    print(f"Processed batch {i//concurrency + 1}, total upcoming hackathons so far: {len(all_hackathons)}")
                    
                    # Add a small delay between batches to avoid rate limiting
                    await smart_wait(2, 4)
            
            # Save results to CSV
            if all_hackathons:
                # Generate timestamp for the filename
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"mlh_upcoming_hackathons_{timestamp}.csv"
                
                # Save to CSV
                save_hackathons_to_csv(all_hackathons, filename)
                
                # Also save as JSON for easier inspection
                json_filename = filename.replace('.csv', '.json')
                with open(json_filename, 'w', encoding='utf-8') as f:
                    json.dump(all_hackathons, f, indent=2)
                
                # Print summary of complete vs incomplete hackathons
                complete_hackathons = [h for h in all_hackathons if is_complete_hackathon(h, REQUIRED_FIELDS)]
                print(f"Complete hackathons: {len(complete_hackathons)}/{len(all_hackathons)}")
                
                for field in REQUIRED_FIELDS:
                    missing_field = sum(1 for h in all_hackathons if field not in h or not h[field])
                    print(f"Hackathons missing {field}: {missing_field}")
            else:
                print("No upcoming hackathons were found.")
                
        except Exception as e:
            print(f"Error during crawling: {e}")
            traceback.print_exc()
            if DEBUG_MODE:
                await take_screenshot(page, "error_state.png")
            
        finally:
            # Close browser
            for p in pages[1:]:  # Close additional pages
                await p.close()
            await browser.close()

if __name__ == "__main__":
    asyncio.run(crawl_mlh_hackathons()) 