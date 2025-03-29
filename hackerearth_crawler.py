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
import hashlib

# Load environment variables
load_dotenv()

# Configuration
BASE_URL = "https://www.hackerearth.com/challenges/hackathon/"
OUTPUT_CSV = f"hackerearth_hackathons_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
REQUIRED_FIELDS = ["title", "start_date", "end_date", "mode", "prize_pool", "url"]
MAX_HACKATHONS = 100
DEBUG_MODE = True
MAX_RETRIES = 3
MIN_RATE_LIMIT_DELAY = 1
MAX_RATE_LIMIT_DELAY = 3

# Create screenshots directory
os.makedirs("screenshots", exist_ok=True)
os.makedirs("debug", exist_ok=True)

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

async def bypass_login_prompt(page):
    """Try to bypass any login prompts that might appear"""
    try:
        # Check for and close any login prompts
        login_selectors = [
            'button:has-text("okay, got it!")', 
            'button:has-text("Skip")',
            'button:has-text("Close")',
            'button:has-text("Not now")',
            '.modal-close',
            '.close-button'
        ]
        
        for selector in login_selectors:
            try:
                if await page.is_visible(selector, timeout=2000):
                    print(f"Closing prompt with selector: {selector}")
                    await page.click(selector)
                    await asyncio.sleep(1)
            except Exception:
                continue
        
        # Accept privacy policy if needed
        if await page.is_visible('text="I have read and I agree."', timeout=2000):
            print("Accepting privacy policy")
            await page.click('text="I have read and I agree."')
            await asyncio.sleep(1)
        
        return True
    except Exception as e:
        print(f"Error bypassing login prompt: {e}")
        return False

async def extract_hackathon_links(page):
    """Extract links to individual hackathon pages"""
    print("Extracting hackathon links...")
    
    try:
        # Take screenshot of listings page
        await take_screenshot(page, "hackerearth_listings.png")
        
        # First try to bypass any login prompts
        await bypass_login_prompt(page)
        
        # Extract hackathons from LIVE and UPCOMING tabs only
        hackathon_data = []
        
        # Click on each tab and extract data - only LIVE and UPCOMING
        tabs = ["LIVE", "UPCOMING"]  # Removed "PREVIOUS"
        
        # Track URLs to ensure no duplicates
        seen_urls = set()
        
        for tab in tabs:
            print(f"\n===== Extracting from {tab} tab =====")
            
            try:
                # Click on the tab
                tab_selector = f'text="{tab}"'
                await page.click(tab_selector)
                await asyncio.sleep(2)  # Wait for content to load
                
                # Get the visible count from the tab (number shown in the tab header)
                tab_count = await page.evaluate(f"""() => {{
                    // Find the tab with the count
                    const tabElement = Array.from(document.querySelectorAll('div, span, button')).find(
                        el => el.textContent && el.textContent.trim().includes('{tab}') && 
                             el.textContent.match(/\\d+/)
                    );
                    
                    if (tabElement) {{
                        // Extract the number from the tab text (like "LIVE 5" -> 5)
                        const match = tabElement.textContent.match(/{tab}\\s*(\\d+)/i);
                        if (match) {{
                            return parseInt(match[1], 10);
                        }}
                    }}
                    return 0; // Default if not found
                }}""")
                
                print(f"Tab shows {tab_count} visible hackathons")
                
                # Extract hackathon cards from this tab
                cards_data = await page.evaluate(f"""() => {{
                    const hackathons = [];
                    
                    // Select all challenge cards under the current tab
                    const cards = Array.from(document.querySelectorAll('.challenge-card, [class*="card"]'));
                    
                    console.log('Found ' + cards.length + ' cards');
                    
                    cards.forEach(card => {{
                        try {{
                            // Extract the title
                            let title = '';
                            let titleEl = card.querySelector('.challenge-name, .title, h3, h4');
                            if (titleEl) {{
                                title = titleEl.textContent.trim();
                            }}
                            
                            // Extract URL
                            let url = '';
                            let linkEl = card.querySelector('a[href*="/challenges/"]');
                            if (linkEl) {{
                                url = linkEl.href;
                            }}
                            
                            // Check if this card is actually visible on screen
                            const rect = card.getBoundingClientRect();
                            const isVisible = rect.top >= 0 && 
                                             rect.left >= 0 && 
                                             rect.bottom <= (window.innerHeight || document.documentElement.clientHeight) &&
                                             rect.right <= (window.innerWidth || document.documentElement.clientWidth);
                            
                            // Extract dates
                            let startDate = '';
                            let endDate = '';
                            let status = '{tab}'; // LIVE, UPCOMING, or PREVIOUS
                            
                            // Look for date information
                            let dateEl = card.querySelector('.date, [class*="date"], .event-date');
                            if (dateEl) {{
                                const dateText = dateEl.textContent.trim();
                                
                                // Try to identify start/end dates from text
                                if (dateText.includes('STARTS ON')) {{
                                    startDate = dateText.replace('STARTS ON', '').trim();
                                }} else if (dateText.includes('ENDS IN')) {{
                                    // For LIVE events - the end date is usually shown as countdown
                                    endDate = 'LIVE - ends soon';
                                    
                                    // The start date might be somewhere else or we can assume it's already started
                                    startDate = 'Already started';
                                }}
                            }}
                            
                            // Extract prize info
                            let prizePool = '';
                            let prizeEl = card.querySelector('.prize, [class*="prize"]');
                            if (prizeEl) {{
                                prizePool = prizeEl.textContent.trim();
                                if (prizePool.includes('Prizes')) {{
                                    prizePool = prizePool.replace('Prizes', '').trim();
                                }}
                            }}
                            
                            // Extract participant count
                            let participantCount = '';
                            let participantEl = card.querySelector('.registrations');
                            if (participantEl) {{
                                participantCount = participantEl.textContent.trim();
                            }}
                            
                            // Skip if we don't have at least a title and URL
                            if (title && url) {{
                                hackathons.push({{
                                    title,
                                    url,
                                    start_date: startDate,
                                    end_date: endDate,
                                    status,
                                    prize_pool: prizePool,
                                    num_participants: participantCount,
                                    source_platform: 'hackerearth',
                                    isVisible
                                }});
                            }}
                        }} catch (e) {{
                            console.error('Error processing card:', e);
                        }}
                    }});
                    
                    return hackathons;
                }}""")
                
                # Process and deduplicate hackathons - taking only the first tab_count items
                tab_hackathons = []
                count = 0
                
                # First try to get the visible cards
                for hackathon in cards_data:
                    url = hackathon.get("url")
                    title = hackathon.get("title")
                    is_visible = hackathon.get("isVisible", False)
                    
                    if url and url not in seen_urls and count < tab_count:
                        seen_urls.add(url)
                        tab_hackathons.append(hackathon)
                        print(f"  - {title} ({url})")
                        count += 1
                        
                        # Stop once we have enough hackathons
                        if count >= tab_count:
                            break
                
                print(f"Found {len(tab_hackathons)} unique hackathons in {tab} tab (out of {tab_count} displayed)")
                hackathon_data.extend(tab_hackathons)
                
            except Exception as e:
                print(f"Error extracting from {tab} tab: {e}")
        
        # Extract just the URLs for returning
        hackathon_links = [hack.get("url") for hack in hackathon_data if hack.get("url")]
        
        print(f"\nTotal extracted: {len(hackathon_links)} unique hackathon links")
        return hackathon_links, hackathon_data
        
    except Exception as e:
        print(f"Error extracting hackathon links: {e}")
        traceback.print_exc()
        return [], []

async def extract_hackathon_details(page, url, listing_data=None):
    """Extract detailed information from a hackathon page"""
    print(f"Extracting details from: {url}")
    
    # Get hackathon ID for debugging purposes
    hackathon_id = url.split('/')[-1] if '/' in url else 'unknown'
    
    # Initialize details dictionary with data from listing page if available
    event_details = {}
    if listing_data:
        for item in listing_data:
            if item.get("url") == url:
                event_details = item.copy()
                print(f"Found listing data for {url}")
                break
    
    try:
        # Navigate to the hackathon page - use domcontentloaded instead of networkidle
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        
        # Don't wait for networkidle as it's causing timeouts
        # Just wait a fixed time instead
        await asyncio.sleep(5)
        
        # Take a screenshot
        await take_screenshot(page, f"hackerearth_event_{hackathon_id}.png")
        
        # Try to bypass any login prompts
        await bypass_login_prompt(page)
        
        # Save HTML for debugging
        if DEBUG_MODE:
            html_content = await page.content()
            os.makedirs("debug/events", exist_ok=True)
            with open(f"debug/events/hackerearth_{hackathon_id}.html", "w", encoding="utf-8") as f:
                f.write(html_content)
        
        # Extract event details from the page
        page_details = await page.evaluate("""() => {
            const data = {};
            
            try {
                // Title
                const titleEl = document.querySelector('h1, .challenge-name, .title');
                if (titleEl) {
                    data.title = titleEl.textContent.trim();
                }
                
                // Description
                const descriptionEl = document.querySelector('.description, .challenge-desc');
                if (descriptionEl) {
                    data.description = descriptionEl.textContent.trim();
                } else {
                    // Look for paragraphs that might contain description
                    const paragraphs = document.querySelectorAll('p');
                    for (const p of paragraphs) {
                        if (p.textContent && p.textContent.length > 50) {
                            data.description = p.textContent.trim();
                            break;
                        }
                    }
                }
                
                // Extract overview section
                let overview = '';
                const overviewHeader = Array.from(document.querySelectorAll('h2')).find(h => 
                    h.textContent.trim().toLowerCase() === 'overview');
                if (overviewHeader) {
                    // Find the content div that follows the overview header
                    let contentDiv = overviewHeader.nextElementSibling;
                    // Skip until we find the content div
                    while (contentDiv && !contentDiv.classList.contains('content')) {
                        contentDiv = contentDiv.nextElementSibling;
                    }
                    
                    if (contentDiv) {
                        // Extract text content from all paragraphs in the overview section
                        const paragraphs = contentDiv.querySelectorAll('p');
                        if (paragraphs.length > 0) {
                            overview = Array.from(paragraphs)
                                .map(p => p.textContent.trim())
                                .filter(text => text.length > 0)
                                .join('\\n\\n');
                        } else {
                            overview = contentDiv.textContent.trim();
                        }
                        
                        data.overview = overview;
                    }
                }
                
                // Extract themes section
                let themes = [];
                const themesHeader = Array.from(document.querySelectorAll('h2')).find(h => 
                    h.textContent.trim().toLowerCase().includes('theme'));
                
                if (themesHeader) {
                    // Find themes containers
                    const themeContainers = document.querySelectorAll('.theme-container');
                    
                    if (themeContainers.length > 0) {
                        themeContainers.forEach(container => {
                            try {
                                const themeObj = {};
                                
                                // Extract theme title
                                const titleEl = container.querySelector('.large.weight-700.dark');
                                if (titleEl) {
                                    themeObj.title = titleEl.textContent.trim();
                                }
                                
                                // Extract theme description
                                const descEl = container.querySelector('.theme-full-description-box');
                                if (descEl) {
                                    themeObj.description = descEl.textContent.trim().replace(/\\s+/g, ' ');
                                }
                                
                                // Extract theme image if available
                                const imgEl = container.querySelector('img');
                                if (imgEl && imgEl.src) {
                                    themeObj.image = imgEl.src;
                                }
                                
                                // Only add if we have at least a title
                                if (themeObj.title) {
                                    themes.push(themeObj);
                                }
                            } catch (e) {
                                console.error('Error extracting theme:', e);
                            }
                        });
                    }
                    
                    // If we found themes, add them to the data
                    if (themes.length > 0) {
                        data.themes = themes;
                        
                        // Create a summary of themes for easy reference
                        const themesSummary = themes.map(t => t.title).join(' | ');
                        data.themes_summary = themesSummary;
                    }
                }
                
                // Banner URL - look for banner image in style attribute with background-image
                const bannerElements = document.querySelectorAll('[style*="background-image"], .banner-image, .cover-image');
                if (bannerElements.length > 0) {
                    for (const el of bannerElements) {
                        const style = el.getAttribute('style');
                        if (style && style.includes('background-image')) {
                            // Extract URL from style="background-image: url('...');"
                            const match = style.match(/url\\(['"]?(.*?)['"]?\\)/);
                            if (match && match[1]) {
                                data.banner_url = match[1];
                                break;
                            }
                        }
                    }
                }
                
                // Logo URL - look for organization logo
                const logoElements = document.querySelectorAll('img[alt*="logo"], img[class*="logo"], header img, .company-logo img, .organizer-logo img, .company-img img');
                if (logoElements.length > 0) {
                    // Use the first logo found
                    data.logo_url = logoElements[0].src;
                } else {
                    // Try to find any images that might be logos
                    const allImages = document.querySelectorAll('img');
                    for (const img of allImages) {
                        const alt = img.getAttribute('alt') || '';
                        if (alt && (
                            alt.includes('Access Development') || 
                            alt.includes('Hathor') || 
                            alt.includes('HackerEarth') ||
                            alt.includes('Company') ||
                            alt.includes('Organizer')
                        )) {
                            data.logo_url = img.src;
                            break;
                        }
                    }
                }
                
                // Registered participants count
                const registeredEl = document.querySelector('.event-participation');
                if (registeredEl) {
                    const registeredText = registeredEl.textContent.trim();
                    const registeredMatch = registeredText.match(/(\\d+)\\s+Registered/i);
                    if (registeredMatch) {
                        data.registered_count = registeredMatch[1];
                    } else {
                        data.registered_count = registeredText;
                    }
                }
                
                // Allowed team size
                const teamSizeEl = document.querySelector('.event-team-size strong');
                if (teamSizeEl) {
                    data.team_size = teamSizeEl.textContent.trim();
                }
                
                // Hackathon phase
                const phaseEl = document.querySelector('.hack-phase .small.caps.light.label.ellipsis');
                if (phaseEl) {
                    data.phase = phaseEl.textContent.trim();
                }
                
                // Mode (online/offline) - more precise detection
                const locationEl = document.querySelector('.location-block .regular.bold.desc.dark');
                if (locationEl) {
                    const locationText = locationEl.textContent.trim();
                    if (locationText.includes('Online')) {
                        data.mode = 'online';
                        data.location = 'Online';
                    } else if (locationText.includes('Offline') || locationText.includes('In-person')) {
                        data.mode = 'offline';
                        data.location = locationText.replace('Offline', '').trim();
                    } else {
                        data.location = locationText;
                    }
                }
                
                // Dates - more precise extraction
                // Start date
                const startDateEl = document.querySelector('.start-time-block .regular.bold.desc.dark');
                if (startDateEl) {
                    data.start_date = startDateEl.textContent.trim();
                }
                
                // End date
                const endDateEl = document.querySelector('.end-time-block .regular.bold.desc.dark');
                if (endDateEl) {
                    data.end_date = endDateEl.textContent.trim();
                }
                
                // For countdown timer, try to extract the raw date (fallback)
                if (!data.end_date) {
                    const countdownEl = document.querySelector('[class*="countdown"], [id*="countdown"]');
                    if (countdownEl && countdownEl.getAttribute('data-end-time')) {
                        const endTimestamp = countdownEl.getAttribute('data-end-time');
                        const endDate = new Date(parseInt(endTimestamp) * 1000); // Convert to milliseconds
                        data.end_date = endDate.toLocaleDateString('en-US', { 
                            month: 'short', 
                            day: 'numeric', 
                            year: 'numeric',
                            hour: 'numeric',
                            minute: 'numeric'
                        });
                    }
                }
                
                // Prize pool - extract detailed prize information
                const prizeData = [];
                let totalPrizePool = '';
                
                // Look for prize section headers
                const prizeHeaders = Array.from(document.querySelectorAll('h2'))
                    .filter(el => el.textContent.toLowerCase().includes('prize'));
                
                if (prizeHeaders.length > 0) {
                    // Found a prize section, now extract individual prizes
                    const prizeContainers = document.querySelectorAll('.prize-container');
                    
                    prizeContainers.forEach((container) => {
                        const prizeInfo = {};
                        
                        // Extract prize title
                        const titleEl = container.querySelector('.large.weight-700, .prize-desc div:first-child');
                        if (titleEl) {
                            prizeInfo.title = titleEl.textContent.trim();
                        }
                        
                        // Extract prize amount
                        const amountEl = container.querySelector('.regular.dark');
                        if (amountEl) {
                            prizeInfo.amount = amountEl.textContent.trim();
                            
                            // Add to total prize pool calculation if it contains currency
                            if (prizeInfo.amount.includes('INR') || 
                                prizeInfo.amount.includes('$') || 
                                prizeInfo.amount.includes('₹')) {
                                if (!totalPrizePool) {
                                    totalPrizePool = prizeInfo.amount;
                                }
                            }
                        }
                        
                        // Extract prize image if available
                        const imgEl = container.querySelector('img');
                        if (imgEl) {
                            prizeInfo.image = imgEl.src;
                        }
                        
                        // Only add if we have a title or amount
                        if (prizeInfo.title || prizeInfo.amount) {
                            prizeData.push(prizeInfo);
                        }
                    });
                    
                    // Store the detailed prize data
                    if (prizeData.length > 0) {
                        data.prizes_detail = prizeData;
                        
                        // Create a summary of the prize pool
                        const prizeSummary = prizeData.map(p => 
                            `${p.title || ''}: ${p.amount || 'Not specified'}`
                        ).join(' | ');
                        
                        data.prize_pool = totalPrizePool || prizeSummary || 'See prizes section';
                    }
                } else {
                    // Fallback to simpler prize detection
                    const prizeEl = document.querySelector('.prize-detail, [class*="prize"], .prize');
                    if (prizeEl) {
                        data.prize_pool = prizeEl.textContent.trim();
                        // Clean up prize text
                        data.prize_pool = data.prize_pool.replace(/(?:Prize|Prizes|PRIZE|PRIZES):\\s*/i, '').trim();
                    }
                }
                
                // Participant count
                const participantsEl = document.querySelector('.registrations, [class*="participant"], .participants-count');
                if (participantsEl) {
                    data.num_participants = participantsEl.textContent.trim();
                    data.num_participants = data.num_participants.replace(/(?:Participants|Registrations):\\s*/i, '').trim();
                }
                
                // Organizer
                const organizerEl = document.querySelector('.organizer, [class*="company"], .company-name');
                if (organizerEl) {
                    data.organizer = organizerEl.textContent.trim();
                } else {
                    // Try to get organizer from logo alt text
                    const logoWithAlt = document.querySelector('.company-img img');
                    if (logoWithAlt && logoWithAlt.alt) {
                        data.organizer = logoWithAlt.alt.trim();
                    }
                }
                
                // Tags/themes
                const tagsEl = document.querySelectorAll('.tag, [class*="tag"], .theme, [class*="theme"]');
                if (tagsEl.length > 0) {
                    const tags = Array.from(tagsEl).map(tag => tag.textContent.trim());
                    data.tags = tags.join(', ');
                }
                
                // Registration URL - usually on the same page but might be a specific button
                const registerButton = document.querySelector('a[href*="register"], button:has-text("Register"), a:has-text("Register")');
                if (registerButton && registerButton.href) {
                    data.registration_url = registerButton.href;
                }
            } catch (e) {
                console.error('Error extracting event details:', e);
            }
            
            return data;
        }""")
        
        # Merge page details with listing data, prioritizing page details
        if page_details:
            for key, value in page_details.items():
                if value:  # Only update if the value is not empty
                    event_details[key] = value
        
        # Set defaults for required fields if missing
        if not event_details.get('title'):
            event_details['title'] = hackathon_id.replace('-', ' ').title()
        
        if not event_details.get('start_date'):
            event_details['start_date'] = 'See event website'
        
        if not event_details.get('end_date'):
            event_details['end_date'] = 'See event website'
        
        if not event_details.get('mode'):
            event_details['mode'] = 'online'  # Default to online for HackerEarth
        
        if not event_details.get('prize_pool'):
            event_details['prize_pool'] = 'Not specified'
        
        # Always set the URL and source platform
        event_details['url'] = url
        event_details['source_platform'] = 'hackerearth'
        
        print(f"Extracted details for: {event_details.get('title', 'Unknown event')}")
        
        # Print extracted image URLs for verification
        if event_details.get('banner_url'):
            print(f"Banner URL: {event_details['banner_url']}")
        
        if event_details.get('logo_url'):
            print(f"Logo URL: {event_details['logo_url']}")
        
        # Print new extracted fields
        if event_details.get('registered_count'):
            print(f"Registered participants: {event_details['registered_count']}")
            
        if event_details.get('team_size'):
            print(f"Allowed team size: {event_details['team_size']}")
            
        if event_details.get('phase'):
            print(f"Hackathon phase: {event_details['phase']}")
            
        # Print prize information if available
        if event_details.get('prizes_detail'):
            print(f"Found {len(event_details['prizes_detail'])} prizes")
            for i, prize in enumerate(event_details['prizes_detail']):
                print(f"  Prize {i+1}: {prize.get('title', 'No title')} - {prize.get('amount', 'No amount')}")
        
        # Print overview and themes information if available
        if event_details.get('overview'):
            print(f"Overview available: {len(event_details['overview'])} characters")
            
        if event_details.get('themes'):
            print(f"Found {len(event_details['themes'])} themes:")
            for i, theme in enumerate(event_details['themes']):
                print(f"  Theme {i+1}: {theme.get('title', 'No title')}")
        
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
            "title": hackathon_id.replace('-', ' ').title(),
            "url": url,
            "source_platform": "hackerearth",
            "start_date": "See event website",
            "end_date": "See event website",
            "mode": "online",
            "prize_pool": "Not specified",
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
    
    # First standardize and normalize the field names
    normalized_hackathons = []
    for hackathon in hackathons:
        # Create a normalized version with standardized field names
        normalized = {
            # Basic information - most important fields first
            "id": hashlib.md5(hackathon.get("url", "").encode()).hexdigest()[:8],
            "title": hackathon.get("title", ""),
            "url": hackathon.get("url", ""),
            "source_platform": "hackerearth",
            "status": hackathon.get("status", ""),
            
            # Timing information
            "start_date": hackathon.get("start_date", ""),
            "end_date": hackathon.get("end_date", ""),
            "registration_deadline": hackathon.get("registration_deadline", ""),
            
            # Location information
            "mode": hackathon.get("mode", ""),
            "location": hackathon.get("location", ""),
            
            # Participation information
            "registered_participants": hackathon.get("registered_count", hackathon.get("num_participants", "")),
            "team_size": hackathon.get("team_size", ""),
            "phase": hackathon.get("phase", ""),
            
            # Prize information
            "prize_pool": hackathon.get("prize_pool", ""),
            
            # Organization information
            "organizer": hackathon.get("organizer", ""),
            
            # Media information
            "logo_url": hackathon.get("logo_url", ""),
            "banner_url": hackathon.get("banner_url", ""),
            
            # Content information
            "description": hackathon.get("description", ""),
            "overview": hackathon.get("overview", ""),
            
            # Theme information
            "themes_summary": hackathon.get("themes_summary", ""),
        }
        
        # Add detailed prizes if available in a clean format
        if hackathon.get("prizes_detail"):
            prizes = []
            for prize in hackathon.get("prizes_detail", []):
                prizes.append({
                    "title": prize.get("title", ""),
                    "amount": prize.get("amount", ""),
                    "image_url": prize.get("image", "")
                })
            normalized["prizes"] = prizes
        
        # Add themes if available in a clean format
        if hackathon.get("themes"):
            themes = []
            for theme in hackathon.get("themes", []):
                themes.append({
                    "title": theme.get("title", ""),
                    "description": theme.get("description", ""),
                    "image_url": theme.get("image", "")
                })
            normalized["themes"] = themes
        
        # Add registration URL if available
        if hackathon.get("registration_url"):
            normalized["registration_url"] = hackathon.get("registration_url")
        
        # Add tags if available
        if hackathon.get("tags"):
            normalized["tags"] = hackathon.get("tags")
        
        normalized_hackathons.append(normalized)
    
    # For CSV, we need to prepare data by flattening complex structures
    csv_hackathons = []
    for hackathon in normalized_hackathons:
        # Create a flattened version for CSV
        flattened = hackathon.copy()
        
        # Convert prizes to string summary for CSV
        if "prizes" in flattened:
            prizes_summary = " | ".join([f"{p.get('title', '')}: {p.get('amount', '')}" for p in flattened["prizes"]])
            flattened["prizes_summary"] = prizes_summary
            del flattened["prizes"]  # Remove complex structure
        
        # Convert themes to string summary for CSV
        if "themes" in flattened:
            # Themes summary already exists so we can remove the complex structure
            del flattened["themes"]
        
        csv_hackathons.append(flattened)
    
    # Collect all fields for CSV header
    fieldnames = set()
    for hackathon in csv_hackathons:
        fieldnames.update(hackathon.keys())
    
    # Ensure required fields are at the beginning in a logical order
    ordered_fieldnames = [
        "id", "title", "url", "source_platform", "status",
        "start_date", "end_date", "registration_deadline",
        "mode", "location",
        "registered_participants", "team_size", "phase",
        "prize_pool", "prizes_summary",
        "organizer", 
        "logo_url", "banner_url",
        "themes_summary",
        "tags"
    ]
    
    # Filter ordered fieldnames to only include those that actually exist
    ordered_fieldnames = [f for f in ordered_fieldnames if f in fieldnames]
    
    # Add any remaining fields not explicitly ordered
    remaining_fields = fieldnames - set(ordered_fieldnames)
    ordered_fieldnames.extend(sorted(remaining_fields))
    
    # Write to CSV
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=ordered_fieldnames)
        writer.writeheader()
        writer.writerows(csv_hackathons)
    
    print(f"Saved {len(hackathons)} hackathons to '{filename}'")
    
    # Also save normalized JSON with proper formatting
    json_filename = filename.replace('.csv', '.json')
    with open(json_filename, 'w', encoding='utf-8') as f:
        json.dump(normalized_hackathons, f, indent=2, ensure_ascii=False)
    
    print(f"Saved normalized data to '{json_filename}'")

async def crawl_hackerearth_hackathons():
    """Main function to crawl HackerEarth hackathons."""
    print("Starting HackerEarth Hackathon Crawler (LIVE and UPCOMING only)...")
    print(f"Targeting hackathons from {BASE_URL}")
    
    async with async_playwright() as p:
        # Launch browser with longer timeouts
        browser = await p.chromium.launch(headless=not DEBUG_MODE)
        context = await browser.new_context(
            viewport={'width': 1280, 'height': 800},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        
        # Set default timeout
        context.set_default_timeout(60000)  # 60 seconds
        
        page = await context.new_page()
        
        # Initialize pages list with the main page
        pages = [page]
        
        try:
            # Navigate to the hackathons page
            print(f"Navigating to: {BASE_URL}")
            await page.goto(BASE_URL, wait_until="domcontentloaded")
            await page.wait_for_load_state("networkidle")
            
            # Bypass any login prompts
            await bypass_login_prompt(page)
            
            # Extract hackathon links and listing data
            hackathon_links, listing_data = await extract_hackathon_links(page)
            
            # Check if we found any hackathons
            if not hackathon_links:
                print("No hackathon links found. Exiting.")
                return
                
            print(f"Found {len(hackathon_links)} hackathon links")
            
            # Limit the total number of hackathons if needed
            if MAX_HACKATHONS and len(hackathon_links) > MAX_HACKATHONS:
                print(f"Limiting to {MAX_HACKATHONS} hackathons out of {len(hackathon_links)}")
                hackathon_links = hackathon_links[:MAX_HACKATHONS]
                # Also limit the listing data to match
                if isinstance(listing_data, list):
                    listing_data = [data for data in listing_data 
                                  if data.get("url") in hackathon_links]
            
            # Process each hackathon page
            print(f"Processing {len(hackathon_links)} individual hackathon pages...")
            
            # Create a pool of pages for parallel processing
            concurrency = min(5, len(hackathon_links))
            # Ensure concurrency is at least 1 to avoid division by zero
            concurrency = max(1, concurrency)
            
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
                
                print(f"Processed batch {i//concurrency + 1}, total hackathons so far: {len(all_hackathons)}")
                
                # Add a small delay between batches to avoid rate limiting
                await smart_wait(2, 4)
            
            # Save results to CSV
            if all_hackathons:
                # Generate timestamp for the filename
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"hackerearth_hackathons_{timestamp}.csv"
                
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
                print("No hackathons were found.")
                
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
    asyncio.run(crawl_hackerearth_hackathons())
