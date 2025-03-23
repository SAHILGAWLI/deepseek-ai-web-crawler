import asyncio
import json
import os
import time
from datetime import datetime
from typing import List, Dict, Any
from playwright.async_api import async_playwright, Error as PlaywrightError
from dotenv import load_dotenv
import csv
import re
import pandas as pd
import traceback

# Load environment variables
load_dotenv()

# Configuration
TARGET_URL = "https://devfolio.co/search?happening=this_year&primary_filter=hackathons&type=application_open"
OUTPUT_CSV = "devfolio_hackathons.csv"
REQUIRED_FIELDS = ["name", "start_date", "end_date", "mode"]
MAX_HACKATHONS = 100

async def take_screenshot(page, filename):
    """Take a screenshot for debugging purposes"""
    os.makedirs("screenshots", exist_ok=True)
    await page.screenshot(path=f"screenshots/{filename}")
    print(f"Screenshot saved to screenshots/{filename}")

async def extract_hackathon_links(page):
    """Extract links to individual hackathon pages"""
    try:
        # Wait for the content to load
        await asyncio.sleep(5)
        await take_screenshot(page, "before_extraction.png")
        
        print("Extracting hackathon links...")
        
        # Extract hackathon links using proper IIFE JavaScript pattern
        hackathon_links = await page.evaluate("""
            (() => {
                // Get all links from the page
                const allLinks = Array.from(document.querySelectorAll('a'))
                    .map(a => a.href)
                    .filter(href => href && href.includes('/hackathons/'));
                
                console.log("All hackathon links found:", allLinks);
                
                // Remove duplicates and return
                return [...new Set(allLinks)];
            })()
        """)
        
        print(f"Found {len(hackathon_links)} hackathon links: {hackathon_links}")
        
        # If no links found with '/hackathons/', try finding links to .devfolio.co subdomains
        if not hackathon_links:
            print("No links with '/hackathons/' found. Trying to find .devfolio.co subdomains...")
            
            hackathon_links = await page.evaluate("""
                (() => {
                    // Get all links that are likely hackathon subdomains
                    const subdomainLinks = Array.from(document.querySelectorAll('a'))
                        .map(a => a.href)
                        .filter(href => {
                            if (!href) return false;
                            
                            // Match pattern like xxx.devfolio.co but exclude specific subdomains
                            const match = href.match(/^https?:\/\/([^.]+)\.devfolio\.co/);
                            if (!match) return false;
                            
                            const subdomain = match[1];
                            const excludedSubdomains = ['guide', 'status', 'blog', 'support', 'api', 'docs', 'www'];
                            return !excludedSubdomains.includes(subdomain);
                        });
                    
                    console.log("Subdomain links found:", subdomainLinks);
                    
                    // Remove duplicates and return
                    return [...new Set(subdomainLinks)];
                })()
            """)
            
            print(f"Found {len(hackathon_links)} hackathon subdomain links: {hackathon_links}")
        
        # If still no links found, use the card selectors to extract links
        if not hackathon_links:
            print("Still no links found. Trying to find links in card containers...")
            
            hackathon_links = await page.evaluate("""
                (() => {
                    // Try various selectors that might contain hackathon cards
                    const selectors = [
                        'div[data-testid="SearchResult"]',
                        'div[data-testid="CardContainer"]',
                        '.card', 
                        '.hackathon-card',
                        '[role="link"]'
                    ];
                    
                    let links = [];
                    for (const selector of selectors) {
                        const elements = document.querySelectorAll(selector);
                        if (elements.length > 0) {
                            console.log(`Found ${elements.length} elements with selector ${selector}`);
                            
                            // For each card, find the link inside it
                            elements.forEach(element => {
                                const linkElement = element.querySelector('a');
                                if (linkElement && linkElement.href) {
                                    links.push(linkElement.href);
                                }
                            });
                            
                            if (links.length > 0) {
                                console.log(`Found ${links.length} links in cards with selector ${selector}`);
                                break;
                            }
                        }
                    }
                    
                    // If no links found in cards, try to find in the entire page
                    if (links.length === 0) {
                        // Specific hackathons we've observed before
                        const knownHackathons = [
                            'hack-nocturne',
                            'seekhothon',
                            'hack-summit',
                            'frosthack-2025'
                        ];
                        
                        const allAnchors = Array.from(document.querySelectorAll('a'));
                        for (const anchor of allAnchors) {
                            if (anchor.href && knownHackathons.some(name => anchor.href.includes(name))) {
                                links.push(anchor.href);
                            }
                        }
                    }
                    
                    // Remove duplicates and return
                    return [...new Set(links)];
                })()
            """)
            
            print(f"Found {len(hackathon_links)} links from cards: {hackathon_links}")
        
        # If still no links, fall back to manual links from previous runs
        if not hackathon_links:
            print("No links found through any method. Using fallback manual links...")
            hackathon_links = [
                'https://hack-nocturne.devfolio.co',
                'https://seekhothon.devfolio.co',
                'https://hack-summit.devfolio.co',
                'https://frosthack-2025.devfolio.co'
            ]
        
        return hackathon_links
    except Exception as e:
        print(f"Error extracting hackathon links: {e}")
        await take_screenshot(page, "error_links.png")
        return []

async def click_all_buttons_and_extract(page):
    """Try to click all buttons and show more information sections on the page"""
    try:
        # Look for buttons that might reveal more information
        buttons = await page.query_selector_all('button, [role="button"], .btn, .button, a.more, a.show-more, div.expand')
        
        for i, button in enumerate(buttons[:10]):
            try:
                # Check if button has text like "more", "show", "details", etc.
                button_text = await button.text_content()
                # Check if button is visible before trying to click
                is_visible = await button.is_visible()
                
                if button_text and is_visible:
                    button_text = button_text.lower()
                    if any(keyword in button_text for keyword in ['more', 'show', 'detail', 'expand', 'read']):
                        print(f"Clicking button with text: {button_text}")
                        try:
                            # Use a much shorter timeout (5 seconds instead of 30)
                            await button.click(timeout=5000)
                        await asyncio.sleep(1)  # Wait for content to appear
                        except PlaywrightError:
                            print(f"  - Button click timed out, skipping")
                            continue
            except Exception as e:
                print(f"Error with button {i}: {str(e)[:100]}")
                continue
        
        # Look for tabs that might contain additional information
        tabs = await page.query_selector_all('[role="tab"], .tab, .nav-item')
        visible_tabs = []
        
        # First check which tabs are actually visible to avoid wasting time
        for tab in tabs:
            if await tab.is_visible():
                visible_tabs.append(tab)
        
        print(f"Found {len(visible_tabs)} visible tabs out of {len(tabs)} total tabs")
        
        # Only try to click visible tabs
        for i, tab in enumerate(visible_tabs[:5]):  # Limit to first 5 tabs to save time
            try:
                print(f"Clicking visible tab {i}")
                try:
                    # Use a much shorter timeout (5 seconds instead of 30)
                    await tab.click(timeout=5000)
                await asyncio.sleep(1)  # Wait for content to appear
                except PlaywrightError:
                    print(f"  - Tab click timed out, skipping")
                    continue
            except Exception as e:
                print(f"Error with tab {i}: {str(e)[:100]}")
                continue
                
        return True
    except Exception as e:
        print(f"Error in click_all_buttons_and_extract: {e}")
        return False

async def extract_text_from_page(page):
    """Extract all text from the page, organized by elements"""
    try:
        all_text = await page.evaluate("""
            () => {
                const textBlocks = [];
                // Extract text from all elements that typically contain content
                document.querySelectorAll('p, h1, h2, h3, h4, h5, h6, div, span, li').forEach(el => {
                    const text = el.textContent.trim();
                    if (text && text.length > 5) {
                        textBlocks.push(text);
                    }
                });
                return textBlocks;
            }
        """)
        
        return all_text
    except Exception as e:
        print(f"Error extracting text from page: {e}")
        return []

async def extract_hackathon_details(page, url):
    """Extract detailed information from a hackathon page"""
    try:
        # Navigate to the hackathon page
        print(f"Navigating to {url}")
        await page.goto(url, wait_until="networkidle", timeout=60000)
        await asyncio.sleep(3)  # Wait for any JavaScript to execute
        
        # Get the hackathon subdomain for the screenshot filename
        subdomain = url.split('//')[1].split('.')[0] if '//' in url else url.split('.')[0]
        await take_screenshot(page, f"hackathon_{subdomain}.png")
        
        # Initialize details dictionary
        details = {
            'title': '',
            'organizer': '',
            'description': '',
            'start_date': '',
            'end_date': '',
            'location': '',
            'registration_deadline': '',
            'prize_pool': '',
            'url': url,
            'num_participants': '',
            'skills_required': [],
            'time_commitment': '',
            'prizes_details': [],
            'schedule_details': '',
            'runs_from_text': '',
            'happening_text': '',
            'mode': '',  # Online/offline
            'logo_url': '',  # Hackathon logo image
            'banner_url': ''  # Hackathon banner image
        }
        
        # Extract sidebar information first based on the observed HTML structure
        sidebar_info = await page.evaluate("""
            () => {
                let runs_from_text = '';
                let happening_text = '';
                let start_date = '';
                let end_date = '';
                let location = '';
                
                // Look for InfoLi elements that match the structure shown in the image
                // This matches the HTML structure with classes like "InfoLi-style_InfoLi-sc-b3e89b4c-0"
                const infoLiElements = document.querySelectorAll('li[class*="InfoLi-style_InfoLi"]');
                
                for (const li of infoLiElements) {
                    const liText = li.textContent.trim();
                    
                    // Check for "RUNS FROM" text in the InfoLi element
                    if (liText.includes('RUNS FROM')) {
                        // Find paragraphs within this li element
                        const paragraphs = li.querySelectorAll('p');
                        for (const p of paragraphs) {
                            if (p.textContent && !p.textContent.includes('RUNS FROM')) {
                                runs_from_text = p.textContent.trim();
                            break;
                            }
                        }
                    }
                    
                    // Check for "Happening" text in the InfoLi element
                    if (liText.includes('Happening')) {
                        // Find paragraphs within this li element
                        const paragraphs = li.querySelectorAll('p');
                        for (const p of paragraphs) {
                            if (p.textContent && !p.textContent.includes('Happening')) {
                                happening_text = p.textContent.trim();
                                location = happening_text;
                            break;
                            }
                        }
                    }
                }
                
                // If we couldn't find with li tags, try a more general approach with selectors
                if (!runs_from_text || !happening_text) {
                    // Look for elements with event-related text
                    document.querySelectorAll('div, p, span').forEach(el => {
                        const text = el.textContent.trim();
                        
                        // Look for date info
                        if (text.includes('RUNS FROM') || text.includes('Runs from')) {
                            // Try to find a sibling or child element with the actual date
                            let nextEl = el.nextElementSibling;
                            if (nextEl && nextEl.textContent && !nextEl.textContent.includes('RUNS FROM')) {
                                runs_from_text = nextEl.textContent.trim();
                            }
                        }
                        
                        // Look for location info
                        if (text.includes('Happening') || text.includes('HAPPENING')) {
                            // Try to find a sibling or child element with the actual location
                            let nextEl = el.nextElementSibling;
                            if (nextEl && nextEl.textContent && !nextEl.textContent.includes('Happening')) {
                                happening_text = nextEl.textContent.trim();
                                location = happening_text;
                            }
                        }
                    });
                }
                
                // Try to parse dates from runs_from_text
                if (runs_from_text) {
                    // Pattern for "Month Day - Day, Year" (e.g., "Jul 20 - 21, 2023")
                    const datePattern1 = /([A-Za-z]+)\s+(\d{1,2})\s*-\s*(\d{1,2}),?\s*(\d{4})/i;
                    // Pattern for "Month Day - Month Day, Year" (e.g., "Jul 20 - Aug 10, 2023")
                    const datePattern2 = /([A-Za-z]+)\s+(\d{1,2})\s*-\s*([A-Za-z]+)\s+(\d{1,2}),?\s*(\d{4})/i;
             
                    const match1 = runs_from_text.match(datePattern1);
                    const match2 = runs_from_text.match(datePattern2);
                    
                    if (match1) {
                        const month = match1[1];
                        const startDay = match1[2];
                        const endDay = match1[3];
                        const year = match1[4];
                        
                        start_date = `${month} ${startDay}, ${year}`;
                        end_date = `${month} ${endDay}, ${year}`;
                    } else if (match2) {
                        const startMonth = match2[1];
                        const startDay = match2[2];
                        const endMonth = match2[3];
                        const endDay = match2[4];
                        const year = match2[5];
                        
                        start_date = `${startMonth} ${startDay}, ${year}`;
                        end_date = `${endMonth} ${endDay}, ${year}`;
                    }
                }
                
                return {
                    runs_from_text,
                    happening_text,
                    start_date,
                    end_date,
                    location
                };
            }
        """);
        
        print(f"Sidebar information: {json.dumps(sidebar_info, indent=2)}")
        
        # Update details with sidebar information
        if sidebar_info['runs_from_text']:
            details['runs_from_text'] = sidebar_info['runs_from_text']
            print(f"Found 'Runs from' text: {details['runs_from_text']}")
        
        if sidebar_info['happening_text']:
            details['happening_text'] = sidebar_info['happening_text']
            print(f"Found 'Happening' text: {details['happening_text']}")
            
        if sidebar_info['start_date']:
            details['start_date'] = sidebar_info['start_date']
            print(f"Parsed start date: {details['start_date']}")
            
        if sidebar_info['end_date']:
            details['end_date'] = sidebar_info['end_date']
            print(f"Parsed end date: {details['end_date']}")
            
        if sidebar_info['location']:
            details['location'] = sidebar_info['location']
            print(f"Found location: {details['location']}")
        
        # Extract main page content
        content = await page.evaluate("""
            () => {
                const title = document.querySelector('h1, h2, h3')?.textContent?.trim() || '';
                const description_elements = Array.from(document.querySelectorAll('p, div')).filter(el => {
                    const text = el.textContent.trim();
                    return text.length > 100 && text.length < 1000 && text.split(' ').length > 20;
                });
                
                const description = description_elements.length > 0 ? description_elements[0].textContent.trim() : '';
                
                // Try to extract organizer from meta tags or page content
                let organizer = '';
                const metaOrganizer = document.querySelector('meta[property="og:site_name"]');
                if (metaOrganizer) {
                    organizer = metaOrganizer.getAttribute('content');
                } else {
                    // Look for organizer in the page content
                    const organizerElements = Array.from(document.querySelectorAll('div, p, span, h1, h2, h3, h4, h5, h6')).filter(el => {
                        const text = el.textContent.trim().toLowerCase();
                        return text.includes('organized by') || text.includes('brought to you by') || text.includes('hosted by');
                    });
                    
                    if (organizerElements.length > 0) {
                        const match = organizerElements[0].textContent.match(/(?:organized|brought to you|hosted) by[:\s]+([^.]+)/i);
                        if (match) {
                            organizer = match[1].trim();
                        }
                    }
                }
                
                // Extract skills
                const skills = [];
                document.querySelectorAll('div, span').forEach(el => {
                    if (el.textContent && el.textContent.trim().length > 0 && el.textContent.trim().length < 30) {
                        const text = el.textContent.trim();
                        if (/python|javascript|react|node|ai|ml|blockchain|web3|frontend|backend|fullstack|data science|cloud|aws|azure|solidity/i.test(text)) {
                            skills.push(text);
                        }
                    }
                });
                
                // Extract participant count if available
                let participantCount = '';
                document.querySelectorAll('div, p, span').forEach(el => {
                    if (el.textContent && el.textContent.trim().match(/\d+\s+participants/i)) {
                        participantCount = el.textContent.trim();
                    }
                });
                
                return {
                    title,
                    description,
                    organizer,
                    skills,
                    participant_count: participantCount
                };
            }
        """)
        
        # Update details with the main content information
        details['title'] = content.get('title', '')
        details['description'] = content.get('description', '')
        details['organizer'] = content.get('organizer', '')
        details['skills_required'] = content.get('skills', [])
        details['num_participants'] = content.get('participant_count', '')
        
        # Create schedule_details from sidebar info if available
        if details.get('runs_from_text') or details.get('happening_text'):
            timeline_summary = []
            
            # Include date information if available
            if details.get('runs_from_text'):
                timeline_summary.append(f"Runs from: {details['runs_from_text']}")
                
                # Add structured date information if we parsed it correctly
                if details.get('start_date') and details.get('end_date'):
                    if not f"{details['start_date']} to {details['end_date']}" in details['runs_from_text']:
                        timeline_summary.append(f"Dates: {details['start_date']} to {details['end_date']}")
            
            # Include location information if available
            if details.get('happening_text'):
                timeline_summary.append(f"Happening: {details['happening_text']}")
            
            if timeline_summary:
                details['schedule_details'] = " | ".join(timeline_summary)
                print(f"Created schedule_details from sidebar: {details['schedule_details']}")
        
        # Check for /prizes URL
        prizes_url = url.rstrip('/') + '/prizes'
        print(f"Checking prizes URL: {prizes_url}")
        try:
            await page.goto(prizes_url, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(2)
            await take_screenshot(page, f"hackathon_{subdomain}_prizes.png")
            
            # Extract prize information
            prize_info = await page.evaluate("""
                () => {
                    const prize_text = [];
                    document.querySelectorAll('div, p, h1, h2, h3, h4, h5, h6, span').forEach(el => {
                        if (el.textContent && el.textContent.trim().length > 0) {
                            const text = el.textContent.trim();
                            if (text.match(/\\$|₹|prize|reward|pool|winner|winning/i)) {
                                prize_text.push(text);
                            }
                        }
                    });
                    
                    return {
                        prize_text: prize_text
                    };
                }
            """)
            
            if prize_info['prize_text'] and len(prize_info['prize_text']) > 0:
                print(f"Found {len(prize_info['prize_text'])} prize-related text elements")
                
                # Look for prize amounts
                prize_pattern = r'(\$[\d,.]+|₹[\d,.]+|[\d,.]+\s*USD|[\d,.]+\s*INR)'
                for text in prize_info['prize_text']:
                    match = re.search(prize_pattern, text)
                    if match and not details['prize_pool']:
                        details['prize_pool'] = match.group(1)
                        print(f"Found prize pool from prizes page: {details['prize_pool']}")
                        break
                
                # Store all prize text for additional context
                details['prizes_details'] = prize_info['prize_text']
        except Exception as e:
            print(f"Error checking prizes URL: {e}")
        
        # Only check schedule page if necessary:
        # 1. We don't have both start and end dates from sidebar AND
        # 2. We don't have runs_from_text from the sidebar (which contains date information)
        if not ((details.get('start_date') and details.get('end_date')) or details.get('runs_from_text')):
        schedule_url = url.rstrip('/') + '/schedule'
            print(f"Checking schedule URL (no sidebar dates found): {schedule_url}")
        try:
            await page.goto(schedule_url, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(2)
            await take_screenshot(page, f"hackathon_{subdomain}_schedule.png")
            
            # Extract schedule information
            schedule_info = await page.evaluate("""
                () => {
                    const schedule_text = [];
                    document.querySelectorAll('div, p, h1, h2, h3, h4, h5, h6, span').forEach(el => {
                        if (el.textContent && el.textContent.trim().length > 0) {
                            const text = el.textContent.trim();
                            if (text.match(/date|start|end|schedule|timeline|deadline|registration|open|close/i)) {
                                schedule_text.push(text);
                            }
                        }
                    });
                    
                    return {
                        schedule_text: schedule_text
                    };
                }
            """)
            
            if schedule_info['schedule_text'] and len(schedule_info['schedule_text']) > 0:
                print(f"Found {len(schedule_info['schedule_text'])} schedule-related text elements")
                
                # Process schedule text to extract key timeline information
                timeline_info = []
                
                # Extract dates from schedule text
                date_patterns = [
                    r'(\w{3,9}\s+\d{1,2})(?:\s*[-–]\s*)(\w{3,9}\s+\d{1,2})',  # March 15 - March 16
                    r'(\d{1,2}\s+\w{3,9})(?:\s*[-–]\s*)(\d{1,2}\s+\w{3,9})',  # 15 March - 16 March
                    r'(\d{1,2}/\d{1,2}/\d{2,4})(?:\s*[-–]\s*)(\d{1,2}/\d{1,2}/\d{2,4})',  # MM/DD/YYYY format
                    r'(\d{1,2}-\d{1,2}-\d{2,4})(?:\s*[-–]\s*)(\d{1,2}-\d{1,2}-\d{2,4})'   # DD-MM-YYYY format
                ]
                
                # Extract key schedule events
                event_patterns = [
                    r'(.*?(?:start|begin|launch|opening).*?)(?:\s*:\s*|\s*-\s*|\s+on\s+)(\w{3,9}\s+\d{1,2}|\d{1,2}[-/]\d{1,2}(?:[-/]\d{2,4})?)',
                    r'(.*?(?:end|closing|finale).*?)(?:\s*:\s*|\s*-\s*|\s+on\s+)(\w{3,9}\s+\d{1,2}|\d{1,2}[-/]\d{1,2}(?:[-/]\d{2,4})?)',
                    r'(registration\s+(?:begins|opens))(?:\s*:\s*|\s*-\s*|\s+on\s+)(\w{3,9}\s+\d{1,2}|\d{1,2}[-/]\d{1,2}(?:[-/]\d{2,4})?)',
                    r'(registration\s+(?:closes|ends|deadline))(?:\s*:\s*|\s*-\s*|\s+on\s+)(\w{3,9}\s+\d{1,2}|\d{1,2}[-/]\d{1,2}(?:[-/]\d{2,4})?)'
                ]
                
                for text in schedule_info['schedule_text']:
                    # Look for date ranges
                    for pattern in date_patterns:
                        match = re.search(pattern, text)
                        if match:
                            start_date = match.group(1)
                            end_date = match.group(2)
                            if not details['start_date'] or not details['end_date']:
                                details['start_date'] = start_date
                                details['end_date'] = end_date
                            timeline_info.append(f"Hackathon: {start_date} to {end_date}")
                            break
                    
                    # Look for specific events
                    for pattern in event_patterns:
                        match = re.search(pattern, text, re.IGNORECASE)
                        if match:
                            event = match.group(1).strip()
                            date = match.group(2).strip()
                            timeline_info.append(f"{event}: {date}")
                
                # Filter out duplicates and format the timeline summary
                timeline_info = list(set(timeline_info))
                
                    # Look for registration deadline
                deadline_patterns = [
                    r'registration\s+(?:closes|deadline)(?:[:\s]+)(\w{3,9}\s+\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]\d{2,4})',
                    r'(?:apply|register)\s+by(?:[:\s]+)(\w{3,9}\s+\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]\d{2,4})'
                ]
                
                for text in schedule_info['schedule_text']:
                    for pattern in deadline_patterns:
                        match = re.search(pattern, text, re.IGNORECASE)
                        if match and not details['registration_deadline']:
                            details['registration_deadline'] = match.group(1)
                            print(f"Found registration deadline from schedule page: {details['registration_deadline']}")
                            break
                    if details['registration_deadline']:
                        break
                        
                    # Update schedule_details if we didn't get it from sidebar
                    if not details.get('schedule_details') and timeline_info:
                        details['schedule_details'] = " | ".join(timeline_info)
                        print(f"Created schedule_details from schedule page: {details['schedule_details']}")
        except Exception as e:
            print(f"Error checking schedule URL: {e}")
        
        # If we're missing crucial information, try to click on elements and extract more
        if not details['start_date'] or not details['prize_pool'] or not details['organizer']:
            print("Missing crucial information. Attempting deeper extraction...")
            
            # Go back to the main page
            await page.goto(url, wait_until="networkidle", timeout=60000)
            await asyncio.sleep(2)
            
            # Try clicking buttons and tabs to reveal more information
            await click_all_buttons_and_extract(page)
            
            # Take another screenshot after clicking buttons
            await take_screenshot(page, f"hackathon_{subdomain}_after_clicks.png")
            
            # Extract all text from the page for regex parsing
            all_text = await extract_text_from_page(page)
            
            # Try to find dates from the text using regex
            if not details['start_date'] or not details['end_date']:
                date_patterns = [
                    r'(\w{3,9}\s+\d{1,2})(?:\s*[-–]\s*)(\w{3,9}\s+\d{1,2})',  # March 15 - March 16
                    r'(\d{1,2}\s+\w{3,9})(?:\s*[-–]\s*)(\d{1,2}\s+\w{3,9})',  # 15 March - 16 March
                    r'(\d{1,2}/\d{1,2}/\d{2,4})(?:\s*[-–]\s*)(\d{1,2}/\d{1,2}/\d{2,4})',  # MM/DD/YYYY format
                    r'(\d{1,2}-\d{1,2}-\d{2,4})(?:\s*[-–]\s*)(\d{1,2}-\d{1,2}-\d{2,4})'   # DD-MM-YYYY format
                ]
                
                for text in all_text:
                    for pattern in date_patterns:
                        match = re.search(pattern, text)
                        if match:
                            details['start_date'] = match.group(1)
                            details['end_date'] = match.group(2)
                            print(f"Found dates in deeper extraction: {details['start_date']} to {details['end_date']}")
                            break
                    if details['start_date'] and details['end_date']:
                        break
            
            # Try to find prize pool
            if not details['prize_pool']:
                prize_pattern = r'(\$[\d,.]+|₹[\d,.]+|[\d,.]+\s*USD|[\d,.]+\s*INR)'
                for text in all_text:
                    match = re.search(prize_pattern, text)
                        if match:
                            details['prize_pool'] = match.group(1)
                        print(f"Found prize pool in deeper extraction: {details['prize_pool']}")
                        break
            
            # Try to find organizer
            if not details['organizer']:
                organizer_patterns = [
                    r'(?:organized|brought to you|hosted)\s+by[:\s]+([^.]+)',
                    r'(?:presented|powered)\s+by[:\s]+([^.]+)'
                ]
                
                for text in all_text:
                    for pattern in organizer_patterns:
                        match = re.search(pattern, text, re.IGNORECASE)
                        if match:
                            details['organizer'] = match.group(1).strip()
                            print(f"Found organizer in deeper extraction: {details['organizer']}")
                            break
                    if details['organizer']:
                        break
            
            # If we still don't have a schedule_details summary, create one
            if not details['schedule_details']:
                timeline_summary = []
                
                if details['start_date'] and details['end_date']:
                    timeline_summary.append(f"Hackathon: {details['start_date']} to {details['end_date']}")
                
                if details['registration_deadline']:
                    timeline_summary.append(f"Registration deadline: {details['registration_deadline']}")
                
                if timeline_summary:
                    details['schedule_details'] = " | ".join(timeline_summary)
                else:
                    # If we couldn't extract specific timeline info, provide a basic summary
                    details['schedule_details'] = "Timeline information not available"
        
        return details
    except Exception as e:
        print(f"Error in extract_hackathon_details: {e}")
        traceback.print_exc()
        return None

def is_complete_hackathon(hackathon: Dict[str, Any], required_fields: List[str]) -> bool:
    """
    Checks if a hackathon has all required fields.
    """
    for field in required_fields:
        if field not in hackathon or not hackathon[field]:
            return False
    return True

def save_hackathons_to_csv(hackathons: List[Dict[str, Any]], filename: str) -> None:
    """
    Saves hackathons to a CSV file.
    """
    if not hackathons:
        print("No hackathons to save")
        return
    
    # Collect all fields from all hackathons
    fieldnames = set()
    for hackathon in hackathons:
        fieldnames.update(hackathon.keys())
    
    # Write to CSV
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=sorted(fieldnames))
        writer.writeheader()
        writer.writerows(hackathons)
    
    print(f"Saved {len(hackathons)} hackathons to '{filename}'")

async def crawl_hackathons():
    """Main function to crawl hackathons"""
    print("Starting Devfolio Hackathon Detailed Crawler...")
    
    # URL for hackathons happening this year with open applications
    url = "https://devfolio.co/search?happening=this_year&primary_filter=hackathons&type=application_open"
    max_hackathons = 100
    
    print(f"Target URL: {url}")
    print(f"Looking for up to {max_hackathons} hackathons")
    
    async with async_playwright() as p:
        # Launch browser with slower navigation to ensure page loads completely
        browser = await p.chromium.launch(headless=False, slow_mo=100)
        context = await browser.new_context(
            viewport={'width': 1280, 'height': 800}
        )
        page = await context.new_page()
        
        try:
            # Navigate to the search page
            print(f"Navigating to {url}...")
            await page.goto(url, wait_until="networkidle", timeout=60000)
            print("Initial page load complete")
            
            # Scroll to load more content
            print("Scrolling to load more content...")
            for i in range(10):
                await page.evaluate("window.scrollBy(0, 1000)")
                await asyncio.sleep(1)  # Give time for content to load
                print(f"Scroll {i+1}/10 complete")
            
            await asyncio.sleep(2)  # Final wait to ensure all content is loaded
            
            # Extract links to individual hackathon pages
            hackathon_links = await extract_hackathon_links(page)
            
            # Limit the number of hackathons to crawl
            hackathon_links = hackathon_links[:max_hackathons]
            
            # Crawl each hackathon page
            all_hackathons = []
            for link in hackathon_links:
                details = await extract_hackathon_details(page, link)
                if details:
                    all_hackathons.append(details)
                await asyncio.sleep(2)  # Wait between requests
            
            # Save results to CSV
            if all_hackathons:
                df = pd.DataFrame(all_hackathons)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"hackathons_{timestamp}.csv"
                df.to_csv(filename, index=False)
                print(f"Saved {len(all_hackathons)} hackathons to {filename}")
            else:
                print("No hackathons were found.")
                
        except Exception as e:
            print(f"Error during crawling: {e}")
            await take_screenshot(page, "error_state.png")
            
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(crawl_hackathons())
