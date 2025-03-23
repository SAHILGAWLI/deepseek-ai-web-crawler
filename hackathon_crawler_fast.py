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
                            const match = href.match(/^https?:\\/\\/([^.]+)\\.devfolio\\.co/);
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
    """Try to click all buttons and show more information sections on the page but with optimizations for speed"""
    try:
        # Look for buttons that might reveal more information - ONLY those that are visible
        buttons = await page.query_selector_all('button:visible, [role="button"]:visible, .btn:visible, .button:visible, a.more:visible, a.show-more:visible, div.expand:visible')
        
        if len(buttons) > 0:
            print(f"Found {len(buttons)} visible buttons/links to try")
        
        # Only process up to 5 buttons to avoid too many clicks
        for i, button in enumerate(buttons[:5]):
            try:
                # Check if button has text like "more", "show", "details", etc.
                button_text = await button.text_content() or ""
                    button_text = button_text.lower()
                
                    if any(keyword in button_text for keyword in ['more', 'show', 'detail', 'expand', 'read']):
                        print(f"Clicking button with text: {button_text}")
                        try:
                        # Use a much shorter timeout (3 seconds instead of 30)
                        await button.click(timeout=3000)
                        await asyncio.sleep(0.5)  # Reduced wait time
                    except Exception:
                        print(f"  - Button click failed, skipping")
                            continue
            except Exception as e:
                print(f"Error with button {i}: {str(e)[:100]}")
                continue
        
        # Only try important tabs that are likely to contain valuable information
        tab_selectors = [
            '[role="tab"]:has-text("Schedule"):visible', 
            '[role="tab"]:has-text("Prizes"):visible',
            '.tab:has-text("Schedule"):visible',
            '.tab:has-text("Prizes"):visible'
        ]
        
        # Try each important tab selector
        for selector in tab_selectors:
            try:
                tab = await page.query_selector(selector)
                if tab:
                    print(f"Clicking important tab: {selector}")
                    try:
                        # Use a much shorter timeout
                        await tab.click(timeout=3000)
                        await asyncio.sleep(0.5)  # Shorter wait
                    except Exception:
                        print(f"  - Tab click failed, skipping")
                    continue
            except Exception as e:
                print(f"Error with tab selector {selector}: {str(e)[:100]}")
                continue
                
        return True
    except Exception as e:
        print(f"Error in click_all_buttons_and_extract: {str(e)[:100]}")
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
            'mode': '',  # Added for online/offline
            'logo_url': '',  # Added for hackathon logo
            'banner_url': ''  # Added for hackathon banner
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
                    const datePattern1 = /([A-Za-z]+)\\s+(\\d{1,2})\\s*-\\s*(\\d{1,2}),?\\s*(\\d{4})/i;
                    // Pattern for "Month Day - Month Day, Year" (e.g., "Jul 20 - Aug 10, 2023")
                    const datePattern2 = /([A-Za-z]+)\\s+(\\d{1,2})\\s*-\\s*([A-Za-z]+)\\s+(\\d{1,2}),?\\s*(\\d{4})/i;
             
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
            
            # Determine mode based on location text
            happening_text_lower = sidebar_info['happening_text'].lower()
            if any(keyword in happening_text_lower for keyword in ['online', 'virtual', 'remote']):
                details['mode'] = 'online'
                print(f"Determined mode: online")
            elif any(keyword in happening_text_lower for keyword in ['offline', 'in-person', 'on-site', 'venue']):
                details['mode'] = 'offline'
                print(f"Determined mode: offline")
            
        if sidebar_info['start_date']:
            details['start_date'] = sidebar_info['start_date']
            print(f"Parsed start date: {details['start_date']}")
            
        if sidebar_info['end_date']:
            details['end_date'] = sidebar_info['end_date']
            print(f"Parsed end date: {details['end_date']}")
            
        if sidebar_info['location']:
            details['location'] = sidebar_info['location']
            print(f"Found location: {details['location']}")
        
        # Extract images (logo and banner)
        images_info = await page.evaluate("""
            () => {
                let logo_url = '';
                let banner_url = '';
                
                // Try to find the logo image
                // Method 1: Look for header logo container
                const logoContainer = document.querySelector('div[class*="Header__HackathonLogo"]');
                if (logoContainer) {
                    const img = logoContainer.querySelector('img');
                    if (img) {
                        logo_url = img.src;
                    }
                }
                
                // Method 2: Look for any favicon or small logo image
                if (!logo_url) {
                    const possibleLogos = Array.from(document.querySelectorAll('img'))
                        .filter(img => {
                            const src = img.src || '';
                            return src.includes('favicon') || 
                                   src.includes('logo') || 
                                   (img.alt && img.alt.includes('logo'));
                        });
                    
                    if (possibleLogos.length > 0) {
                        logo_url = possibleLogos[0].src;
                    }
                }
                
                // Try to find the banner image
                // Method 1: Look for a large image at the top of the page
                const bannerImages = Array.from(document.querySelectorAll('img'))
                    .filter(img => {
                        const src = img.src || '';
                        const style = window.getComputedStyle(img);
                        const width = parseInt(style.width) || 0;
                        return (src.includes('cover') || 
                                src.includes('banner') || 
                                src.includes('hero')) ||
                               (width > 500 && img.offsetParent !== null);
                    });
                
                if (bannerImages.length > 0) {
                    banner_url = bannerImages[0].src;
                }
                
                // Method 2: If no banner found, look for background images
                if (!banner_url) {
                    const elementsWithBg = Array.from(document.querySelectorAll('div, section, header'))
                        .filter(el => {
                            const style = window.getComputedStyle(el);
                            const bgImage = style.backgroundImage || '';
                            return bgImage.includes('url') && 
                                  (bgImage.includes('cover') || 
                                   bgImage.includes('banner') || 
                                   bgImage.includes('hero'));
                        });
                    
                    if (elementsWithBg.length > 0) {
                        const bgImage = window.getComputedStyle(elementsWithBg[0]).backgroundImage;
                        // Extract URL from background-image: url("...")
                        const match = bgImage.match(/url\\(['"]?([^'"\\)]+)['"]?\\)/);
                        if (match) {
                            banner_url = match[1];
                        }
                    }
                }
                
                return {
                    logo_url,
                    banner_url
                };
            }
        """);
        
        # Update details with image URLs
        if images_info['logo_url']:
            details['logo_url'] = images_info['logo_url']
            print(f"Found logo image URL: {details['logo_url']}")
        
        if images_info['banner_url']:
            details['banner_url'] = images_info['banner_url']
            print(f"Found banner image URL: {details['banner_url']}")
        
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
                        const match = organizerElements[0].textContent.match(/(?:organized|brought to you|hosted) by[:\\s]+([^.]+)/i);
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
                    if (el.textContent && el.textContent.trim().match(/\\d+\\s+participants/i)) {
                        participantCount = el.textContent.trim();
                    }
                });
                
                // Look for mode information if not found in sidebar
                let mode = '';
                const modeTexts = Array.from(document.querySelectorAll('div, p, span')).map(el => el.textContent.trim().toLowerCase());
                
                for (const text of modeTexts) {
                    if (text.includes('online hackathon') || 
                        text.includes('virtual hackathon') || 
                        text.includes('remote participation')) {
                        mode = 'online';
                        break;
                    } else if (text.includes('offline hackathon') || 
                               text.includes('in-person hackathon') || 
                               text.includes('on-site hackathon') ||
                               text.includes('at venue')) {
                        mode = 'offline';
                        break;
                    }
                }
                
                return {
                    title,
                    description,
                    organizer,
                    skills,
                    participant_count: participantCount,
                    mode
                };
            }
        """)
        
        # Update details with the main content information
        details['title'] = content.get('title', '')
        details['description'] = content.get('description', '')
        details['organizer'] = content.get('organizer', '')
        details['skills_required'] = content.get('skills', [])
        details['num_participants'] = content.get('participant_count', '')
        
        # Set mode if found in content and not already set from sidebar
        if content.get('mode') and not details['mode']:
            details['mode'] = content.get('mode')
            print(f"Determined mode from content: {details['mode']}")
        
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
            
            # If we still don't have mode information, try to infer from text
            if not details['mode']:
                for text in all_text:
                    text_lower = text.lower()
                    if any(keyword in text_lower for keyword in ['online', 'virtual', 'remote']):
                        details['mode'] = 'online'
                        print(f"Inferred mode from text: online")
                        break
                    elif any(keyword in text_lower for keyword in ['offline', 'in-person', 'on-site', 'venue']):
                        details['mode'] = 'offline'
                        print(f"Inferred mode from text: offline")
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
    """Main function to crawl hackathons with performance optimizations"""
    print("Starting Devfolio Hackathon Fast Crawler...")
    
    # URL for hackathons happening this year with open applications
    url = "https://devfolio.co/search?happening=this_year&primary_filter=hackathons&type=application_open"
    max_hackathons = 100
    
    print(f"Target URL: {url}")
    print(f"Looking for up to {max_hackathons} hackathons")
    
    async with async_playwright() as p:
        # Launch browser with optimized settings
        browser = await p.chromium.launch(headless=True, slow_mo=50)  # Changed to headless for speed
        context = await browser.new_context(
            viewport={'width': 1280, 'height': 800},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        
        # Important: Set default timeout to be shorter
        context.set_default_timeout(30000)  # 30 seconds instead of default 60
        
        page = await context.new_page()
        
        try:
            # Navigate to the search page
            print(f"Navigating to {url}...")
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)  # Changed from networkidle to domcontentloaded
            print("Initial page load complete")
            
            # Scroll to load more content - fewer scrolls, faster sleep times
            print("Scrolling to load content...")
            for i in range(15):  # Increased from 5 to 15 scrolls
                await page.evaluate("window.scrollBy(0, 1000)")
                await asyncio.sleep(0.5)  # Reduced from 1 second
                print(f"Scroll {i+1}/15 complete")
            
            await asyncio.sleep(1)  # Reduced wait time
            
            # Extract links to individual hackathon pages
            hackathon_links = await extract_hackathon_links(page)
            
            if not hackathon_links:
                print("No hackathon links found. Taking screenshot for debugging...")
                await take_screenshot(page, "no_links_found.png")
                return
            
            print(f"Found {len(hackathon_links)} hackathon links")
            
            # Limit the number of hackathons to crawl
            hackathon_links = hackathon_links[:max_hackathons]
            
            # Create a context manager for multiple pages
            pages = []
            max_concurrent = 3  # Process 3 hackathons at once
            
            # Create pages
            for i in range(max_concurrent):
                if i == 0:
                    pages.append(page)  # Reuse existing page
                else:
                    pages.append(await context.new_page())
            
            # Crawl each hackathon page with concurrency
            all_hackathons = []
            for i in range(0, len(hackathon_links), max_concurrent):
                batch = hackathon_links[i:i+max_concurrent]
                tasks = []
                
                for j, link in enumerate(batch):
                    if j < len(pages):
                        # Process each hackathon in its own page
                        tasks.append(extract_hackathon_details(pages[j], link))
                
                # Wait for all tasks in this batch to complete
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process results
                for result in batch_results:
                    if isinstance(result, Exception):
                        print(f"Error during extraction: {result}")
                    elif result:  # If not None
                        all_hackathons.append(result)
                
                print(f"Processed batch {i//max_concurrent + 1}, total hackathons so far: {len(all_hackathons)}")
            
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
            for p in pages[1:]:  # Close additional pages
                await p.close()
            await browser.close()

if __name__ == "__main__":
    asyncio.run(crawl_hackathons())
