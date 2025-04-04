import asyncio
import json
import os
import time
import random
from datetime import datetime, timedelta
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
BASE_URL = "https://devpost.com/hackathons"
# Support URL with bracket parameters like open_to[]
DEFAULT_PARAMS = "?open_to[]=public&status[]=open"
OUTPUT_CSV = "devpost_hackathons.csv"
REQUIRED_FIELDS = ["title", "start_date", "end_date", "mode"]
MAX_HACKATHONS = 1000  # Increased to 30 to make sure we get all hackathons on the page
START_PAGE = 3  # Updated to page 3 as requested by user
PROCESS_SINGLE_PAGE = True  # Only process the specified page
MAX_PAGES = 5  # Number of pagination pages to crawl
MAX_SCROLL_ATTEMPTS = 50  # Increased from 30 to 50 for more thorough scrolling
SCROLL_PAUSE_TIME = 1.2  # Increased from 0.8 to 1.2 seconds
FINAL_PAUSE_TIME = 8  # Longer final pause after scrolling
MAX_RETRIES = 3  # Maximum number of retries for page loading

async def take_screenshot(page, filename):
    """Take a screenshot for debugging purposes"""
    os.makedirs("screenshots", exist_ok=True)
    await page.screenshot(path=f"screenshots/{filename}")
    print(f"Screenshot saved to screenshots/{filename}")

async def extract_hackathon_links(page):
    """Extract links to individual hackathon pages"""
    try:
        print("Extracting hackathon links...")
        
        # Save HTML for debugging
        html_content = await page.content()
        os.makedirs("debug", exist_ok=True)
        with open("debug/page_dump.html", "w", encoding="utf-8") as f:
            f.write(html_content)
        
        # Take a screenshot at this point
        await take_screenshot(page, "before_extraction.png")
        
        # IMPROVED LOGO EXTRACTION - Directly target the hackathon-thumbnail class
        print("Using direct targeting for logo images and hackathon cards...")
        
        # First, let's extract all logo images specifically
        logo_data = await page.evaluate("""
            () => {
                const results = [];
                
                // Specifically target the hackathon-thumbnail class as described in the user's prompt
                const thumbnails = document.querySelectorAll('.hackathon-thumbnail, img[src*="challenge_thumbnails"], img[src*="medium_square.png"]');
                console.log(`Found ${thumbnails.length} elements with hackathon-thumbnail class or similar`);
                
                thumbnails.forEach(img => {
                    if (img.src) {
                        let logoUrl = img.src;
                        // Ensure we have a full URL by adding https: if it starts with //
                        if (logoUrl.startsWith('//')) {
                            logoUrl = 'https:' + logoUrl;
                        }
                        
                        // Get the parent elements to find related hackathon info
                        let parentCard = img;
                        let linkUrl = '';
                        let title = '';
                        
                        // Traverse up to find the card container - go up to 6 levels to ensure we catch all structures
                        let searchDepth = 0;
                        while (parentCard && searchDepth < 6 && 
                               !parentCard.classList.contains('challenge-listing') && 
                               !parentCard.classList.contains('card') && 
                               !parentCard.classList.contains('tile') &&
                               !parentCard.querySelector('a[href*="devpost.com"]') &&
                               parentCard.tagName !== 'BODY') {
                            parentCard = parentCard.parentElement;
                            searchDepth++;
                        }
                        
                        // If we found a parent card, extract link and title
                        if (parentCard && parentCard.tagName !== 'BODY') {
                            // Find the first link in the card
                            const link = parentCard.querySelector('a');
                            if (link && link.href) {
                                linkUrl = link.href;
                            }
                            
                            // Try to find the title
                            const titleEl = parentCard.querySelector('h3, .title, .challenge-title, [class*="title"]');
                            if (titleEl) {
                                title = titleEl.textContent.trim();
                            }
                        }
                        
                        results.push({
                            logoUrl: logoUrl,
                            linkUrl: linkUrl,
                            title: title
                        });
                    }
                });
                
                return results;
            }
        """)
        
        print(f"Found {len(logo_data)} logo images with class 'hackathon-thumbnail' or similar")
        
        # Extract full hackathon data from cards
        hackathon_data = await page.evaluate("""
            () => {
                const hackathonData = [];
                
                // Process all cards that might be hackathons
                const processCard = (card) => {
                    let data = {
                        url: '',
                        logo_url: '',
                        title: '',
                        tags: [],
                        prize_amount: '',
                        participants: '',
                        date_range: '' // Add field for date range
                    };
                    
                    // Get any links in the card
                    const links = card.querySelectorAll('a');
                    for (const link of links) {
                        if (link.href && 
                            link.href.includes('devpost.com') && 
                            !link.href.includes('devpost.com/hackathons') &&
                            !link.href.includes('help.devpost.com') && 
                            !link.href.includes('info.devpost.com') &&
                            !link.href.includes('devpost.com/software')) {
                            data.url = link.href;
                            
                            // If the link has text, it might be the title
                            if (link.textContent.trim() && !data.title) {
                                data.title = link.textContent.trim();
                            }
                            
                            break; // Just take the first valid link
                        }
                    }
                    
                    // Get the logo image - specifically target hackathon-thumbnail class
                    const logoImg = card.querySelector('.hackathon-thumbnail, img[src*="challenge_thumbnails"], img[src*="medium_square.png"]');
                    if (logoImg && logoImg.src) {
                        // Ensure we have a full URL by adding https: if it starts with //
                        let logoSrc = logoImg.src;
                        if (logoSrc.startsWith('//')) {
                            logoSrc = 'https:' + logoSrc;
                        }
                        data.logo_url = logoSrc;
                        console.log("Found logo URL: " + logoSrc);
                    } 
                    
                    // Fallback to any image if hackathon-thumbnail is not found
                    if (!data.logo_url) {
                        const anyImg = card.querySelector('img[class*="thumbnail"], img[class*="logo"], img');
                        if (anyImg && anyImg.src) {
                            let logoSrc = anyImg.src;
                            if (logoSrc.startsWith('//')) {
                                logoSrc = 'https:' + logoSrc;
                            }
                            data.logo_url = logoSrc;
                            console.log("Found fallback logo URL: " + logoSrc);
                        }
                    }
                    
                    // Get the title if we don't have it already
                    if (!data.title) {
                        const titleEl = card.querySelector('.challenge-title, h3, .title, h2, [class*="title"]');
                        if (titleEl) {
                            data.title = titleEl.textContent.trim();
                        }
                    }
                    
                    // Get tags
                    const tags = card.querySelectorAll('.theme-label, .label.theme-label, [class*="theme"], [class*="tag"]');
                    if (tags.length > 0) {
                        data.tags = Array.from(tags).map(tag => tag.textContent.trim());
                    }
                    
                    // Get prize amount
                    const prizeEl = card.querySelector('.prize .prize-amount, .prize-amount, [class*="prize"]');
                    if (prizeEl) {
                        data.prize_amount = prizeEl.textContent.trim();
                    }
                    
                    // Get participants count
                    const participantsEl = card.querySelector('.participants, [class*="participants"]');
                    if (participantsEl) {
                        data.participants = participantsEl.textContent.trim();
                    }
                    
                    // Look for date range in the card
                    // First, try to find it in a dedicated date element
                    const dateElements = card.querySelectorAll('.date, [class*="date"], .time, [class*="time"], .duration, [class*="duration"]');
                    for (const dateEl of dateElements) {
                        const text = dateEl.textContent.trim();
                        // Month abbreviated followed by day range and year
                        if (/[A-Za-z]{3}\\s*\\d{1,2}\\s*-\\s*(?:[A-Za-z]{3}\\s*)?\\d{1,2},?\\s*\\d{4}/.test(text)) {
                            data.date_range = text;
                            break;
                        }
                    }
                    
                    // If we still don't have a date range, look in the full card text
                    if (!data.date_range) {
                        const cardText = card.textContent;
                        // Try to match common date patterns - Month day - Month day, year
                        const dateRegex = new RegExp("([A-Za-z]{3}\\s+\\d{1,2}\\s*-\\s*[A-Za-z]{3}\\s+\\d{1,2},?\\s*\\d{4}|[A-Za-z]{3}\\s+\\d{1,2}\\s*-\\s*\\d{1,2},?\\s*\\d{4})");
                        const dateMatches = cardText.match(dateRegex);
                        if (dateMatches) {
                            data.date_range = dateMatches[1];
                        }
                    }
                    
                    // If we have a URL, add this to our results
                    if (data.url) {
                        hackathonData.push(data);
                    }
                };
                
                // Process all potential hackathon cards
                document.querySelectorAll('.challenge-listing, .challenge-card, [class*="challenge"]:not([class*="challenge-empty"])').forEach(processCard);
                
                // Also check for other card types that might be hackathons
                document.querySelectorAll('.card:not(.challenge-listing), [class*="card"]:not(.challenge-listing), .tile, [class*="tile"]').forEach(processCard);
                
                // Process any elements that contain a valid hackathon link
                document.querySelectorAll('a[href*="devpost.com"]:not([href*="devpost.com/hackathons"]):not([href*="help.devpost.com"]):not([href*="info.devpost.com"]):not([href*="devpost.com/software"])').forEach(link => {
                    // Find the parent container
                    const container = link.closest('[class*="card"], [class*="challenge"], [class*="tile"], [class*="listing"]') || link.parentElement;
                    if (container && container.tagName !== 'BODY') {
                        processCard(container);
                    }
                });
                
                return hackathonData;
            }
        """)
        
        # Merge the logo data with the hackathon data
        # Create a map of URLs to logo URLs from logo_data
        logo_map = {}
        for item in logo_data:
            if item.get('linkUrl') and item.get('logoUrl'):
                logo_map[item['linkUrl']] = item['logoUrl']
        
        # Fill in missing logo URLs in hackathon_data from logo_map
        for item in hackathon_data:
            if not item.get('logo_url') and item.get('url') in logo_map:
                item['logo_url'] = logo_map[item['url']]
                print(f"Added logo URL from logo_map: {item['logo_url']}")
        
        # Extract just the URLs for backward compatibility
        links = [item['url'] for item in hackathon_data if item.get('url')]
        
        print(f"Found {len(links)} potential hackathon links with direct DOM targeting")
        
        # Store the additional data keyed by URL for later use
        hackathon_listing_data = {item['url']: item for item in hackathon_data if item.get('url')}
        
        # Add any logos that weren't matched to hackathons yet
        for link_url, logo_url in logo_map.items():
            if link_url not in hackathon_listing_data and link_url:
                hackathon_listing_data[link_url] = {
                    'url': link_url,
                    'logo_url': logo_url
                }
                print(f"Added entry from logo_map: {link_url} -> {logo_url}")
        
        # Check directly in the HTML for "medium_square.png" in img src attributes
        additional_logos = await page.evaluate("""
            () => {
                const results = {};
                const images = document.querySelectorAll('img[src*="medium_square.png"]');
                console.log(`Found ${images.length} images with medium_square.png in their src`);
                
                images.forEach(img => {
                    // Find the nearest link
                    let el = img;
                    while (el && el.tagName !== 'A' && el.parentElement && el.tagName !== 'BODY') {
                        el = el.parentElement;
                    }
                    
                    if (el && el.tagName === 'A' && el.href) {
                        let logoUrl = img.src;
                        if (logoUrl.startsWith('//')) {
                            logoUrl = 'https:' + logoUrl;
                        }
                        results[el.href] = logoUrl;
                    }
                });
                
                return results;
            }
        """)
        
        # Add additional logos found
        for link_url, logo_url in additional_logos.items():
            if link_url in hackathon_listing_data:
                if not hackathon_listing_data[link_url].get('logo_url'):
                    hackathon_listing_data[link_url]['logo_url'] = logo_url
                    print(f"Added missing logo URL from additional_logos: {logo_url}")
            else:
                hackathon_listing_data[link_url] = {
                    'url': link_url,
                    'logo_url': logo_url
                }
                links.append(link_url)
                print(f"Added new entry from additional_logos: {link_url} -> {logo_url}")
        
        # Save the data for later
        with open('hackathon_listing_data.json', 'w') as f:
            json.dump(hackathon_listing_data, f, indent=2)
        
        # If we have few links, try comprehensive search
        if len(links) < 20:
            print("Few links found, trying comprehensive search...")
            
            all_links = await page.evaluate("""
                () => {
                    const allLinks = new Set();
                    
                    // Function to extract links from a collection
                    const extractLinks = (elements, description) => {
                        console.log(`Processing ${elements.length} ${description}`);
                        
                        for (const element of elements) {
                            // Try to find links directly in this element
                            const directLinks = element.querySelectorAll('a');
                            for (const link of directLinks) {
                                if (link.href && 
                                    link.href.includes('devpost.com') && 
                                    !link.href.includes('devpost.com/hackathons') &&
                                    !link.href.includes('help.devpost.com') && 
                                    !link.href.includes('info.devpost.com') &&
                                    !link.href.includes('devpost.com/software')) {
                                    allLinks.add(link.href);
                                }
                            }
                        }
                        
                        return allLinks.size;
                    };
                    
                    // Extract from all potential containers
                    extractLinks(document.querySelectorAll('.challenge-listing'), 'challenge listings');
                    extractLinks(document.querySelectorAll('.card, [class*="card"]'), 'card elements');
                    extractLinks(document.querySelectorAll('[class*="challenge"]'), 'challenge cards');
                    extractLinks(document.querySelectorAll('[class*="hackathon"]'), 'hackathon cards');
                    
                    // Also look for any links that might be hackathon pages
                    const allPageLinks = document.querySelectorAll('a');
                    for (const link of allPageLinks) {
                        if (link.href && 
                            link.href.includes('devpost.com') && 
                            link.href.includes('?ref_feature=challenge') &&
                            !link.href.includes('devpost.com/hackathons') &&
                            !link.href.includes('help.devpost.com') &&
                            !link.href.includes('info.devpost.com') &&
                            !link.href.includes('devpost.com/software')) {
                            allLinks.add(link.href);
                        }
                    }
                    
                    console.log(`Total unique links found: ${allLinks.size}`);
                    return Array.from(allLinks);
                }
            """)
            
            # If we have more links from the comprehensive search, add them
            if len(all_links) > len(links):
                print(f"Adding {len(all_links) - len(links)} additional links from comprehensive search")
                for link in all_links:
                    if link not in links:
                        links.append(link)
                        if link not in hackathon_listing_data:
                            hackathon_listing_data[link] = {'url': link}
        
        # If still no links, extract from image elements specifically
        if len(links) < 20:
            print("Still few links, trying to extract from hackathon images...")
            
            # Extract links from images - hackathon cards often have images
            image_links = await page.evaluate("""
                () => {
                    const results = [];
                    const logoMap = {};
                    
                    // Look for all images that might be inside hackathon cards
                    const images = document.querySelectorAll('img[alt*="hackathon"], img[alt*="challenge"], img[src*="challenge"], img[class*="challenge"], img[src*="thumbnail"], img');
                    console.log("Found " + images.length + " potential hackathon images");
                    
                    for (const img of images) {
                        // Find closest link
                        let el = img;
                        while (el && el.tagName !== 'A' && el.parentElement && el.tagName !== 'BODY') {
                            el = el.parentElement;
                        }
                        
                        if (el && el.tagName === 'A' && el.href && 
                            !el.href.includes('help.devpost.com') && 
                            !el.href.includes('info.devpost.com')) {
                            console.log("Found image link: " + el.href);
                            results.push(el.href);
                            
                            // Store the logo URL for this link
                            let logoUrl = img.src;
                            if (logoUrl.startsWith('//')) {
                                logoUrl = 'https:' + logoUrl;
                            }
                            logoMap[el.href] = logoUrl;
                        }
                    }
                    
                    return {links: [...new Set(results)], logoMap: logoMap};
                }
            """)
            
            # Add image links to our links list
            links.extend(image_links['links'])
            links = list(set(links))  # Remove duplicates
            
            # Add logo URLs to our listing data
            for link_url, logo_url in image_links['logoMap'].items():
                if link_url in hackathon_listing_data:
                    if not hackathon_listing_data[link_url].get('logo_url'):
                        hackathon_listing_data[link_url]['logo_url'] = logo_url
                        print(f"Added missing logo URL from image_links: {logo_url}")
                else:
                    hackathon_listing_data[link_url] = {
                        'url': link_url,
                        'logo_url': logo_url
                    }
                    print(f"Added new entry from image_links: {link_url} -> {logo_url}")
            
            print(f"Found {len(links)} links after image extraction")
        
        # Filter out help/info/documentation pages
        links = [link for link in links if 
                not link.startswith("https://help.devpost.com") and 
                not link.startswith("https://info.devpost.com") and
                not link.startswith("https://support.devpost.com") and
                not link.startswith("https://blog.devpost.com") and
                not link.startswith("https://docs.devpost.com") and
                not link.startswith("https://devpost.com/software") and
                not link.startswith("https://devpost.com/terms") and
                not link.startswith("https://devpost.com/privacy")]
        
        # Update the hackathon_listing_data to match our filtered links
        hackathon_listing_data = {url: data for url, data in hackathon_listing_data.items() if url in links}
        
        # Save the updated data
        with open('hackathon_listing_data.json', 'w') as f:
            json.dump(hackathon_listing_data, f, indent=2)
        
        print(f"Final count after filtering: {len(links)} hackathon links")
        print(f"Saved detailed information for {len(hackathon_listing_data)} hackathons in hackathon_listing_data.json")
        
        # Print summary of logos found
        logos_found = sum(1 for data in hackathon_listing_data.values() if data.get('logo_url'))
        print(f"Found logos for {logos_found} out of {len(hackathon_listing_data)} hackathons ({logos_found/len(hackathon_listing_data)*100:.2f}%)")
        
        return links, hackathon_listing_data
        
    except Exception as e:
        print(f"Error extracting hackathon links: {e}")
        traceback.print_exc()
        await take_screenshot(page, "error_links.png")
        return [], {}

async def extract_hackathon_details(page, url, listing_data=None):
    """Extract detailed information from a hackathon page"""
    try:
        # Navigate to the hackathon page
        print(f"Navigating to {url}")
        
        # Check if we have pre-collected listing data for this URL
        logo_from_listing = None
        date_range_from_listing = None
        dates_from_listing = {"start_date": "", "end_date": "", "date_range": ""}
        
        if listing_data and url in listing_data:
            # Extract listing details including dates
            item_data = listing_data[url]
            logo_from_listing = item_data.get('logo_url')
            
            # Try to extract dates from listing data direct date_range field
            if item_data.get('date_range'):
                date_range_from_listing = item_data.get('date_range')
                print(f"Found date range directly from listing: {date_range_from_listing}")
                dates_from_listing["date_range"] = date_range_from_listing
                
                # Try to split into start/end dates
                parts = re.split(r'\s*[-–—]\s*', date_range_from_listing)
                if len(parts) == 2:
                    start_part, end_part = parts
                    # Add year to start date if missing
                    if not re.search(r'\d{4}', start_part) and re.search(r'\d{4}', end_part):
                        year_match = re.search(r'(\d{4})', end_part)
                        if year_match:
                            dates_from_listing["start_date"] = f"{start_part}, {year_match.group(1)}"
                    else:
                        dates_from_listing["start_date"] = start_part
                        
                    dates_from_listing["end_date"] = end_part
                    print(f"Parsed date range: {dates_from_listing['start_date']} to {dates_from_listing['end_date']}")
            
            # If no direct date range, try to extract from title
            if not dates_from_listing["date_range"] and item_data.get('title'):
                title_text = item_data.get('title', '')
                # Look for date patterns like Mar 15 - Apr 10, 2025
                date_match = re.search(r'([A-Za-z]{3}\s+\d{1,2}\s*[-–—]\s*[A-Za-z]{3}\s+\d{1,2},?\s*\d{4}|[A-Za-z]{3}\s+\d{1,2}\s*[-–—]\s*\d{1,2},?\s*\d{4})', title_text)
                if date_match:
                    dates_from_listing["date_range"] = date_match.group(1)
                    print(f"Found date range in title: {dates_from_listing['date_range']}")
                    
                    # Try to split into start/end dates
                    parts = re.split(r'\s*[-–—]\s*', dates_from_listing["date_range"])
                    if len(parts) == 2:
                        start_part, end_part = parts
                        # Add year to start date if missing
                        if not re.search(r'\d{4}', start_part) and re.search(r'\d{4}', end_part):
                            year_match = re.search(r'(\d{4})', end_part)
                            if year_match:
                                dates_from_listing["start_date"] = f"{start_part}, {year_match.group(1)}"
                        else:
                            dates_from_listing["start_date"] = start_part
                            
                        dates_from_listing["end_date"] = end_part
                        print(f"Parsed date range: {dates_from_listing['start_date']} to {dates_from_listing['end_date']}")
            
            # As a last resort, try to find dates in participants field which often contains them
            if not dates_from_listing["date_range"] and item_data.get('participants'):
                participants_text = item_data.get('participants', '')
                date_match = re.search(r'([A-Za-z]{3}\s+\d{1,2}\s*[-–—]\s*[A-Za-z]{3}\s+\d{1,2},?\s*\d{4}|[A-Za-z]{3}\s+\d{1,2}\s*[-–—]\s*\d{1,2},?\s*\d{4})', participants_text)
                if date_match:
                    dates_from_listing["date_range"] = date_match.group(1)
                    print(f"Found date range in participants field: {dates_from_listing['date_range']}")
                    
                    # Try to split into start/end dates
                    parts = re.split(r'\s*[-–—]\s*', dates_from_listing["date_range"])
                    if len(parts) == 2:
                        start_part, end_part = parts
                        # Add year to start date if missing
                        if not re.search(r'\d{4}', start_part) and re.search(r'\d{4}', end_part):
                            year_match = re.search(r'(\d{4})', end_part)
                            if year_match:
                                dates_from_listing["start_date"] = f"{start_part}, {year_match.group(1)}"
                        else:
                            dates_from_listing["start_date"] = start_part
                            
                        dates_from_listing["end_date"] = end_part
                        print(f"Parsed date range from participants field: {dates_from_listing['start_date']} to {dates_from_listing['end_date']}")
                
            if logo_from_listing:
                print(f"Found logo URL from listing data: {logo_from_listing}")
        
        # Use a longer timeout and wait for more page elements to load
        navigation_success = await goto_with_retry(page, url)
        if not navigation_success:
            print(f"Failed to navigate to {url} after multiple retries")
            # Return basic information we have from listing data
            if listing_data and url in listing_data:
                basic_details = {
                    'title': listing_data[url].get('title', url),
                    'url': url,
                    'logo_url': logo_from_listing or '',
                    'start_date': dates_from_listing.get('start_date', ''),
                    'end_date': dates_from_listing.get('end_date', ''),
                    'tags': listing_data[url].get('tags', []),
                    'mode': 'online',  # Default to online
                    'source_platform': 'devpost'
                }
                print(f"Returning basic details from listing for {url}")
                return basic_details
            return None
            
        await asyncio.sleep(3)  # Wait longer for JavaScript to execute
        
        # Get the hackathon slug for the screenshot filename
        hackathon_slug = url.replace("https://", "").replace("http://", "").split('/')[0].split('.')[0]
        if not hackathon_slug or hackathon_slug == "devpost":
            # Try a different approach to get a meaningful slug
            parts = url.split('/')
            if len(parts) > 3:
                hackathon_slug = parts[3].split('?')[0]
            if not hackathon_slug or hackathon_slug in ["hackathons", "challenges"]:
                hackathon_slug = "unknown"
        
        print(f"Hackathon slug identified as: {hackathon_slug}")
        await take_screenshot(page, f"hackathon_{hackathon_slug}.png")
        
        # Initialize details dictionary with basic info and logo from listing if available
        details = {
            'title': '',
            'description': '',
            'organizer': url.replace("https://", "").replace("http://", "").split('.')[0],  # Use domain as fallback organizer
            'start_date': dates_from_listing.get('start_date', ''),  # Use date from listing if available
            'end_date': dates_from_listing.get('end_date', ''),      # Use date from listing if available
            'location': '',
            'registration_deadline': '',
            'prize_pool': '',
            'url': url,
            'num_participants': '',
            'skills_required': [],
            'time_commitment': '',
            'prizes_details': {},
            'schedule_details': {},
            'mode': 'online',  # Default to online
            'logo_url': logo_from_listing or '',  # Initialize with logo from listing if available
            'banner_url': '',
            'tags': [],
            'status': '',
            'submission_deadline': '',
            'source_platform': 'devpost'
        }
        
        # Get additional listing data if available
        if listing_data and url in listing_data:
            listing_item = listing_data[url]
            
            # Pre-fill any available data from the listing
            if not details['title'] and listing_item.get('title'):
                details['title'] = listing_item['title']
                print(f"Using title from listing: {details['title']}")
                
                # Extract date range from title if we haven't already
                if not dates_from_listing.get('date_range') and not details['start_date'] and not details['end_date']:
                    date_match = re.search(r'([A-Za-z]{3}\s+\d{1,2}\s*-\s*[A-Za-z]{3}\s+\d{1,2},?\s*\d{4}|[A-Za-z]{3}\s+\d{1,2}\s*-\s*\d{1,2},?\s*\d{4})', details['title'])
                    if date_match:
                        date_range = date_match.group(1)
                        parts = re.split(r'\s*-\s*', date_range)
                        if len(parts) == 2:
                            start_part, end_part = parts
                            if not re.search(r'\d{4}', start_part) and re.search(r'\d{4}', end_part):
                                year_match = re.search(r'(\d{4})', end_part)
                                if year_match:
                                    details['start_date'] = f"{start_part}, {year_match.group(1)}"
                            else:
                                details['start_date'] = start_part
                            details['end_date'] = end_part
                            print(f"Extracted date range from title: {details['start_date']} to {details['end_date']}")
                
            if not details['tags'] and listing_item.get('tags'):
                details['tags'] = listing_item['tags']
                print(f"Using tags from listing: {details['tags']}")
                
                # Try to extract dates from tags if needed
                if not details['start_date'] or not details['end_date']:
                    for tag in details['tags']:
                        date_match = re.search(r'([A-Za-z]{3}\s+\d{1,2}\s*-\s*[A-Za-z]{3}\s+\d{1,2},?\s*\d{4}|[A-Za-z]{3}\s+\d{1,2}\s*-\s*\d{1,2},?\s*\d{4})', tag)
                        if date_match:
                            date_range = date_match.group(1)
                            parts = re.split(r'\s*-\s*', date_range)
                            if len(parts) == 2:
                                if not details['start_date']:
                                    details['start_date'] = parts[0]
                                if not details['end_date']:
                                    details['end_date'] = parts[1]
                                print(f"Extracted date range from tag: {details['start_date']} to {details['end_date']}")
                                break
                
            if not details['prize_pool'] and listing_item.get('prize_amount'):
                details['prize_pool'] = listing_item['prize_amount']
                print(f"Using prize from listing: {details['prize_pool']}")
                
            if not details['num_participants'] and listing_item.get('participants'):
                details['num_participants'] = listing_item['participants']
                print(f"Using participants from listing: {details['num_participants']}")
                
            # Also try to extract dates from participants or prize info which often contains the date range
            if (not details['start_date'] or not details['end_date']) and listing_item.get('participants'):
                participant_text = listing_item.get('participants', '')
                date_match = re.search(r'([A-Za-z]{3}\s+\d{1,2}\s*-\s*[A-Za-z]{3}\s+\d{1,2},?\s*\d{4}|[A-Za-z]{3}\s+\d{1,2}\s*-\s*\d{1,2},?\s*\d{4})', participant_text)
                if date_match:
                    date_range = date_match.group(1)
                    parts = re.split(r'\s*-\s*', date_range)
                    if len(parts) == 2:
                        if not details['start_date']:
                            details['start_date'] = parts[0]
                        if not details['end_date']:
                            details['end_date'] = parts[1]
                        print(f"Extracted date range from participant info: {details['start_date']} to {details['end_date']}")
        
        # Wait for the page to fully load - properly using try/except instead of catch
        try:
            await page.wait_for_selector("h1, .title, .header, .banner", timeout=5000, state="visible")
        except Exception as e:
            print(f"Selector wait timed out: {e}")
            # Continue with extraction anyway
        
        # Save HTML content for debugging logo extraction
        html_content = await page.content()
        os.makedirs(f"debug/{hackathon_slug}", exist_ok=True)
        with open(f"debug/{hackathon_slug}/page.html", "w", encoding="utf-8") as f:
            f.write(html_content)
        
        # Extract additional data including logo images - trying multiple specific methods
        logo_extraction_results = await page.evaluate("""
            () => {
                const result = {
                    logos_found: [],
                    medium_square_found: false,
                    selectors_used: []
                };
                
                // First, look specifically for medium_square.png in any image src
                const mediumSquareImgs = document.querySelectorAll('img[src*="medium_square.png"]');
                if (mediumSquareImgs.length > 0) {
                    result.medium_square_found = true;
                    
                    for (const img of mediumSquareImgs) {
                        let logoUrl = img.src;
                        if (logoUrl.startsWith('//')) {
                            logoUrl = 'https:' + logoUrl;
                        }
                        result.logos_found.push({
                            url: logoUrl,
                            source: 'medium_square_pattern'
                        });
                    }
                }
                
                // Try common logo selectors
                const logoSelectors = [
                    '.hackathon-thumbnail',
                    '.logo img', 
                    'header img', 
                    '.challenge-logo img',
                    '.challenge-header img',
                    '.challenge-header .logo img',
                    'img[src*="challenge_thumbnails"]',
                    'img[class*="logo"]',
                    'img[class*="thumbnail"]'
                ];
                
                for (const selector of logoSelectors) {
                    const logos = document.querySelectorAll(selector);
                    if (logos.length > 0) {
                        result.selectors_used.push(selector + ' (' + logos.length + ')');
                        
                        for (const img of logos) {
                            if (img.src) {
                                let logoUrl = img.src;
                                if (logoUrl.startsWith('//')) {
                                    logoUrl = 'https:' + logoUrl;
                                }
                                result.logos_found.push({
                                    url: logoUrl,
                                    source: selector
                                });
                            }
                        }
                    }
                }
                
                // Also look for any images that might be logos in the header or challenge info
                const potentialLogoContainers = [
                    'header',
                    '.challenge-header',
                    '.challenge-info',
                    '.hackathon-header',
                    '.hackathon-info'
                ];
                
                for (const container of potentialLogoContainers) {
                    const containerEl = document.querySelector(container);
                    if (containerEl) {
                        const images = containerEl.querySelectorAll('img:not([src*="banner"]):not([src*="cover"]):not([width="0"]):not([height="0"])');
                        if (images.length > 0) {
                            result.selectors_used.push(container + ' imgs (' + images.length + ')');
                            
                            for (const img of images) {
                                if (img.src) {
                                    let logoUrl = img.src;
                                    if (logoUrl.startsWith('//')) {
                                        logoUrl = 'https:' + logoUrl;
                                    }
                                    result.logos_found.push({
                                        url: logoUrl,
                                        source: 'container: ' + container
                                    });
                                }
                            }
                        }
                    }
                }
                
                return result;
            }
        """)
        
        # Print logo extraction results 
        print(f"Logo extraction results for {hackathon_slug}:")
        print(f"  Medium square pattern found: {logo_extraction_results['medium_square_found']}")
        print(f"  Selectors used: {', '.join(logo_extraction_results['selectors_used'])}")
        print(f"  Total logos found: {len(logo_extraction_results['logos_found'])}")
        
        # Only use page logos if we don't have one from the listing
        if not details['logo_url'] and logo_extraction_results['logos_found']:
            # Prioritize medium_square.png logos
            medium_square_logos = [logo['url'] for logo in logo_extraction_results['logos_found'] 
                                 if 'medium_square.png' in logo['url']]
            
            if medium_square_logos:
                details['logo_url'] = medium_square_logos[0]
                print(f"Using medium_square.png logo from page: {details['logo_url']}")
            else:
                # Use the first logo found
                details['logo_url'] = logo_extraction_results['logos_found'][0]['url']
                print(f"Using first logo from page: {details['logo_url']}")
        
        # Extract additional data including banner image, themes/tags
        additional_data = await page.evaluate("""
            () => {
                const data = {
                    banner_url: '',
                    tags: [],
                    participants_count: '',
                    status: '',
                    submission_deadline: '',
                    location: '',
                    organizer: '',
                    prize_pool: ''
                };
                
                // Extract banner image from header
                const bannerImg = document.querySelector('.header-image img, [class*="banner"] img, [class*="cover"] img');
                if (bannerImg && bannerImg.src) {
                    data.banner_url = bannerImg.src;
                }
                
                // Extract tags
                const tags = document.querySelectorAll('.theme-label, .label.theme-label');
                if (tags.length > 0) {
                    data.tags = Array.from(tags).map(tag => tag.textContent.trim());
                }
                
                // Extract location - Look specifically for map-marker icon
                const locationElement = document.querySelector('.info-with-icon .fa-map-marker-alt, .info-with-icon .fas.fa-map-marker-alt');
                if (locationElement) {
                    const locationInfo = locationElement.closest('.info-with-icon');
                    if (locationInfo) {
                        const locationText = locationInfo.querySelector('.info')?.textContent.trim();
                        if (locationText) {
                            data.location = locationText;
                        }
                    }
                    
                    // Try to get the full address from the link
                    const mapLink = locationElement.closest('.info-with-icon').querySelector('a[href*="maps.google.com"]');
                    if (mapLink && mapLink.getAttribute('href')) {
                        const mapUrl = mapLink.getAttribute('href');
                        // Extract the address from the map URL query parameter
                        const addressMatch = mapUrl.match(/\\?q=([^&]+)/);
                        if (addressMatch && addressMatch[1]) {
                            data.location = decodeURIComponent(addressMatch[1]);
                        }
                    }
                }
                
                // Extract participants count more directly
                const participantsEl = document.querySelector('td.nowrap > strong, .participants strong');
                if (participantsEl) {
                    data.participants_count = participantsEl.textContent.trim();
                }
                
                // Extract prize amount more directly
                const prizeEl = document.querySelector('a.prizes-link strong span[data-currency-value], [data-currency="true"] [data-currency-value]');
                if (prizeEl) {
                    data.prize_pool = prizeEl.textContent.trim();
                    
                    // Check if there's a currency symbol
                    const currencyEl = prizeEl.closest('[data-currency="true"]');
                    if (currencyEl) {
                        const fullPrize = currencyEl.textContent.trim();
                        data.prize_pool = fullPrize;
                    }
                }
                
                // Extract organizer
                const organizerEl = document.querySelector('.host-label, .info-with-icon .fa-flag, .info-with-icon .fas.fa-flag');
                if (organizerEl) {
                    const orgInfo = organizerEl.closest('.info-with-icon');
                    if (orgInfo) {
                        const orgText = orgInfo.querySelector('.info span')?.textContent.trim();
                        if (orgText) {
                            data.organizer = orgText;
                        }
                    }
                }
                
                // Extract status
                const statusEl = document.querySelector('.hackathon-status .status-label, [class*="status"], .cp-tag.status-label');
                if (statusEl) {
                    data.status = statusEl.textContent.trim();
                }
                
                // Extract exact deadline date with timezone - FIX: capture the full month string
                const exactDateEl = document.querySelector('[data-dates-text], [data-date-info-tag]');
                if (exactDateEl) {
                    const dateText = exactDateEl.textContent.trim();
                    if (dateText) {
                        // Try to extract the date and timezone
                        const dateMatch = dateText.match(/Deadline.*?(\\w+ \\d{1,2}, \\d{4} @ \\d{1,2}:\\d{2}[ap]m) ([A-Z0-9+-:]+)/i);
                        if (dateMatch && dateMatch[1]) {
                            data.submission_deadline = dateMatch[1] + ' ' + (dateMatch[2] || '');
                        }
                    }
                }
                
                // If we still don't have a submission deadline, try other methods
                if (!data.submission_deadline) {
                    // Look for elements that might contain a deadline
                    const deadlineElements = document.querySelectorAll('.deadline, [class*="deadline"], .end-date, [class*="end-date"]');
                    for (const el of deadlineElements) {
                        const text = el.textContent.trim();
                        if (text && (text.toLowerCase().includes('deadline') || text.toLowerCase().includes('submit by') || text.toLowerCase().includes('end'))) {
                            const fullMatch = text.match(/(\\w+ \\d{1,2},? \\d{4}\\s*@?\\s*\\d{1,2}:\\d{2}[ap]m)/i);
                            if (fullMatch) {
                                data.submission_deadline = fullMatch[1];
                                break;
                            }
                        }
                    }
                }
                
                return data;
            }
        """)
        
        # Update details with additional data
        if additional_data['banner_url']:
            details['banner_url'] = additional_data['banner_url']
            print(f"Found banner URL from page: {details['banner_url']}")
            
        if additional_data['tags']:
            details['tags'] = additional_data['tags']
        if additional_data['participants_count']:
            details['num_participants'] = additional_data['participants_count']
        if additional_data['status']:
            details['status'] = additional_data['status']
        if additional_data['submission_deadline']:
            details['submission_deadline'] = additional_data['submission_deadline']
        if additional_data['location']:
            details['location'] = additional_data['location']
            
            # Update mode based on location text
            location_lower = additional_data['location'].lower()
            if 'online' in location_lower or 'virtual' in location_lower:
                details['mode'] = 'online'
            elif 'hybrid' in location_lower:
                details['mode'] = 'hybrid'
            else:
                details['mode'] = 'offline'  # If we have a physical location with no online mention
                
            print(f"Found location: {details['location']} (Mode: {details['mode']})")
            
        if additional_data['organizer']:
            details['organizer'] = additional_data['organizer']
            print(f"Found organizer: {details['organizer']}")
            
        if additional_data['prize_pool']:
            details['prize_pool'] = additional_data['prize_pool']
            print(f"Found prize pool: {details['prize_pool']}")
        
        # Extract title/name with a more reliable approach targeting specific elements
        name = await page.evaluate("""
            () => {
                // Try multiple selectors for the hackathon name
                const titleSelectors = [
                    'h1.challenge-title', 
                    'h1.title',
                    '.challenge-header h1', 
                    '.challenge-header .title',
                    '.banner h1',
                    '.banner .title',
                    'header h1',
                    '.header h1',
                    'h1'
                ];
                
                for (const selector of titleSelectors) {
                    const el = document.querySelector(selector);
                    if (el && el.textContent.trim()) {
                        return el.textContent.trim();
                    }
                }
                
                // Try the meta title
                const metaTitle = document.querySelector('meta[property="og:title"]');
                if (metaTitle && metaTitle.getAttribute('content')) {
                    return metaTitle.getAttribute('content');
                }
                
                // Fallback to page title
                return document.title.split('|')[0].trim();
            }
        """)
        
        if name and name.lower() != "devpost":
            details['title'] = name
            print(f"Found title: {name}")
        else:
            # Try harder to find a real title
            title_from_url = hackathon_slug.replace('-', ' ').replace('_', ' ').title()
            details['title'] = title_from_url
            print(f"Using title from URL: {title_from_url}")
        
        # Extract description - target specific content areas
        description = await page.evaluate("""
            () => {
                const descriptionSelectors = [
                    '.challenge-description', 
                    '.description', 
                    '.about',
                    '#challenge-description',
                    '#challenge-about',
                    'section.content p'
                ];
                
                for (const selector of descriptionSelectors) {
                    const el = document.querySelector(selector);
                    if (el && el.textContent.trim()) {
                        return el.textContent.trim().substring(0, 500); // Limit length
                    }
                }
                
                // Try meta description
                const metaDesc = document.querySelector('meta[name="description"]');
                if (metaDesc && metaDesc.getAttribute('content')) {
                    return metaDesc.getAttribute('content');
                }
                
                return '';
            }
        """)
        
        if description:
            details['description'] = description
            print(f"Found description: {description[:50]}...")
        
        # Fix for escape sequence in JavaScript regex
        dates = await page.evaluate("""
            () => {
                const result = {
                    start_date: '',
                    end_date: '',
                    registration_deadline: ''
                };
                
                // Try to find specific date containers
                const dateContainers = document.querySelectorAll('.dates, .timeline, .schedule, .important-dates');
                for (const container of dateContainers) {
                    const text = container.textContent.toLowerCase();
                    
                    // Look for specific patterns
                    if (text.includes('start') && text.includes('end')) {
                        // Parse the text to extract dates
                        const dateTexts = container.textContent.split(/[\\n\\r]+/).filter(t => t.trim());
                        
                        for (const line of dateTexts) {
                            if (line.toLowerCase().includes('start')) {
                                const dateMatch = line.match(/(\\w+ \\d{1,2},? \\d{4}|\\d{1,2}[\\/\\-]\\d{1,2}[\\/\\-]\\d{2,4})/);
                                if (dateMatch) result.start_date = dateMatch[0];
                            }
                            else if (line.toLowerCase().includes('end')) {
                                const dateMatch = line.match(/(\\w+ \\d{1,2},? \\d{4}|\\d{1,2}[\\/\\-]\\d{1,2}[\\/\\-]\\d{2,4})/);
                                if (dateMatch) result.end_date = dateMatch[0];
                            }
                            else if (line.toLowerCase().includes('deadline') || line.toLowerCase().includes('register by')) {
                                const dateMatch = line.match(/(\\w+ \\d{1,2},? \\d{4}|\\d{1,2}[\\/\\-]\\d{1,2}[\\/\\-]\\d{2,4})/);
                                if (dateMatch) result.registration_deadline = dateMatch[0];
                            }
                        }
                    }
                }
                
                return result;
            }
        """)
        
        # Update details with extracted dates
        if dates['start_date'] and not details['start_date']:
            details['start_date'] = dates['start_date']
        if dates['end_date'] and not details['end_date']:
            details['end_date'] = dates['end_date']
        if dates['registration_deadline'] and not details['registration_deadline']:
            details['registration_deadline'] = dates['registration_deadline']
        
        print(f"Extracted dates: {details['start_date']} to {details['end_date']}")
        
        # If we still don't have some of the required fields, use fallbacks
        # Only use current date as fallback if we have no date from any source
        if not details['start_date']:
            details['start_date'] = datetime.now().strftime("%B %d, %Y")
            print(f"Using current date as fallback for start_date: {details['start_date']}")
        
        if not details['end_date']:
            # If we have a start date, set end date to 30 days after start date
            if details['start_date']:
                try:
                    # Parse the start date
                    start_date_dt = None
                    for fmt in ["%B %d, %Y", "%b %d, %Y", "%b %d %Y", "%Y-%m-%d"]:
                        try:
                            start_date_dt = datetime.strptime(details['start_date'], fmt)
                            break
                        except ValueError:
                            continue
                    
                    if start_date_dt:
                        future_date = start_date_dt + timedelta(days=30)
                        details['end_date'] = future_date.strftime("%B %d, %Y")
                        print(f"Setting end_date to 30 days after start_date: {details['end_date']}")
                    else:
                        # If parsing failed, use current date + 30 days
            future_date = datetime.now() + timedelta(days=30)
            details['end_date'] = future_date.strftime("%B %d, %Y")
                        print(f"Using current date + 30 days as fallback for end_date: {details['end_date']}")
                except Exception as e:
                    # If date parsing fails, use simple fallback
                    future_date = datetime.now() + timedelta(days=30)
                    details['end_date'] = future_date.strftime("%B %d, %Y")
                    print(f"Error parsing start date, using current date + 30 days as fallback: {details['end_date']}")
            else:
                future_date = datetime.now() + timedelta(days=30)
                details['end_date'] = future_date.strftime("%B %d, %Y")
                print(f"Using current date + 30 days as fallback for end_date: {details['end_date']}")
        
        # Double-check that we're using logo from listing if available - 
        # this ensures we prioritize the logo from the listing page as mentioned in the prompt
        try:
            with open('hackathon_listing_data.json', 'r') as f:
                all_listing_data = json.load(f)
                
            # If this URL is in our listing data, prioritize logo from there
            if url in all_listing_data and all_listing_data[url].get('logo_url'):
                details['logo_url'] = all_listing_data[url]['logo_url']
                print(f"Final check: Using logo URL from listing data: {details['logo_url']}")
        except:
            # If we can't load the listing data, just continue with what we have
            pass
        
        # Add back the final extraction results with all the new fields
        print(f"Final extraction results for {hackathon_slug}:")
        print(f"  Title: {details['title']}")
        print(f"  Dates: {details['start_date']} to {details['end_date']}")
        print(f"  Mode: {details['mode']}")
        print(f"  Organizer: {details.get('organizer', 'Unknown')}")
        print(f"  Banner URL: {details.get('banner_url', 'Not found')}")
        print(f"  Logo URL: {details.get('logo_url', 'Not found')}")
        print(f"  Tags: {', '.join(details.get('tags', []))}")
        print(f"  Participants: {details.get('num_participants', 'Not found')}")
        print(f"  Prize Pool: {details.get('prize_pool', 'Not found')}")
        print(f"  Status: {details.get('status', 'Not found')}")
        print(f"  Submission Deadline: {details.get('submission_deadline', 'Not found')}")
        
        # If we still don't have dates, try pattern matching on the whole page content
        if not details['start_date'] or not details['end_date']:
            # Common date patterns
            date_patterns = [
                r'(\d{1,2}/\d{1,2}/\d{2,4})\s*(?:to|-)\s*(\d{1,2}/\d{1,2}/\d{2,4})',
                r'([A-Za-z]+ \d{1,2},? \d{4})\s*(?:to|-)\s*([A-Za-z]+ \d{1,2},? \d{4})',
                r'(\d{1,2} [A-Za-z]+ \d{4})\s*(?:to|-)\s*(\d{1,2} [A-Za-z]+ \d{4})',
                r'([A-Za-z]{3} \d{1,2})\s*[-–—]\s*([A-Za-z]{3} \d{1,2}, \d{4})',
                r'([A-Za-z]{3} \d{1,2})\s*[-–—]\s*(\d{1,2}, \d{4})'
            ]
            
            # Get page content
            page_content = await page.evaluate("document.body.innerText")
            
            # Look for date patterns in the page text
            for pattern in date_patterns:
                matches = re.findall(pattern, page_content)
                if matches:
                    # Only use if we don't already have start/end dates
                    if not details['start_date']:
                        details['start_date'] = matches[0][0]
                    if not details['end_date']:
                        details['end_date'] = matches[0][1]
                    print(f"Found dates with pattern matching: {details['start_date']} to {details['end_date']}")
                    break
        
        # Final check before returning results - Fix for submission_deadline truncation
        if details.get('submission_deadline') and len(details['submission_deadline']) > 0:
            # Check if the submission deadline starts with just a letter that might be a truncated month
            if re.match(r'^[a-zA-Z] \d', details['submission_deadline']):
                # Find what month it should be based on the first letter
                month_letter = details['submission_deadline'][0].lower()
                possible_months = {
                    'j': ['January', 'June', 'July'],
                    'f': ['February'],
                    'm': ['March', 'May'],
                    'a': ['April', 'August'],
                    's': ['September'],
                    'o': ['October'],
                    'n': ['November'],
                    'd': ['December']
                }
                
                # Try to determine the month from context (end date or status)
                probable_month = None
                if month_letter in possible_months:
                    if month_letter == 'a':
                        # Differentiate April vs August
                        if details.get('end_date') and 'apr' in details['end_date'].lower():
                            probable_month = 'April'
                        elif details.get('end_date') and 'aug' in details['end_date'].lower():
                            probable_month = 'August'
                    elif month_letter == 'm':
                        # Differentiate March vs May
                        if details.get('end_date') and 'mar' in details['end_date'].lower():
                            probable_month = 'March'
                        elif details.get('end_date') and 'may' in details['end_date'].lower():
                            probable_month = 'May'
                    elif month_letter == 'j':
                        # Differentiate Jan vs Jun vs Jul
                        if details.get('end_date') and 'jan' in details['end_date'].lower():
                            probable_month = 'January'
                        elif details.get('end_date') and 'jun' in details['end_date'].lower():
                            probable_month = 'June'
                        elif details.get('end_date') and 'jul' in details['end_date'].lower():
                            probable_month = 'July'
                    else:
                        # For other letters, just take the first possible month
                        probable_month = possible_months[month_letter][0]
                
                if probable_month:
                    # Replace the single letter with the full month name
                    details['submission_deadline'] = probable_month + details['submission_deadline'][1:]
                    print(f"Fixed truncated month in submission deadline: {details['submission_deadline']}")
            
            # Also fix any 2-letter month abbreviations (like Ma for May)
            elif re.match(r'^[a-zA-Z]{2} \d', details['submission_deadline']):
                month_abbr = details['submission_deadline'][:2].lower()
                month_mapping = {
                    'ja': 'January',
                    'fe': 'February',
                    'ma': 'March',
                    'ap': 'April',
                    'ma': 'May',
                    'ju': 'June',
                    'ju': 'July',
                    'au': 'August',
                    'se': 'September',
                    'oc': 'October',
                    'no': 'November',
                    'de': 'December'
                }
                
                # Try to determine which month based on context
                if month_abbr in month_mapping:
                    if month_abbr == 'ma':
                        # Disambiguate March vs May
                        if details.get('end_date') and 'march' in details['end_date'].lower():
                            details['submission_deadline'] = 'March' + details['submission_deadline'][2:]
                        elif details.get('end_date') and 'may' in details['end_date'].lower():
                            details['submission_deadline'] = 'May' + details['submission_deadline'][2:]
                    elif month_abbr == 'ju':
                        # Disambiguate June vs July
                        if details.get('end_date') and 'june' in details['end_date'].lower():
                            details['submission_deadline'] = 'June' + details['submission_deadline'][2:]
                        elif details.get('end_date') and 'july' in details['end_date'].lower():
                            details['submission_deadline'] = 'July' + details['submission_deadline'][2:]
                    else:
                        details['submission_deadline'] = month_mapping[month_abbr] + details['submission_deadline'][2:]
                
                print(f"Fixed abbreviated month in submission deadline: {details['submission_deadline']}")
        
        return details
        
    except Exception as e:
        print(f"Error extracting hackathon details: {e}")
        traceback.print_exc()
        await take_screenshot(page, f"error_{hackathon_slug}.png")
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
    
    # Write to CSV
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=sorted(fieldnames))
        writer.writeheader()
        writer.writerows(cleaned_hackathons)
    
    print(f"Saved {len(hackathons)} hackathons to '{filename}'")

async def goto_with_retry(page, url, max_retries=MAX_RETRIES):
    """Navigate to a URL with retries on timeout or other common errors"""
    for attempt in range(max_retries):
        try:
            print(f"Navigation attempt {attempt+1}/{max_retries} to {url}")
            # Add a random delay between retries to avoid rate limiting
            if attempt > 0:
                delay = random.uniform(2, 5) * attempt
                print(f"Waiting {delay:.1f} seconds before retry...")
                await asyncio.sleep(delay)
            
            # Try different wait_until strategies on subsequent attempts
            if attempt == 0:
                await page.goto(url, wait_until="domcontentloaded", timeout=90000)
            elif attempt == 1:
                await page.goto(url, wait_until="load", timeout=90000)
            else:
                await page.goto(url, timeout=120000)  # No wait_until on last attempt, longer timeout
            
            # Successfully navigated
            print(f"Successfully navigated to {url}")
            return True
        except PlaywrightError as e:
            print(f"Error navigating to {url} (attempt {attempt+1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                print(f"Failed all {max_retries} attempts to navigate to {url}")
                return False
            
            # Try to recover the page if it's in a bad state
            try:
                await page.reload()
            except:
                pass

async def crawl_devpost_hackathons():
    """Main function to crawl hackathons from Devpost"""
    print("Starting Devpost Hackathon Crawler...")
    
    # Starting URL to target specific page
    start_url = f"{BASE_URL}{DEFAULT_PARAMS}&page={START_PAGE}"
    print(f"Target URL: {start_url}")
    if PROCESS_SINGLE_PAGE:
        print(f"Processing ONLY page {START_PAGE}")
    else:
        print(f"Looking for up to {MAX_HACKATHONS} hackathons")
        print(f"Starting from page {START_PAGE}")
    
    async with async_playwright() as p:
        # Launch browser in headless mode for better performance
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={'width': 1280, 'height': 800},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        
        # Set default timeout
        context.set_default_timeout(90000)  # 90 seconds
        
        page = await context.new_page()
        all_hackathon_links = []
        hackathon_listing_data = {}
        
        # Initialize pages list early to prevent UnboundLocalError
        pages = [page]
        
        try:
            # Process only the specified page
            if PROCESS_SINGLE_PAGE:
                page_url = f"{BASE_URL}{DEFAULT_PARAMS}&page={START_PAGE}"
                print(f"\nProcessing page {START_PAGE}: {page_url}")
                
                # Navigate to the page with retry mechanism
                if not await goto_with_retry(page, page_url):
                    raise Exception(f"Failed to load page {page_url} after multiple attempts")
                
                await asyncio.sleep(5)  # Wait longer for initial load
                
                # Take screenshot on initial load
                await take_screenshot(page, f"page_{START_PAGE}_initial.png")
                
                # AGGRESSIVE SCROLLING: Devpost uses lazy loading, so we need to scroll a lot
                print(f"Starting aggressive scrolling on page {START_PAGE}...")
                
                # Get initial height
                initial_height = await page.evaluate("document.body.scrollHeight")
                print(f"Initial page height: {initial_height}px")
                
                # Scroll multiple times with pauses to allow content to load
                prev_height = initial_height
                same_height_count = 0
                
                for i in range(MAX_SCROLL_ATTEMPTS):
                    # Scroll down by a larger amount
                    await page.evaluate(f"window.scrollBy(0, 1200)")
                    
                    # Small pause to let content load
                    await asyncio.sleep(SCROLL_PAUSE_TIME)
                    
                    # Check if page height has changed
                    current_height = await page.evaluate("document.body.scrollHeight")
                    
                    # If height hasn't changed for 3 consecutive attempts, we might have reached the bottom
                    if current_height == prev_height:
                        same_height_count += 1
                        if same_height_count >= 3:
                            print(f"Page height unchanged for 3 scrolls, likely reached the bottom after {i+1} scrolls")
                            break
                    else:
                        same_height_count = 0  # Reset counter
                    
                    prev_height = current_height
                    
                    # Every 5 scrolls, take a screenshot and do a longer pause
                    if i % 5 == 0:
                        print(f"Completed {i+1} scrolls")
                        print(f"Current page height: {current_height}px")
                        await take_screenshot(page, f"page_{START_PAGE}_scroll_{i+1}.png")
                        await asyncio.sleep(2)  # Longer pause every 5 scrolls
                
                # Final scroll to the bottom to ensure all content is loaded
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(FINAL_PAUSE_TIME)  # Wait longer for final load

                # Additional random scrolls to trigger any remaining lazy-loaded content
                for _ in range(3):
                    random_scroll = await page.evaluate("Math.floor(Math.random() * document.body.scrollHeight * 0.8)")
                    await page.evaluate(f"window.scrollTo(0, {random_scroll})")
                    await asyncio.sleep(1)
                
                # Final scroll back to top and then to bottom again
                await page.evaluate("window.scrollTo(0, 0)")
                await asyncio.sleep(1)
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(3)
                
                # Extract hackathon links from this page
                print(f"Extracting hackathon links from page {START_PAGE}...")
                page_links, page_data = await extract_hackathon_links(page)
                
                # Print detailed results
                if page_links:
                    print(f"\n==== RESULTS FROM PAGE {START_PAGE} ====")
                    print(f"Found {len(page_links)} hackathon links on page {START_PAGE}")
                    for i, link in enumerate(page_links):
                        data = page_data.get(link, {})
                        logo = data.get('logo_url', 'No logo')
                        title = data.get('title', 'No title')
                        print(f"{i+1}. {title} - {link}")
                        print(f"   Logo: {logo}")
                        if data.get('tags'):
                            print(f"   Tags: {', '.join(data.get('tags', []))}")
                        if data.get('prize_amount'):
                            print(f"   Prize: {data.get('prize_amount')}")
                        if data.get('participants'):
                            print(f"   Participants: {data.get('participants')}")
                        print()
                    
                    all_hackathon_links = page_links
                    hackathon_listing_data = page_data
                else:
                    print(f"No hackathon links found on page {START_PAGE}")
            else:
                # Process multiple pages starting from START_PAGE
                current_page = START_PAGE
                end_page = min(current_page + MAX_PAGES - 1, 10)  # Limit to 10 pages maximum
                
                while current_page <= end_page and len(all_hackathon_links) < MAX_HACKATHONS:
                    # Construct the page URL with proper pagination
                    page_url = f"{BASE_URL}{DEFAULT_PARAMS}&page={current_page}"
                    print(f"\nProcessing page {current_page} of {end_page}: {page_url}")
                    
                    # Navigate to the page with retry mechanism
                    if not await goto_with_retry(page, page_url):
                        raise Exception(f"Failed to load page {page_url} after multiple attempts")
                    
                    await asyncio.sleep(5)  # Wait longer for initial load
                    
                    # Take screenshot on initial load
                    await take_screenshot(page, f"page_{current_page}_initial.png")
                    
                    # AGGRESSIVE SCROLLING: Devpost uses lazy loading, so we need to scroll a lot
                    print(f"Starting aggressive scrolling on page {current_page}...")
                    
                    # Get initial height
                    initial_height = await page.evaluate("document.body.scrollHeight")
                    print(f"Initial page height: {initial_height}px")
                    
                    # Scroll multiple times with pauses to allow content to load
                    prev_height = initial_height
                    same_height_count = 0
                    
                    for i in range(MAX_SCROLL_ATTEMPTS):
                        # Scroll down by a larger amount
                        await page.evaluate(f"window.scrollBy(0, 1200)")
                        
                        # Small pause to let content load
                        await asyncio.sleep(SCROLL_PAUSE_TIME)
                        
                        # Check if page height has changed
                        current_height = await page.evaluate("document.body.scrollHeight")
                        
                        # If height hasn't changed for 3 consecutive attempts, we might have reached the bottom
                        if current_height == prev_height:
                            same_height_count += 1
                            if same_height_count >= 3:
                                print(f"Page height unchanged for 3 scrolls, likely reached the bottom after {i+1} scrolls")
                                break
                        else:
                            same_height_count = 0  # Reset counter
                        
                        prev_height = current_height
                        
                        # Every 5 scrolls, take a screenshot and do a longer pause
                        if i % 5 == 0:
                            print(f"Completed {i+1} scrolls")
                            print(f"Current page height: {current_height}px")
                            await take_screenshot(page, f"page_{current_page}_scroll_{i+1}.png")
                            await asyncio.sleep(2)  # Longer pause every 5 scrolls
                    
                    # Take final screenshot after scrolling
                    await take_screenshot(page, f"page_{current_page}_after_scrolling.png")
                    
                    # Extract hackathon links from this page
                    print(f"Extracting hackathon links from page {current_page}...")
                    page_links, page_data = await extract_hackathon_links(page)
                    
                    print(f"Found {len(page_links)} hackathon links on page {current_page}")
                    
                    if page_links:
                        # Add these links to our master list
                        all_hackathon_links.extend(page_links)
                        print(f"Total links collected so far: {len(all_hackathon_links)}")
                        hackathon_listing_data.update(page_data)
                    else:
                        print(f"No links found on page {current_page}, might be at the end of available hackathons")
                        break  # Exit the loop if no links found
                    
                    # Check if we've reached the maximum hackathons limit
                    if len(all_hackathon_links) >= MAX_HACKATHONS:
                        print(f"Reached the maximum number of hackathons ({MAX_HACKATHONS}), stopping pagination")
                        break
                    
                    # Move to the next page
                    current_page += 1
            
            # Limit to MAX_HACKATHONS
            all_hackathon_links = all_hackathon_links[:MAX_HACKATHONS]
            print(f"\nProcessing {len(all_hackathon_links)} hackathon links")
            
            # Crawl each hackathon page (using concurrency)
            all_hackathons = []
            max_concurrent = 3  # Process 3 hackathons at once
            
            # Create additional pages for concurrency
            for i in range(max_concurrent - 1):  # -1 because we already have one page
                pages.append(await context.new_page())
            
            # Process hackathons in batches
            for i in range(0, len(all_hackathon_links), max_concurrent):
                batch = all_hackathon_links[i:i+max_concurrent]
                tasks = []
                
                for j, url in enumerate(batch):
                    print(f"Processing hackathon {i+j+1}/{len(all_hackathon_links)}: {url}")
                    # Pass the hackathon_listing_data to extract_hackathon_details
                    tasks.append(extract_hackathon_details(pages[j], url, hackathon_listing_data))
                
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
                # Filter out non-hackathon entries and deduplicate
                filtered_hackathons = []
                seen_urls = set()
                seen_titles = set()
                
                # System pages to exclude
                system_page_patterns = [
                    r"secure\.devpost\.com/users/login",
                    r"secure\.devpost\.com/users/register",
                    r"devpost\.com/settings",
                    r"devpost\.com/portfolio",
                    r"devpost\.com/users/register",
                    r"devpost\.com$",  # Main Devpost homepage
                    r"devpost\.com/$",  # Main Devpost homepage with trailing slash
                    r"devpost\.com/users",
                    r"devpost\.com/software",
                    r"devpost\.com/jobs",
                    r"devpost\.com/challenges",
                    r"devpost\.com/auth",
                    r"devpost\.com/api",
                    r"devpost\.com/account",
                    r"devpost\.com/profile",
                    r"devpost\.com/faq",
                    r"devpost\.com/terms"
                ]
                
                # System titles to exclude
                system_titles = [
                    "Log in to Devpost", 
                    "Join Devpost", 
                    "Settings", 
                    "Your projects", 
                    "Your hackathons", 
                    "The home for hackathons",
                    "Match my eligibility",
                    "Featured",
                    "Devpost",
                    "Login",
                    "Sign up",
                    "Register",
                    "Profile",
                    "FAQ",
                    "Terms of Service",
                    "Privacy Policy"
                ]
                
                # Helper function to normalize title for fuzzy matching
                def normalize_title(title):
                    """Normalize a title for better comparison by removing common words and symbols"""
                    # Convert to lowercase
                    title = title.lower()
                    # Remove special characters and replace with spaces
                    title = re.sub(r'[^\w\s]', ' ', title)
                    # Remove common words that don't help with matching
                    common_words = ['hackathon', 'challenge', 'competition', 'the', 'a', 'an', 'and', 'at', 'in', 'on', 'by', 'for', 'with', 'edition']
                    for word in common_words:
                        title = re.sub(r'\b' + word + r'\b', '', title)
                    # Remove extra spaces
                    title = re.sub(r'\s+', ' ', title).strip()
                    return title
                
                # Helper function to check title similarity
                def title_similarity(title1, title2):
                    """Calculate similarity between two titles"""
                    # Don't compare very short titles (less than 10 chars) to avoid false positives
                    if len(title1) < 10 or len(title2) < 10:
                        return False
                        
                    # Normalize both titles
                    norm1 = normalize_title(title1)
                    norm2 = normalize_title(title2)
                    
                    # Don't compare normalized titles that are too short
                    if len(norm1) < 5 or len(norm2) < 5:
                        return False
                    
                    # Direct containment is a strong signal
                    if norm1 in norm2 or norm2 in norm1:
                        return True
                    
                    # Extract and compare words
                    words1 = set(norm1.split())
                    words2 = set(norm2.split())
                    
                    # Both should have some meaningful content
                    if len(words1) < 2 or len(words2) < 2:
                        return False
                        
                    # Calculate word overlap
                    intersection = words1.intersection(words2)
                    
                    # We need enough matching words
                    if len(intersection) < 2:
                        return False
                        
                    # Check if the intersection is a significant portion of the smaller set
                    similarity_ratio = len(intersection) / min(len(words1), len(words2))
                    
                    # More words in common = higher confidence
                    if len(intersection) >= 3 and similarity_ratio >= 0.5:
                        return True
                        
                    # With just 2 words in common, we need a higher ratio
                    if len(intersection) == 2 and similarity_ratio >= 0.6:
                        return True
                    
                    # Special case for unique identifiers that strongly indicate the same event
                    unique_identifiers = ['2025', 'techkriti', 'dawson', 'illuminati', 'agentforce', 'phystech']
                    shared_identifiers = [word for word in intersection if any(id in word for id in unique_identifiers)]
                    
                    if len(shared_identifiers) >= 1 and similarity_ratio >= 0.4:
                        return True
                    
                    return False
                
                for hackathon in all_hackathons:
                    url = hackathon.get("url", "")
                    title = hackathon.get("title", "")
                    
                    # Skip system pages
                    if any(re.search(pattern, url) for pattern in system_page_patterns):
                        print(f"Skipping system page by URL: {title} - {url}")
                        continue
                        
                    # Skip entries with generic/system titles
                    if any(title == system_title or title.startswith(system_title) for system_title in system_titles):
                        print(f"Skipping system page by title: {title}")
                        continue
                        
                    # Skip entries without proper dates or descriptions
                    if not hackathon.get("start_date") or not hackathon.get("end_date"):
                        print(f"Skipping incomplete entry: {title}")
                        continue
                    
                    # Detect if URL has ref=challenge parameter, which is typical for real hackathon pages
                    is_likely_hackathon = "?ref_feature=challenge" in url or "?ref=challenge" in url or "?ref_medium=discover" in url
                    
                    # If URL doesn't have challenge parameter, apply more scrutiny
                    if not is_likely_hackathon:
                        # Require both title and description to be non-empty
                        if not title or not hackathon.get("description"):
                            print(f"Skipping likely non-hackathon page: {title} - {url}")
                            continue
                        
                        # Skip URLs that are definitely not hackathon detail pages
                        if "/p/" not in url and not re.search(r'\.devpost\.com', url):
                            print(f"Skipping non-detail page: {title} - {url}")
                            continue
                        
                    # Additional checks to ensure this is a real hackathon page:
                    
                    # 1. Check for minimum data completeness
                    if not hackathon.get("tags") and not hackathon.get("organizer") and not hackathon.get("prize_pool"):
                        # If missing ALL these fields, likely not a real hackathon
                        print(f"Skipping entry missing key data: {title}")
                        continue
                            
                    # 2. Check for reasonable date ranges (hackathons typically don't last more than 6 months)
                    try:
                        start_date = datetime.strptime(hackathon["start_date"], "%B %d, %Y")
                        end_date = datetime.strptime(hackathon["end_date"], "%B %d, %Y")
                        date_diff = (end_date - start_date).days
                        
                        # If hackathon lasts more than 180 days, it's suspicious
                        if date_diff > 180:
                            print(f"Skipping entry with suspicious date range ({date_diff} days): {title}")
                            continue
                    except:
                        # Don't skip if date parsing fails, as this could be a formatting issue
                        pass
                    
                    # Deduplicate by URL (but allow for URL variants of the same hackathon)
                    url_base = re.sub(r'\?.*$', '', url)  # Remove query parameters
                    if url_base in seen_urls:
                        print(f"Skipping duplicate by URL: {title}")
                        continue
                    seen_urls.add(url_base)
                    
                    # Skip empty or very short titles
                    if not title or len(title) < 5:
                        continue
                    
                    # Additional deduplication by title
                    if title in seen_titles:
                        print(f"Skipping duplicate by title: {title}")
                        continue
                    
                    # Fuzzy title matching for near-duplicates
                    is_duplicate = False
                    for seen_title in seen_titles:
                        # Skip very short titles for comparison
                        if len(seen_title) < 5:
                            continue
                            
                        # Use our improved similarity function
                        if title_similarity(title, seen_title):
                            # Double check by comparing organizers to confirm it's really a duplicate
                            if (hackathon.get("organizer") and any(h.get("organizer") == hackathon.get("organizer") for h in filtered_hackathons if h.get("title") == seen_title)):
                                print(f"Skipping fuzzy duplicate: '{title}' matches '{seen_title}'")
                                is_duplicate = True
                                break
                    
                    if is_duplicate:
                        continue
                        
                    seen_titles.add(title)
                    
                    # If we got here, the hackathon passes all filters
                    filtered_hackathons.append(hackathon)
                
                print(f"Filtered out {len(all_hackathons) - len(filtered_hackathons)} non-hackathon or duplicate entries")
                print(f"Remaining hackathons: {len(filtered_hackathons)}")
                
                # Replace original list with filtered list
                all_hackathons = filtered_hackathons
                
                # Generate timestamp for the filename
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                page_info = f"page{START_PAGE}_only" if PROCESS_SINGLE_PAGE else "multipages"
                filename = f"devpost_hackathons_{page_info}_{timestamp}.csv"
                
                # Save to CSV
                save_hackathons_to_csv(all_hackathons, filename)
                
                # Also save to pandas DataFrame for additional processing if needed
                df = pd.DataFrame(all_hackathons)
                df.to_csv(f"devpost_hackathons_{page_info}_{timestamp}_df.csv", index=False)
                
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
            await take_screenshot(page, "error_state.png")
            
        finally:
            # Close browser
            for p in pages[1:]:  # Close additional pages
                await p.close()
            await browser.close()

if __name__ == "__main__":
    asyncio.run(crawl_devpost_hackathons())
