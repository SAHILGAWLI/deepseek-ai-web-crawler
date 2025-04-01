import asyncio
import csv
import json
import os
import time
import re
from datetime import datetime
from typing import List, Dict, Any
from playwright.async_api import async_playwright, Error as PlaywrightError
import pandas as pd
from dotenv import load_dotenv
import traceback
import cloudinary
import cloudinary.uploader
import aiohttp

# Load environment variables
load_dotenv()

# Cloudinary Configuration
CLOUDINARY_CLOUD_NAME = os.getenv("CLOUDINARY_CLOUD_NAME")
CLOUDINARY_API_KEY = os.getenv("CLOUDINARY_API_KEY")
CLOUDINARY_API_SECRET = os.getenv("CLOUDINARY_API_SECRET")

# Configure Cloudinary if credentials are available
if CLOUDINARY_CLOUD_NAME and CLOUDINARY_API_KEY and CLOUDINARY_API_SECRET:
    cloudinary.config(
        cloud_name=CLOUDINARY_CLOUD_NAME,
        api_key=CLOUDINARY_API_KEY,
        api_secret=CLOUDINARY_API_SECRET
    )
    CLOUDINARY_ENABLED = True
    print("Cloudinary configuration loaded successfully.")
else:
    CLOUDINARY_ENABLED = False
    print("Warning: Cloudinary credentials not found. Images will not be uploaded to Cloudinary.")

# Configuration
BASE_URL = "https://www.kaggle.com/competitions"
DEFAULT_PARAMS = "?listOption=active"
OUTPUT_CSV = f"kaggle_competitions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
REQUIRED_FIELDS = ["title", "start_date", "end_date", "prize_pool", "logo_url"]
MAX_COMPETITIONS = 100
DEBUG_MODE = True
MAX_RETRIES = 3
MIN_RATE_LIMIT_DELAY = 1
MAX_RATE_LIMIT_DELAY = 3
MAX_PAGES = 10  # Maximum number of pages to check

# Directory for screenshots and debug info
os.makedirs("screenshots", exist_ok=True)
os.makedirs("debug", exist_ok=True)

# Add delay between requests to avoid rate limiting
async def smart_wait(min_delay=MIN_RATE_LIMIT_DELAY, max_delay=MAX_RATE_LIMIT_DELAY):
    """Wait for a random amount of time to avoid rate limiting."""
    import random
    delay = random.uniform(min_delay, max_delay)
    print(f"Waiting for {delay:.2f} seconds to avoid rate limiting...")
    await asyncio.sleep(delay)

async def upload_image_to_cloudinary(image_url, competition_id, image_type):
    """Upload an image to Cloudinary and return the secure URL.
    
    Args:
        image_url (str): The URL of the image to upload
        competition_id (str): The ID of the competition
        image_type (str): The type of image (logo, banner, etc.)
        
    Returns:
        str: The Cloudinary URL if successful, otherwise the original URL
    """
    if not CLOUDINARY_ENABLED or not image_url:
        return image_url
    
    try:
        print(f"Uploading {image_type} for {competition_id} to Cloudinary...")
        
        # Create a unique public_id based on competition and image type
        public_id = f"kaggle_{image_type}_{competition_id}"
        
        # Remove any query parameters from URL for more reliable uploads
        clean_url = image_url.split('?')[0]
        
        # Upload using Cloudinary's upload API
        result = cloudinary.uploader.upload(
            clean_url,
            public_id=public_id,
            folder="kaggle_images",
            overwrite=True,
            resource_type="auto"
        )
        
        # Return the secure URL
        cloudinary_url = result['secure_url']
        print(f"Uploaded {image_type} to Cloudinary: {cloudinary_url}")
        return cloudinary_url
    
    except Exception as e:
        print(f"Error uploading {image_type} to Cloudinary: {e}")
        # Try a second approach with aiohttp for images that might need special handling
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(image_url) as response:
                    if response.status == 200:
                        # Create a temporary file
                        temp_file = f"temp_{competition_id}_{image_type}.jpg"
                        with open(temp_file, 'wb') as f:
                            f.write(await response.read())
                        
                        # Upload the file
                        result = cloudinary.uploader.upload(
                            temp_file,
                            public_id=public_id,
                            folder="kaggle_images",
                            overwrite=True,
                            resource_type="auto"
                        )
                        
                        # Clean up temp file
                        if os.path.exists(temp_file):
                            os.remove(temp_file)
                            
                        cloudinary_url = result['secure_url']
                        print(f"Uploaded {image_type} to Cloudinary using alternative method: {cloudinary_url}")
                        return cloudinary_url
        except Exception as inner_e:
            print(f"Alternative upload method also failed: {inner_e}")
        
        return image_url  # Return original URL as fallback

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

async def extract_competition_links(page):
    """Extract links to individual competition pages from the listing page."""
    print(f"Extracting competition links from: {page.url}")
    
    # Take a screenshot for debugging
    if DEBUG_MODE:
        await take_screenshot(page, "competition_listing.png")
        
        # Also save the HTML for debugging
        html_content = await page.content()
        with open("debug/competition_listing.html", "w", encoding="utf-8") as f:
            f.write(html_content)
        print("Saved HTML content for debugging")
        
        # Extract and save a detailed analysis of the page structure for debugging
        structure_analysis = await page.evaluate("""() => {
            // Check for the specific structure mentioned in the user example
            const specialStructure = document.querySelectorAll('div[class*="sc-"]');
            const imgElements = document.querySelectorAll('img');
            
            // Sample a few competition cards for detailed analysis
            const competitionLinks = document.querySelectorAll('a[href*="/competitions/"]');
            
            const samples = [];
            // Take the first 3 links as samples
            for (let i = 0; i < Math.min(3, competitionLinks.length); i++) {
                const link = competitionLinks[i];
                samples.push({
                    href: link.getAttribute('href'),
                    outerHTML: link.outerHTML.slice(0, 1000), // Limit size
                    // Get any images inside
                    images: Array.from(link.querySelectorAll('img')).map(img => ({
                        src: img.src,
                        class: img.className,
                        parentClass: img.parentElement ? img.parentElement.className : 'none'
                    }))
                });
            }
            
            return {
                totalImgs: imgElements.length,
                specialStructureCount: specialStructure.length,
                imgElementsSample: Array.from(imgElements).slice(0, 5).map(img => ({
                    src: img.src,
                    class: img.className,
                    parentInfo: img.parentElement ? {
                        tagName: img.parentElement.tagName,
                        className: img.parentElement.className
                    } : 'none'
                })),
                competitionLinksSample: samples
            };
        }""");
        
        # Save the structure analysis
        with open("debug/page_structure_analysis.json", "w", encoding="utf-8") as f:
            json.dump(structure_analysis, f, indent=2)
        print("Saved page structure analysis for debugging")
    
    try:
        # Wait for the page to load completely
        await page.wait_for_load_state("networkidle", timeout=30000)
        
        # Try to identify the structure of the page first
        print("Analyzing page structure...")
        
        # Check for different potential selectors in order of specificity
        selectors = [
            '.competitions-list-view .competition-item',
            '.competitions-list .competition-item',
            '.competitionsContainer .competition-item',
            '.competitions .competition-item',
            '.competition-item',
            '.competition-card',
            'a[href*="/competitions/"]',
            '.card, .card-item'
        ]
        
        used_selector = None
        for selector in selectors:
            try:
                count = await page.evaluate(f'document.querySelectorAll("{selector}").length')
                if count > 0:
                    print(f"Found {count} elements with selector: {selector}")
                    used_selector = selector
                    break
            except Exception as e:
                print(f"Error checking selector '{selector}': {e}")
                continue
        
        if not used_selector:
            print("Could not find competition elements with known selectors.")
            print("Attempting to extract any links to competition pages...")
            
            # Extract all links that look like competition links
            competition_links = await page.evaluate("""() => {
                const links = Array.from(document.querySelectorAll('a[href*="/c/"], a[href*="/competitions/"]'))
                    .filter(a => {
                        const href = a.getAttribute('href');
                        // Filter out links to the competition listing page itself
                        return href && 
                               !href.includes('/competitions?') && 
                               !href.includes('/competitions/list') &&
                               !href.includes('listOption=');
                    })
                    .map(a => {
                        // Ensure full URLs
                        let href = a.getAttribute('href');
                        if (href.startsWith('/')) {
                            href = 'https://www.kaggle.com' + href;
                        }
                        return href;
                    });
                
                // Remove duplicates
                return [...new Set(links)];
            }""")
            
            print(f"Found {len(competition_links)} competition links by URL pattern")
            
            # Scan DOM for the EXACT structure mentioned by the user for logo extraction
            exact_pattern_logo_results = await page.evaluate("""() => {
                // Map to store competition URLs and their logos
                const logoMap = {};
                const debugInfo = {
                    sc_divs_count: 0,
                    sc_divs_with_img: 0,
                    logos_found: 0,
                    html_samples: []
                };
                
                // First find all divs that match the sc-* pattern
                const scDivs = document.querySelectorAll('div[class*="sc-"]');
                debugInfo.sc_divs_count = scDivs.length;
                
                // Look specifically for the pattern <div class="sc-kJZLhT jvoaUf"><img src="..."></div>
                scDivs.forEach(div => {
                    const imgElement = div.querySelector('img');
                    if (imgElement && imgElement.src) {
                        debugInfo.sc_divs_with_img++;
                        
                        // Sample some HTML for debugging
                        if (debugInfo.html_samples.length < 5) {
                            debugInfo.html_samples.push({
                                divClass: div.className,
                                imgSrc: imgElement.src,
                                outerHTML: div.outerHTML.substring(0, 200) // Limit size
                            });
                        }
                        
                        // Find the closest parent that is a link to a competition
                        let current = div;
                        let found = false;
                        
                        // Look up to 5 levels up
                        for (let i = 0; i < 5 && current && !found; i++) {
                            // Check if the current element is an anchor to a competition
                            if (current.tagName === 'A' && 
                                current.href && 
                                (current.href.includes('/competitions/') || current.href.includes('/c/'))) {
                                
                                let href = current.href;
                                // Store the logo URL
                                logoMap[href] = imgElement.src;
                                debugInfo.logos_found++;
                                found = true;
                                break;
                            }
                            
                            // Move up to parent
                            current = current.parentElement;
                        }
                    }
                });
                
                return {
                    logoMap,
                    debugInfo
                };
            }""");
            
            print(f"Exact pattern search results: {exact_pattern_logo_results['debugInfo']}")
            if exact_pattern_logo_results['debugInfo']['logos_found'] > 0:
                print(f"Found {exact_pattern_logo_results['debugInfo']['logos_found']} logos with exact pattern!")
                # Print some samples for debugging
                for i, sample in enumerate(exact_pattern_logo_results['debugInfo']['html_samples']):
                    print(f"Sample {i+1}: {sample['divClass']} -> {sample['imgSrc'][:60]}...")
            
            # Try an entirely different approach: full DOM scan for logos
            scan_results = await page.evaluate("""() => {
                // Create a map to store competition slugs and their logos
                const competitionLogoMap = {};
                
                // Function to extract the competition slug from a URL
                function getCompetitionSlugFromUrl(url) {
                    if (!url) return null;
                    const match = url.match(/\\/competitions\\/([a-zA-Z0-9-_]+)/);
                    return match ? match[1] : null;
                }
                
                // 1. First scan: Find all images and check if they're near competition links
                const allImages = document.querySelectorAll('img');
                
                for (const img of allImages) {
                    // Skip if no src
                    if (!img.src) continue;
                    
                    // Skip very small images (less than 20px)
                    if (img.width < 20 || img.height < 20) continue;
                    
                    // Look for competition links nearby - traverse up
                    let current = img.parentElement;
                    let foundCompetitionSlug = null;
                    
                    // Check up to 5 levels up
                    for (let i = 0; i < 5 && current; i++) {
                        // If we find an anchor with competition URL
                        const anchors = current.querySelectorAll('a[href*="/competitions/"]');
                        for (const anchor of anchors) {
                            const slug = getCompetitionSlugFromUrl(anchor.href);
                            if (slug) {
                                foundCompetitionSlug = slug;
                                break;
                            }
                        }
                        
                        // Also check the element itself if it's an anchor
                        if (current.tagName === 'A' && current.href && current.href.includes('/competitions/')) {
                            const slug = getCompetitionSlugFromUrl(current.href);
                            if (slug) {
                                foundCompetitionSlug = slug;
                                break;
                            }
                        }
                        
                        if (foundCompetitionSlug) break;
                        current = current.parentElement;
                    }
                    
                    if (foundCompetitionSlug) {
                        // Check if it looks like a logo
                        const isLikelyLogo = 
                            img.src.includes('logo') || 
                            img.src.includes('thumb') || 
                            img.src.includes('icon') ||
                            img.src.includes('badges') ||
                            (img.width <= 100 && img.height <= 100) ||
                            img.width === img.height; // Square images are often logos
                        
                        if (isLikelyLogo) {
                            competitionLogoMap[foundCompetitionSlug] = img.src;
                        }
                    }
                }
                
                // 2. Second scan: Look specifically for cards and then any images inside them
                const possibleCards = document.querySelectorAll('div[class*="card"], div[class*="item"], div[class*="container"]');
                
                for (const card of possibleCards) {
                    // Check if the card contains a competition link
                    const competitionLinks = card.querySelectorAll('a[href*="/competitions/"]');
                    if (competitionLinks.length === 0) continue;
                    
                    // Get the competition slug
                    const href = competitionLinks[0].href;
                    const slug = getCompetitionSlugFromUrl(href);
                    if (!slug) continue;
                    
                    // Already found a logo for this competition
                    if (competitionLogoMap[slug]) continue;
                    
                    // Look for images in this card
                    const images = card.querySelectorAll('img');
                    if (images.length > 0) {
                        // Prioritize images that look like logos
                        let logoFound = false;
                        
                        for (const img of images) {
                            if (!img.src) continue;
                            
                            const isLikelyLogo = 
                                img.src.includes('logo') || 
                                img.src.includes('thumb') || 
                                img.src.includes('icon') ||
                                img.src.includes('badges') ||
                                (img.width <= 100 && img.height <= 100) ||
                                img.width === img.height; // Square images are often logos
                            
                            if (isLikelyLogo) {
                                competitionLogoMap[slug] = img.src;
                                logoFound = true;
                                break;
                            }
                        }
                        
                        // If no logo-like image was found, use the first image
                        if (!logoFound && images[0].src) {
                            competitionLogoMap[slug] = images[0].src;
                        }
                    }
                }
                
                // Convert from slug-based map to URL-based map
                const result = {};
                for (const [slug, logoUrl] of Object.entries(competitionLogoMap)) {
                    const fullUrl = `https://www.kaggle.com/competitions/${slug}`;
                    result[fullUrl] = logoUrl;
                }
                
                return {
                    logoMap: result,
                    stats: {
                        totalLogosFound: Object.keys(result).length
                    }
                };
            }""")
            
            print(f"Full DOM scan found logos for {scan_results['stats']['totalLogosFound']} competitions")
            
            # Extract logo images from the first approach
            logo_images = await page.evaluate("""() => {
                // For direct mapping from URLs to logos
                const logoMap = {};
                
                // Find all competition card elements or containers
                const competitionCards = document.querySelectorAll('a[href*="/competitions/"]');
                
                competitionCards.forEach(card => {
                    try {
                        // Get the URL (key)
                        let href = card.getAttribute('href');
                        if (href.startsWith('/')) {
                            href = 'https://www.kaggle.com' + href;
                        }
                        
                        // APPROACH 1: Look for the specific structure mentioned by the user
                        // <div class="sc-kJZLhT jvoaUf"><img src="...">
                        const logoWrapper = card.querySelector('div[class*="sc-"]');
                        if (logoWrapper) {
                            const img = logoWrapper.querySelector('img');
                            if (img && img.src) {
                                // Ensure absolute URL
                                let imgSrc = img.src;
                                if (imgSrc.startsWith('/')) {
                                    imgSrc = 'https://www.kaggle.com' + imgSrc;
                                }
                                logoMap[href] = imgSrc;
                                return; // Found logo, skip other approaches
                            }
                        }
                        
                        // APPROACH 2: Look for any nested img elements with classes that might be logos
                        const logoImgs = card.querySelectorAll('img[class*="sc-"]');
                        if (logoImgs.length > 0 && logoImgs[0].src) {
                            // Ensure absolute URL
                            let imgSrc = logoImgs[0].src;
                            if (imgSrc.startsWith('/')) {
                                imgSrc = 'https://www.kaggle.com' + imgSrc;
                            }
                            logoMap[href] = imgSrc;
                            return; // Found logo, skip other approaches
                        }
                        
                        // APPROACH 3: Look for any image that might be a logo (thumb, small size, etc.)
                        const imgs = card.querySelectorAll('img');
                        for (const img of imgs) {
                            if (img.src) {
                                // Check for logo indicators in the src URL
                                if (img.src.includes('logo') || 
                                    img.src.includes('thumb') || 
                                    img.src.includes('icon') || 
                                    (img.width > 0 && img.width < 100) ||  // Small images are likely logos
                                    (img.height > 0 && img.height < 100)) {
                                    // Ensure absolute URL
                                    let imgSrc = img.src;
                                    if (imgSrc.startsWith('/')) {
                                        imgSrc = 'https://www.kaggle.com' + imgSrc;
                                    }
                                    logoMap[href] = imgSrc;
                                    break;
                                }
                            }
                        }
                        
                        // APPROACH 4: Fall back to any image if nothing else worked
                        if (!logoMap[href] && imgs.length > 0 && imgs[0].src) {
                            // Ensure absolute URL
                            let imgSrc = imgs[0].src;
                            if (imgSrc.startsWith('/')) {
                                imgSrc = 'https://www.kaggle.com' + imgSrc;
                            }
                            logoMap[href] = imgSrc;
                        }
                    } catch (e) {
                        console.error('Error extracting logo for card:', e);
                    }
                });
                
                // Log the results for debugging
                console.log('Found logos for', Object.keys(logoMap).length, 'competitions');
                return logoMap;
            }""")
            
            # Combine all the approaches to extract logos
            combined_logo_map = {}
            
            # 1. First add the logos from the exact pattern search
            if exact_pattern_logo_results['logoMap']:
                for url, logo in exact_pattern_logo_results['logoMap'].items():
                    # Ensure full URL
                    if url.startswith('/'):
                        url = "https://www.kaggle.com" + url
                    combined_logo_map[url] = logo
                
                print(f"Added {len(exact_pattern_logo_results['logoMap'])} logos from exact pattern matching")
            
            # 2. Add logos from the DOM scan
            if scan_results['logoMap']:
                for url, logo in scan_results['logoMap'].items():
                    if url not in combined_logo_map:
                        combined_logo_map[url] = logo
                
                print(f"Added {len(scan_results['logoMap'])} logos from DOM scanning")
            
            # 3. Add logos from the direct approach
            if logo_images:
                for url, logo in logo_images.items():
                    if url not in combined_logo_map:
                        combined_logo_map[url] = logo
                
                print(f"Added {len(logo_images)} logos from direct approach")
            
            # 4. Direct scan for storage.googleapis.com URLs which contain Kaggle logo patterns
            storage_google_results = await page.evaluate("""() => {
                const logoMap = {};
                const imgElements = document.querySelectorAll('img[src*="storage.googleapis.com"][src*="logos"], img[src*="storage.googleapis.com"][src*="thumb"], img[src*="thumb76_76.png"]');
                
                // For each matching image, find the competition link it's associated with
                imgElements.forEach(img => {
                    // Skip if no src
                    if (!img.src) return;
                    
                    // Make sure we use the direct URL without any modifications
                    const imgSrc = img.src;
                    
                    // Find nearest competition link - go up the DOM tree
                    let current = img.parentElement;
                    let foundCompetitionLink = null;
                    
                    // Check up to 5 levels up
                    for (let i = 0; i < 5 && current; i++) {
                        if (current.tagName === 'A' && 
                            current.href && 
                            (current.href.includes('/competitions/') || current.href.includes('/c/'))) {
                            
                            foundCompetitionLink = current.href;
                            break;
                        }
                        
                        // Also check for any child links
                        const childLinks = current.querySelectorAll('a[href*="/competitions/"]');
                        if (childLinks.length > 0) {
                            foundCompetitionLink = childLinks[0].href;
                            break;
                        }
                        
                        current = current.parentElement;
                    }
                    
                    // If we found a competition link, save the image URL
                    if (foundCompetitionLink) {
                        // Make sure the competition link is absolute
                        if (foundCompetitionLink.startsWith('/')) {
                            foundCompetitionLink = 'https://www.kaggle.com' + foundCompetitionLink;
                        }
                        logoMap[foundCompetitionLink] = imgSrc;
                        console.log('Found logo URL:', imgSrc);
                    }
                });
                
                return {
                    logoMap,
                    count: Object.keys(logoMap).length
                };
            }""");
            
            print(f"Found {storage_google_results['count']} logos from storage.googleapis.com URLs")
            
            # Add these to the combined map
            for url, logo in storage_google_results['logoMap'].items():
                if url not in combined_logo_map:
                    combined_logo_map[url] = logo
            
            # Create minimal placeholder data for these links
            competition_data = []
            for link in competition_links:
                logo_url = combined_logo_map.get(link, "")
                if logo_url:
                    print(f"Found logo for {link.split('/')[-1]}: {logo_url[:60]}...")
                
                competition_data.append({
                    "url": link,
                    "title": link.split('/').pop(),  # Use the last part of the URL as a title placeholder
                    "logo_url": logo_url  # Add logo URL if found
                })
            
            return competition_links, competition_data
        
        # Scroll to load all competitions 
        previous_height = 0
        current_height = await page.evaluate("document.body.scrollHeight")
        scroll_attempts = 0
        max_scroll_attempts = 20
        
        while previous_height != current_height and scroll_attempts < max_scroll_attempts:
            previous_height = current_height
            
            # Scroll to the bottom of the page
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            print(f"Scroll attempt {scroll_attempts+1}: Previous height: {previous_height}, scrolling down...")
            
            # Wait for potential new content to load
            await asyncio.sleep(3)
            
            # Check the new height
            current_height = await page.evaluate("document.body.scrollHeight")
            print(f"New height: {current_height}")
            
            scroll_attempts += 1
        
        # Take a screenshot after scrolling
        if DEBUG_MODE:
            await take_screenshot(page, "after_scrolling.png")
        
        # Enhanced logo extraction based on the provided HTML structure
        # Extract competition links and basic information using the identified selector
        competition_data = await page.evaluate(f"""(selector) => {{
            const competitions = [];
            
            // Select all competition card elements
            const competitionCards = document.querySelectorAll(selector);
            console.log('Found ' + competitionCards.length + ' competition cards');
            
            // Extract data from each card
            competitionCards.forEach(card => {{
                // Try multiple ways to get the title
                let title = '';
                let titleElement = card.querySelector('.competition-name, .card-title, h3, h4');
                if (titleElement) {{
                    title = titleElement.textContent.trim();
                }}
                
                // Try multiple ways to get the link
                let url = '';
                let linkElement = card.querySelector('a[href*="/c/"], a[href*="/competitions/"]');
                if (linkElement) {{
                    url = linkElement.href;
                }} else if (card.tagName === 'A') {{
                    url = card.href;
                }}
                
                // Try to get description
                let description = '';
                let descElement = card.querySelector('.competition-description, .description, p');
                if (descElement) {{
                    description = descElement.textContent.trim();
                }}
                
                // Try to get tags
                let tags = [];
                let tagsElement = card.querySelector('.competition-tags, .tags');
                if (tagsElement) {{
                    tags = Array.from(tagsElement.querySelectorAll('.badge, .tag')).map(tag => tag.textContent.trim());
                }}
                
                // Try to get deadline
                let deadline = '';
                let deadlineElement = card.querySelector('.competition-deadline, .deadline, [data-deadline]');
                if (deadlineElement) {{
                    deadline = deadlineElement.textContent.trim();
                }} else if (card.querySelector('[data-deadline]')) {{
                    deadline = card.querySelector('[data-deadline]').getAttribute('data-deadline');
                }}
                
                // Try to get prize info
                let prize = '';
                let prizeElement = card.querySelector('.prizes-awarded, .prize, [data-prize]');
                if (prizeElement) {{
                    prize = prizeElement.textContent.trim();
                }}
                
                // Try to get team info
                let teams = '';
                let teamElement = card.querySelector('.team-count, .teams');
                if (teamElement) {{
                    teams = teamElement.textContent.trim();
                }}
                
                // Try to get organizer
                let organizer = '';
                let organizerElement = card.querySelector('.competition-organizer, .organizer');
                if (organizerElement) {{
                    organizer = organizerElement.textContent.trim();
                }}
                
                // Extract logo image from the listing page - NEW CODE
                let image_url = '';
                
                // First check for the specific structure provided in the example
                let logoWrapper = card.querySelector('.sc-kJZLhT, [class*="sc-"]');
                if (logoWrapper) {{
                    let logoImg = logoWrapper.querySelector('img');
                    if (logoImg && logoImg.src) {{
                        image_url = logoImg.src;
                    }}
                }}
                
                // Fallback to other potential selectors if the specific one doesn't work
                if (!image_url) {{
                    let imageElement = card.querySelector('img[src*="logos"], img[src*="thumb"], .competition-logo img, img.competition-image');
                    if (imageElement && imageElement.src) {{
                        image_url = imageElement.src;
                    }}
                }}
                
                // Generic fallback for any image
                if (!image_url) {{
                    let imageElement = card.querySelector('img');
                    if (imageElement && imageElement.src) {{
                        image_url = imageElement.src;
                    }}
                }}
                
                // Only add if we have at least a URL or title
                if (url || title) {{
                    competitions.push({{
                        title: title,
                        url: url,
                        description: description,
                        tags: tags,
                        deadline: deadline,
                        prize: prize,
                        teams: teams,
                        organizer: organizer,
                        logo_url: image_url  // Store the logo URL
                    }});
                }}
            }});
            
            return competitions;
        }}""", used_selector)
        
        # Log the results
        print(f"Found {len(competition_data)} competitions")
        
        # Get just the URLs for simplified return
        competition_links = [comp["url"] for comp in competition_data if comp.get("url")]
        
        # If no competition links found, try a more direct approach
        if not competition_links:
            print("No competition links found with primary selectors, trying fallback extraction...")
            
            # Extract all links that look like competition links
            competition_links = await page.evaluate("""() => {
                const links = Array.from(document.querySelectorAll('a[href*="/c/"], a[href*="/competitions/"]'))
                    .filter(a => {
                        const href = a.getAttribute('href');
                        // Filter out links to the competition listing page itself
                        return href && 
                               !href.includes('/competitions?') && 
                               !href.includes('/competitions/list') &&
                               !href.includes('listOption=');
                    })
                    .map(a => {
                        // Ensure full URLs
                        let href = a.getAttribute('href');
                        if (href.startsWith('/')) {
                            href = 'https://www.kaggle.com' + href;
                        }
                        return href;
                    });
                
                // Remove duplicates
                return [...new Set(links)];
            }""")
            
            print(f"Found {len(competition_links)} competition links by URL pattern")
            
            # Try an entirely different approach: full DOM scan for logos
            scan_results = await page.evaluate("""() => {
                // Create a map to store competition slugs and their logos
                const competitionLogoMap = {};
                
                // Function to extract the competition slug from a URL
                function getCompetitionSlugFromUrl(url) {
                    if (!url) return null;
                    const match = url.match(/\\/competitions\\/([a-zA-Z0-9-_]+)/);
                    return match ? match[1] : null;
                }
                
                // 1. First scan: Find all images and check if they're near competition links
                const allImages = document.querySelectorAll('img');
                
                for (const img of allImages) {
                    // Skip if no src
                    if (!img.src) continue;
                    
                    // Skip very small images (less than 20px)
                    if (img.width < 20 || img.height < 20) continue;
                    
                    // Look for competition links nearby - traverse up
                    let current = img.parentElement;
                    let foundCompetitionSlug = null;
                    
                    // Check up to 5 levels up
                    for (let i = 0; i < 5 && current; i++) {
                        // If we find an anchor with competition URL
                        const anchors = current.querySelectorAll('a[href*="/competitions/"]');
                        for (const anchor of anchors) {
                            const slug = getCompetitionSlugFromUrl(anchor.href);
                            if (slug) {
                                foundCompetitionSlug = slug;
                                break;
                            }
                        }
                        
                        // Also check the element itself if it's an anchor
                        if (current.tagName === 'A' && current.href && current.href.includes('/competitions/')) {
                            const slug = getCompetitionSlugFromUrl(current.href);
                            if (slug) {
                                foundCompetitionSlug = slug;
                                break;
                            }
                        }
                        
                        if (foundCompetitionSlug) break;
                        current = current.parentElement;
                    }
                    
                    if (foundCompetitionSlug) {
                        // Check if it looks like a logo
                        const isLikelyLogo = 
                            img.src.includes('logo') || 
                            img.src.includes('thumb') || 
                            img.src.includes('icon') ||
                            img.src.includes('badges') ||
                            (img.width <= 100 && img.height <= 100) ||
                            img.width === img.height; // Square images are often logos
                        
                        if (isLikelyLogo) {
                            competitionLogoMap[foundCompetitionSlug] = img.src;
                        }
                    }
                }
                
                // 2. Second scan: Look specifically for cards and then any images inside them
                const possibleCards = document.querySelectorAll('div[class*="card"], div[class*="item"], div[class*="container"]');
                
                for (const card of possibleCards) {
                    // Check if the card contains a competition link
                    const competitionLinks = card.querySelectorAll('a[href*="/competitions/"]');
                    if (competitionLinks.length === 0) continue;
                    
                    // Get the competition slug
                    const href = competitionLinks[0].href;
                    const slug = getCompetitionSlugFromUrl(href);
                    if (!slug) continue;
                    
                    // Already found a logo for this competition
                    if (competitionLogoMap[slug]) continue;
                    
                    // Look for images in this card
                    const images = card.querySelectorAll('img');
                    if (images.length > 0) {
                        // Prioritize images that look like logos
                        let logoFound = false;
                        
                        for (const img of images) {
                            if (!img.src) continue;
                            
                            const isLikelyLogo = 
                                img.src.includes('logo') || 
                                img.src.includes('thumb') || 
                                img.src.includes('icon') ||
                                img.src.includes('badges') ||
                                (img.width <= 100 && img.height <= 100) ||
                                img.width === img.height; // Square images are often logos
                            
                            if (isLikelyLogo) {
                                competitionLogoMap[slug] = img.src;
                                logoFound = true;
                                break;
                            }
                        }
                        
                        // If no logo-like image was found, use the first image
                        if (!logoFound && images[0].src) {
                            competitionLogoMap[slug] = images[0].src;
                        }
                    }
                }
                
                // Convert from slug-based map to URL-based map
                const result = {};
                for (const [slug, logoUrl] of Object.entries(competitionLogoMap)) {
                    const fullUrl = `https://www.kaggle.com/competitions/${slug}`;
                    result[fullUrl] = logoUrl;
                }
                
                return {
                    logoMap: result,
                    stats: {
                        totalLogosFound: Object.keys(result).length
                    }
                };
            }""")
            
            print(f"Full DOM scan found logos for {scan_results['stats']['totalLogosFound']} competitions")
            
            # Extract logo images from the first approach
            logo_images = await page.evaluate("""() => {
                // For direct mapping from URLs to logos
                const logoMap = {};
                
                // Find all competition card elements or containers
                const competitionCards = document.querySelectorAll('a[href*="/competitions/"]');
                
                competitionCards.forEach(card => {
                    try {
                        // Get the URL (key)
                        let href = card.getAttribute('href');
                        if (href.startsWith('/')) {
                            href = 'https://www.kaggle.com' + href;
                        }
                        
                        // APPROACH 1: Look for the specific structure mentioned by the user
                        // <div class="sc-kJZLhT jvoaUf"><img src="...">
                        const logoWrapper = card.querySelector('div[class*="sc-"]');
                        if (logoWrapper) {
                            const img = logoWrapper.querySelector('img');
                            if (img && img.src) {
                                // Ensure absolute URL
                                let imgSrc = img.src;
                                if (imgSrc.startsWith('/')) {
                                    imgSrc = 'https://www.kaggle.com' + imgSrc;
                                }
                                logoMap[href] = imgSrc;
                                return; // Found logo, skip other approaches
                            }
                        }
                        
                        // APPROACH 2: Look for any nested img elements with classes that might be logos
                        const logoImgs = card.querySelectorAll('img[class*="sc-"]');
                        if (logoImgs.length > 0 && logoImgs[0].src) {
                            // Ensure absolute URL
                            let imgSrc = logoImgs[0].src;
                            if (imgSrc.startsWith('/')) {
                                imgSrc = 'https://www.kaggle.com' + imgSrc;
                            }
                            logoMap[href] = imgSrc;
                            return; // Found logo, skip other approaches
                        }
                        
                        // APPROACH 3: Look for any image that might be a logo (thumb, small size, etc.)
                        const imgs = card.querySelectorAll('img');
                        for (const img of imgs) {
                            if (img.src) {
                                // Check for logo indicators in the src URL
                                if (img.src.includes('logo') || 
                                    img.src.includes('thumb') || 
                                    img.src.includes('icon') || 
                                    (img.width > 0 && img.width < 100) ||  // Small images are likely logos
                                    (img.height > 0 && img.height < 100)) {
                                    // Ensure absolute URL
                                    let imgSrc = img.src;
                                    if (imgSrc.startsWith('/')) {
                                        imgSrc = 'https://www.kaggle.com' + imgSrc;
                                    }
                                    logoMap[href] = imgSrc;
                                    break;
                                }
                            }
                        }
                        
                        // APPROACH 4: Fall back to any image if nothing else worked
                        if (!logoMap[href] && imgs.length > 0 && imgs[0].src) {
                            // Ensure absolute URL
                            let imgSrc = imgs[0].src;
                            if (imgSrc.startsWith('/')) {
                                imgSrc = 'https://www.kaggle.com' + imgSrc;
                            }
                            logoMap[href] = imgSrc;
                        }
                    } catch (e) {
                        console.error('Error extracting logo for card:', e);
                    }
                });
                
                // Log the results for debugging
                console.log('Found logos for', Object.keys(logoMap).length, 'competitions');
                return logoMap;
            }""")
            
            # Combine the two approaches
            combined_logo_map = {**logo_images, **scan_results['logoMap']}
            
            # Create minimal placeholder data for these links
            competition_data = []
            for link in competition_links:
                logo_url = combined_logo_map.get(link, "")
                if logo_url:
                    print(f"Found logo for {link.split('/')[-1]}: {logo_url[:60]}...")
                
                competition_data.append({
                    "url": link,
                    "title": link.split('/').pop(),  # Use the last part of the URL as a title placeholder
                    "logo_url": logo_url  # Add logo URL if found
                })
        
        # Save the detailed listing data for use in detail extraction
        with open('competition_listing_data.json', 'w') as f:
            json.dump(competition_data, f, indent=2)
        
        # Upload logo images to Cloudinary if enabled
        if CLOUDINARY_ENABLED:
            updated_competition_data = []
            for item in competition_data:
                if item.get('logo_url'):
                    # Extract competition ID from URL
                    competition_id = item.get('url', '').split('/')[-1]
                    if not competition_id:
                        competition_id = item.get('title', '').lower().replace(' ', '_')
                    
                    # Upload logo to Cloudinary
                    item['logo_url'] = await upload_image_to_cloudinary(
                        item['logo_url'],
                        competition_id,
                        'logo_listing'
                    )
                updated_competition_data.append(item)
            
            # Replace with updated data
            competition_data = updated_competition_data
            
            # Save updated data with Cloudinary URLs
            with open('competition_listing_data_cloudinary.json', 'w') as f:
                json.dump(competition_data, f, indent=2)
        
        return competition_links, competition_data
    
    except Exception as e:
        print(f"Error extracting competition links: {e}")
        traceback.print_exc()
        
        # Try a last-resort approach to find competition links
        try:
            print("Attempting last-resort extraction of competition links...")
            competition_links = await page.evaluate("""() => {
                // Get all links on the page
                const allLinks = Array.from(document.querySelectorAll('a[href]'));
                
                // Filter for likely competition links
                const competitionLinks = allLinks
                    .filter(a => {
                        const href = a.getAttribute('href');
                        return href && 
                               (href.includes('/c/') || href.includes('/competitions/')) &&
                               !href.includes('/competitions?') && 
                               !href.includes('/competitions/list') &&
                               !href.includes('listOption=');
                    })
                    .map(a => {
                        let href = a.getAttribute('href');
                        if (href.startsWith('/')) {
                            href = 'https://www.kaggle.com' + href;
                        }
                        return href;
                    });
                
                // Remove duplicates
                return [...new Set(competitionLinks)];
            }""")
            
            print(f"Last-resort extraction found {len(competition_links)} links")
            
            # Create minimal competition data
            competition_data = []
            for link in competition_links:
                competition_data.append({
                    "url": link,
                    "title": link.split('/').pop(),  # Use the last part of the URL as a title placeholder
                })
                
            return competition_links, competition_data
            
        except Exception as fallback_error:
            print(f"Last-resort extraction also failed: {fallback_error}")
            return [], []

async def extract_competition_details(page, url, listing_data=None):
    """Extract detailed information from a competition page by visiting specific section URLs."""
    try:
        print(f"Extracting details from: {url}")
        
        # Set a competition ID for debugging purposes
        competition_id = url.split('/')[-1]
        
        # Use listing data if available
        competition_details = {}
        if listing_data and isinstance(listing_data, dict):
            # Get data for this URL if available
            url_data = listing_data.get(url, {})
            if url_data:
                competition_details = url_data.copy()
                print(f"Found listing data for {url}")
                # Explicitly log if we found a logo URL in the listing data
                if "logo_url" in url_data and url_data["logo_url"]:
                    print(f"Using logo URL from listing: {url_data['logo_url']}")
        
        # Define section URLs to visit
        section_urls = {
            "abstract": f"{url}/overview/abstract",
            "description": f"{url}/overview/description",
            "timeline": f"{url}/overview/timeline",
            "prizes": f"{url}/overview/prizes"
        }
        
        # First visit the main page to get the banner and other basic details
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_load_state("networkidle", timeout=30000)
        
        # Take a screenshot of the main page
        if DEBUG_MODE:
            await take_screenshot(page, f"competition_{competition_id}_main.png")
            
            # Save HTML for debugging
            html_content = await page.content()
            os.makedirs(f"debug/competitions", exist_ok=True)
            with open(f"debug/competitions/{competition_id}_main.html", "w", encoding="utf-8") as f:
                f.write(html_content)
        
        # Extract basic information from the main page
        main_page_info = await page.evaluate("""() => {
            const data = {};
            
            try {
                // Title
                const titleElement = document.querySelector('h1.competition-name');
                if (titleElement) {
                    data.title = titleElement.textContent.trim();
                }
                
                // Banner image - with specific selector for the pattern provided
                let bannerElement = document.querySelector('div[class*="sc-"] img[src*="/competitions/"][src*="/images/header"]');
                if (bannerElement) {
                    // Make sure the URL is absolute
                    let bannerSrc = bannerElement.src;
                    if (bannerSrc.startsWith('/')) {
                        bannerSrc = 'https://www.kaggle.com' + bannerSrc;
                    }
                    data.banner_url = bannerSrc;
                }
                
                // If not found, try generic selectors as fallback
                if (!data.banner_url) {
                    bannerElement = document.querySelector('.competition-banner img, .competition-header-bg img, .header-image img, .banner img');
                    if (bannerElement) {
                        let bannerSrc = bannerElement.src;
                        if (bannerSrc.startsWith('/')) {
                            bannerSrc = 'https://www.kaggle.com' + bannerSrc;
                        }
                        data.banner_url = bannerSrc;
                    }
                }
                
                // If still not found, try any large image near the top of the page
                if (!data.banner_url) {
                    const allImages = document.querySelectorAll('img');
                    // Look at the first 10 images, as banner is likely at the top
                    for (let i = 0; i < Math.min(10, allImages.length); i++) {
                        const img = allImages[i];
                        // If the image is large, it might be a banner
                        if (img.width > 400 || img.height > 150) {
                            let imgSrc = img.src;
                            if (imgSrc.startsWith('/')) {
                                imgSrc = 'https://www.kaggle.com' + imgSrc;
                            }
                            data.banner_url = imgSrc;
                            break;
                        }
                    }
                }
                
                // Logo - only get this if we don't already have it from the listing page
                const logoElement = document.querySelector('.competition-header-logo img, .competition-logo img');
                if (logoElement) {
                    data.detail_page_logo_url = logoElement.src;
                }
                
                // Extract host/organizer information
                try {
                    const organizerElement = document.querySelector('p.sc-gQaihK, .competition-organizer, .organizer-name');
                    if (organizerElement) {
                        data.organizer = organizerElement.textContent.trim();
                    }
                    
                    // Try to get host avatar/image
                    const hostAvatarElement = document.querySelector('div[data-testid="avatar-image"], div[style*="background-image"]');
                    if (hostAvatarElement) {
                        // Extract background-image URL from style
                        const bgImageStyle = hostAvatarElement.style.backgroundImage;
                        if (bgImageStyle) {
                            // Extract URL from style="background-image: url("URL")"
                            const match = bgImageStyle.match(/url\\(["']?([^"'\\)]+)["']?\\)/);
                            if (match && match[1]) {
                                data.organizer_logo_url = match[1];
                            }
                        }
                    }
                } catch (e) {
                    console.error('Error extracting organizer info:', e);
                }
                
                // Extract participation stats from the sidebar
                try {
                    // Look for participation section in the sidebar
                    const participationElements = document.querySelectorAll('.sc-ifbJqq .sc-fkSjGX p.sc-gQaihK');
                    const participationStats = {};
                    
                    participationElements.forEach(el => {
                        const text = el.textContent.trim();
                        
                        if (text.includes('Entrants')) {
                            const match = text.match(/(\\d[\\d,]*)/);
                            if (match) participationStats.entrants = match[0];
                        }
                        else if (text.includes('Participants')) {
                            const match = text.match(/(\\d[\\d,]*)/);
                            if (match) participationStats.participants = match[0];
                        }
                        else if (text.includes('Teams')) {
                            const match = text.match(/(\\d[\\d,]*)/);
                            if (match) participationStats.teams = match[0];
                        }
                        else if (text.includes('Submissions')) {
                            const match = text.match(/(\\d[\\d,]*)/);
                            if (match) participationStats.submissions = match[0];
                        }
                    });
                    
                    if (Object.keys(participationStats).length > 0) {
                        data.participation_stats = participationStats;
                    }
                } catch (e) {
                    console.error('Error extracting participation stats:', e);
                }
                
                // Extract tags from the sidebar
                try {
                    // Use more specific tag selectors like in the old approach
                    const tagElements = document.querySelectorAll('a[href*="tagIds"], div[class*="sc-fIOXfZ"] span, .sc-eUlrpB');
                    if (tagElements.length > 0) {
                        const tags = [];
                        tagElements.forEach(el => {
                            const tagText = el.textContent.trim();
                            if (tagText) tags.push(tagText);
                        });
                        
                        if (tags.length > 0) {
                            data.tags = tags;
                        }
                    }
                    
                    // Fall back to old structure for tags if not found
                    if (!data.tags) {
                        const tagElements = document.querySelectorAll('.competition-tags .badge, .competition-categories .badge');
                        if (tagElements.length > 0) {
                            data.tags = Array.from(tagElements).map(tag => tag.textContent.trim());
                        }
                    }
                    
                    // Also check for tags in a section specifically for tags
                    if (!data.tags) {
                        // Find sections with tag headings
                        const sections = document.querySelectorAll('div[class*="sc-gzFJfr"], div[class*="sc-"] > div');
                        for (const section of sections) {
                            const headingElement = section.querySelector('h2, h3');
                            if (headingElement && headingElement.textContent.trim().includes('Tags')) {
                                const sectionTagElements = section.querySelectorAll('a[href*="tagIds"], span[class*="sc-"]');
                                if (sectionTagElements.length > 0) {
                                    data.tags = Array.from(sectionTagElements).map(tag => tag.textContent.trim())
                                        .filter(tag => tag); // Filter out empty strings
                                    break;
                                }
                            }
                        }
                    }
                } catch (e) {
                    console.error('Error extracting tags:', e);
                }
            } catch (e) {
                console.error('Error extracting basic information:', e);
            }
            
            return data;
        }""")
        
        # Update competition details with main page info
        competition_details.update(main_page_info)
        
        # Process each section URL
        for section_name, section_url in section_urls.items():
            print(f"Visiting section: {section_name} at {section_url}")
            
            try:
                # Navigate to section page
                await page.goto(section_url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_load_state("networkidle", timeout=30000)
                
                # Take screenshots for debugging
                if DEBUG_MODE:
                    await take_screenshot(page, f"competition_{competition_id}_{section_name}.png")
                    
                    # Save HTML for debugging
                    html_content = await page.content()
                    os.makedirs(f"debug/competitions", exist_ok=True)
                    with open(f"debug/competitions/{competition_id}_{section_name}.html", "w", encoding="utf-8") as f:
                        f.write(html_content)
                
                # Extract section data
                if section_name == "abstract":
                    # Extract abstract section
                    abstract_data = await page.evaluate("""() => {
                        const data = {};
                        
                        try {
                            // Abstract content
                            const abstractSection = document.querySelector('#abstract');
                            if (abstractSection) {
                                const paragraphs = abstractSection.querySelectorAll('p');
                                if (paragraphs.length > 0) {
                                    data.abstract = Array.from(paragraphs)
                                        .map(p => p.textContent.trim())
                                        .join('\\n\\n');
                                }
                            }
                        } catch (e) {
                            console.error('Error extracting abstract:', e);
                        }
                        
                        return data;
                    }""")
                    
                    # Update with abstract data
                    competition_details.update(abstract_data)
                
                elif section_name == "description":
                    # Extract description section
                    description_data = await page.evaluate("""() => {
                        const data = {};
                        
                        try {
                            // Description content
                            const descriptionSection = document.querySelector('#description .sc-ePpfBx');
                            if (descriptionSection) {
                                data.description = descriptionSection.textContent.trim();
                            }
                        } catch (e) {
                            console.error('Error extracting description:', e);
                        }
                        
                        return data;
                    }""")
                    
                    # Update with description data
                    competition_details.update(description_data)
                
                elif section_name == "timeline":
                    # Extract timeline section
                    timeline_data = await page.evaluate("""() => {
                        const data = {};
                        
                        try {
                            // Timeline content
                            const timelineSection = document.querySelector('#timeline .sc-ePpfBx');
                            if (timelineSection) {
                                const timelineItems = timelineSection.querySelectorAll('li');
                                if (timelineItems.length > 0) {
                                    const timeline = [];
                                    
                                    timelineItems.forEach(item => {
                                        const text = item.textContent.trim();
                                        
                                        // Extract date and event description
                                        // Format is typically: "Date - Description"
                                        const dateMatch = text.match(/(\\w+ \\d+, \\d{4})\\s*(?:-|–|—)\\s*(.*)/);
                                        if (dateMatch) {
                                            timeline.push({
                                                date: dateMatch[1],
                                                event: dateMatch[2].trim()
                                            });
                                        } else {
                                            timeline.push({
                                                text: text
                                            });
                                        }
                                    });
                                    
                                    data.timeline = timeline;
                                    
                                    // Extract start date and end date from timeline
                                    for (const item of timeline) {
                                        if (item.event && (
                                            item.event.toLowerCase().includes('start') || 
                                            item.event.toLowerCase().includes('launch') ||
                                            item.event.toLowerCase().includes('begins'))) {
                                            data.start_date = item.date;
                                        }
                                        else if (item.event && (
                                            item.event.toLowerCase().includes('final submission') || 
                                            item.event.toLowerCase().includes('deadline') ||
                                            item.event.toLowerCase().includes('close') ||
                                            item.event.toLowerCase().includes('end'))) {
                                            data.end_date = item.date;
                                        }
                                    }
                                }
                            }
                            
                            // If start date wasn't found, check the sidebar
                            if (!data.start_date) {
                                const startElement = document.querySelector('.sc-JEPTY .sc-ffwOux:first-child .sc-hbtGpV');
                                if (startElement) {
                                    const startText = startElement.textContent.trim();
                                    if (startText) {
                                        data.start_date = startText;
                                    }
                                }
                            }
                            
                            // If end date wasn't found, check the sidebar
                            if (!data.end_date) {
                                const endElement = document.querySelector('.sc-JEPTY .sc-ffwOux:last-child .sc-hbtGpV');
                                if (endElement) {
                                    const endText = endElement.textContent.trim();
                                    if (endText) {
                                        data.end_date = endText;
                                    }
                                }
                            }
                        } catch (e) {
                            console.error('Error extracting timeline:', e);
                        }
                        
                        return data;
                    }""")
                    
                    # Update with timeline data
                    competition_details.update(timeline_data)
                
                elif section_name == "prizes":
                    # Extract prizes section
                    prize_data = await page.evaluate("""() => {
                        const data = {};
                        
                        try {
                            // Prizes content
                            const prizesSection = document.querySelector('#prizes .sc-ePpfBx');
                            if (prizesSection) {
                                // Get the full prize text
                                data.prize_details = prizesSection.textContent.trim();
                                
                                // Try to extract just the main prize amount
                                const totalPrizeMatch = prizesSection.textContent.match(/TOTAL PRIZES AVAILABLE:\\s*\\$(\\d[\\d,]*(?:\\.\\d+)?)/i);
                                if (totalPrizeMatch) {
                                    data.prize_pool = '$' + totalPrizeMatch[1];
                                } else {
                                    // Look for dollar amounts
                                    const dollarMatch = prizesSection.textContent.match(/\\$(\\d[\\d,]*(?:\\.\\d+)?)/);
                                    if (dollarMatch) {
                                        data.prize_pool = '$' + dollarMatch[1];
                                    } else {
                                        // Check if it's a learning/knowledge competition
                                        if (prizesSection.textContent.toLowerCase().includes('knowledge')) {
                                            data.prize_pool = 'Knowledge';
                                        } else {
                                            data.prize_pool = 'See competition details';
                                        }
                                    }
                                }
                                
                                // Extract prize breakdown
                                const prizeItems = [];
                                const liElements = prizesSection.querySelectorAll('li');
                                
                                liElements.forEach(li => {
                                    const text = li.textContent.trim();
                                    // Look for patterns like "First Prize: $X,XXX"
                                    const prizeMatch = text.match(/(First|Second|Third|Fourth|Fifth|\\d+(?:st|nd|rd|th))[^:]*:\\s*\\$(\\d[\\d,]*(?:\\.\\d+)?)/i);
                                    if (prizeMatch) {
                                        prizeItems.push({
                                            rank: prizeMatch[1].trim(),
                                            amount: '$' + prizeMatch[2]
                                        });
                                    }
                                });
                                
                                if (prizeItems.length > 0) {
                                    data.prize_breakdown = prizeItems;
                                }
                            }
                        } catch (e) {
                            console.error('Error extracting prizes:', e);
                        }
                        
                        return data;
                    }""")
                    
                    # Update with prize data
                    competition_details.update(prize_data)
            
            except Exception as section_error:
                print(f"Error processing section {section_name}: {section_error}")
        
        # Preserve logo_url from the listing page if we have it
        # If we don't have a logo_url from the listing, but found one on the detail page,
        # use the detail page version
        if 'detail_page_logo_url' in main_page_info and not competition_details.get('logo_url'):
            competition_details['logo_url'] = main_page_info['detail_page_logo_url']
            del competition_details['detail_page_logo_url']
        elif 'detail_page_logo_url' in main_page_info:
            # We already have a logo from the listing, remove the duplicate
            del competition_details['detail_page_logo_url']
        
        # Ensure required fields are present
        if not competition_details.get('title'):
            competition_details['title'] = competition_id
        
        if not competition_details.get('start_date'):
            competition_details['start_date'] = 'Unknown'
        
        if not competition_details.get('end_date'):
            competition_details['end_date'] = 'Unknown'
        
        if not competition_details.get('prize_pool'):
            competition_details['prize_pool'] = 'See competition details'
        
        # URL is always known
        competition_details['url'] = url
        
        # Add source platform
        competition_details['source_platform'] = 'kaggle'
        
        # Upload images to Cloudinary if enabled
        if CLOUDINARY_ENABLED:
            # Upload logo image if available
            if competition_details.get('logo_url'):
                competition_details['logo_url'] = await upload_image_to_cloudinary(
                    competition_details['logo_url'],
                    competition_id,
                    'logo'
                )
            
            # Upload banner image if available
            if competition_details.get('banner_url'):
                competition_details['banner_url'] = await upload_image_to_cloudinary(
                    competition_details['banner_url'],
                    competition_id,
                    'banner'
                )
            
            # Upload organizer logo if available
            if competition_details.get('organizer_logo_url'):
                competition_details['organizer_logo_url'] = await upload_image_to_cloudinary(
                    competition_details['organizer_logo_url'],
                    competition_id,
                    'organizer_logo'
                )
        
        print(f"Extracted details for: {competition_details.get('title', 'Unknown competition')}")
        return competition_details
    
    except Exception as e:
        print(f"Error extracting competition details: {e}")
        traceback.print_exc()
        
        # Return minimal data with error information
        return {
            "title": competition_id,
            "url": url,
            "description": f"Error during extraction: {str(e)}",
            "source_platform": "kaggle",
            "error": str(e)
        }

def is_complete_competition(competition: Dict[str, Any], required_fields: List[str]) -> bool:
    """Check if a competition has all required fields."""
    for field in required_fields:
        if field not in competition or not competition[field]:
            return False
    return True

def save_competitions_to_csv(competitions: List[Dict[str, Any]], filename: str) -> None:
    """Save competitions to a CSV file."""
    if not competitions:
        print("No competitions to save")
        return
    
    # Prepare data for CSV
    cleaned_competitions = []
    for competition in competitions:
        # Convert any dict/list fields to JSON strings for CSV compatibility
        cleaned_competition = {}
        for key, value in competition.items():
            if isinstance(value, (dict, list)):
                cleaned_competition[key] = json.dumps(value)
            else:
                cleaned_competition[key] = value
                
        # Ensure the logo_url field exists
        if 'logo_url' not in cleaned_competition:
            cleaned_competition['logo_url'] = ""
            
        cleaned_competitions.append(cleaned_competition)
    
    # Collect all fields from all competitions
    fieldnames = set()
    for competition in cleaned_competitions:
        fieldnames.update(competition.keys())
    
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
        writer.writerows(cleaned_competitions)
    
    print(f"Saved {len(competitions)} competitions to '{filename}'")
    
    # Print statistics about logo URLs
    with_logos = sum(1 for comp in cleaned_competitions if comp.get('logo_url'))
    print(f"Competitions with logo URLs: {with_logos}/{len(cleaned_competitions)}")

async def crawl_kaggle_competitions():
    """Main function to crawl Kaggle competitions."""
    print("Starting Kaggle Competition Crawler...")
    
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
        all_competition_links = []
        all_listing_data = {}
        
        try:
            # Process each page of results
            for page_num in range(1, MAX_PAGES + 1):
                # Construct URL with page parameter (page=1 is implicit in the base URL)
                page_param = f"&page={page_num}" if page_num > 1 else ""
                page_url = f"{BASE_URL}{DEFAULT_PARAMS}{page_param}"
                
                print(f"\nProcessing page {page_num}: {page_url}")
                
                # Navigate to the competitions page
                await page.goto(page_url, wait_until="domcontentloaded")
                await page.wait_for_load_state("networkidle")
                
                # Take a screenshot of each page
                if DEBUG_MODE:
                    await take_screenshot(page, f"competition_page_{page_num}.png")
                
                # Extract competition links and listing data
                page_links, page_data_array = await extract_competition_links(page)
                
                # Convert listing_data_array to dict for this page
                page_data = {item["url"]: item for item in page_data_array if "url" in item}
                
                # Check if we found any competitions on this page
                if len(page_links) == 0:
                    print(f"No competitions found on page {page_num}, stopping pagination")
                    break
                
                # Add these links to our collection
                all_competition_links.extend(page_links)
                all_listing_data.update(page_data)
                
                print(f"Found {len(page_links)} competitions on page {page_num}")
                print(f"Total competitions found so far: {len(all_competition_links)}")
                
                # Check if we've reached our max competitions
                if len(all_competition_links) >= MAX_COMPETITIONS:
                    print(f"Reached maximum number of competitions ({MAX_COMPETITIONS})")
                    break
                
                # Small delay between pages
                await smart_wait()
            
            # Limit the total number of competitions if needed
            if MAX_COMPETITIONS and len(all_competition_links) > MAX_COMPETITIONS:
                print(f"Limiting to {MAX_COMPETITIONS} competitions out of {len(all_competition_links)}")
                all_competition_links = all_competition_links[:MAX_COMPETITIONS]
            
            # Check if we found any competitions
            if not all_competition_links:
                print("No competition links found. Exiting.")
                return
            
            # Process each competition in parallel
            print(f"\nProcessing {len(all_competition_links)} competitions...")
            
            # Create a pool of pages for parallel processing
            concurrency = min(5, len(all_competition_links))
            # Ensure concurrency is at least 1 to avoid division by zero
            concurrency = max(1, concurrency)
            
            pages = [page]  # Reuse the initial page
            
            for _ in range(concurrency - 1):
                pages.append(await context.new_page())
            
            # Process competitions in batches
            all_competitions = []
            for i in range(0, len(all_competition_links), concurrency):
                batch = all_competition_links[i:i+concurrency]
                tasks = []
                
                for j, url in enumerate(batch):
                    # Use the appropriate page from the pool
                    competition_page = pages[j % len(pages)]
                    
                    # Create the task with appropriate listing data
                    tasks.append(extract_competition_details(competition_page, url, all_listing_data))
                
                # Wait for all tasks in this batch to complete
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process results
                for result in batch_results:
                    if isinstance(result, Exception):
                        print(f"Error during extraction: {result}")
                    elif result:  # If not None
                        all_competitions.append(result)
                
                print(f"Processed batch {i//concurrency + 1}, total competitions so far: {len(all_competitions)}")
                
                # Add a small delay between batches to avoid rate limiting
                await smart_wait(2, 4)
            
            # Save results to CSV
            if all_competitions:
                # Generate timestamp for the filename
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"kaggle_competitions_{timestamp}.csv"
                
                # Save to CSV
                save_competitions_to_csv(all_competitions, filename)
                
                # Also save as JSON for easier inspection
                json_filename = filename.replace('.csv', '.json')
                with open(json_filename, 'w', encoding='utf-8') as f:
                    json.dump(all_competitions, f, indent=2)
                
                # Print summary of complete vs incomplete competitions
                complete_competitions = [c for c in all_competitions if is_complete_competition(c, REQUIRED_FIELDS)]
                print(f"Complete competitions: {len(complete_competitions)}/{len(all_competitions)}")
                
                for field in REQUIRED_FIELDS:
                    missing_field = sum(1 for c in all_competitions if field not in c or not c[field])
                    print(f"Competitions missing {field}: {missing_field}")
            else:
                print("No competitions were found.")
                
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
    asyncio.run(crawl_kaggle_competitions())
