import os
import csv
import json
import asyncio
import re
import time
import random
from datetime import datetime
from playwright.async_api import async_playwright, Error as PlaywrightError
import pandas as pd
from dotenv import load_dotenv

# Constants
BASE_URL = "https://unstop.com/hackathons"
DEFAULT_PARAMS = "?oppstatus=open&domain=2&course=6&specialization=Artificial%20Intelligence%20and%20Machine%20Learning%20Engineering&passingOutYear=2025&quickApply=true"
OUTPUT_CSV = f"unstop_hackathons_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
REQUIRED_FIELDS = ["title", "start_date", "end_date", "mode"]
PROCESS_SINGLE_PAGE = True
MAX_HACKATHONS = 1000  # Increased to process all hackathons
DEBUG_MODE = False  # Set to False to avoid timeouts with screenshots/HTML saving
MAX_RETRIES = 3     # Maximum number of retries for rate-limited requests
LOGIN_EMAIL = os.getenv("UNSTOP_EMAIL", "sahilgawli333@gmail.com")  # Add your email in .env file
LOGIN_PASSWORD = os.getenv("UNSTOP_PASSWORD", "3aSzLXL4w@Pc4YP")  # Add your password in .env file
PARALLEL_WORKERS = 4  # Number of parallel workers for processing hackathons
MIN_RATE_LIMIT_DELAY = 1  # Minimum delay in seconds (reduced from 5)
MAX_RATE_LIMIT_DELAY = 3  # Maximum delay in seconds (reduced from 10)

# Data storage for listing page info
hackathon_listing_data = {}

# Add delay between requests to avoid 429 errors
async def smart_wait(min_delay=MIN_RATE_LIMIT_DELAY, max_delay=MAX_RATE_LIMIT_DELAY):
    """Wait for a random amount of time to avoid rate limiting."""
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

async def extract_hackathon_links(page):
    """Extract hackathon links from the listing page."""
    print(f"Extracting hackathon links from: {page.url}")
    
    # Save HTML for debugging (only if DEBUG_MODE is True)
    html_content = ""
    if DEBUG_MODE:
        try:
            html_content = await page.content()
            with open("unstop_debug_html.html", "w", encoding="utf-8") as f:
                f.write(html_content)
            await page.screenshot(path="unstop_debug_screenshot.png", timeout=10000)
            print(f"Debug HTML and screenshot saved")
        except Exception as e:
            print(f"Error saving debug info: {e}")
    else:
        # Still get HTML content for parsing even if not saving to file
        html_content = await page.content()
    
    # Check for cookie error
    try:
        cookie_error = await page.query_selector('text="Error: Cookies Disabled"')
        if cookie_error:
            print("Found cookie error message. Attempting to bypass...")
            # Try to bypass by setting a cookie manually
            await page.context.add_cookies([{
                "name": "accept_cookies", 
                "value": "true", 
                "domain": ".unstop.com", 
                "path": "/"
            }])
            
            # Reload the page
            await page.reload(wait_until="domcontentloaded")
            await page.wait_for_load_state("networkidle")
            
            # Check if error persists
            cookie_error = await page.query_selector('text="Error: Cookies Disabled"')
            if cookie_error:
                print("Cookie error still persists - might need a different approach")
    except Exception as e:
        print(f"Error handling cookie message: {e}")
    
    # Function to bypass login walls during scrolling
    async def bypass_login_walls():
        try:
            await page.evaluate("""() => {
                // Remove login modals/overlays
                document.querySelectorAll('.modal, .login-modal, .overlay, .login-overlay, [class*="login"], [class*="modal"]')
                    .forEach(el => el && el.parentNode && el.parentNode.removeChild(el));
                
                // Remove any background overlay
                document.querySelectorAll('.overlay, .modal-backdrop, [class*="overlay"]')
                    .forEach(el => el && el.parentNode && el.parentNode.removeChild(el));
                
                // Re-enable scrolling
                document.body.style.overflow = 'auto';
                document.documentElement.style.overflow = 'auto';
                document.body.style.position = 'static';
                
                // Set body height to auto to allow scrolling
                document.body.style.height = 'auto';
                document.documentElement.style.height = 'auto';
                
                // Make all content visible
                document.querySelectorAll('main, .content, #content, [class*="content"]')
                    .forEach(el => {
                        if (el) el.style.display = 'block';
                    });
                
                // Remove any fixed position elements that might block scrolling
                document.querySelectorAll('[style*="position: fixed"]')
                    .forEach(el => {
                        // Don't remove important UI elements like headers/nav
                        if (!el.matches('header, nav, .header, .navigation')) {
                            el.style.position = 'absolute';
                            el.style.zIndex = '-1';
                        }
                    });
                    
                // If there's a hackathon-specific container, make sure it's visible
                document.querySelectorAll('.hackathon-container, .hackathons-listing, [class*="hackathon"]')
                    .forEach(el => {
                        if (el) {
                            el.style.display = 'block';
                            el.style.visibility = 'visible';
                        }
                    });
            }""")
            print("Bypassed login walls during scrolling")
        except Exception as e:
            print(f"Error bypassing login walls: {e}")
    
    # Try to find the specific Angular container element
    print("Looking for the Angular container element to scroll...")
    try:
        # First check if the specific selector exists
        has_angular_container = await page.evaluate("""() => {
            const container = document.querySelector('#s_menu > app-root > div > main > app-global-search > div > div.panel_container.ng-tns-c\\\\d+-\\\\d+.ng-star-inserted > div.user_list.custom-scrollbar.thin.bdr-rds-none.ng-tns-c\\\\d+-\\\\d+.ng-star-inserted');
            if (container) {
                return true;
            }
            
            // Try a more general selector if the specific one isn't found
            const generalContainer = document.querySelector('.panel_container .user_list, app-global-search .user_list, .custom-scrollbar.thin');
            return !!generalContainer;
        }""")
        
        if has_angular_container:
            print("Found Angular container element. Scrolling within this container...")
            
            # Perform scrolling on the specific container
            previous_height = 0
            current_height = await page.evaluate("""() => {
                // Try the specific selector first
                const specificSelector = '#s_menu > app-root > div > main > app-global-search > div > div.panel_container.ng-tns-c\\d+-\\d+.ng-star-inserted > div.user_list.custom-scrollbar.thin.bdr-rds-none.ng-tns-c\\d+-\\d+.ng-star-inserted';
                let container = document.querySelector(specificSelector);
                
                // If specific selector not found, try more general ones
                if (!container) {
                    container = document.querySelector('.panel_container .user_list, app-global-search .user_list, .custom-scrollbar.thin');
                }
                
                if (container) {
                    return container.scrollHeight;
                }
                return document.body.scrollHeight;
            }""")
            
            scroll_attempts = 0
            max_scroll_attempts = 20
            
            while previous_height != current_height and scroll_attempts < max_scroll_attempts:
                previous_height = current_height
                
                # Apply login bypass JS before scrolling
                await bypass_login_walls()
                
                # Scroll the specific container
                await page.evaluate("""() => {
                    // Try the specific selector first
                    const specificSelector = '#s_menu > app-root > div > main > app-global-search > div > div.panel_container.ng-tns-c\\d+-\\d+.ng-star-inserted > div.user_list.custom-scrollbar.thin.bdr-rds-none.ng-tns-c\\d+-\\d+.ng-star-inserted';
                    let container = document.querySelector(specificSelector);
                    
                    // If specific selector not found, try more general ones
                    if (!container) {
                        container = document.querySelector('.panel_container .user_list, app-global-search .user_list, .custom-scrollbar.thin');
                    }
                    
                    if (container) {
                        container.scrollTop = container.scrollHeight;
                        console.log('Scrolled container to height:', container.scrollHeight);
                    } else {
                        window.scrollTo(0, document.body.scrollHeight);
                    }
                }""")
                
                print(f"Scroll attempt {scroll_attempts+1}: Previous height: {previous_height}, scrolling down...")
                
                # Wait longer for Angular to render content
                print("Waiting 10 seconds for content to load fully...")
                await asyncio.sleep(10)
                
                # Apply login bypass again after waiting
                await bypass_login_walls()
                
                # Check the new height
                current_height = await page.evaluate("""() => {
                    // Try the specific selector first
                    const specificSelector = '#s_menu > app-root > div > main > app-global-search > div > div.panel_container.ng-tns-c\\d+-\\d+.ng-star-inserted > div.user_list.custom-scrollbar.thin.bdr-rds-none.ng-tns-c\\d+-\\d+.ng-star-inserted';
                    let container = document.querySelector(specificSelector);
                    
                    // If specific selector not found, try more general ones
                    if (!container) {
                        container = document.querySelector('.panel_container .user_list, app-global-search .user_list, .custom-scrollbar.thin');
                    }
                    
                    if (container) {
                        return container.scrollHeight;
                    }
                    return document.body.scrollHeight;
                }""")
                
                print(f"New height: {current_height}")
                
                if current_height == previous_height:
                    # Try one more time with a longer wait to ensure it's really the end
                    print("Height unchanged, waiting additional 5 seconds to confirm...")
                    await asyncio.sleep(5)
                    
                    # Trigger events that might load more content
                    await page.evaluate("""() => {
                        // Dispatch scroll events to trigger lazy loading
                        window.dispatchEvent(new Event('scroll'));
                        document.dispatchEvent(new Event('scroll'));
                        
                        // Try to find and click any "Load More" or "Show More" buttons
                        const loadMoreButtons = Array.from(document.querySelectorAll('button, a'))
                            .filter(el => {
                                const text = el.textContent.toLowerCase();
                                return text.includes('load more') || text.includes('show more') || 
                                       text.includes('view more') || text.includes('see more');
                            });
                        
                        loadMoreButtons.forEach(btn => btn.click());
                    }""")
                    
                    await asyncio.sleep(3)
                    
                    # Apply login bypass one more time
                    await bypass_login_walls()
                    
                    current_height = await page.evaluate("""() => {
                        // Try the specific selector first
                        const specificSelector = '#s_menu > app-root > div > main > app-global-search > div > div.panel_container.ng-tns-c\\d+-\\d+.ng-star-inserted > div.user_list.custom-scrollbar.thin.bdr-rds-none.ng-tns-c\\d+-\\d+.ng-star-inserted';
                        let container = document.querySelector(specificSelector);
                        
                        // If specific selector not found, try more general ones
                        if (!container) {
                            container = document.querySelector('.panel_container .user_list, app-global-search .user_list, .custom-scrollbar.thin');
                        }
                        
                        if (container) {
                            return container.scrollHeight;
                        }
                        return document.body.scrollHeight;
                    }""")
                    
                    print(f"Final height check: {current_height}")
                
                scroll_attempts += 1
            
            if scroll_attempts >= max_scroll_attempts:
                print(f"Reached maximum scroll attempts ({max_scroll_attempts}). Proceeding with extraction.")
            else:
                print(f"Finished scrolling after {scroll_attempts} attempts. All content should be loaded.")
            
        else:
            print("Angular container not found, falling back to regular page scrolling...")
            # Perform regular page scrolling as before
            previous_height = 0
            current_height = await page.evaluate("document.body.scrollHeight")
            scroll_attempts = 0
            max_scroll_attempts = 20  # Maximum number of scrolling attempts to prevent infinite loops
            
            while previous_height != current_height and scroll_attempts < max_scroll_attempts:
                previous_height = current_height
                
                # Apply login bypass JS before scrolling
                await bypass_login_walls()
                
                # Check for the 'Login to View More' button and try to remove it
                try:
                    login_button = await page.query_selector('button:has-text("Login to View More"), a:has-text("Login to View More")')
                    if login_button:
                        print("Found 'Login to View More' button, attempting to bypass...")
                        
                        # Try to automatically load more content using JavaScript
                        await page.evaluate("""() => {
                            // Try to find any load more buttons
                            const loadMoreButtons = Array.from(document.querySelectorAll('button, a'))
                                .filter(el => el.textContent.includes('Load More') || 
                                             el.textContent.includes('View More') || 
                                             el.textContent.includes('Show More'));
                            
                            // Click all load more buttons
                            loadMoreButtons.forEach(btn => btn.click());
                            
                            // Trigger scroll events that might load content
                            window.dispatchEvent(new Event('scroll'));
                        }""")
                except Exception as e:
                    print(f"Error handling 'Login to View More' button: {e}")
                
                # Scroll to the bottom of the page
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                print(f"Scroll attempt {scroll_attempts+1}: Previous height: {previous_height}, scrolling down...")
                
                # Wait for potential new content to load - increased to 10 seconds
                print("Waiting 10 seconds for content to load fully...")
                await asyncio.sleep(10)
                
                # Apply login bypass again after waiting
                await bypass_login_walls()
                
                # Check the new height
                current_height = await page.evaluate("document.body.scrollHeight")
                print(f"New height: {current_height}")
                
                if current_height == previous_height:
                    # Try one more time with a longer wait to ensure it's really the end
                    print("Height unchanged, waiting additional 5 seconds to confirm...")
                    await asyncio.sleep(5)
                    
                    # Apply login bypass one more time
                    await bypass_login_walls()
                    
                    current_height = await page.evaluate("document.body.scrollHeight")
                    print(f"Final height check: {current_height}")
                
                scroll_attempts += 1
    except Exception as e:
        print(f"Error during Angular container detection and scrolling: {e}")
        print("Falling back to regular page scrolling...")
        
        # Perform regular page scrolling as before
        previous_height = 0
        current_height = await page.evaluate("document.body.scrollHeight")
        scroll_attempts = 0
        max_scroll_attempts = 20
        
        while previous_height != current_height and scroll_attempts < max_scroll_attempts:
            previous_height = current_height
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            print(f"Scroll attempt {scroll_attempts+1}: Previous height: {previous_height}, scrolling down...")
            await asyncio.sleep(10)
            current_height = await page.evaluate("document.body.scrollHeight")
            print(f"New height: {current_height}")
            
            if current_height == previous_height:
                break
            
            scroll_attempts += 1
    
    # Final login bypass before extraction
    await bypass_login_walls()
    
    # Scroll back to top for consistent extraction
    await page.evaluate("window.scrollTo(0, 0)")
    await asyncio.sleep(1)
    
    # Scroll once more through the page to ensure all elements are rendered
    scroll_steps = 5
    total_height = await page.evaluate("document.body.scrollHeight")
    for i in range(scroll_steps):
        position = total_height * (i / scroll_steps)
        await page.evaluate(f"window.scrollTo(0, {position})")
        await asyncio.sleep(0.5)
        await bypass_login_walls()  # Apply login bypass during progressive scroll
    
    # Finally scroll back to top
    await page.evaluate("window.scrollTo(0, 0)")
    await asyncio.sleep(1)
    
    # Apply login bypass one final time
    await bypass_login_walls()
    
    # Refresh HTML content after scrolling
    html_content = await page.content()
    
    # Try another approach: Use XPath to find all elements and extract links via JavaScript
    try:
        print("Using advanced JavaScript to extract all possible hackathon links...")
        links = await page.evaluate("""() => {
            // Function to extract all possible hackathon links
            const extractLinks = () => {
                const allLinks = [];
                
                // Check all anchor tags on the page
                document.querySelectorAll('a').forEach(a => {
                    const href = a.getAttribute('href');
                    if (href && (href.includes('/p/') || href.includes('/hackathon/')) && 
                        !href.includes('/hackathons?') && !href.includes('/hackathons/amp')) {
                        
                        const fullHref = href.startsWith('http') ? href : 'https://unstop.com' + href;
                        allLinks.push({
                            href: fullHref,
                            title: a.textContent.trim() || 'Unknown'
                        });
                    }
                });
                
                // Look for cards/divs with IDs containing 'opp_'
                document.querySelectorAll('[id^="opp_"]').forEach(card => {
                    const id = card.id.replace('opp_', '');
                    // Create both potential URL formats
                    allLinks.push({
                        href: `https://unstop.com/hackathon/${id}`,
                        title: 'ID-based link',
                        id: id
                    });
                    allLinks.push({
                        href: `https://unstop.com/p/impactathon-jims-noida-extension-${id}`,
                        title: 'ID-based link (alternate format)',
                        id: id
                    });
                });
                
                return allLinks;
            };
            
            return extractLinks();
        }""")
        
        print(f"Advanced JavaScript extraction found {len(links)} potential links")
        
        for link in links:
            url = link.get('href')
            hackathon_links.append(url)
            
            # Extract additional data if available
            if link.get('title') and link.get('title') != 'Unknown' and link.get('title') != 'ID-based link':
                hackathon_listing_data[url] = {
                    "title": link.get('title'),
                    "logo_url": "",  # Will try to extract logo separately
                    "organizer": "Unknown"
                }
                
            print(f"Added potential hackathon link: {url}")
    except Exception as e:
        print(f"Error during advanced JavaScript extraction: {e}")
    
    print("Extracting hackathon cards...")
    
    # Look for hackathon cards using different approaches
    hackathon_links = []
    
    # First, try to get IDs from cursor-pointer elements
    try:
        print("Extracting hackathon IDs from cursor-pointer elements...")
        hackathon_ids = await page.evaluate("""
            Array.from(document.querySelectorAll('.cursor-pointer[id^="opp_"]'))
                .map(el => el.id.replace('opp_', ''))
                .filter(id => id && id.length > 0)
        """)
        
        print(f"Found {len(hackathon_ids)} hackathon IDs")
        
        for hackathon_id in hackathon_ids:
            # First try to construct the URL with /p/ format
            hackathon_url = f"https://unstop.com/p/impactathon-jims-noida-extension-{hackathon_id}"
            hackathon_links.append(hackathon_url)
            
            # Also add the direct ID URL as an alternative
            alt_url = f"https://unstop.com/hackathon/{hackathon_id}"
            hackathon_links.append(alt_url)
            
            # Try to extract additional data for this hackathon
            try:
                element = await page.query_selector(f'#opp_{hackathon_id}')
                if element:
                    # Extract title
                    title_el = await element.query_selector('h2')
                    title = await title_el.text_content() if title_el else "Unknown"
                    
                    # Extract logo
                    img_el = await element.query_selector('img')
                    logo_url = await img_el.get_attribute('src') if img_el else ""
                    
                    # Extract organizer
                    org_el = await element.query_selector('p')
                    organizer = await org_el.text_content() if org_el else "Unknown"
                    
                    # Store data for use later - store for both possible URLs
                    for url in [hackathon_url, alt_url]:
                        hackathon_listing_data[url] = {
                            "logo_url": logo_url,
                            "title": title.strip(),
                            "organizer": organizer.strip()
                        }
                    
                    print(f"Extracted data for {title.strip()}: Logo: {'Yes' if logo_url else 'No'}")
            except Exception as e:
                print(f"Error extracting data for hackathon ID {hackathon_id}: {e}")
    except Exception as e:
        print(f"Error extracting hackathon IDs: {e}")
    
    # Try a more comprehensive extraction approach to find all cards
    try:
        print("Trying comprehensive extraction of all hackathon cards...")
        cards_info = await page.evaluate("""
            () => {
                // Look for all possible card elements
                const cards = document.querySelectorAll('.event-card, .opp-card, .challenge-card, app-competition-listing > div, [class*="card"], [id^="opp_"]');
                
                return Array.from(cards).map(card => {
                    // Try to extract a link directly from the card
                    const linkEl = card.querySelector('a[href*="/p/"], a[href*="/hackathon/"]');
                    const href = linkEl ? linkEl.getAttribute('href') : null;
                    
                    // Extract ID if available
                    const id = card.id && card.id.startsWith('opp_') ? card.id.replace('opp_', '') : null;
                    
                    return {
                        id: id,
                        href: href
                    };
                }).filter(item => item.id || item.href);  // Only keep items with either id or href
            }
        """)
        
        print(f"Found {len(cards_info)} potential hackathon cards")
        
        for card in cards_info:
            if card.get('href'):
                link = card['href']
                if not link.startswith('http'):
                    link = f"https://unstop.com{link}"
                hackathon_links.append(link)
                print(f"Added link from card: {link}")
            
            if card.get('id'):
                hackathon_id = card['id']
                alt_url = f"https://unstop.com/hackathon/{hackathon_id}"
                hackathon_links.append(alt_url)
                print(f"Added ID-based link: {alt_url}")
    except Exception as e:
        print(f"Error during comprehensive card extraction: {e}")
    
    # As a fallback, try to find all links in the HTML that could be hackathon pages
    print("Searching for all possible hackathon links in the page...")
    try:
        # Look for links with different patterns
        patterns = [
            r'href="(/p/[^"]+)"',          # /p/ links
            r'href="(/hackathon/[^"]+)"',  # /hackathon/ links
            r'href="([^"]+opp_[^"]+)"'     # opp_ in the URL
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, html_content)
            for match in matches:
                if "/hackathons?" not in match and "/hackathons/amp" not in match:
                    link = f"https://unstop.com{match}" if not match.startswith('http') else match
                    hackathon_links.append(link)
                    print(f"Found potential hackathon link: {link}")
    except Exception as e:
        print(f"Error extracting links with regex: {e}")
    
    # Deduplicate links
    hackathon_links = list(set(hackathon_links))
    print(f"Extracted {len(hackathon_links)} unique hackathon links")
    
    # Check if we have enough links
    if len(hackathon_links) == 0:
        print("WARNING: No hackathon links found. The website structure might have changed.")
        
    return hackathon_links[:MAX_HACKATHONS]

async def extract_hackathon_details(page, url):
    """Extract details from a hackathon page."""
    print(f"Extracting details from: {url}")
    
    # Get hackathon ID from URL for debugging
    hackathon_id = re.search(r'opp_(\d+)|/p/([^/]+)|/hackathon/([^/]+)', url)
    hackathon_id = hackathon_id.group(1) if hackathon_id and hackathon_id.group(1) else \
                  hackathon_id.group(2) if hackathon_id and hackathon_id.group(2) else \
                  hackathon_id.group(3) if hackathon_id and hackathon_id.group(3) else "unknown"
    print(f"Processing hackathon ID: {hackathon_id}")
    
    # Create a dictionary to store hackathon details
    hackathon_data = {}
    
    # Create referral link - append referral parameters to original URL
    referral_params = "?lb=91VCSzkY&utm_medium=Share&utm_source=shortUrl"
    # Check if URL already has parameters
    if "?" in url:
        referral_url = f"{url}&lb=91VCSzkY&utm_medium=Share&utm_source=shortUrl"
    else:
        referral_url = f"{url}{referral_params}"
    
    hackathon_data["referral_url"] = referral_url
    print(f"Generated referral URL: {referral_url}")
    
    # Use data from listing page if available
    if url in hackathon_listing_data:
        listing_data = hackathon_listing_data[url]
        hackathon_data.update({
            "logo_url": listing_data.get("logo_url", ""),
            "title": listing_data.get("title", ""),
            "organizer": listing_data.get("organizer", "")
        })
    
    try:
        # Try to navigate to the URL
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_load_state("networkidle", timeout=15000)
        except Exception as e:
            print(f"Navigation error: {e}")
        
        # Save detailed page for debugging - only if DEBUG_MODE is True
        if DEBUG_MODE:
            try:
                detail_html = await page.content()
                with open(f"unstop_detail_{hackathon_id}.html", "w", encoding="utf-8") as f:
                    f.write(detail_html)
                await page.screenshot(path=f"unstop_detail_{hackathon_id}.png", timeout=10000)
            except Exception as e:
                print(f"Error saving detail page debug info: {e}")
        
        # Wait for Angular components to load - try different selectors
        selectors = [
            ".listing_dt_logo-box", 
            ".all_box_wrapper", 
            "app-competition-basic-form", 
            "div.cptn", 
            "h1.ttl"
        ]
        
        for selector in selectors:
            try:
                await page.wait_for_selector(selector, timeout=5000)
                print(f"Found selector: {selector}")
                break
            except Exception:
                continue
                
        # Give Angular app time to render everything
        await asyncio.sleep(2)
        
        # Use more specific JavaScript extractions matching the exact HTML structure
        extracted_data = await page.evaluate("""() => {
            const data = {};
            
            // TITLE - Use the exact selector from the provided HTML
            try {
                const titleEl = document.querySelector('.listing_dt_logo-box .cptn h1.ttl');
                if (titleEl) {
                    data.title = titleEl.textContent.trim();
                }
            } catch (e) {
                console.error("Error extracting title:", e);
            }
            
            // LOGO URL
            try {
                const logoEl = document.querySelector('.listing_dt_logo-box .my_sect .logo img, .logo img[width="110"][height="110"]');
                if (logoEl) {
                    data.logo_url = logoEl.getAttribute('src');
                }
            } catch (e) {
                console.error("Error extracting logo:", e);
            }
            
            // BANNER URL - Look specifically in the main banner area
            try {
                // Try multiple possible banner selectors
                const bannerSelectors = [
                    // Detail page banner
                    '.banner img', 
                    '.opp-header__banner-img',
                    // Listing page thumbnail that could be a banner
                    'div.img img',
                    'd2c-img img',
                    '.listing_dt_logo-box .my_sect .logo img',
                    'img[alt*="banner"]',
                    'img[width="110"][height="110"]' 
                ];
                
                for (const selector of bannerSelectors) {
                    const bannerEl = document.querySelector(selector);
                    if (bannerEl) {
                        data.banner_url = bannerEl.getAttribute('src');
                        break;
                    }
                }
            } catch (e) {
                console.error("Error extracting banner:", e);
            }
            
            // ORGANIZER - Get the text inside the anchor tag within h3
            try {
                const organizerSelectors = [
                    '.listing_dt_logo-box .cptn h3 a', 
                    'h3 a[href*="/c/"]',
                    'p:contains("University")', // For listing items with University/College names
                    '.content p' // From the listing example
                ];
                
                for (const selector of organizerSelectors) {
                    const organizerEl = document.querySelector(selector);
                    if (organizerEl) {
                        data.organizer = organizerEl.textContent.trim();
                        break;
                    }
                }
            } catch (e) {
                console.error("Error extracting organizer:", e);
            }
            
            // LOCATION - Look for the location div with specific icon
            try {
                const locationEls = document.querySelectorAll('.location');
                for (const el of locationEls) {
                    // Check if this element has the location icon
                    if (el.querySelector('un-icon [style*="location_on_color.svg"]')) {
                        data.location = el.querySelector('div').textContent.trim();
                        
                        // Determine mode based on location text
                        if (data.location.toLowerCase().includes('online')) {
                            data.mode = 'Online';
                        } else {
                            data.mode = 'Offline';
                        }
                        break;
                    }
                }
            } catch (e) {
                console.error("Error extracting location:", e);
            }
            
            // TAGS - From the specific tags div
            try {
                // Try multiple tag selectors
                const tagSelectors = [
                    '.tags[_ngcontent-serverapp-c482636425] div',
                    '.skills .chip_text',
                    '.chip_text',
                    '.skill_list .un-chip-items .chip_text'
                ];
                
                for (const selector of tagSelectors) {
                    const tagsEls = document.querySelectorAll(selector);
                    if (tagsEls.length > 0) {
                        data.tags = Array.from(tagsEls).map(el => el.textContent.trim());
                        break;
                    }
                }
            } catch (e) {
                console.error("Error extracting tags:", e);
            }
            
            // REGISTRATION DEADLINE - Specific selector from the provided HTML
            try {
                // Try multiple deadline selectors including the Angular component structure
                const deadlineSelectors = [
                    // Angular component selectors from the provided HTML
                    'ul.deadlines li .cptn:has(span:contains("Registration Deadline")) strong',
                    'ul[_ngcontent-serverapp-c2061680160] li .cptn:has(span:contains("Registration Deadline")) strong',
                    'ul.deadlines li .cptn strong',
                    
                    // Previous selectors as fallbacks
                    '.reg_box .item span:contains("Registration Deadline") + strong',
                    '.cptn span:contains("Registration Deadline") + strong',
                    '.deadlines li .cptn:contains("Registration Deadline") strong',
                    '.item .cptn:contains("Registration Deadline") strong'
                ];
                
                for (const selector of deadlineSelectors) {
                    try {
                        const deadlineEl = document.querySelector(selector);
                        if (deadlineEl) {
                            data.registration_deadline = deadlineEl.textContent.trim();
                            break;
                        }
                    } catch (e) {
                        // Continue trying other selectors
                    }
                }
                
                // If we still don't have a deadline, try scanning all deadline items
                if (!data.registration_deadline) {
                    const allDeadlines = document.querySelectorAll('ul.deadlines li, ul[_ngcontent-serverapp-c] li');
                    for (const item of allDeadlines) {
                        const itemText = item.textContent.toLowerCase();
                        if (itemText.includes('registration deadline')) {
                            const strong = item.querySelector('strong');
                            if (strong) {
                                data.registration_deadline = strong.textContent.trim();
                                break;
                            }
                        }
                    }
                }
            } catch (e) {
                console.error("Error extracting registration deadline:", e);
            }
            
            // START DATE and END DATE from important dates section
            try {
                // Look for the "Important dates & deadlines" section
                const dateItems = document.querySelectorAll('ul.deadlines li, ul[_ngcontent-serverapp-c] li');
                
                // Scan all date items for event start and end dates
                for (const item of dateItems) {
                    const itemText = item.textContent.toLowerCase();
                    const dateValue = item.querySelector('strong')?.textContent.trim();
                    
                    if (dateValue) {
                        if (itemText.includes('start of') || itemText.includes('start date') || 
                            itemText.includes('event start') || itemText.includes('hackathon start')) {
                            data.start_date = dateValue;
                        }
                        else if (itemText.includes('end of') || itemText.includes('end date') || 
                                itemText.includes('event end') || itemText.includes('hackathon end')) {
                            data.end_date = dateValue;
                        }
                        // If we find dates that look like the camp/hackathon event dates, record them
                        else if (itemText.includes('camp') || itemText.includes('event') || 
                                itemText.includes('round 1') || itemText.includes('phase 1')) {
                            // If no start date yet, use this as a potential start date
                            if (!data.start_date) {
                                data.start_date = dateValue;
                            }
                        }
                    }
                }
                
                // Convert date formats to be consistent
                if (data.start_date) {
                    // Extract just the date part if there's time included (ignoring time and timezone)
                    const dateMatch = data.start_date.match(/(\d{1,2}\s+[A-Za-z]{3}\s+\d{2,4})/);
                    if (dateMatch) {
                        data.start_date = dateMatch[1];
                    }
                }
                
                if (data.end_date) {
                    // Extract just the date part if there's time included
                    const dateMatch = data.end_date.match(/(\d{1,2}\s+[A-Za-z]{3}\s+\d{2,4})/);
                    if (dateMatch) {
                        data.end_date = dateMatch[1];
                    }
                }
            } catch (e) {
                console.error("Error extracting event start/end dates:", e);
            }
            
            // TEAM SIZE - From the provided HTML structure with group icon
            try {
                const teamSizeSelectors = [
                    // Specific selector for the structure shared by the user
                    'div.item:has(un-icon span[style*="group.svg"]) .cptn strong',
                    'div.item:has(span[apptranslate="teamSize"]) strong',
                    '.cptn:has(span:contains("Team Size")) strong',
                    
                    // Previous selectors as fallback
                    '.item .cptn span:contains("Team Size") + strong',
                    '.reg_box .item .cptn:contains("Team Size") strong'
                ];
                
                for (const selector of teamSizeSelectors) {
                    const teamSizeEl = document.querySelector(selector);
                    if (teamSizeEl) {
                        data.team_size = teamSizeEl.textContent.trim();
                        
                        // Determine participation type based on team size
                        if (data.team_size.includes('Individual') || 
                            data.team_size.includes('1 Member') || 
                            data.team_size.toLowerCase().includes('individual')) {
                            data.participation_type = 'Individual';
                        } else {
                            data.participation_type = 'Team';
                        }
                        break;
                    }
                }
                
                // Fallback to looking in any elements with "Team Size" text
                if (!data.team_size) {
                    document.querySelectorAll('.item, .reg_box .item').forEach(el => {
                        if (el.textContent.includes('Team Size')) {
                            const strong = el.querySelector('strong');
                            if (strong) {
                                data.team_size = strong.textContent.trim();
                            }
                        }
                    });
                }
                
                // Additional fallback - check for register_count with limited slots
                // Format: "18 / 20 (Limited Slots)" indicates team size limit of 20
                if (!data.team_size) {
                    const registerCountEl = document.querySelector('.register_count');
                    if (registerCountEl && registerCountEl.textContent.includes('/')) {
                        const countParts = registerCountEl.textContent.trim().split('/');
                        if (countParts.length > 1) {
                            const maxCount = countParts[1].trim().split(' ')[0].trim();
                            if (!isNaN(parseInt(maxCount))) {
                                data.team_size = `Max ${maxCount} Members`;
                            }
                        }
                    }
                }
            } catch (e) {
                console.error("Error extracting team size:", e);
            }
            
            // PARTICIPANTS/IMPRESSIONS - Also target the specific HTML structure
            try {
                const impressionSelectors = [
                    // From the specific HTML provided by user
                    '.register_count',
                    'div.item:has(un-icon span[style*="stars.svg"]) .cptn strong',
                    'div.item:has(span[apptranslate="impressions"]) strong',
                    
                    // Previous selectors
                    '.reg_box .item span:contains("Impressions") + strong',
                    '.item .cptn:contains("Impressions") strong',
                    '.item .cptn:contains("Registered") strong',
                    '.circle-progress .register_count',
                    '.seperate_box:contains("Registered")'
                ];
                
                for (const selector of impressionSelectors) {
                    try {
                        const impressionEl = document.querySelector(selector);
                        if (impressionEl) {
                            let participantText = impressionEl.textContent.trim();
                            
                            // Clean up the text
                            participantText = participantText.replace('Registered', '').trim();
                            
                            // If it looks like "18 / 20", get just the first number
                            if (participantText.includes('/')) {
                                participantText = participantText.split('/')[0].trim();
                            }
                            
                            data.participants = participantText;
                            break;
                        }
                    } catch (e) {
                        // Continue trying other selectors
                    }
                }
            } catch (e) {
                console.error("Error extracting participants:", e);
            }
            
            // PRIZE POOL - Try both detail and listing page formats
            try {
                // First try the listing page prize format
                const listingPrizeEl = document.querySelector('.seperate_box.prize, div:contains("🏆")');
                if (listingPrizeEl) {
                    const prizeText = listingPrizeEl.textContent.trim();
                    // Extract just the numeric amount
                    const prizeMatch = prizeText.match(/[₹₨]?\s?([0-9,]+)/);
                    if (prizeMatch && prizeMatch[1]) {
                        const amount = prizeMatch[1].replace(/,/g, '');
                        data.prize_pool = `₹${amount}`;
                    } else {
                        data.prize_pool = prizeText;
                    }
                }
                
                // Then try the detail page formats for prizes
                if (!data.prize_pool) {
                    const prizeEls = document.querySelectorAll('app-competition-prizes-form .opp_types .item');
                    if (prizeEls.length > 0) {
                        const prizes = [];
                        let totalPrize = 0;
                        
                        prizeEls.forEach(prize => {
                            const category = prize.querySelector('h4')?.textContent.trim() || '';
                            const amount = prize.querySelector('.trophy')?.textContent.trim() || '';
                            
                            // Try to extract numeric amount
                            let numericAmount = 0;
                            if (amount) {
                                const amountMatch = amount.match(/[0-9,]+/);
                                if (amountMatch) {
                                    numericAmount = parseInt(amountMatch[0].replace(/,/g, ''), 10);
                                    totalPrize += numericAmount;
                                }
                            }
                            
                            prizes.push({
                                category,
                                amount,
                                numeric_amount: numericAmount
                            });
                        });
                        
                        data.prizes = prizes;
                        
                        // Set overall prize pool
                        if (totalPrize > 0) {
                            data.prize_pool = `₹${totalPrize}`;
                        }
                    }
                }
            } catch (e) {
                console.error("Error extracting prize pool:", e);
            }
            
            // DESCRIPTION - From the app-competition-about-form section
            try {
                const descriptionEl = document.querySelector('.un_editor_text_live');
                if (descriptionEl) {
                    data.description = descriptionEl.textContent.trim();
                }
            } catch (e) {
                console.error("Error extracting description:", e);
            }
            
            // ELIGIBILITY - From the eligibility section
            try {
                const eligibilitySelectors = [
                    '.eligibility_sect .eligi',
                    '.eligibility_sect .items .eligi'
                ];
                
                for (const selector of eligibilitySelectors) {
                    const eligibilityEls = document.querySelectorAll(selector);
                    if (eligibilityEls.length > 0) {
                        data.eligibility = Array.from(eligibilityEls).map(el => el.textContent.trim());
                        break;
                    }
                }
            } catch (e) {
                console.error("Error extracting eligibility:", e);
            }
            
            // STAGES/ROUNDS
            try {
                const stagesEls = document.querySelectorAll('app-competition-round-form .rounds .list');
                if (stagesEls.length > 0) {
                    const stages = [];
                    stagesEls.forEach(stage => {
                        const stageName = stage.querySelector('h4')?.textContent.trim();
                        const stageDesc = stage.querySelector('p')?.textContent.trim();
                        
                        // Get dates from this stage
                        let stageStartDate = '';
                        let stageEndDate = '';
                        
                        const dateEls = stage.querySelectorAll('.date div');
                        dateEls.forEach(dateEl => {
                            const dateText = dateEl.textContent.trim();
                            if (dateText.includes('Start:')) {
                                stageStartDate = dateText.replace('Start:', '').trim();
                            } else if (dateText.includes('End:')) {
                                stageEndDate = dateText.replace('End:', '').trim();
                            }
                        });
                        
                        stages.push({
                            name: stageName,
                            description: stageDesc,
                            start_date: stageStartDate,
                            end_date: stageEndDate
                        });
                    });
                    
                    data.stages = stages;
                    
                    // Set hackathon start/end dates from first and last stage
                    if (stages.length > 0) {
                        if (stages[0].start_date) {
                            data.start_date = stages[0].start_date;
                        }
                        
                        if (stages[stages.length - 1].end_date) {
                            data.end_date = stages[stages.length - 1].end_date;
                        }
                    }
                }
            } catch (e) {
                console.error("Error extracting stages:", e);
            }
            
            // Look for days left (alternative date information)
            try {
                const daysLeftEl = document.querySelector('.other_fields .seperate_box:contains("days left")');
                if (daysLeftEl) {
                    data.days_left = daysLeftEl.textContent.trim();
                    
                    // If we don't have end_date, try to infer from days left
                    if (!data.end_date && data.days_left) {
                        // Extract just the number of days
                        const daysMatch = data.days_left.match(/(\d+)/);
                        if (daysMatch && daysMatch[1]) {
                            const daysLeft = parseInt(daysMatch[1], 10);
                            
                            // Create a date daysLeft days from now
                            const endDate = new Date();
                            endDate.setDate(endDate.getDate() + daysLeft);
                            
                            // Format as DD MMM YY
                            const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
                            data.end_date = `${endDate.getDate()} ${months[endDate.getMonth()]} ${endDate.getFullYear().toString().substr(-2)}`;
                        }
                    }
                }
            } catch (e) {
                console.error("Error extracting days left:", e);
            }
            
            // FEE - Check if it's free or paid
            try {
                const feeEl = document.querySelector('.reg_fee');
                if (feeEl) {
                    data.fee = feeEl.textContent.trim();
                }
            } catch (e) {
                console.error("Error extracting fee:", e);
            }
            
            return data;
        }""")
        
        # Merge extracted data into hackathon_data
        if extracted_data:
            hackathon_data.update(extracted_data)
        
        # Convert complex objects to JSON strings for CSV compatibility
        if "stages" in hackathon_data:
            hackathon_data["stages_json"] = json.dumps(hackathon_data["stages"])
            del hackathon_data["stages"]
            
        if "prizes" in hackathon_data:
            hackathon_data["prizes_json"] = json.dumps(hackathon_data["prizes"])
            del hackathon_data["prizes"]
            
        if "eligibility" in hackathon_data and isinstance(hackathon_data["eligibility"], list):
            hackathon_data["eligibility"] = ", ".join(hackathon_data["eligibility"])
            
        if "tags" in hackathon_data and isinstance(hackathon_data["tags"], list):
            hackathon_data["tags"] = ", ".join(hackathon_data["tags"])
        
        # Ensure required fields have values
        if not hackathon_data.get("title"):
            hackathon_data["title"] = f"Hackathon {hackathon_id}"
        
        # Never use placeholder dates
        if not hackathon_data.get("start_date") or hackathon_data.get("start_date") == "01 Jan 1970":
            # Check if we have a registration deadline to estimate from
            reg_deadline = hackathon_data.get("registration_deadline", "")
            if reg_deadline and reg_deadline not in ["01 Jan 1970", ""]:
                # Try to parse the registration deadline and set start date to 1 week after
                try:
                    for date_format in ["%d %b %y", "%d %B %Y", "%B %d %Y", "%b %d %Y", "%d %b %y, %I:%M %p %Z"]:
                        try:
                            # Extract date part if there's time included
                            date_part = re.match(r"(\d{1,2}\s+[A-Za-z]{3,}\s+\d{2,4})", reg_deadline)
                            if date_part:
                                date_str = date_part.group(1)
                            else:
                                date_str = reg_deadline
                                
                            deadline_date = datetime.strptime(date_str, date_format)
                            # Start date is 1 week after registration deadline
                            start_date = deadline_date + pd.Timedelta(days=7)
                            hackathon_data["start_date"] = start_date.strftime("%d %b %y")
                            print(f"Setting start date from registration deadline: {hackathon_data['start_date']}")
                            break
                        except ValueError:
                            continue
                except Exception as e:
                    print(f"Error setting start date from registration deadline: {e}")
            
            # If we still don't have a start date, use a default
            if not hackathon_data.get("start_date") or hackathon_data.get("start_date") == "01 Jan 1970":
                # Default to 2 weeks from now
                default_start = datetime.now() + pd.Timedelta(days=14)
                hackathon_data["start_date"] = default_start.strftime("%d %b %y")
                print(f"Setting default start date: {hackathon_data['start_date']}")
        
        if not hackathon_data.get("end_date") or hackathon_data.get("end_date") == "01 Jan 1970":
            # Parse the start date (which should now have a value)
            try:
                start_date = None
                for date_format in ["%d %b %y", "%d %B %Y", "%B %d %Y", "%b %d %Y"]:
                    try:
                        start_date = datetime.strptime(hackathon_data["start_date"], date_format)
                        break
                    except ValueError:
                        continue
                
                if start_date:
                    # Set end date to start date + 4 weeks
                    end_date = start_date + pd.Timedelta(days=28)
                    hackathon_data["end_date"] = end_date.strftime("%d %b %y")
                    print(f"Setting end date based on start date: {hackathon_data['end_date']}")
                else:
                    # If start date parsing failed, set end date to 6 weeks from now
                    end_date = datetime.now() + pd.Timedelta(days=42)
                    hackathon_data["end_date"] = end_date.strftime("%d %b %y")
                    print(f"Setting fallback end date: {hackathon_data['end_date']}")
            except Exception as e:
                # If all else fails
                end_date = datetime.now() + pd.Timedelta(days=42)
                hackathon_data["end_date"] = end_date.strftime("%d %b %y")
                print(f"Error setting end date, using default: {hackathon_data['end_date']}, Error: {e}")
        
        if not hackathon_data.get("mode"):
            hackathon_data["mode"] = "Online"  # Default to Online as most hackathons are online these days
        
        # Add URL to data
        hackathon_data["url"] = url
        
        # Print the extracted data for debugging
        print(f"Extracted data for {hackathon_data.get('title', 'Unknown')}:")
        for key, value in hackathon_data.items():
            if isinstance(value, str):
                print(f"  - {key}: {value[:50]}..." if len(value) > 50 else f"  - {key}: {value}")
            else:
                print(f"  - {key}: {value}")
        
        return hackathon_data
        
    except Exception as e:
        print(f"Error extracting details from {url}: {e}")
        # Return minimal data with error information
        default_start = datetime.now() + pd.Timedelta(days=14)
        default_end = default_start + pd.Timedelta(days=28)
        return {
            "title": f"Hackathon {hackathon_id}",
            "url": url,
            "start_date": default_start.strftime("%d %b %y"),
            "end_date": default_end.strftime("%d %b %y"),
            "mode": "Online",  # Default to online
            "description": f"Error: {str(e)}",
            "logo_url": "",
            "banner_url": "",
            "organizer": "Error",
            "tags": [],
            "prize_pool": "Unknown",
            "participants": "Unknown"
        }

async def login_to_unstop(page):
    """Login to Unstop to access more hackathons."""
    print("Attempting to login to Unstop...")
    
    # Check if we can use stored credentials
    if not LOGIN_EMAIL or not LOGIN_PASSWORD:
        print("No login credentials provided. Will try to continue without login.")
        return False
    
    try:
        # Navigate directly to login page
        login_url = "https://unstop.com/auth/login?returnUrl=%2Fhackathons%3Foppstatus%3Dopen&quickApply=true"
        print(f"Navigating to login page: {login_url}")
        
        await page.goto(login_url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_load_state("networkidle", timeout=30000)
        
        # Check for cookie consent dialog and accept it
        try:
            cookie_button = await page.query_selector('button:has-text("Accept"), button:has-text("I agree"), .cookie-accept')
            if cookie_button:
                await cookie_button.click()
                print("Clicked on cookie consent button")
                await page.wait_for_timeout(2000)
        except Exception as e:
            print(f"No cookie consent button found or error: {e}")
        
        # Wait for login form
        print("Waiting for login form...")
        await page.wait_for_selector('input[type="email"], input[name="email"], input[placeholder*="Email"]', timeout=30000)
        
        # Enter email
        await page.fill('input[type="email"], input[name="email"], input[placeholder*="Email"]', LOGIN_EMAIL)
        print("Entered email")
        
        # Enter password
        await page.fill('input[type="password"], input[name="password"], input[placeholder*="Password"]', LOGIN_PASSWORD)
        print("Entered password")
        
        # Take a screenshot of the login form (for debugging)
        if DEBUG_MODE:
            await page.screenshot(path="login_form.png")
            print("Saved login form screenshot")
        
        # Click login/submit button
        login_submit = await page.query_selector('button:has-text("Login"), button[type="submit"], .login-btn')
        if login_submit:
            await login_submit.click()
            print("Clicked login submit button")
            
            # Wait for navigation to complete
            await page.wait_for_load_state("networkidle", timeout=30000)
            
            # Check if login was successful
            if await page.query_selector('a:has-text("Profile"), a:has-text("Dashboard"), .user-avatar'):
                print("Login successful!")
                return True
            else:
                print("Login might have failed. Checking for error messages...")
                
                # Check for error messages
                error_message = await page.query_selector('.error-message, .alert-danger, [class*="error"]')
                if error_message:
                    message_text = await error_message.text_content()
                    print(f"Login error: {message_text}")
                
                print("Continuing anyway...")
                return False
        else:
            print("Could not find login submit button")
            return False
            
    except Exception as e:
        print(f"Error during login: {e}")
        return False

async def crawl_unstop_hackathons():
    """Main function to crawl Unstop hackathons."""
    try:
        # Launch browser with performance optimizations
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,  # Set back to headless for speed
                args=[
                    '--disable-gpu',
                    '--disable-dev-shm-usage',
                    '--disable-setuid-sandbox',
                    '--no-sandbox',
                    '--single-process',  # Less resource usage
                    '--disable-extensions',
                    '--disable-background-networking',
                    '--disable-default-apps',
                    '--disable-sync',
                    '--disable-translate',
                    '--hide-scrollbars',
                    '--metrics-recording-only',
                    '--mute-audio',
                    '--no-first-run',
                    '--safebrowsing-disable-auto-update',
                ]
            )
            
            context = await browser.new_context(
                viewport={"width": 1280, "height": 800},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                java_script_enabled=True,
                ignore_https_errors=True,
                bypass_csp=True
            )
            
            # Set navigation timeout lower to fail faster on problematic pages
            context.set_default_navigation_timeout(20000)
            context.set_default_timeout(10000)
            
            # Set up a semaphore to limit concurrent requests
            semaphore = asyncio.Semaphore(PARALLEL_WORKERS)
            
            # Create a page
            page = await context.new_page()
            
            # Login to Unstop
            await login_to_unstop(page)
            
            # Navigate to the hackathon listing page
            hackathon_list_url = f"{BASE_URL}{DEFAULT_PARAMS}"
            print(f"Navigating to: {hackathon_list_url}")
            await page.goto(hackathon_list_url, wait_until="domcontentloaded")
            await page.wait_for_load_state("networkidle", timeout=15000)
            
            # Extract hackathon links
            hackathon_links = await extract_hackathon_links(page)
            
            # Deduplicate links based on hackathon ID, not just URL
            print(f"Found {len(hackathon_links)} total hackathon links, deduplicating...")
            
            # Extract hackathon IDs from URLs for deduplication
            deduplicated_links = []
            seen_ids = set()
            
            for url in hackathon_links:
                # Extract the ID from the URL
                hackathon_id = None
                
                # Try different URL patterns
                id_match1 = re.search(r'/hackathon/(\d+)', url)
                id_match2 = re.search(r'/p/[^-]+-(\d+)', url)
                
                if id_match1:
                    hackathon_id = id_match1.group(1)
                elif id_match2:
                    hackathon_id = id_match2.group(1)
                else:
                    # If we can't extract an ID, just use the full URL
                    hackathon_id = url
                
                # Only add if we haven't seen this ID before
                if hackathon_id not in seen_ids:
                    seen_ids.add(hackathon_id)
                    deduplicated_links.append(url)
            
            unique_links = deduplicated_links
            print(f"Deduplicated to {len(unique_links)} unique hackathons")
            
            # Limit the number of hackathons to process
            unique_links = unique_links[:MAX_HACKATHONS]
            
            # Process hackathons in parallel with a semaphore to control concurrency
            async def process_hackathon_with_semaphore(url):
                async with semaphore:
                    try:
                        # Create a new page for each hackathon to avoid state issues
                        hackathon_page = await context.new_page()
                        hackathon_details = await extract_hackathon_details(hackathon_page, url)
                        await hackathon_page.close()
                        
                        # Never return hackathon with placeholder dates
                        if hackathon_details:
                            # If we still have invalid dates after extraction, set defaults
                            if hackathon_details.get("start_date") == "01 Jan 1970" or not hackathon_details.get("start_date"):
                                # Set to 2 weeks from now
                                default_start = datetime.now() + pd.Timedelta(days=14)
                                hackathon_details["start_date"] = default_start.strftime("%d %b %y")
                                print(f"Setting default start date for {hackathon_details.get('title')}: {hackathon_details['start_date']}")
                            
                            if hackathon_details.get("end_date") == "01 Jan 1970" or not hackathon_details.get("end_date"):
                                # Parse start date (which is now guaranteed to have a value)
                                start_date = None
                                try:
                                    for date_format in ["%d %b %y", "%d %B %Y", "%B %d %Y", "%b %d %Y"]:
                                        try:
                                            start_date = datetime.strptime(hackathon_details["start_date"], date_format)
                                            break
                                        except ValueError:
                                            continue
                                except Exception:
                                    start_date = datetime.now() + pd.Timedelta(days=14)
                                
                                # Set end date to start date + 4 weeks
                                default_end = start_date + pd.Timedelta(days=28)
                                hackathon_details["end_date"] = default_end.strftime("%d %b %y")
                                print(f"Setting default end date for {hackathon_details.get('title')}: {hackathon_details['end_date']}")
                        
                        return hackathon_details
                    except Exception as e:
                        print(f"Error processing {url}: {e}")
                        return None
            
            # Process hackathons in parallel
            print(f"Processing {len(unique_links)} hackathons with {PARALLEL_WORKERS} parallel workers...")
            tasks = [process_hackathon_with_semaphore(url) for url in unique_links]
            hackathon_details_list = await asyncio.gather(*tasks)
            
            # Filter out None values (failed extractions)
            hackathon_details_list = [details for details in hackathon_details_list if details]
            
            # Process the descriptions to extract additional information 
            print("Analyzing descriptions to extract additional information...")
            for details in hackathon_details_list:
                if details and details.get("description"):
                    # Process each hackathon to extract information from description
                    extract_info_from_description(details)
            
            # Additional deduplication step based on title and start date
            deduplicated_hackathons = {}
            
            # Print all hackathon IDs for debugging
            print("\nIdentifying unique hackathons to remove duplicates...")
            
            for details in hackathon_details_list:
                if details:
                    # Extract ID from URL for a reliable deduplication key
                    url = details.get("url", "")
                    hackathon_id = None
                    
                    # Try different URL patterns to extract ID - fix regex to properly capture ID
                    id_pattern = re.compile(r'/hackathon/(\d+)|/p/.*?-(\d+)')
                    id_match = id_pattern.search(url)
                    
                    if id_match:
                        # Get whichever group matched (group 1 or group 2)
                        hackathon_id = id_match.group(1) if id_match.group(1) else id_match.group(2)
                        print(f"Found ID {hackathon_id} from URL: {url}")
                    
                    if hackathon_id:
                        # Use ID as the key
                        key = hackathon_id
                    else:
                        # Normalize title by removing special characters and converting to lowercase
                        title = details.get("title", "")
                        normalized_title = re.sub(r'[^\w\s]', '', title).lower()
                        normalized_title = re.sub(r'\s+', '_', normalized_title)
                        
                        # Use normalized title and start_date as key
                        start_date = details.get("start_date", "")
                        key = f"{normalized_title}_{start_date}"
                    
                    print(f"Deduplication key: {key}")
                    
                    # Keep the more complete entry if we have duplicates
                    if key not in deduplicated_hackathons:
                        deduplicated_hackathons[key] = details
                    else:
                        # If we already have this hackathon, compare completeness
                        current = deduplicated_hackathons[key]
                        new = details
                        
                        # Check if new entry has more filled fields
                        current_empty = sum(1 for k, v in current.items() if not v or v in ["Unknown", "01 Jan 1970"])
                        new_empty = sum(1 for k, v in new.items() if not v or v in ["Unknown", "01 Jan 1970"])
                        
                        print(f"  - Found duplicate: {key}")
                        print(f"  - Current entry has {current_empty} empty fields")
                        print(f"  - New entry has {new_empty} empty fields")
                        
                        if new_empty < current_empty:
                            print(f"  - Replacing less complete entry for {key}")
                            deduplicated_hackathons[key] = new
                        else:
                            print(f"  - Keeping existing entry for {key}")
            
            # Convert deduplicated dictionary back to list
            hackathon_details_list = list(deduplicated_hackathons.values())
            print(f"Final deduplication: {len(hackathon_details_list)} unique hackathons")
            
            # Create a list of hackathons
            hackathons = []
            for details in hackathon_details_list:
                if details:
                    hackathon = {
                        "logo_url": details.get("logo_url", ""),
                        "banner_url": details.get("banner_url", ""),
                        "title": details.get("title", ""),
                        "organizer": details.get("organizer", ""),
                        "description": details.get("description", ""),
                        "mode": details.get("mode", ""),
                        "start_date": details.get("start_date", ""),
                        "end_date": details.get("end_date", ""),
                        "tags": details.get("tags", ""),
                        "prize_pool": details.get("prize_pool", "Unknown"),
                        "participants": details.get("participants", "Unknown"),
                        "team_size": details.get("team_size", ""),
                        "registration_deadline": details.get("registration_deadline", ""),
                        "url": details.get("url", ""),
                        "referral_url": details.get("referral_url", "")  # Add the referral URL to output
                    }
                    hackathons.append(hackathon)
            
            # FINAL VALIDATION: Make one last check for invalid dates before saving
            print("\nPerforming final validation to ensure no invalid dates remain...")
            for hackathon in hackathons:
                # If any hackathon still has invalid dates, set reasonable defaults
                if hackathon["start_date"] == "01 Jan 1970" or not hackathon["start_date"]:
                    default_start = datetime.now() + pd.Timedelta(days=14)
                    hackathon["start_date"] = default_start.strftime("%d %b %y")
                    print(f"Final fix: Setting default start date for {hackathon['title']}")
                
                if hackathon["end_date"] == "01 Jan 1970" or not hackathon["end_date"]:
                    # Parse the start date which is now guaranteed to exist
                    try:
                        for date_format in ["%d %b %y", "%d %B %Y", "%B %d %Y", "%b %d %Y"]:
                            try:
                                start_date = datetime.strptime(hackathon["start_date"], date_format)
                                # Set end date to start date + 4 weeks
                                default_end = start_date + pd.Timedelta(days=28)
                                hackathon["end_date"] = default_end.strftime("%d %b %y")
                                print(f"Final fix: Setting default end date for {hackathon['title']}")
                                break
                            except ValueError:
                                continue
                    except Exception:
                        # If all else fails, set end date to 6 weeks from now
                        default_end = datetime.now() + pd.Timedelta(days=42)
                        hackathon["end_date"] = default_end.strftime("%d %b %y")
                        print(f"Final fix: Setting emergency default end date for {hackathon['title']}")
            
            if hackathons:
                print(f"Successfully extracted {len(hackathons)} hackathons")
                
                # Convert to DataFrame and save to CSV
                df = pd.DataFrame(hackathons)
                df.to_csv(OUTPUT_CSV, index=False)
                print(f"Saved data to {OUTPUT_CSV}")
                
                # Check which hackathons are missing required fields
                missing_required_fields = []
                for i, hackathon in enumerate(hackathons):
                    missing_fields = [field for field in REQUIRED_FIELDS if not hackathon.get(field)]
                    if missing_fields:
                        missing_required_fields.append((i, hackathon["title"], missing_fields))
                
                if missing_required_fields:
                    print("\nSome hackathons are missing required fields:")
                    for i, title, fields in missing_required_fields:
                        print(f"  - #{i+1} {title}: Missing {', '.join(fields)}")
                else:
                    print("\nAll hackathons have the required fields")
                
                # Also save to JSON for easier debugging
                with open(OUTPUT_CSV.replace(".csv", ".json"), "w", encoding="utf-8") as f:
                    json.dump(hackathons, f, indent=2)
                
                return hackathons
            else:
                print("No hackathons found!")
                return []
                
    except Exception as e:
        print(f"Error during crawling: {e}")
        return []

def extract_info_from_description(hackathon_data):
    """Extract missing information from the description text"""
    description = hackathon_data.get("description", "")
    if not description:
        return
    
    title = hackathon_data.get("title", "Unknown")
    print(f"Analyzing description for {title}")
    print(f"  - Description preview: {description[:100]}...")
    
    # Get registration deadline first as it might help with date inference
    reg_deadline = hackathon_data.get("registration_deadline", "")
    if reg_deadline:
        print(f"  - Registration deadline already found: {reg_deadline}")
    
    # Extract prize pool information if missing
    if hackathon_data.get("prize_pool") == "Unknown" or not hackathon_data.get("prize_pool"):
        # Look for prize pool mentions with rupee symbols or prize words
        prize_patterns = [
            r'prize\s+pool\s*(?:of)?\s*[₹₨]?\s*([0-9,]+)', # Prize pool of ₹100,000
            r'[pP]rizes?\s*(?:&|and)?\s*[rR]ewards?.*?[₹₨]\s*([0-9,]+)', # Prizes & Rewards: ₹100,000
            r'[pP]rize\s*[pP]ool.*?[₹₨]\s*([0-9,]+)', # Prize Pool: ₹100,000
            r'[₹₨]\s*([0-9,]+).*?[pP]rize', # ₹100,000 prize pool
            r'[tT]otal\s*[pP]rize\s*[₹₨]\s*([0-9,]+)', # Total Prize ₹100,000
        ]
        
        for pattern in prize_patterns:
            prize_match = re.search(pattern, description, re.IGNORECASE | re.DOTALL)
            if prize_match:
                prize_amount = prize_match.group(1).replace(',', '')
                hackathon_data["prize_pool"] = f"₹{prize_amount}"
                print(f"  - Extracted prize pool: {hackathon_data['prize_pool']}")
                break
    
    # Extract date information if missing
    date_extraction_attempted = False
    
    # First try to find explicit dates in the description
    if hackathon_data.get("start_date") == "01 Jan 1970" or not hackathon_data.get("start_date"):
        date_extraction_attempted = True
        print(f"  - Looking for explicit dates in description for {title}")
        
        # Look for date patterns with more variations
        date_patterns = [
            # Round-specific dates like "Round 1: April 01 - April 15"
            r'[rR]ound\s*\d+.*?([A-Za-z]+\s+\d{1,2})[\s-]+([A-Za-z]+\s+\d{1,2})',
            # General start-end date ranges
            r'([A-Za-z]+\s+\d{1,2})[\s-]+([A-Za-z]+\s+\d{1,2})',
            r'([A-Za-z]+\s+\d{1,2})[\s-]+\d{1,2}',  # April 1-15
            # Dates with year specified
            r'([A-Za-z]+\s+\d{1,2},?\s*20\d{2})',
            # Dates with formats like "15th May"
            r'(\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]+)',
            # Event start/begins/ends phrases
            r'(?:event|hackathon|competition)\s+(?:start|begin|commence).*?([A-Za-z]+\s+\d{1,2})',
            r'(?:starting|beginning|commencing)\s+(?:on|from)?\s+([A-Za-z]+\s+\d{1,2})',
            r'(?:event|hackathon|competition)\s+(?:ends?|closing).*?([A-Za-z]+\s+\d{1,2})',
            # Specific event mentions like "will be held on"
            r'will\s+be\s+held\s+on\s+([A-Za-z]+\s+\d{1,2})',
            r'scheduled\s+for\s+([A-Za-z]+\s+\d{1,2})',
        ]
        
        all_dates = []
        for pattern in date_patterns:
            matches = re.finditer(pattern, description, re.IGNORECASE | re.DOTALL)
            for match in matches:
                if len(match.groups()) >= 2:  # Date range
                    start_date = match.group(1).strip()
                    end_date = match.group(2).strip()
                    all_dates.append((start_date, end_date))
                    print(f"  - Found date range: {start_date} to {end_date}")
                else:  # Single date
                    date = match.group(1).strip()
                    all_dates.append((date, None))
                    print(f"  - Found single date: {date}")
        
        if all_dates:
            # Process the dates to get valid datetime objects
            parsed_dates = []
            current_year = datetime.now().year
            
            for start, end in all_dates:
                # Try to parse the start date with multiple formats
                start_dt = None
                
                # Clean up the date string
                start = re.sub(r'(?:st|nd|rd|th)', '', start)  # Remove ordinal suffixes
                
                # Try multiple date formats
                date_formats = [
                    "%B %d %Y", "%b %d %Y",  # April 15 2024, Apr 15 2024
                    "%d %B %Y", "%d %b %Y",  # 15 April 2024, 15 Apr 2024
                    "%B %d", "%b %d",        # April 15, Apr 15
                    "%d %B", "%d %b"         # 15 April, 15 Apr
                ]
                
                # Add year if it's not in the string
                if not re.search(r'20\d{2}', start):
                    start_with_year = f"{start} {current_year}"
                else:
                    start_with_year = start
                
                # Try each format
                for fmt in date_formats:
                    try:
                        if fmt.endswith("%Y"):
                            start_dt = datetime.strptime(start_with_year, fmt)
                        else:
                            start_dt = datetime.strptime(f"{start} {current_year}", fmt)
                        print(f"  - Parsed date {start} as {start_dt.strftime('%d %b %Y')}")
                        break
                    except ValueError:
                        continue
                
                # Process end date if it exists
                end_dt = None
                if end:
                    # Clean up the date string
                    end = re.sub(r'(?:st|nd|rd|th)', '', end)
                    
                    # Add year if it's not in the string
                    if not re.search(r'20\d{2}', end):
                        end_with_year = f"{end} {current_year}"
                    else:
                        end_with_year = end
                    
                    # Try each format
                    for fmt in date_formats:
                        try:
                            if fmt.endswith("%Y"):
                                end_dt = datetime.strptime(end_with_year, fmt)
                            else:
                                end_dt = datetime.strptime(f"{end} {current_year}", fmt)
                            print(f"  - Parsed date {end} as {end_dt.strftime('%d %b %Y')}")
                            break
                        except ValueError:
                            continue
                
                # Only add if we could parse the start date
                if start_dt:
                    parsed_dates.append((start_dt, end_dt if end_dt else start_dt))
            
            if parsed_dates:
                # Sort by start date to find earliest
                parsed_dates.sort(key=lambda x: x[0])
                first_date = parsed_dates[0][0]
                
                # Find latest end date
                latest_date = None
                for _, end_dt in parsed_dates:
                    if end_dt and (latest_date is None or end_dt > latest_date):
                        latest_date = end_dt
                
                if not latest_date:
                    latest_date = parsed_dates[-1][0]  # Use last start date if no end dates
                
                # Format dates for hackathon data
                hackathon_data["start_date"] = first_date.strftime("%d %b %y")
                hackathon_data["end_date"] = latest_date.strftime("%d %b %y")
                
                print(f"  - SUCCESSFULLY extracted dates: {hackathon_data['start_date']} to {hackathon_data['end_date']}")
    
    # Try to infer from event duration if we have start date but not end date
    if date_extraction_attempted and ((hackathon_data.get("start_date") and hackathon_data.get("start_date") != "01 Jan 1970") and 
        (not hackathon_data.get("end_date") or hackathon_data.get("end_date") == "01 Jan 1970")):
        
        print(f"  - Looking for duration info to calculate end date for {title}")
        
        # Extract duration information
        duration_patterns = [
            # Durations like "three-week", "3-day", "48-hour", etc.
            r'(\d+)(?:-|\s+)(?:day|days|week|weeks|month|months)',
            r'(?:one|two|three|four|five|six|seven|eight|nine|ten)(?:-|\s+)(?:day|days|week|weeks|month|months)',
            r'(?:Event|Camp|Hackathon|Program)\s+Duration\s*:?\s*(\d+)(?:-|\s+)(?:day|days|week|weeks|month|months)',
            r'duration\s+of\s+(\d+|one|two|three|four|five|six|seven|eight|nine|ten)(?:-|\s+)(?:day|days|week|weeks|month|months)',
            r'(?:day|week|month)s?(?:\s+long)?(?:\s+event)?\s*:?\s*(\d+|one|two|three|four|five|six|seven|eight|nine|ten)',
            # Specific days count
            r'(\d+)\s+(?:day|week|month)s?',
            r'(one|two|three|four|five|six|seven|eight|nine|ten)\s+(?:day|week|month)s?',
            # Time spans
            r'(?:lasts?|runs?)\s+for\s+(\d+|one|two|three|four|five|six|seven|eight|nine|ten)(?:-|\s+)(?:day|days|week|weeks|month|months)',
        ]
        
        duration_days = 0
        for pattern in duration_patterns:
            duration_match = re.search(pattern, description, re.IGNORECASE | re.DOTALL)
            if duration_match:
                duration_text = duration_match.group(1).lower() if duration_match.groups() else ""
                
                # Convert text number to integer
                if duration_text in ["one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"]:
                    number_map = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, 
                                  "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10}
                    duration_value = number_map.get(duration_text, 0)
                else:
                    try:
                        duration_value = int(duration_text)
                    except (ValueError, TypeError):
                        duration_value = 0
                
                # Determine the unit (day, week, month)
                if "week" in duration_match.group(0).lower():
                    duration_days = duration_value * 7
                elif "month" in duration_match.group(0).lower():
                    duration_days = duration_value * 30
                else:  # days or hours
                    if "hour" in duration_match.group(0).lower():
                        # Convert hours to days (round up)
                        duration_days = max(1, (duration_value + 23) // 24)
                    else:
                        duration_days = duration_value
                
                print(f"  - Found duration: {duration_value} ({duration_days} days)")
                break
        
        # If no specific duration found, look for week counts (Week 1, Week 2, Week 3...)
        if duration_days == 0:
            week_counts = re.findall(r'[wW]eek\s+(\d+)', description)
            if week_counts:
                max_week = max(int(week) for week in week_counts)
                if max_week > 0:
                    duration_days = max_week * 7
                    print(f"  - Inferred duration from week count: {max_week} weeks ({duration_days} days)")
        
        # If no specific duration found, look for specific phase counts
        if duration_days == 0:
            phase_counts = re.findall(r'(?:[pP]hase|[rR]ound|[sS]tage)\s+(\d+)', description)
            if phase_counts:
                max_phase = max(int(phase) for phase in phase_counts)
                if max_phase > 0:
                    # Assume each phase takes approximately 1-2 weeks
                    duration_days = max_phase * 10
                    print(f"  - Inferred duration from phase count: {max_phase} phases (~{duration_days} days)")
        
        # If we found a duration and have a valid start date, calculate the end date
        if duration_days > 0 and hackathon_data.get("start_date") and hackathon_data.get("start_date") != "01 Jan 1970":
            try:
                # Parse the start date
                start_date_text = hackathon_data.get("start_date")
                # Try different format patterns for the start date
                start_date = None
                for date_format in ["%d %b %y", "%d %B %Y", "%B %d %Y", "%b %d %Y"]:
                    try:
                        start_date = datetime.strptime(start_date_text, date_format)
                        break
                    except ValueError:
                        continue
                
                if start_date:
                    # Calculate end date
                    end_date = start_date + pd.Timedelta(days=duration_days)
                    hackathon_data["end_date"] = end_date.strftime("%d %b %y")
                    print(f"  - SUCCESSFULLY calculated end date from duration: {hackathon_data['end_date']}")
            except Exception as e:
                print(f"  - Error calculating end date: {e}")
    
    # If we still don't have dates, try to use the registration deadline to estimate
    if (date_extraction_attempted and 
       (not hackathon_data.get("start_date") or hackathon_data.get("start_date") == "01 Jan 1970")):
        
        reg_deadline = hackathon_data.get("registration_deadline", "")
        if reg_deadline and reg_deadline not in ["01 Jan 1970", ""]:
            print(f"  - Using registration deadline to estimate event dates: {reg_deadline}")
            
            try:
                # Parse the registration deadline
                deadline_date = None
                for date_format in ["%d %b %y", "%d %B %Y", "%B %d %Y", "%b %d %Y", "%d %b %y, %I:%M %p %Z"]:
                    try:
                        # First try to extract just the date part if there's time included
                        date_part = re.match(r"(\d{1,2}\s+[A-Za-z]{3,}\s+\d{2,4})", reg_deadline)
                        if date_part:
                            date_str = date_part.group(1)
                        else:
                            date_str = reg_deadline
                            
                        deadline_date = datetime.strptime(date_str, date_format)
                        break
                    except ValueError:
                        continue
                
                if deadline_date:
                    # Assume event starts shortly after the registration deadline (1 week)
                    start_date = deadline_date + pd.Timedelta(days=7)
                    # Assume event lasts for a typical duration (3 weeks)
                    end_date = start_date + pd.Timedelta(days=21)
                    
                    hackathon_data["start_date"] = start_date.strftime("%d %b %y")
                    hackathon_data["end_date"] = end_date.strftime("%d %b %y")
                    print(f"  - SUCCESSFULLY estimated dates from registration deadline: {hackathon_data['start_date']} to {hackathon_data['end_date']}")
            except Exception as e:
                print(f"  - Error estimating dates from registration deadline: {e}")
    
    # FINAL FALLBACK: Set default dates if we still couldn't extract them
    # This ensures we never have the "01 Jan 1970" placeholder in final output
    if not hackathon_data.get("start_date") or hackathon_data.get("start_date") == "01 Jan 1970":
        # Default to starting 2 weeks from today
        default_start = datetime.now() + pd.Timedelta(days=14)
        hackathon_data["start_date"] = default_start.strftime("%d %b %y")
        print(f"  - WARNING: Using default start date: {hackathon_data['start_date']}")
        
        # Also set end date if needed (4 weeks after start)
        if not hackathon_data.get("end_date") or hackathon_data.get("end_date") == "01 Jan 1970":
            default_end = default_start + pd.Timedelta(days=28)
            hackathon_data["end_date"] = default_end.strftime("%d %b %y")
            print(f"  - WARNING: Using default end date: {hackathon_data['end_date']}")
    elif not hackathon_data.get("end_date") or hackathon_data.get("end_date") == "01 Jan 1970":
        # If we have start date but not end date, default to 4 weeks after start
        try:
            start_date_text = hackathon_data.get("start_date")
            start_date = None
            for date_format in ["%d %b %y", "%d %B %Y", "%B %d %Y", "%b %d %Y"]:
                try:
                    start_date = datetime.strptime(start_date_text, date_format)
                    break
                except ValueError:
                    continue
            
            if start_date:
                default_end = start_date + pd.Timedelta(days=28)
                hackathon_data["end_date"] = default_end.strftime("%d %b %y")
                print(f"  - WARNING: Using calculated end date (start+28 days): {hackathon_data['end_date']}")
            else:
                # If we couldn't parse the start date, use a default
                default_end = datetime.now() + pd.Timedelta(days=42)  # 6 weeks from now
                hackathon_data["end_date"] = default_end.strftime("%d %b %y")
                print(f"  - WARNING: Using default end date: {hackathon_data['end_date']}")
        except Exception as e:
            print(f"  - Error setting default end date: {e}")
    
    # Extract team size information if missing
    if not hackathon_data.get("team_size"):
        team_size_patterns = [
            r'[tT]eam\s*[sS]ize.*?(\d+)\s*(?:to|-)\s*(\d+)',  # Team Size: 1 to 5
            r'[tT]eam\s*[sS]ize.*?[mM]ax\s*(\d+)',  # Team Size: Max 5
            r'[tT]eam\s*[sS]ize.*?[mM]in\s*(\d+)',  # Team Size: Min 1
        ]
        
        for pattern in team_size_patterns:
            team_size_match = re.search(pattern, description, re.IGNORECASE | re.DOTALL)
            if team_size_match:
                if len(team_size_match.groups()) >= 2:  # Range
                    min_size = team_size_match.group(1)
                    max_size = team_size_match.group(2)
                    hackathon_data["team_size"] = f"{min_size} - {max_size} Members"
                else:  # Single number
                    size = team_size_match.group(1)
                    hackathon_data["team_size"] = f"Max {size} Members"
                
                print(f"  - Extracted team size: {hackathon_data['team_size']}")
                break
    
    # Extract registration deadline if missing
    if not hackathon_data.get("registration_deadline"):
        reg_deadline_patterns = [
            r'[rR]egistration\s*[dD]eadline.*?([A-Za-z]+\s+\d{1,2}(?:,?\s*20\d{2})?)',
            r'[rR]egister\s*[bB]efore.*?([A-Za-z]+\s+\d{1,2}(?:,?\s*20\d{2})?)',
            r'[rR]egistrations?\s*[cC]lose.*?([A-Za-z]+\s+\d{1,2}(?:,?\s*20\d{2})?)',
        ]
        
        for pattern in reg_deadline_patterns:
            deadline_match = re.search(pattern, description, re.IGNORECASE | re.DOTALL)
            if deadline_match:
                deadline_date = deadline_match.group(1).strip()
                
                # Add year if missing
                if not re.search(r'20\d{2}', deadline_date):
                    deadline_date = f"{deadline_date} {datetime.now().year}"
                
                # Try to parse and format the date
                try:
                    deadline_dt = datetime.strptime(deadline_date, "%B %d %Y")
                    hackathon_data["registration_deadline"] = deadline_dt.strftime("%d %b %y")
                except ValueError:
                    try:
                        deadline_dt = datetime.strptime(deadline_date, "%b %d %Y")
                        hackathon_data["registration_deadline"] = deadline_dt.strftime("%d %b %y")
                    except ValueError:
                        # Just use the raw text if parsing fails
                        hackathon_data["registration_deadline"] = deadline_date
                
                print(f"  - Extracted registration deadline: {hackathon_data['registration_deadline']}")
                break
    
    # Extract eligibility information if missing
    if not hackathon_data.get("eligibility"):
        eligibility_patterns = [
            r'[eE]ligibility:?(.*?)(?:\n\n|\n[A-Z])',  # Eligibility: followed by text until double newline
            r'[oO]pen\s+to(.*?)(?:\.|,|\n)',  # Open to followed by text until period or comma
        ]
        
        for pattern in eligibility_patterns:
            eligibility_match = re.search(pattern, description, re.IGNORECASE | re.DOTALL)
            if eligibility_match:
                eligibility_text = eligibility_match.group(1).strip()
                
                # Clean up the text
                eligibility_text = re.sub(r'\s+', ' ', eligibility_text)
                
                # Extract list items if there are any
                items = re.findall(r'(?:•|\*|\-|\d+\.)\s*([^\n•\*\-\d\.]+)', eligibility_text)
                if items:
                    hackathon_data["eligibility"] = ", ".join(item.strip() for item in items)
                else:
                    hackathon_data["eligibility"] = eligibility_text
                
                print(f"  - Extracted eligibility: {hackathon_data['eligibility'][:50]}...")
                break
    
    # Extract mode information if missing or unknown
    if hackathon_data.get("mode") == "Unknown" or not hackathon_data.get("mode"):
        mode_patterns = [
            r'(?:fully|completely)\s+online',  # fully online
            r'(?:fully|completely)\s+offline',  # fully offline
            r'[rR]ound\s*\d+.*?\(([oO]nline|[oO]ffline)\)',  # Round 1 (Online)
            r'[fF]inal\s*[rR]ound.*?\(([oO]nline|[oO]ffline)\)',  # Final Round (Offline)
        ]
        
        online_count = 0
        offline_count = 0
        
        for pattern in mode_patterns:
            mode_matches = re.finditer(pattern, description, re.IGNORECASE | re.DOTALL)
            for match in mode_matches:
                if match.groups():
                    mode = match.group(1).lower()
                    if mode == "online":
                        online_count += 1
                    else:
                        offline_count += 1
                else:
                    if "online" in match.group(0).lower():
                        online_count += 1
                    else:
                        offline_count += 1
        
        # Determine the primary mode
        if online_count > 0 and offline_count > 0:
            hackathon_data["mode"] = "Hybrid"
        elif online_count > 0:
            hackathon_data["mode"] = "Online"
        elif offline_count > 0:
            hackathon_data["mode"] = "Offline"
        else:
            # If we can't determine the mode, default to "Online" as most hackathons these days are online
            hackathon_data["mode"] = "Online"
            print(f"  - Setting default mode: {hackathon_data['mode']}")
        
        if hackathon_data.get("mode") != "Unknown":
            print(f"  - Extracted/set mode: {hackathon_data['mode']}")

    return hackathon_data

# Run the crawler
if __name__ == "__main__":
    load_dotenv()
    asyncio.run(crawl_unstop_hackathons()) 