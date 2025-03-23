import json
import os
import time
from typing import List, Set, Tuple

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CacheMode,
    CrawlerRunConfig,
    LLMExtractionStrategy,
)

from models.hackathon import Hackathon
from utils.hackathon_utils import is_complete_hackathon, is_duplicate_hackathon


def get_browser_config() -> BrowserConfig:
    """
    Returns the browser configuration for the crawler.

    Returns:
        BrowserConfig: The configuration settings for the browser.
    """
    return BrowserConfig(
        browser_type="chromium",  # Type of browser to simulate
        headless=False,  # Run with GUI to see the crawling in action
        verbose=True,  # Enable verbose logging
        # Additional settings recommended for Devfolio
        viewport_height=1080,
        viewport_width=1920,
        # Playwright options for disabling security restrictions
        extra_args=[
            "--disable-web-security",
            "--disable-features=IsolateOrigins,site-per-process",
            "--disable-site-isolation-trials",
        ],
        ignore_https_errors=True,
    )


def get_llm_strategy() -> LLMExtractionStrategy:
    """
    Returns the configuration for the language model extraction strategy.

    Returns:
        LLMExtractionStrategy: The settings for how to extract data using LLM.
    """
    return LLMExtractionStrategy(
        provider="groq/deepseek-r1-distill-llama-70b",  # Name of the LLM provider
        api_token=os.getenv("GROQ_API_KEY"),  # API token for authentication
        schema=Hackathon.model_json_schema(),  # JSON schema of the data model
        extraction_type="schema",  # Type of extraction to perform
        instruction=(
            "Extract hackathon information including: 'name', 'start_date', 'end_date', 'mode' "
            "(online/offline/hybrid), 'location' (if available), 'prize_pool' (monetary value), "
            "'organization' (organizing entity), 'application_deadline', 'url' (event link), and "
            "a brief 'description'. Dates should be in YYYY-MM-DD format. For search result cards, "
            "focus on extracting the visible information like name, dates, and location. If any information "
            "is not available, leave the field null."
        ),
        input_format="markdown",  # Format of the input content
        verbose=True,  # Enable verbose logging
    )


async def take_screenshot(crawler: AsyncWebCrawler, filename: str) -> bool:
    """
    Takes a screenshot of the current page for debugging purposes.
    
    Args:
        crawler (AsyncWebCrawler): The web crawler instance.
        filename (str): The name of the file to save the screenshot to.
        
    Returns:
        bool: True if the screenshot was saved successfully, False otherwise.
    """
    try:
        # Get the page from the crawler strategy
        page = crawler.crawler_strategy.page
        if page:
            await page.screenshot(path=filename)
            print(f"Screenshot saved to {filename}")
            return True
        else:
            print("No active page found in crawler_strategy")
            return False
    except Exception as e:
        print(f"Error taking screenshot: {e}")
        return False


async def wait_for_search_results(crawler: AsyncWebCrawler, timeout_ms: int = 30000) -> bool:
    """
    Wait for the search results to load on the Devfolio search page.
    
    Args:
        crawler (AsyncWebCrawler): The web crawler instance.
        timeout_ms (int): Maximum time to wait in milliseconds.
        
    Returns:
        bool: True if search results were found, False otherwise.
    """
    try:
        # Get the page from the crawler strategy
        page = crawler.crawler_strategy.page
        if not page:
            print("No active page found in crawler_strategy")
            return False
            
        # Wait for search results to appear
        print("Waiting for search results to load...")
        # Try multiple potential selectors that might indicate search results
        for selector in [
            "div[data-testid='SearchResult']", 
            "div[data-testid='CardContainer']",
            "div[data-testid='HackathonCard']",
            "a[href*='/hackathons/']",
            ".css-1dbjc4n"  # More generic class used by Devfolio
        ]:
            try:
                await page.wait_for_selector(selector, timeout=timeout_ms / 4)
                print(f"Found search results with selector: {selector}")
                return True
            except Exception:
                print(f"Selector not found: {selector}")
                continue
                
        print("No search results found after trying multiple selectors")
        return False
    except Exception as e:
        print(f"Error waiting for search results: {e}")
        return False


async def extract_with_javascript(crawler: AsyncWebCrawler) -> List[dict]:
    """
    Extracts hackathon data directly using JavaScript in the browser.
    This approach can be more reliable for SPAs like Devfolio.
    
    Args:
        crawler (AsyncWebCrawler): The web crawler instance.
        
    Returns:
        List[dict]: A list of extracted hackathons, or an empty list if extraction failed.
    """
    try:
        # Get the page from the crawler strategy
        page = crawler.crawler_strategy.page
        if not page:
            print("No active page found in crawler_strategy")
            return []
            
        print("Attempting direct JavaScript extraction...")
        
        # This JavaScript code will extract hackathon cards from the page
        # It's customized for Devfolio's React structure
        js_extraction_code = """
            function extractHackathons() {
                // Different approaches to find hackathon cards
                const hackathonElements = Array.from(document.querySelectorAll('a[href*="/hackathons/"]'));
                
                if (hackathonElements.length === 0) {
                    return [];
                }
                
                return hackathonElements.map(element => {
                    // Try to extract information from the card
                    const nameElement = element.querySelector('div[dir="auto"]');
                    const dateElements = Array.from(element.querySelectorAll('div[dir="auto"]')).slice(1); // Skip the first one (name)
                    const locationElement = Array.from(element.querySelectorAll('div')).find(el => 
                        el.textContent && (el.textContent.includes('Online') || el.textContent.includes(',')));
                    
                    // Get the URL
                    const url = element.href;
                    
                    // Parse date information - dates are usually in format like "Feb 10 - Mar 10"
                    let startDate = '', endDate = '';
                    const dateText = dateElements.length > 0 ? dateElements[0].textContent : '';
                    if (dateText) {
                        const dateMatch = dateText.match(/(\\w+\\s\\d+)\\s*-\\s*(\\w+\\s\\d+)/);
                        if (dateMatch) {
                            startDate = dateMatch[1];
                            endDate = dateMatch[2];
                        }
                    }
                    
                    // Determine mode (online or in-person)
                    let mode = 'Unknown';
                    if (locationElement) {
                        mode = locationElement.textContent.toLowerCase().includes('online') ? 'Online' : 'In-person';
                    }
                    
                    // Extract location
                    let location = '';
                    if (locationElement && !locationElement.textContent.toLowerCase().includes('online')) {
                        location = locationElement.textContent.trim();
                    }
                    
                    return {
                        name: nameElement ? nameElement.textContent.trim() : 'Unknown Hackathon',
                        start_date: startDate,
                        end_date: endDate,
                        mode: mode,
                        location: location,
                        url: url
                    };
                }).filter(hack => hack.name !== 'Unknown Hackathon');
            }
            
            return extractHackathons();
        """
        
        # Execute the JavaScript in the browser
        results = await page.evaluate(js_extraction_code)
        print(f"JavaScript extraction found {len(results)} hackathons")
        
        # Format the dates properly
        for hackathon in results:
            if hackathon['start_date']:
                # Convert informal date like "Feb 10" to "2024-02-10"
                try:
                    # Add current year as we usually only get month and day
                    current_year = time.strftime("%Y")
                    start_date_parts = hackathon['start_date'].split()
                    month_abbr = start_date_parts[0]
                    day = start_date_parts[1]
                    month_num = {
                        'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
                        'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
                    }.get(month_abbr, '01')
                    
                    hackathon['start_date'] = f"{current_year}-{month_num}-{day.zfill(2)}"
                except:
                    # If parsing fails, keep the original
                    pass
                    
            if hackathon['end_date']:
                # Same for end date
                try:
                    current_year = time.strftime("%Y")
                    end_date_parts = hackathon['end_date'].split()
                    month_abbr = end_date_parts[0]
                    day = end_date_parts[1]
                    month_num = {
                        'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
                        'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
                    }.get(month_abbr, '01')
                    
                    hackathon['end_date'] = f"{current_year}-{month_num}-{day.zfill(2)}"
                except:
                    # If parsing fails, keep the original
                    pass
        
        return results
    except Exception as e:
        print(f"Error during JavaScript extraction: {e}")
        return []


async def fetch_hackathons(
    crawler: AsyncWebCrawler,
    exact_url: str,
    css_selector: str,
    llm_strategy: LLMExtractionStrategy,
    session_id: str,
    required_keys: List[str],
    max_hackathons: int,
) -> List[dict]:
    """
    Fetches and processes hackathon data from Devfolio.

    Args:
        crawler (AsyncWebCrawler): The web crawler instance.
        exact_url (str): The exact URL to scrape.
        css_selector (str): The CSS selector to target the content.
        llm_strategy (LLMExtractionStrategy): The LLM extraction strategy.
        session_id (str): The session identifier.
        required_keys (List[str]): List of required keys in the hackathon data.
        max_hackathons (int): Maximum number of hackathons to extract.

    Returns:
        List[dict]: A list of processed hackathons.
    """
    print(f"Loading hackathons from: {exact_url}")

    # Set up page navigation first
    try:
        # First make sure the crawler is started
        if not crawler.ready:
            await crawler.start()
            
        # Get the page from the crawler strategy
        page = crawler.crawler_strategy.page
        if not page:
            print("No active page found in crawler_strategy")
            return []
            
        print(f"Navigating to {exact_url}...")
        # Force no-cache to ensure we get the freshest content
        await page.route("**/*", lambda route: route.continue_(
            headers={**route.request.headers, "Cache-Control": "no-cache, no-store, must-revalidate"}
        ))
        
        # Go to the exact URL with networkidle wait
        await page.goto(exact_url, wait_until="networkidle", timeout=60000)
        print("Initial page load complete")
        
        # Wait for a few seconds to ensure all content is loaded
        await page.wait_for_timeout(8000)
        print("Waited for initial content to load")
        
        # Take a screenshot after initial page load
        await take_screenshot(crawler, "devfolio_initial_load.png")
        
        # Print the current URL to debug any redirects
        current_url = page.url
        print(f"Current URL after navigation: {current_url}")
        
        # If we got redirected, try to navigate back to our target URL
        if current_url != exact_url:
            print(f"Redirected to {current_url}, attempting to navigate back to {exact_url}")
            await page.goto(exact_url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(5000)
            print(f"Current URL after redirection fix: {page.url}")
        
        # Wait for search results to appear
        search_results_found = await wait_for_search_results(crawler)
        
        if search_results_found:
            print("Search results found, proceeding with extraction")
            # Perform some scrolling to load more content
            for i in range(10):  # More scrolls to ensure all content is loaded
                await page.evaluate("window.scrollBy(0, 500)")
                await page.wait_for_timeout(1000)
                print(f"Scroll iteration {i+1}/10 completed")
                
            # Take another screenshot after scrolling
            await take_screenshot(crawler, "devfolio_after_scrolling.png")
        else:
            print("No search results found, will still attempt extraction")
            
    except Exception as e:
        print(f"Error during manual navigation: {e}")
        await take_screenshot(crawler, "devfolio_navigation_error.png")
        
    # First try direct JavaScript extraction
    js_extracted_hackathons = await extract_with_javascript(crawler)
    
    if js_extracted_hackathons:
        print(f"Successfully extracted {len(js_extracted_hackathons)} hackathons using JavaScript")
        return js_extracted_hackathons[:max_hackathons]  # Return the JavaScript results directly
    
    print("JavaScript extraction failed or found no hackathons, falling back to LLM extraction")

    # Try several selectors to find hackathon cards
    possible_selectors = [
        css_selector,  # The one from config
        "div[data-testid='SearchResult']", 
        "div[data-testid='CardContainer']",
        "div[data-testid='HackathonCard']",
        "a[href*='/hackathons/']",
        ".css-1dbjc4n",  # Generic Devfolio class
        "div.css-0"  # More generic fallback
    ]
    
    result = None
    
    # Try each selector until we find one that works
    for selector in possible_selectors:
        print(f"Attempting extraction with selector: {selector}")
        
        # Fetch page content with the extraction strategy
        # Use correct CrawlerRunConfig parameters
        result = await crawler.arun(
            url=exact_url,
            config=CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,  # Do not use cached data
                extraction_strategy=llm_strategy,  # Strategy for data extraction
                css_selector=selector,  # Target specific content on the page
                session_id=session_id,  # Unique session ID for the crawl
                scan_full_page=True,  # Scan the full page (replaces scroll_config)
                scroll_delay=2.0,  # Wait 2 seconds between scrolls (replaces scroll_config)
                wait_for=selector,  # Wait for this selector to appear
                page_timeout=60000,  # 60 second timeout
                delay_before_return_html=5.0,  # Wait 5 seconds before extracting HTML
            ),
        )
        
        # If we got content, break out of the loop
        if result and result.success and result.extracted_content:
            print(f"Extraction successful with selector: {selector}")
            break
        else:
            print(f"Extraction failed with selector: {selector}")
            # Take a screenshot to see what's happening
            await take_screenshot(crawler, f"devfolio_failed_{selector.replace('[', '_').replace(']', '_').replace('.', '_')}.png")

    # Take a screenshot after content extraction attempt
    await take_screenshot(crawler, "devfolio_after_extraction.png")

    # If all selectors failed, try one more approach - extract from the entire page
    if not result or not result.success or not result.extracted_content:
        print("All selectors failed. Attempting to extract from the entire page.")
        result = await crawler.arun(
            url=exact_url,
            config=CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                extraction_strategy=llm_strategy,
                session_id=session_id,
                scan_full_page=True,  # Scan the full page
                scroll_delay=2.0,      # 2 second delay between scrolls
                delay_before_return_html=5.0,  # Wait 5 seconds before extracting HTML
            ),
        )
        
        # Take another screenshot after trying the whole page
        await take_screenshot(crawler, "devfolio_whole_page.png")
        
    if not result or not result.success:
        print(f"Error fetching hackathons: {result.error_message if result else 'No result'}")
        return []
    
    if not result.extracted_content:
        print("No content could be extracted from the page.")
        return []

    # Parse extracted content
    try:
        # Print the raw HTML to help with debugging
        print(f"Raw HTML length: {len(result.cleaned_html) if result.cleaned_html else 0} characters")
        
        # Save a sample of the HTML for inspection
        if result.cleaned_html:
            with open("sample_devfolio_html.txt", "w", encoding="utf-8") as f:
                f.write(result.cleaned_html[:10000])  # Save first 10K characters
            print("Saved sample HTML to sample_devfolio_html.txt")
        
        # Try to parse the JSON
        extracted_data = json.loads(result.extracted_content) if result.extracted_content else []
        if not extracted_data:
            print("No hackathons found.")
            return []
        
        print(f"Extracted {len(extracted_data)} hackathon(s)")
        print("Extracted data:", extracted_data)
    except json.JSONDecodeError:
        print("Error decoding JSON from extracted content")
        print("Raw extracted content:", result.extracted_content)
        return []

    # Process hackathons
    complete_hackathons = []
    seen_names = set()
    
    for hackathon in extracted_data:
        # Debugging: Print each hackathon to understand its structure
        print("Processing hackathon:", hackathon)

        if not is_complete_hackathon(hackathon, required_keys):
            print(f"Skipping incomplete hackathon: {hackathon.get('name', 'Unknown')}")
            continue  # Skip incomplete hackathons

        if is_duplicate_hackathon(hackathon["name"], seen_names):
            print(f"Duplicate hackathon '{hackathon['name']}' found. Skipping.")
            continue  # Skip duplicate hackathons

        # Add hackathon to the list
        seen_names.add(hackathon["name"])
        complete_hackathons.append(hackathon)
        
        # Limit the number of hackathons
        if len(complete_hackathons) >= max_hackathons:
            print(f"Reached maximum number of hackathons ({max_hackathons})")
            break

    return complete_hackathons 