# config.py

# Original venue crawling configuration (commented out for reference)
# BASE_URL = "https://www.theknot.com/marketplace/wedding-reception-venues-atlanta-ga"
# CSS_SELECTOR = "[class^='info-container']"
# REQUIRED_KEYS = [
#     "name",
#     "price",
#     "location",
#     "capacity",
#     "rating",
#     "reviews",
#     "description",
# ]

# Devfolio hackathon crawling configuration
BASE_URL = "https://devfolio.co/search"
URL_PARAMS = "?happening=this_month&primary_filter=hackathons&type=application_open"
CSS_SELECTOR = "div[data-testid='SearchResult']"  # Updated based on likely search results structure
REQUIRED_KEYS = [
    "name",
    "start_date",
    "end_date",
    "mode",
]

# The number of hackathons to crawl (adjust as needed)
MAX_HACKATHONS = 100
