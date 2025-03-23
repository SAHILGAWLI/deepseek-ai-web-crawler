# Hackathon Marketplace Crawler

This project is a web crawler built with Python that extracts hackathon data from Devfolio and potentially other platforms. It collects information about current hackathons, including details like name, dates, mode (online/offline), prizes, and more.

## Purpose

The goal of this project is to build an all-in-one hackathon marketplace that aggregates information about hackathons from various platforms into a single location. This crawler doesn't host any hackathons itself but serves as a data collection tool for marketing and displaying hackathons from multiple sources.

## Features

- Asynchronous web crawling using [Crawl4AI](https://pypi.org/project/Crawl4AI/)
- Data extraction powered by a language model (LLM)
- CSV export of extracted hackathon information
- Modular and easy-to-follow code structure

## Project Structure
```
.
├── hackathon_crawler.py     # Main entry point for the hackathon crawler
├── config.py                # Contains configuration constants
├── models
│   ├── venue.py             # Original venue model (for reference)
│   └── hackathon.py         # Defines the Hackathon data model using Pydantic
├── utils
│   ├── __init__.py          # Package marker for utils
│   ├── hackathon_utils.py   # Utility functions for processing hackathon data
│   └── hackathon_scraper.py # Utility functions for configuring and running the crawler
├── requirements.txt         # Python package dependencies
└── HACKATHON_README.md      # This file
```

## Installation

1. **Create and Activate a Conda Environment**

   ```bash
   conda create -n hackathon-crawler python=3.12 -y
   conda activate hackathon-crawler
   ```

2. **Install Dependencies**

   ```bash
   pip install -r requirements.txt
   ```

3. **Set Up Your Environment Variables**

   Create a `.env` file in the root directory with content similar to:

   ```env
   GROQ_API_KEY=your_groq_api_key_here
   ```

## Usage

To start the crawler, run:

```bash
python hackathon_crawler.py
```

The script will crawl Devfolio's hackathon listings, extract data, and save the complete hackathon information to a `devfolio_hackathons.csv` file in the project directory.

## Configuration

The `config.py` file contains key constants used throughout the project:

- **BASE_URL**: The base URL of the website from which to extract hackathon data.
- **URL_PARAMS**: The query parameters to filter hackathons (e.g., happening this month, application open).
- **CSS_SELECTOR**: CSS selector string used to target hackathon content.
- **REQUIRED_KEYS**: List of required fields to consider a hackathon complete.
- **MAX_HACKATHONS**: Maximum number of hackathons to extract.

## Extending the Project

To extend this project to crawl additional hackathon sources:

1. Create a new crawler script based on `hackathon_crawler.py`
2. Update the CSS selectors and URL parameters in `config.py` or create a new config file
3. Modify the extraction strategy as needed for the specific platform

## Future Improvements

- Add support for more hackathon platforms (HackerEarth, MLH, etc.)
- Implement a scheduling system to regularly update the hackathon database
- Create a simple web frontend to display the aggregated hackathon data
- Add filtering and search capabilities to the marketplace

## Ethical Considerations

When web scraping, always:
- Respect robots.txt and the website's terms of service
- Include appropriate delays between requests (rate limiting)
- Consider the load your crawler places on the target servers
- Only extract publicly available information 