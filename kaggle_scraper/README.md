# Kaggle Competition Crawler with Cloudinary Integration

This script crawls Kaggle competitions and extracts details including logos and banners. It integrates with Cloudinary to upload images, making them easier to display in web applications.

## Requirements

- Python 3.8+
- Playwright (`pip install playwright`)
- Cloudinary (`pip install cloudinary`)
- Other dependencies in `requirements.txt`

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Install Playwright browsers:
   ```bash
   playwright install
   ```

3. Set up Cloudinary:
   - Create a free account at [cloudinary.com](https://cloudinary.com)
   - Get your Cloud name, API Key, and API Secret from the Dashboard
   - Copy `.env.example` to `.env` and add your Cloudinary credentials:
     ```
     CLOUDINARY_CLOUD_NAME=your_cloud_name
     CLOUDINARY_API_KEY=your_api_key
     CLOUDINARY_API_SECRET=your_api_secret
     ```

## Usage

Run the crawler:

```bash
python kaggle_crawler.py
```

This will:
1. Crawl active Kaggle competitions
2. Extract details including titles, descriptions, deadlines, and prizes
3. Download logo and banner images
4. Upload images to Cloudinary for better web display
5. Save results to CSV and JSON files with timestamps

## Output

- `kaggle_competitions_YYYYMMDD_HHMMSS.csv`: CSV file with competition data
- `kaggle_competitions_YYYYMMDD_HHMMSS.json`: JSON file with competition data
- `competition_listing_data.json`: Raw data from competition listings
- `competition_listing_data_cloudinary.json`: Listing data with Cloudinary URLs

## Cloudinary Benefits

- CDN for fast global delivery
- Automatic format optimization
- Responsive images
- Image transformations (resizing, cropping) via URL parameters
- Reliable image hosting

## Troubleshooting

If images aren't uploading to Cloudinary:
1. Check your Cloudinary credentials in `.env`
2. Ensure you have internet connectivity
3. Check Cloudinary quota limits
4. Look for error messages in the console output 