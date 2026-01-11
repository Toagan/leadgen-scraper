# LeadGen Master

**LeadGen Master** is a professional-grade lead extraction suite built with Python (Flask) and the Serper API. It automates the process of finding business leads from Google Maps across multiple countries, providing a real-time dashboard interface for monitoring and exporting data.

## Features

### Core Functionality
* **Google Maps Scraping:** Leverages the Serper API to extract comprehensive business data (16 fields including Name, Address, Phone, Website, Rating, Reviews, Categories, Coordinates, and more).
* **Multi-Region Support:** Built-in city data for 13 markets: Germany, USA, UK, Australia, Russia, China, Canada, France, Spain, Italy, Brazil, India, and Japan.
* **Global Deduplication:** Automatically prevents duplicate entries across all cities using unique Place IDs.
* **Live Dashboard:** Real-time terminal-style feed showing leads as they are discovered.

### Smart Scraping (Germany Optimized)
* **Population-Based City Prioritization:** Cities are sorted by population (largest first) for maximum lead coverage.
* **Dynamic Zoom Levels:** Larger cities use wider zoom (12z) for more coverage, smaller cities use tighter zoom (15z).
* **Dynamic Pagination:** Continues fetching pages until no new unique results are found.
* **Four Scrape Modes:**
  - **Quick:** Major cities only (50k+ population, ~240 cities)
  - **Smart:** Balanced coverage (10k+ population, ~1,700 cities)
  - **Thorough:** High coverage (5k+ population, ~2,900 cities)
  - **Maximum (PLZ):** Complete coverage using 8,300 postal codes - includes ALL rural areas

### Maximum Coverage Mode (PLZ-Based)
* **Postal Code Grid:** Uses 8,298 German postal codes (PLZ) for complete geographic coverage.
* **Rural Area Coverage:** Unlike city-based modes, PLZ mode covers every corner of Germany including small villages and rural areas.
* **Dynamic Pagination:** Continues scraping each PLZ until no new unique results are found.
* **Ideal For:** Getting ALL businesses of a type in a specific region (e.g., all dentists in Bavaria).

### Bundesland Filtering (Germany Only)
* **State-Level Filtering:** Select specific German federal states (Bundesländer) to scrape.
* **All 16 States Supported:** Bavaria, Baden-Württemberg, Berlin, Brandenburg, Bremen, Hamburg, Hesse, Mecklenburg-Vorpommern, Lower Saxony, North Rhine-Westphalia, Rhineland-Palatinate, Saarland, Saxony, Saxony-Anhalt, Schleswig-Holstein, Thuringia.
* **Coordinate-Based Detection:** Cities are automatically assigned to states based on GPS coordinates.
* **Multi-State Selection:** Select multiple states or leave empty for all of Germany.

### Batch Processing
* **Country-Specific Search Terms:** Configure different search terms for each country.
* **Multi-Country Batch Scrape:** Run searches across multiple countries in one operation.
* **Separate CSV Exports:** Each country gets its own CSV file during batch operations.

### Quality Filters
* **Minimum Rating Filter:** Only include businesses with 3+, 3.5+, 4+, or 4.5+ star ratings.
* **Minimum Reviews Filter:** Only include businesses with 5+, 10+, 25+, 50+, or 100+ reviews.

### Export Options
* **Download All:** Export all scraped leads.
* **Download With Website:** Export only leads that have a website URL.
* **Download With Phone:** Export only leads that have a phone number.
* **Download With Both:** Export only leads with both website and phone.

### CSV Export Fields (16 columns)
1. Search Term
2. City
3. Name
4. Address
5. Phone
6. Website
7. Rating
8. Review Count
9. Category
10. Categories (all)
11. Business Latitude
12. Business Longitude
13. Place ID
14. Opening Hours
15. Price Range
16. Description

## Tech Stack

* **Backend:** Python 3, Flask, Threading
* **Frontend:** HTML5, TailwindCSS, JavaScript (Fetch API)
* **Data Source:** [Serper.dev](https://serper.dev) (Google Maps API)
* **Data Storage:** CSV (Exports) & JSON (History/Config)

## Project Structure

```
leadgen-scraper/
├── data/                      # City coordinates for each country
│   ├── cities.txt             # Germany (with population data)
│   ├── cities_us.txt          # USA
│   ├── cities_uk.txt          # UK
│   ├── cities_au.txt          # Australia
│   └── ...
├── data_exports/              # Generated CSV files
├── templates/
│   └── index.html             # Dashboard UI
├── app.py                     # Main application logic
├── search_history.json        # Database of past searches
├── search_terms_config.json   # Country-specific search terms
├── requirements.txt           # Python dependencies
└── .env                       # API Key configuration
```

## Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/Toagan/leadgen-scraper.git
cd leadgen-scraper
```

### 2. Set up Virtual Environment
```bash
# Mac/Linux
python3 -m venv venv
source venv/bin/activate

# Windows
python -m venv venv
venv\Scripts\activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure API Key
Create a `.env` file in the root directory:
```
SERPER_API_KEY=your_actual_api_key_here
```
Get your API key from [serper.dev](https://serper.dev).

### 5. Run the Application
```bash
python app.py
```
Open your browser: http://127.0.0.1:5000

## Usage Guide

### Single Search
1. Enter a keyword (e.g., "Marketing Agency")
2. Set maximum leads and select target region
3. (Germany only) Optionally select specific Bundesländer
4. Choose scrape mode (Quick/Smart/Thorough)
5. Set quality filters if needed
6. Click "Generate Leads"
7. Download results using the filter buttons

### Batch Search
1. Go to "Configure Countries" tab
2. Add search terms for each country you want to scrape
3. Go to "Batch Search" tab
4. Select countries to include
5. Set leads per search term
6. Click "Start Batch Scrape"

### Bundesland Filtering (Germany)
1. Select "Germany" as the target region
2. The Bundesland selector appears automatically
3. Select one or more states (e.g., Bavaria, Berlin)
4. Only cities within selected states will be scraped
5. Leave empty to scrape all of Germany

## API Usage Notes

This tool uses the Serper API, which costs credits per search. The tool is optimized to:
- Stop immediately when the lead limit is reached
- Use smart pagination to maximize results per API call
- Prioritize high-population cities for better lead quality

## License

MIT
