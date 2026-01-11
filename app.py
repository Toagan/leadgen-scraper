import os
import io
import csv
import json
import time
import threading
import requests
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_from_directory, Response
from dotenv import load_dotenv

# 1. Setup & Configuration
load_dotenv()
app = Flask(__name__)

# Configuration
API_KEY = os.getenv("SERPER_API_KEY")

# Map regions to specific city files in the data/ folder
REGION_FILES = {
    'de': 'data/cities.txt',
    'us': 'data/cities_us.txt',
    'uk': 'data/cities_uk.txt',
    'au': 'data/cities_au.txt',
    'ru': 'data/cities_ru.txt',
    'cn': 'data/cities_cn.txt',
    'ca': 'data/cities_ca.txt',
    'fr': 'data/cities_fr.txt',
    'es': 'data/cities_es.txt',
    'it': 'data/cities_it.txt',
    'br': 'data/cities_br.txt',
    'in': 'data/cities_in.txt',
    'jp': 'data/cities_jp.txt'
}

# Smart scraping configuration based on city population
# Larger cities get wider zoom (more coverage) and more pages
def get_city_scrape_config(population):
    """Returns (zoom_level, max_pages) based on city population."""
    if population >= 500000:  # Major cities (Berlin, Hamburg, Munich, etc.)
        return (12, 6)  # Wide zoom, 6 pages = up to 120 results
    elif population >= 200000:  # Large cities
        return (13, 5)  # 5 pages = up to 100 results
    elif population >= 100000:  # Medium-large cities
        return (14, 4)  # 4 pages = up to 80 results
    elif population >= 50000:  # Medium cities
        return (14, 3)  # 3 pages = up to 60 results
    elif population >= 20000:  # Small cities
        return (15, 2)  # Tighter zoom, 2 pages = up to 40 results
    else:  # Towns
        return (15, 1)  # 1 page = up to 20 results

# Minimum population thresholds for different scrape modes
MIN_POPULATION_DEFAULT = 10000  # Skip cities smaller than this by default
MIN_POPULATION_THOROUGH = 5000  # Thorough mode includes smaller cities

# PLZ (Postal Code) file for maximum Germany coverage
PLZ_FILE = 'data/plz_germany.csv'

# =============================================================================
# QUERY EXPANSION & CATEGORY BUNDLES
# =============================================================================

# Category bundles with synonyms and German translations
# Each category has variations that will be searched when category mode is enabled
CATEGORY_BUNDLES = {
    'marketing': {
        'name': 'Marketing & Advertising',
        'queries': ['marketing agency', 'Werbeagentur', 'digital marketing agency', 'Marketingagentur', 'ad agency', 'advertising agency']
    },
    'dental': {
        'name': 'Dental & Dentists',
        'queries': ['dentist', 'Zahnarzt', 'dental clinic', 'Zahnklinik', 'dental practice', 'Zahnarztpraxis']
    },
    'legal': {
        'name': 'Legal & Lawyers',
        'queries': ['lawyer', 'Rechtsanwalt', 'law firm', 'Anwaltskanzlei', 'attorney', 'legal services']
    },
    'accounting': {
        'name': 'Accounting & Tax',
        'queries': ['accountant', 'Steuerberater', 'tax advisor', 'accounting firm', 'Buchhaltung', 'Wirtschaftsprüfer']
    },
    'real_estate': {
        'name': 'Real Estate',
        'queries': ['real estate agent', 'Immobilienmakler', 'real estate agency', 'Immobilienbüro', 'property agent']
    },
    'restaurant': {
        'name': 'Restaurants & Dining',
        'queries': ['restaurant', 'Restaurant', 'eatery', 'Gaststätte', 'bistro', 'dining']
    },
    'hotel': {
        'name': 'Hotels & Accommodation',
        'queries': ['hotel', 'Hotel', 'accommodation', 'Unterkunft', 'inn', 'Pension', 'guesthouse', 'Gasthaus']
    },
    'automotive': {
        'name': 'Automotive & Car Services',
        'queries': ['car dealership', 'Autohaus', 'auto repair', 'Autowerkstatt', 'car service', 'KFZ Werkstatt']
    },
    'medical': {
        'name': 'Medical & Healthcare',
        'queries': ['doctor', 'Arzt', 'medical practice', 'Arztpraxis', 'clinic', 'Klinik', 'healthcare']
    },
    'fitness': {
        'name': 'Fitness & Gyms',
        'queries': ['gym', 'Fitnessstudio', 'fitness center', 'personal trainer', 'sports club', 'Sportverein']
    },
    'beauty': {
        'name': 'Beauty & Wellness',
        'queries': ['beauty salon', 'Kosmetikstudio', 'spa', 'Wellness', 'hair salon', 'Friseur', 'nail salon']
    },
    'construction': {
        'name': 'Construction & Building',
        'queries': ['construction company', 'Bauunternehmen', 'builder', 'contractor', 'Handwerker', 'renovation']
    },
    'it_services': {
        'name': 'IT & Technology',
        'queries': ['IT company', 'IT Unternehmen', 'software company', 'tech company', 'IT services', 'web agency']
    },
    'consulting': {
        'name': 'Consulting & Advisory',
        'queries': ['consulting firm', 'Unternehmensberatung', 'business consultant', 'Berater', 'management consulting']
    },
    'photography': {
        'name': 'Photography & Video',
        'queries': ['photographer', 'Fotograf', 'photography studio', 'Fotostudio', 'videographer', 'wedding photographer']
    }
}

def expand_query_variations(base_query, include_german=True, include_broad=True):
    """
    Expand a single query into multiple variations.
    Returns list of query variations to search.
    """
    variations = []

    # Always include exact match (quoted)
    variations.append(f'"{base_query}"')

    # Include broad match (unquoted) if enabled
    if include_broad:
        variations.append(base_query)

    # Include German translation if enabled and query is in English
    if include_german:
        # Common English to German business term mappings
        translations = {
            'marketing agency': 'Werbeagentur',
            'digital marketing': 'Online Marketing',
            'dentist': 'Zahnarzt',
            'lawyer': 'Rechtsanwalt',
            'accountant': 'Steuerberater',
            'real estate agent': 'Immobilienmakler',
            'doctor': 'Arzt',
            'restaurant': 'Restaurant',
            'hotel': 'Hotel',
            'gym': 'Fitnessstudio',
            'photographer': 'Fotograf',
            'consultant': 'Berater',
            'contractor': 'Handwerker',
            'plumber': 'Klempner',
            'electrician': 'Elektriker',
            'architect': 'Architekt',
            'insurance agent': 'Versicherungsmakler',
            'financial advisor': 'Finanzberater',
            'web designer': 'Webdesigner',
            'graphic designer': 'Grafikdesigner',
        }

        base_lower = base_query.lower()
        for eng, ger in translations.items():
            if eng in base_lower:
                # Add German equivalent
                german_query = base_query.lower().replace(eng, ger)
                variations.append(f'"{german_query}"')
                if include_broad:
                    variations.append(german_query)
                break

    return variations

def get_category_queries(category_key):
    """Get all query variations for a category bundle."""
    if category_key in CATEGORY_BUNDLES:
        return CATEGORY_BUNDLES[category_key]['queries']
    return []

def load_plz_data(bundeslaender=None):
    """
    Load German PLZ (postal code) data with coordinates.
    Returns list of dicts with plz, lat, lon keys.
    Optionally filters by Bundesländer.
    """
    plz_list = []
    filtered_count = 0

    try:
        with open(PLZ_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith(',lat'):  # Skip header
                    continue
                parts = line.split(',')
                if len(parts) >= 3:
                    plz = parts[0].strip()
                    lat = parts[1].strip()
                    lon = parts[2].strip()

                    # Filter by Bundesland if specified
                    if bundeslaender and len(bundeslaender) > 0:
                        plz_bundesland = get_bundesland(lat, lon)
                        if plz_bundesland not in bundeslaender:
                            filtered_count += 1
                            continue

                    plz_list.append({
                        'plz': plz,
                        'lat': lat,
                        'lon': lon
                    })
    except FileNotFoundError:
        print(f"PLZ file not found: {PLZ_FILE}")
        return [], 0

    return plz_list, filtered_count

# German Bundesländer (Federal States) with refined bounding boxes
# Format: (min_lat, max_lat, min_lon, max_lon)
# Bounding boxes adjusted to minimize overlaps at state borders
BUNDESLAENDER = {
    'BY': {'name': 'Bavaria (Bayern)', 'bounds': (47.27, 50.57, 9.87, 13.84)},
    'BW': {'name': 'Baden-Württemberg', 'bounds': (47.53, 49.79, 7.51, 10.50)},
    'BE': {'name': 'Berlin', 'bounds': (52.33, 52.68, 13.08, 13.77)},
    'BB': {'name': 'Brandenburg', 'bounds': (51.36, 53.56, 11.26, 14.77)},
    'HB': {'name': 'Bremen', 'bounds': (53.01, 53.61, 8.48, 8.99)},
    'HH': {'name': 'Hamburg', 'bounds': (53.39, 53.74, 9.73, 10.33)},
    'HE': {'name': 'Hesse (Hessen)', 'bounds': (49.39, 51.66, 8.20, 10.24)},
    'MV': {'name': 'Mecklenburg-Vorpommern', 'bounds': (53.11, 54.69, 10.59, 14.41)},
    'NI': {'name': 'Lower Saxony (Niedersachsen)', 'bounds': (51.29, 53.89, 6.65, 11.60)},
    'NW': {'name': 'North Rhine-Westphalia (NRW)', 'bounds': (50.32, 52.53, 5.87, 9.46)},
    'RP': {'name': 'Rhineland-Palatinate', 'bounds': (48.97, 50.94, 6.11, 8.50)},
    'SL': {'name': 'Saarland', 'bounds': (49.11, 49.64, 6.36, 7.41)},
    'SN': {'name': 'Saxony (Sachsen)', 'bounds': (50.17, 51.69, 11.87, 15.04)},
    'ST': {'name': 'Saxony-Anhalt', 'bounds': (50.94, 53.04, 10.56, 12.10)},
    'SH': {'name': 'Schleswig-Holstein', 'bounds': (53.36, 55.06, 8.31, 11.31)},
    'TH': {'name': 'Thuringia (Thüringen)', 'bounds': (50.20, 51.65, 9.87, 12.65)}
}

# Border city coordinate overrides - for cities on state borders where bounding boxes fail
# Format: (lat, lon, state_code, tolerance)
BORDER_CITY_COORDS = [
    (50.08, 8.24, 'HE', 0.03),   # Wiesbaden - east bank of Rhine, in Hesse
    (49.99, 8.25, 'RP', 0.03),   # Mainz - west bank of Rhine, in Rhineland-Palatinate
    (49.79, 9.95, 'BY', 0.05),   # Würzburg - northwest Bavaria, near Hesse border
    (49.87, 10.88, 'BY', 0.05),  # Schweinfurt - northwest Bavaria
]

def get_bundesland(lat, lon):
    """Determine which Bundesland a city belongs to based on coordinates."""
    lat, lon = float(lat), float(lon)

    # Check border city overrides first
    for city_lat, city_lon, state, tolerance in BORDER_CITY_COORDS:
        if abs(lat - city_lat) < tolerance and abs(lon - city_lon) < tolerance:
            return state
    matches = []

    for code, data in BUNDESLAENDER.items():
        min_lat, max_lat, min_lon, max_lon = data['bounds']
        if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
            # Calculate distance to center of bounding box for ranking
            center_lat = (min_lat + max_lat) / 2
            center_lon = (min_lon + max_lon) / 2
            dist = ((lat - center_lat) ** 2 + (lon - center_lon) ** 2) ** 0.5
            matches.append((code, dist))

    if not matches:
        return None

    # Handle overlapping regions - prioritize smaller states (city-states)
    codes = [m[0] for m in matches]
    if 'BE' in codes:
        return 'BE'  # Berlin
    if 'HH' in codes:
        return 'HH'  # Hamburg
    if 'HB' in codes:
        return 'HB'  # Bremen

    # Return the state whose center is closest to the point
    matches.sort(key=lambda x: x[1])
    return matches[0][0]

# Ensure directories exist for data storage
DATA_DIR = "data_exports"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

HISTORY_FILE = "search_history.json"
SEARCH_TERMS_CONFIG = "search_terms_config.json"

# Country display names for UI
COUNTRY_NAMES = {
    'de': 'Germany',
    'us': 'USA',
    'uk': 'UK',
    'au': 'Australia',
    'ru': 'Russia',
    'cn': 'China',
    'ca': 'Canada',
    'fr': 'France',
    'es': 'Spain',
    'it': 'Italy',
    'br': 'Brazil',
    'in': 'India',
    'jp': 'Japan'
}

# Global Job Status
job_status = {
    "is_running": False,
    "current_city": "",
    "total_leads": 0,
    "total_skipped": 0,
    "status_message": "Idle",
    "new_logs": [],
    "current_filename": ""
}

# CSV Header for exports - comprehensive fields for email outbound
CSV_HEADERS = [
    'Search Term', 'City', 'Name', 'Address', 'Phone', 'Website',
    'Rating', 'Review Count', 'Category', 'Categories',
    'Business Lat', 'Business Lon', 'Place ID',
    'Opening Hours', 'Price Range', 'Description'
]

def extract_place_data(place, search_term, city_name):
    """Extract all available fields from a place result."""
    # Get coordinates - prefer actual business coords, fallback to None
    lat = place.get('latitude', '')
    lon = place.get('longitude', '')

    # Handle categories - can be string or list
    category = place.get('category', place.get('type', ''))
    categories_list = place.get('categories', [])
    if isinstance(categories_list, list):
        categories = ', '.join(categories_list)
    else:
        categories = str(categories_list) if categories_list else ''

    # Opening hours - can be string or object
    hours = place.get('openingHours', place.get('hours', ''))
    if isinstance(hours, dict):
        hours = hours.get('status', str(hours))
    elif isinstance(hours, list):
        hours = '; '.join(hours)

    return {
        'search_term': search_term,
        'city': city_name,
        'name': place.get('title', 'Unknown'),
        'address': place.get('address', ''),
        'phone': place.get('phoneNumber', place.get('phone', '')),
        'website': place.get('website', ''),
        'rating': place.get('rating', ''),
        'review_count': place.get('ratingCount', place.get('reviews', place.get('reviewCount', ''))),
        'category': category,
        'categories': categories,
        'lat': lat,
        'lon': lon,
        'place_id': place.get('cid') or place.get('place_id') or place.get('placeId', ''),
        'hours': hours,
        'price': place.get('price', place.get('priceRange', '')),
        'description': place.get('description', place.get('snippet', ''))
    }

def passes_filters(place_data, min_rating=0, min_reviews=0, require_website=False, require_phone=False):
    """Check if a place passes the configured filters."""
    # Rating filter
    try:
        rating = float(place_data['rating']) if place_data['rating'] else 0
    except (ValueError, TypeError):
        rating = 0
    if min_rating > 0 and rating < min_rating:
        return False

    # Review count filter
    try:
        reviews = int(place_data['review_count']) if place_data['review_count'] else 0
    except (ValueError, TypeError):
        reviews = 0
    if min_reviews > 0 and reviews < min_reviews:
        return False

    # Website filter
    if require_website and not place_data['website']:
        return False

    # Phone filter
    if require_phone and not place_data['phone']:
        return False

    return True

def write_place_to_csv(writer, place_data):
    """Write a place data dict to CSV."""
    writer.writerow([
        place_data['search_term'],
        place_data['city'],
        place_data['name'],
        place_data['address'],
        place_data['phone'],
        place_data['website'],
        place_data['rating'],
        place_data['review_count'],
        place_data['category'],
        place_data['categories'],
        place_data['lat'],
        place_data['lon'],
        place_data['place_id'],
        place_data['hours'],
        place_data['price'],
        place_data['description']
    ])

# --- HELPER FUNCTIONS ---

def load_search_terms_config():
    """Loads search terms configuration from JSON file."""
    if os.path.exists(SEARCH_TERMS_CONFIG):
        with open(SEARCH_TERMS_CONFIG, 'r') as f:
            try:
                return json.load(f)
            except:
                return {}
    return {}

def save_search_terms_config(config):
    """Saves search terms configuration to JSON file."""
    with open(SEARCH_TERMS_CONFIG, 'w') as f:
        json.dump(config, f, indent=4)

def save_to_history(term, region, leads_count, filename):
    """Saves the search details to a JSON file."""
    entry = {
        "timestamp": time.time(),
        "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "term": term,
        "region": region.upper(),
        "leads_requested": leads_count,
        "filename": filename
    }
    
    history = []
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'r') as f:
            try:
                history = json.load(f)
            except:
                history = []
    
    # Add new entry to the TOP of the list
    history.insert(0, entry)
    
    with open(HISTORY_FILE, 'w') as f:
        json.dump(history, f, indent=4)

def get_places_by_gps(query, lat, lon, country_code, start_index=0, zoom=14):
    url = "https://google.serper.dev/places"
    location_bias = f"@{lat},{lon},{zoom}z"

    # Adjust for Serper's specific country codes if needed
    if country_code == 'uk': country_code = 'gb'

    payload = json.dumps({
        "q": query,
        "gl": country_code,
        "hl": country_code,
        "ll": location_bias,
        "start": start_index
    })
    
    headers = {
        'X-API-KEY': API_KEY,
        'Content-Type': 'application/json'
    }

    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        return response.json()
    except Exception as e:
        print(f"⚠️ API Error: {e}")
        return None

def scraper_worker(search_term, num_leads, match_type, region, filename,
                   min_rating=0, min_reviews=0, scrape_mode='smart', bundeslaender=None):
    """
    Smart scraper that adapts to city size.
    scrape_mode: 'smart' (default), 'thorough', or 'quick'
    bundeslaender: list of Bundesland codes to filter by (Germany only)
    """
    global job_status
    job_status["is_running"] = True
    job_status["total_leads"] = 0
    job_status["total_skipped"] = 0
    job_status["new_logs"] = []
    job_status["current_filename"] = filename
    job_status["status_message"] = f"Starting scrape for '{search_term}' in {region.upper()}..."

    final_query = search_term
    if match_type == 'literal':
        final_query = f'"{search_term}"'

    # Log filters if any are active
    filters_active = []
    if min_rating > 0:
        filters_active.append(f"min rating: {min_rating}")
    if min_reviews > 0:
        filters_active.append(f"min reviews: {min_reviews}")
    filters_active.append(f"mode: {scrape_mode}")

    # Log Bundesland filter if active
    if region == 'de' and bundeslaender and len(bundeslaender) > 0:
        state_names = [BUNDESLAENDER[bl]['name'] for bl in bundeslaender if bl in BUNDESLAENDER]
        filters_active.append(f"states: {', '.join(state_names)}")

    job_status["new_logs"].append(f"Config: {', '.join(filters_active)}")

    # Correctly select the target file from the map
    target_file = REGION_FILES.get(region, 'data/cities.txt')
    full_path = os.path.join(DATA_DIR, filename)

    # Determine minimum population based on mode
    if scrape_mode == 'quick':
        min_pop = 50000  # Only major cities
    elif scrape_mode == 'thorough':
        min_pop = MIN_POPULATION_THOROUGH  # Include smaller cities (5k+)
    else:  # smart (default)
        min_pop = MIN_POPULATION_DEFAULT  # 10k+ cities

    # Load Cities with population-based filtering
    cities = []
    total_in_file = 0
    filtered_by_state = 0
    try:
        with open(target_file, 'r', encoding='utf-8-sig') as f:
            for line in f:
                line = line.strip()
                if not line or line.lower().startswith("name,latitude"):
                    continue
                total_in_file += 1
                parts = line.split(',')
                if len(parts) >= 3:
                    lat = parts[1].strip()
                    lon = parts[2].strip()

                    # Try to get population (4th column if exists)
                    population = 0
                    if len(parts) >= 4:
                        try:
                            population = int(parts[3].strip())
                        except ValueError:
                            population = 50000  # Default if can't parse

                    # Skip cities below minimum population (for Germany with pop data)
                    if region == 'de' and population < min_pop:
                        continue

                    # Filter by Bundesland if specified (Germany only)
                    if region == 'de' and bundeslaender and len(bundeslaender) > 0:
                        city_bundesland = get_bundesland(lat, lon)
                        if city_bundesland not in bundeslaender:
                            filtered_by_state += 1
                            continue

                    cities.append({
                        "name": parts[0].strip(),
                        "lat": lat,
                        "lon": lon,
                        "population": population
                    })

        # Sort by population (largest first) to prioritize big cities
        cities.sort(key=lambda x: x['population'], reverse=True)

        log_msg = f"Selected {len(cities)} cities from {total_in_file} total (min pop: {min_pop:,})"
        if filtered_by_state > 0:
            log_msg += f", filtered {filtered_by_state} by state"
        job_status["new_logs"].append(log_msg)

    except FileNotFoundError:
        error_msg = f"Error: City list {target_file} not found."
        print(error_msg)
        job_status["status_message"] = error_msg
        job_status["is_running"] = False
        return

    # Initialize CSV with comprehensive headers
    with open(full_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADERS)

    # Global set to track all seen business IDs across ALL cities (prevents duplicates)
    seen_ids = set()

    # Scrape Loop with smart configuration per city
    for city in cities:
        if job_status["total_leads"] >= int(num_leads): break
        if not job_status["is_running"]: break

        # Get dynamic config based on city population
        zoom_level, max_pages = get_city_scrape_config(city['population'])

        pop_str = f" ({city['population']:,})" if city['population'] > 0 else ""
        job_status["current_city"] = f"{city['name']}{pop_str}"
        city_specific_query = f"{final_query} in {city['name']}"

        city_leads_before = job_status["total_leads"]

        # Dynamic pages based on city size
        for page in range(max_pages):
            if job_status["total_leads"] >= int(num_leads): break
            if not job_status["is_running"]: break

            data = get_places_by_gps(city_specific_query, city['lat'], city['lon'], region, page * 20, zoom_level)

            if not data or 'places' not in data or not data['places']:
                break

            new_items_count = 0
            with open(full_path, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for p in data['places']:
                    if job_status["total_leads"] >= int(num_leads): break

                    # Extract all place data
                    place_data = extract_place_data(p, final_query, city['name'])
                    pid = place_data['place_id']

                    if pid and pid not in seen_ids:
                        seen_ids.add(pid)

                        # Apply filters
                        if not passes_filters(place_data, min_rating, min_reviews, False, False):
                            job_status["total_skipped"] += 1
                            continue

                        new_items_count += 1
                        job_status["total_leads"] += 1

                        # Log visible to user
                        rating_str = f" ({place_data['rating']})" if place_data['rating'] else ""
                        job_status["new_logs"].append(f"{place_data['name']}{rating_str} ({city['name']})")

                        # Write to CSV
                        write_place_to_csv(writer, place_data)

            if new_items_count == 0: break
            time.sleep(0.5)  # Respectful API delay

        # Log city summary for large cities
        city_leads = job_status["total_leads"] - city_leads_before
        if city['population'] >= 100000 and city_leads > 0:
            job_status["new_logs"].append(f"  → {city['name']}: {city_leads} leads (zoom:{zoom_level}, pages:{max_pages})")

    # Job Finished
    job_status["is_running"] = False
    if job_status["total_leads"] >= int(num_leads):
        job_status["status_message"] = "Limit reached."
    else:
        job_status["status_message"] = "Job finished."

    if job_status["total_skipped"] > 0:
        job_status["new_logs"].append(f"Filtered out {job_status['total_skipped']} businesses")

    job_status["current_city"] = "Done"

    # Save the completed run to history
    save_to_history(search_term, region, job_status["total_leads"], filename)


def plz_scraper_worker(search_term, num_leads, match_type, filename,
                       min_rating=0, min_reviews=0, bundeslaender=None):
    """
    Maximum coverage scraper using PLZ (postal code) grid for Germany.
    Uses dynamic pagination - continues until no new results are found.
    Covers all of Germany including rural areas.
    """
    global job_status
    job_status["is_running"] = True
    job_status["total_leads"] = 0
    job_status["total_skipped"] = 0
    job_status["new_logs"] = []
    job_status["current_filename"] = filename
    job_status["status_message"] = f"Starting PLZ-based scrape for '{search_term}'..."

    final_query = search_term
    if match_type == 'literal':
        final_query = f'"{search_term}"'

    # Log configuration
    filters_active = ["mode: PLZ (maximum coverage)"]
    if min_rating > 0:
        filters_active.append(f"min rating: {min_rating}")
    if min_reviews > 0:
        filters_active.append(f"min reviews: {min_reviews}")

    if bundeslaender and len(bundeslaender) > 0:
        state_names = [BUNDESLAENDER[bl]['name'] for bl in bundeslaender if bl in BUNDESLAENDER]
        filters_active.append(f"states: {', '.join(state_names)}")

    job_status["new_logs"].append(f"Config: {', '.join(filters_active)}")

    # Load PLZ data
    plz_list, filtered_count = load_plz_data(bundeslaender)

    if not plz_list:
        job_status["status_message"] = "Error: No PLZ data found."
        job_status["is_running"] = False
        return

    log_msg = f"Loaded {len(plz_list)} postal codes"
    if filtered_count > 0:
        log_msg += f" (filtered {filtered_count} by state)"
    job_status["new_logs"].append(log_msg)

    full_path = os.path.join(DATA_DIR, filename)

    # Initialize CSV with comprehensive headers
    with open(full_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADERS)

    # Global set to track all seen business IDs (prevents duplicates)
    seen_ids = set()

    # Progress tracking
    total_plz = len(plz_list)
    processed_plz = 0

    # Scrape each PLZ with dynamic pagination
    for plz_data in plz_list:
        if job_status["total_leads"] >= int(num_leads):
            break
        if not job_status["is_running"]:
            break

        processed_plz += 1
        plz = plz_data['plz']
        lat = plz_data['lat']
        lon = plz_data['lon']

        # Update status with progress
        progress_pct = int((processed_plz / total_plz) * 100)
        job_status["current_city"] = f"PLZ {plz} ({progress_pct}% - {processed_plz}/{total_plz})"

        plz_leads_before = job_status["total_leads"]

        # Dynamic pagination - continue until no new unique results
        page = 0
        consecutive_empty = 0
        max_pages = 50  # Safety limit

        while page < max_pages:
            if job_status["total_leads"] >= int(num_leads):
                break
            if not job_status["is_running"]:
                break

            # Use zoom 15 for precise PLZ coverage
            data = get_places_by_gps(final_query, lat, lon, 'de', page * 20, zoom=15)

            if not data or 'places' not in data or not data['places']:
                break

            new_items_count = 0
            with open(full_path, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for p in data['places']:
                    if job_status["total_leads"] >= int(num_leads):
                        break

                    # Extract all place data
                    place_data = extract_place_data(p, final_query, f"PLZ {plz}")
                    pid = place_data['place_id']

                    if pid and pid not in seen_ids:
                        seen_ids.add(pid)

                        # Apply filters
                        if not passes_filters(place_data, min_rating, min_reviews, False, False):
                            job_status["total_skipped"] += 1
                            continue

                        new_items_count += 1
                        job_status["total_leads"] += 1

                        # Log visible to user (less verbose for PLZ mode)
                        if job_status["total_leads"] % 10 == 0:  # Log every 10th lead
                            job_status["new_logs"].append(
                                f"{job_status['total_leads']} leads... (PLZ {plz})"
                            )

                        # Write to CSV
                        write_place_to_csv(writer, place_data)

            # Dynamic pagination: stop if no new unique items found
            if new_items_count == 0:
                consecutive_empty += 1
                if consecutive_empty >= 2:  # Stop after 2 consecutive empty pages
                    break
            else:
                consecutive_empty = 0

            page += 1
            time.sleep(0.3)  # Respectful API delay

        # Log PLZ summary if we got results
        plz_leads = job_status["total_leads"] - plz_leads_before
        if plz_leads >= 10:  # Only log PLZs with significant results
            job_status["new_logs"].append(f"  → PLZ {plz}: {plz_leads} leads")

    # Job Finished
    job_status["is_running"] = False
    if job_status["total_leads"] >= int(num_leads):
        job_status["status_message"] = "Limit reached."
    else:
        job_status["status_message"] = "Job finished - all PLZ areas scraped."

    if job_status["total_skipped"] > 0:
        job_status["new_logs"].append(f"Filtered out {job_status['total_skipped']} businesses")

    job_status["new_logs"].append(f"Total unique businesses found: {job_status['total_leads']}")
    job_status["current_city"] = "Done"

    # Save the completed run to history
    save_to_history(search_term, 'de', job_status["total_leads"], filename)


# --- ROUTES ---

@app.route('/')
def index():
    return render_template('index.html')

def multi_query_scraper_worker(queries, num_leads, region, filename,
                               min_rating=0, min_reviews=0, scrape_mode='smart',
                               bundeslaender=None):
    """
    Scraper that runs multiple query variations sequentially.
    All results go into a single CSV with global deduplication.
    """
    global job_status
    job_status["is_running"] = True
    job_status["total_leads"] = 0
    job_status["total_skipped"] = 0
    job_status["new_logs"] = []
    job_status["current_filename"] = filename
    job_status["status_message"] = f"Starting multi-query scrape ({len(queries)} variations)..."

    job_status["new_logs"].append(f"Running {len(queries)} query variations:")
    for i, q in enumerate(queries[:5], 1):  # Show first 5
        job_status["new_logs"].append(f"  {i}. {q}")
    if len(queries) > 5:
        job_status["new_logs"].append(f"  ... and {len(queries) - 5} more")

    full_path = os.path.join(DATA_DIR, filename)

    # Initialize CSV
    with open(full_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADERS)

    # Global deduplication across ALL queries
    seen_ids = set()

    # Determine min population
    if scrape_mode == 'quick':
        min_pop = 50000
    elif scrape_mode == 'thorough':
        min_pop = MIN_POPULATION_THOROUGH
    else:
        min_pop = MIN_POPULATION_DEFAULT

    # Load cities once
    target_file = REGION_FILES.get(region, 'data/cities.txt')
    cities = []
    try:
        with open(target_file, 'r', encoding='utf-8-sig') as f:
            for line in f:
                line = line.strip()
                if not line or line.lower().startswith("name,latitude"):
                    continue
                parts = line.split(',')
                if len(parts) >= 3:
                    lat = parts[1].strip()
                    lon = parts[2].strip()
                    population = 0
                    if len(parts) >= 4:
                        try:
                            population = int(parts[3].strip())
                        except ValueError:
                            population = 50000

                    if region == 'de' and population < min_pop:
                        continue

                    if region == 'de' and bundeslaender and len(bundeslaender) > 0:
                        city_bundesland = get_bundesland(lat, lon)
                        if city_bundesland not in bundeslaender:
                            continue

                    cities.append({
                        "name": parts[0].strip(),
                        "lat": lat,
                        "lon": lon,
                        "population": population
                    })
        cities.sort(key=lambda x: x['population'], reverse=True)
    except FileNotFoundError:
        job_status["status_message"] = f"Error: City list not found."
        job_status["is_running"] = False
        return

    job_status["new_logs"].append(f"Loaded {len(cities)} cities")

    # Run each query
    for query_idx, query in enumerate(queries):
        if job_status["total_leads"] >= int(num_leads):
            break
        if not job_status["is_running"]:
            break

        job_status["new_logs"].append(f"--- Query {query_idx + 1}/{len(queries)}: {query} ---")

        for city in cities:
            if job_status["total_leads"] >= int(num_leads):
                break
            if not job_status["is_running"]:
                break

            zoom_level, max_pages = get_city_scrape_config(city['population'])
            job_status["current_city"] = f"{city['name']} [{query[:20]}...]"

            city_specific_query = f"{query} in {city['name']}"

            for page in range(max_pages):
                if job_status["total_leads"] >= int(num_leads):
                    break
                if not job_status["is_running"]:
                    break

                data = get_places_by_gps(city_specific_query, city['lat'], city['lon'], region, page * 20, zoom_level)

                if not data or 'places' not in data or not data['places']:
                    break

                new_items_count = 0
                with open(full_path, mode='a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    for p in data['places']:
                        if job_status["total_leads"] >= int(num_leads):
                            break

                        place_data = extract_place_data(p, query, city['name'])
                        pid = place_data['place_id']

                        if pid and pid not in seen_ids:
                            seen_ids.add(pid)

                            if not passes_filters(place_data, min_rating, min_reviews, False, False):
                                job_status["total_skipped"] += 1
                                continue

                            new_items_count += 1
                            job_status["total_leads"] += 1

                            if job_status["total_leads"] % 25 == 0:
                                job_status["new_logs"].append(f"{job_status['total_leads']} leads found...")

                            write_place_to_csv(writer, place_data)

                if new_items_count == 0:
                    break
                time.sleep(0.3)

    # Job Finished
    job_status["is_running"] = False
    job_status["status_message"] = "Job finished." if job_status["total_leads"] < int(num_leads) else "Limit reached."
    job_status["new_logs"].append(f"Total unique businesses: {job_status['total_leads']}")
    job_status["current_city"] = "Done"

    save_to_history(queries[0] if queries else "multi-query", region, job_status["total_leads"], filename)


@app.route('/run-scrape', methods=['POST'])
def run_scrape():
    if job_status["is_running"]:
        return jsonify({"status": "error", "message": "Job already running."})

    data = request.json

    # Extract parameters
    search_term = data.get('search_term', '')
    region = data.get('region', 'de')
    num_leads = int(data.get('num_leads', 10))
    match_type = data.get('match_type', 'literal')
    min_rating = float(data.get('min_rating', 0))
    min_reviews = int(data.get('min_reviews', 0))
    scrape_mode = data.get('scrape_mode', 'smart')
    bundeslaender = data.get('bundeslaender', [])

    # NEW: Query expansion options
    expand_queries = data.get('expand_queries', False)  # Enable query variations
    category_key = data.get('category', None)  # Use category bundle instead of search term

    # Generate filename
    if category_key:
        safe_term = category_key
    else:
        safe_term = "".join([c if c.isalnum() else "_" for c in search_term])
    timestamp = int(time.time())
    filename = f"{safe_term}_{region}_{timestamp}.csv"

    # Determine queries to run
    queries = []

    if category_key and category_key in CATEGORY_BUNDLES:
        # Use category bundle queries
        queries = CATEGORY_BUNDLES[category_key]['queries']
    elif expand_queries and region == 'de':
        # Expand single query into variations
        queries = expand_query_variations(search_term, include_german=True, include_broad=True)
    else:
        # Single query mode (original behavior)
        if match_type == 'literal':
            queries = [f'"{search_term}"']
        else:
            queries = [search_term]

    # Choose worker based on mode
    if len(queries) > 1:
        # Multiple queries - use multi-query worker
        thread = threading.Thread(
            target=multi_query_scraper_worker,
            args=(
                queries,
                num_leads,
                region,
                filename,
                min_rating,
                min_reviews,
                scrape_mode,
                bundeslaender
            )
        )
    elif scrape_mode == 'max' and region == 'de':
        # PLZ-based scraper for maximum coverage
        thread = threading.Thread(
            target=plz_scraper_worker,
            args=(
                queries[0] if queries else search_term,
                num_leads,
                match_type,
                filename,
                min_rating,
                min_reviews,
                bundeslaender
            )
        )
    else:
        # Regular city-based scraper
        thread = threading.Thread(
            target=scraper_worker,
            args=(
                search_term,
                num_leads,
                match_type,
                region,
                filename,
                min_rating,
                min_reviews,
                scrape_mode,
                bundeslaender
            )
        )

    thread.daemon = True
    thread.start()

    return jsonify({"status": "success", "message": "Started scraping."})

@app.route('/status', methods=['GET'])
def status():
    response = job_status.copy()
    # Clear logs after sending so we don't duplicate on frontend
    job_status["new_logs"] = [] 
    return jsonify(response)

@app.route('/history', methods=['GET'])
def get_history():
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'r') as f:
            try:
                return jsonify(json.load(f))
            except:
                return jsonify([])
    return jsonify([])

@app.route('/download/<path:filename>')
def download_file(filename):
    try:
        filter_type = request.args.get('filter', None)

        # If no filter, just return the original file
        if not filter_type:
            return send_from_directory(DATA_DIR, filename, as_attachment=True)

        # Read the CSV and filter it
        full_path = os.path.join(DATA_DIR, filename)

        with open(full_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)

        if len(rows) < 2:
            return send_from_directory(DATA_DIR, filename, as_attachment=True)

        header = rows[0]
        data_rows = rows[1:]

        # Find column indices (Website is col 5, Phone is col 4 in our CSV)
        try:
            website_idx = header.index('Website')
            phone_idx = header.index('Phone')
        except ValueError:
            # Fallback if headers don't match
            return send_from_directory(DATA_DIR, filename, as_attachment=True)

        # Filter based on filter_type
        filtered_rows = []
        for row in data_rows:
            has_website = len(row) > website_idx and row[website_idx].strip()
            has_phone = len(row) > phone_idx and row[phone_idx].strip()

            if filter_type == 'website' and has_website:
                filtered_rows.append(row)
            elif filter_type == 'phone' and has_phone:
                filtered_rows.append(row)
            elif filter_type == 'both' and has_website and has_phone:
                filtered_rows.append(row)

        # Create filtered CSV in memory
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(header)
        writer.writerows(filtered_rows)

        # Generate filtered filename
        base_name = filename.rsplit('.', 1)[0]
        filtered_filename = f"{base_name}_filtered_{filter_type}.csv"

        return Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={filtered_filename}'}
        )

    except Exception as e:
        return str(e), 404

# --- SEARCH TERMS CONFIG ROUTES ---

@app.route('/search-terms', methods=['GET'])
def get_search_terms():
    """Get all search terms configuration."""
    config = load_search_terms_config()
    return jsonify(config)

@app.route('/search-terms/<region>', methods=['GET', 'POST'])
def manage_region_search_terms(region):
    """Get or update search terms for a specific region."""
    config = load_search_terms_config()

    if request.method == 'GET':
        return jsonify(config.get(region, []))

    if request.method == 'POST':
        data = request.json
        terms = data.get('terms', [])
        config[region] = terms
        save_search_terms_config(config)
        return jsonify({"status": "success", "terms": terms})

@app.route('/countries', methods=['GET'])
def get_countries():
    """Get list of available countries with their codes and names."""
    countries = []
    for code, name in COUNTRY_NAMES.items():
        countries.append({
            "code": code,
            "name": name,
            "has_cities": os.path.exists(REGION_FILES.get(code, ''))
        })
    return jsonify(countries)

@app.route('/bundeslaender', methods=['GET'])
def get_bundeslaender():
    """Get list of German Bundesländer (federal states)."""
    states = []
    for code, data in BUNDESLAENDER.items():
        states.append({
            "code": code,
            "name": data['name']
        })
    # Sort alphabetically by name
    states.sort(key=lambda x: x['name'])
    return jsonify(states)

@app.route('/categories', methods=['GET'])
def get_categories():
    """Get list of available category bundles for search."""
    categories = []
    for key, data in CATEGORY_BUNDLES.items():
        categories.append({
            "key": key,
            "name": data['name'],
            "query_count": len(data['queries']),
            "queries": data['queries']
        })
    # Sort alphabetically by name
    categories.sort(key=lambda x: x['name'])
    return jsonify(categories)

# --- BATCH SCRAPE WORKER ---

def batch_scraper_worker(selected_countries, num_leads_per_term, match_type,
                         min_rating=0, min_reviews=0, scrape_mode='smart'):
    """
    Worker that scrapes multiple countries with their configured search terms.
    Creates one CSV per country containing all leads for all search terms.
    Uses smart city prioritization based on population.
    """
    global job_status
    job_status["is_running"] = True
    job_status["total_leads"] = 0
    job_status["total_skipped"] = 0
    job_status["new_logs"] = []
    job_status["status_message"] = "Starting batch scrape..."

    # Log filters if any are active
    filters_active = []
    if min_rating > 0:
        filters_active.append(f"min rating: {min_rating}")
    if min_reviews > 0:
        filters_active.append(f"min reviews: {min_reviews}")
    filters_active.append(f"mode: {scrape_mode}")
    job_status["new_logs"].append(f"Config: {', '.join(filters_active)}")

    # Determine minimum population based on mode
    if scrape_mode == 'quick':
        min_pop = 50000
    elif scrape_mode == 'thorough':
        min_pop = MIN_POPULATION_THOROUGH
    else:
        min_pop = MIN_POPULATION_DEFAULT

    config = load_search_terms_config()
    timestamp = int(time.time())

    # Global set to track ALL seen business IDs across ALL countries/terms (prevents duplicates)
    global_seen_ids = set()

    for region in selected_countries:
        if not job_status["is_running"]:
            break

        terms = config.get(region, [])
        if not terms:
            job_status["new_logs"].append(f"Skipping {COUNTRY_NAMES.get(region, region)} - no search terms configured")
            continue

        # Create filename for this country
        filename = f"batch_{region}_{timestamp}.csv"
        full_path = os.path.join(DATA_DIR, filename)
        job_status["current_filename"] = filename

        # Initialize CSV with comprehensive headers
        with open(full_path, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(CSV_HEADERS)

        # Load cities for this region with population-based filtering
        target_file = REGION_FILES.get(region, 'data/cities.txt')
        cities = []
        total_in_file = 0
        try:
            with open(target_file, 'r', encoding='utf-8-sig') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.lower().startswith("name,latitude"):
                        continue
                    total_in_file += 1
                    parts = line.split(',')
                    if len(parts) >= 3:
                        # Try to get population
                        population = 0
                        if len(parts) >= 4:
                            try:
                                population = int(parts[3].strip())
                            except ValueError:
                                population = 50000

                        # Skip small cities for Germany
                        if region == 'de' and population < min_pop:
                            continue

                        cities.append({
                            "name": parts[0].strip(),
                            "lat": parts[1].strip(),
                            "lon": parts[2].strip(),
                            "population": population
                        })

            # Sort by population
            cities.sort(key=lambda x: x['population'], reverse=True)
            job_status["new_logs"].append(f"[{COUNTRY_NAMES.get(region, region)}] Selected {len(cities)} cities (min pop: {min_pop:,})")
        except FileNotFoundError:
            job_status["new_logs"].append(f"[{COUNTRY_NAMES.get(region, region)}] City file not found: {target_file}")
            continue

        country_lead_count = 0
        country_skipped = 0

        # Process each search term for this country
        for search_term in terms:
            if not job_status["is_running"]:
                break

            final_query = search_term
            if match_type == 'literal':
                final_query = f'"{search_term}"'

            job_status["new_logs"].append(f"[{COUNTRY_NAMES.get(region, region)}] Searching: {search_term}")
            term_lead_count = 0

            # Scrape each city with smart config
            for city in cities:
                if term_lead_count >= int(num_leads_per_term):
                    break
                if not job_status["is_running"]:
                    break

                # Get dynamic config based on city population
                zoom_level, max_pages = get_city_scrape_config(city['population'])

                pop_str = f" ({city['population']:,})" if city['population'] > 0 else ""
                job_status["current_city"] = f"{city['name']}{pop_str} ({region.upper()})"
                city_specific_query = f"{final_query} in {city['name']}"

                # Dynamic pages based on city size
                for page in range(max_pages):
                    if term_lead_count >= int(num_leads_per_term):
                        break
                    if not job_status["is_running"]:
                        break

                    data = get_places_by_gps(city_specific_query, city['lat'], city['lon'], region, page * 20, zoom_level)

                    if not data or 'places' not in data or not data['places']:
                        break

                    new_items_count = 0
                    with open(full_path, mode='a', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        for p in data['places']:
                            if term_lead_count >= int(num_leads_per_term):
                                break

                            # Extract all place data
                            place_data = extract_place_data(p, search_term, city['name'])
                            pid = place_data['place_id']

                            if pid and pid not in global_seen_ids:
                                global_seen_ids.add(pid)

                                # Apply filters (no website/phone requirements during scrape)
                                if not passes_filters(place_data, min_rating, min_reviews, False, False):
                                    job_status["total_skipped"] += 1
                                    country_skipped += 1
                                    continue

                                new_items_count += 1
                                term_lead_count += 1
                                country_lead_count += 1
                                job_status["total_leads"] += 1

                                # Log visible to user
                                rating_str = f" ({place_data['rating']})" if place_data['rating'] else ""
                                job_status["new_logs"].append(f"{place_data['name']}{rating_str} ({city['name']})")

                                # Write to CSV
                                write_place_to_csv(writer, place_data)

                    if new_items_count == 0:
                        break
                    time.sleep(0.5)

        # Save to history for this country
        if country_lead_count > 0:
            save_to_history(f"Batch: {', '.join(terms)}", region, country_lead_count, filename)
            skipped_msg = f" (filtered: {country_skipped})" if country_skipped > 0 else ""
            job_status["new_logs"].append(f"[{COUNTRY_NAMES.get(region, region)}] Completed: {country_lead_count} leads{skipped_msg}")

    job_status["is_running"] = False
    job_status["status_message"] = "Batch job finished."
    job_status["current_city"] = "Done"

    if job_status["total_skipped"] > 0:
        job_status["new_logs"].append(f"Total filtered out: {job_status['total_skipped']} businesses")

@app.route('/run-batch-scrape', methods=['POST'])
def run_batch_scrape():
    """Start a batch scrape across multiple countries with their configured search terms."""
    if job_status["is_running"]:
        return jsonify({"status": "error", "message": "Job already running."})

    data = request.json
    selected_countries = data.get('countries', [])
    num_leads_per_term = int(data.get('num_leads_per_term', 50))
    match_type = data.get('match_type', 'literal')

    # Extract filter parameters
    min_rating = float(data.get('min_rating', 0))
    min_reviews = int(data.get('min_reviews', 0))
    scrape_mode = data.get('scrape_mode', 'smart')

    if not selected_countries:
        return jsonify({"status": "error", "message": "No countries selected."})

    thread = threading.Thread(
        target=batch_scraper_worker,
        args=(selected_countries, num_leads_per_term, match_type,
              min_rating, min_reviews, scrape_mode)
    )
    thread.daemon = True
    thread.start()

    return jsonify({"status": "success", "message": "Batch scrape started."})

@app.route('/stop', methods=['POST'])
def stop_scrape():
    """Stop the current scraping job."""
    global job_status
    job_status["is_running"] = False
    job_status["status_message"] = "Job stopped by user."
    return jsonify({"status": "success", "message": "Stop signal sent."})

if __name__ == '__main__':
    app.run(debug=True)