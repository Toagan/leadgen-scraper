import os
import csv
import json
import time
import threading
import requests
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_from_directory
from dotenv import load_dotenv

# 1. Setup & Configuration
load_dotenv()
app = Flask(__name__)

# Configuration
API_KEY = os.getenv("SERPER_API_KEY")

# Map regions to your data files
# 'de' now correctly points to 'data/cities.txt' which contains your State data
REGION_FILES = {
    'de': 'data/cities.txt', 
    'au': 'data/cities_au.txt',
    'ch': 'data/cities_ch.txt',      
    'us': 'data/cities_us.txt',
    'uk': 'data/cities_uk.txt',
    'fr': 'data/cities_fr.txt',
    'es': 'data/cities_es.txt',
    'it': 'data/cities_it.txt',
    'br': 'data/cities_br.txt',
    'in': 'data/cities_in.txt',
    'jp': 'data/cities_jp.txt'
}

# Ensure directories exist for data storage
DATA_DIR = "data_exports"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

HISTORY_FILE = "search_history.json"

# Global Job Status
job_status = {
    "is_running": False,
    "current_city": "",
    "total_leads": 0,
    "api_calls": 0, # Tracks Serper Credits
    "status_message": "Idle",
    "new_logs": [],
    "current_filename": ""
}

# --- HELPER FUNCTIONS ---

def save_to_history(term, region, leads_count, filename):
    """Saves the search details to a JSON file."""
    entry = {
        "timestamp": time.time(),
        "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "term": term,
        "region": region.upper(),
        "leads_found": leads_count,
        "filename": filename
    }
    
    history = []
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'r') as f:
            try: history = json.load(f)
            except: history = []
    
    # Add new entry to the TOP of the list
    history.insert(0, entry)
    
    with open(HISTORY_FILE, 'w') as f:
        json.dump(history, f, indent=4)

def get_places_by_gps(query, lat, lon, country_code, start_index=0):
    """Calls the Serper.dev 'places' endpoint."""
    url = "https://google.serper.dev/places"
    location_bias = f"@{lat},{lon},14z"
    
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

def scraper_worker(search_term, limit_val, limit_mode, match_type, region, filename, sub_region=None):
    global job_status
    job_status["is_running"] = True
    job_status["total_leads"] = 0
    job_status["api_calls"] = 0
    job_status["new_logs"] = []
    job_status["current_filename"] = filename
    
    # Global Deduplication Set for this specific job
    # This ensures we NEVER save the same Place ID twice in one run
    job_seen_ids = set()

    try:
        limit_val = int(limit_val)
    except:
        limit_val = 50
    
    # Display Text Logic
    location_msg = region.upper()
    target_states = []
    
    # Handle Multi-Select List vs String
    if sub_region:
        if isinstance(sub_region, list):
            target_states = [s for s in sub_region if s] # Remove empty strings
            if target_states:
                location_msg += f" ({len(target_states)} States)"
        else:
            target_states = [sub_region]
            location_msg += f" ({sub_region})"
    
    limit_msg = f"{limit_val} Leads" if limit_mode == 'leads' else f"{limit_val} Credits"
    job_status["status_message"] = f"Scraping '{search_term}' in {location_msg} (Stop: {limit_msg})..."
    
    final_query = search_term
    if match_type == 'literal':
        final_query = f'"{search_term}"'
    
    target_file = REGION_FILES.get(region, 'data/cities.txt')
    full_path = os.path.join(DATA_DIR, filename)

    # --- 1. LOAD CITIES & FILTER BY STATE ---
    cities = []
    try:
        with open(target_file, 'r', encoding='utf-8-sig') as f:
            for line in f:
                line = line.strip()
                # Skip header or empty lines
                if not line or line.lower().startswith("name,latitude"):
                    continue
                
                parts = line.split(',')
                if len(parts) >= 3:
                    city_name = parts[0].strip()
                    city_lat = parts[1].strip()
                    city_lon = parts[2].strip()
                    # Get State from 4th column if it exists
                    city_state = parts[3].strip() if len(parts) > 3 else ""

                    # Filter Logic:
                    # If user selected specific states (and didn't select "All"),
                    # skip cities that don't match.
                    if region == 'de' and target_states:
                        if city_state not in target_states:
                            continue

                    cities.append({
                        "name": city_name, 
                        "lat": city_lat, 
                        "lon": city_lon,
                        "state": city_state
                    })
        
        if not cities:
            job_status["status_message"] = f"No cities found for selected states."
            job_status["is_running"] = False
            return

        job_status["new_logs"].append(f"Targeting {len(cities)} cities...")

    except FileNotFoundError:
        error_msg = f"Error: City list {target_file} not found."
        print(error_msg)
        job_status["status_message"] = error_msg
        job_status["is_running"] = False
        return

    # --- 2. INITIALIZE CSV ---
    with open(full_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # Optimized for Agency Work: Includes specific placeholder for Email
        writer.writerow(['Search Term', 'City', 'State', 'Name', 'Address', 'Phone', 'Website', 'Email (Pending)', 'Rating', 'Place ID', 'Lat', 'Lon'])

    # --- 3. SCRAPE LOOP ---
    for city in cities:
        # Check limits before starting a new city
        if not job_status["is_running"]: break
        if limit_mode == 'leads' and job_status["total_leads"] >= limit_val: break
        if limit_mode == 'credits' and job_status["api_calls"] >= limit_val: break

        job_status["current_city"] = city['name']
        city_specific_query = f"{final_query} in {city['name']}"
        
        # Pagination: 5 pages (100 results) Safety Cap per City
        for page in range(5): 
            # Check Limits before API call
            if limit_mode == 'leads' and job_status["total_leads"] >= limit_val: break
            if limit_mode == 'credits' and job_status["api_calls"] >= limit_val: break
            if not job_status["is_running"]: break
            
            # --- API CALL ---
            data = get_places_by_gps(city_specific_query, city['lat'], city['lon'], region, page * 20)
            job_status["api_calls"] += 1 
            
            # Stop if no data returned
            if not data or 'places' not in data or not data['places']: 
                break

            new_items_count = 0
            with open(full_path, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for p in data['places']:
                    if limit_mode == 'leads' and job_status["total_leads"] >= limit_val: break

                    pid = p.get('cid') or p.get('place_id')
                    
                    # --- DUPLICATE CHECK ---
                    # If we have seen this Place ID in this job already, SKIP IT.
                    if pid not in job_seen_ids:
                        job_seen_ids.add(pid)
                        new_items_count += 1
                        job_status["total_leads"] += 1
                        
                        company_name = p.get('title', 'Unknown')
                        job_status["new_logs"].append(f"{company_name} ({city['name']})")
                        
                        writer.writerow([
                            final_query, 
                            city['name'], 
                            city['state'], 
                            company_name, 
                            p.get('address', ''),
                            p.get('phoneNumber', ''), 
                            p.get('website', ''),     
                            "", # Empty Email column for enrichment tools
                            p.get('rating', ''),
                            pid, city['lat'], city['lon']
                        ])
            
            # If this page had 0 *new* items (or API returned duplicates), stop paginating this city
            if new_items_count == 0: break
            time.sleep(0.2) # Gentle delay for API stability

    job_status["is_running"] = False
    
    # Final Status Logic
    if limit_mode == 'leads' and job_status["total_leads"] >= limit_val:
        job_status["status_message"] = "Target Lead count reached."
    elif limit_mode == 'credits' and job_status["api_calls"] >= limit_val:
        job_status["status_message"] = "API Credit limit reached."
    else:
        job_status["status_message"] = "Job finished (End of list)."
        
    job_status["current_city"] = "Done"
    save_to_history(search_term, region, job_status["total_leads"], filename)

# --- ROUTES ---

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/run-scrape', methods=['POST'])
def run_scrape():
    if job_status["is_running"]:
        return jsonify({"status": "error", "message": "Job already running."})
    
    data = request.json
    
    # Filename generation
    safe_term = "".join([c if c.isalnum() else "_" for c in data.get('search_term')])
    timestamp = int(time.time())
    
    region_code = data.get('region')
    sub_region = data.get('sub_region')
    
    filename_suffix = region_code
    if isinstance(sub_region, list) and len(sub_region) > 0:
        if "" not in sub_region: filename_suffix += "_MultiState"
    elif isinstance(sub_region, str) and sub_region:
        filename_suffix += f"_{sub_region}"
        
    filename = f"{safe_term}_{filename_suffix}_{timestamp}.csv"

    thread = threading.Thread(
        target=scraper_worker, 
        args=(
            data.get('search_term'), 
            data.get('limit_value'), 
            data.get('limit_mode'),  # 'leads' or 'credits'
            data.get('match_type'), 
            data.get('region'), 
            filename,
            sub_region
        )
    )
    thread.daemon = True
    thread.start()

    return jsonify({"status": "success", "message": "Started scraping."})

@app.route('/status', methods=['GET'])
def status():
    response = job_status.copy()
    job_status["new_logs"] = [] 
    return jsonify(response)

@app.route('/history', methods=['GET'])
def get_history():
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'r') as f:
            try: return jsonify(json.load(f))
            except: return jsonify([])
    return jsonify([])

@app.route('/download/<path:filename>')
def download_file(filename):
    try: return send_from_directory(DATA_DIR, filename, as_attachment=True)
    except Exception as e: return str(e), 404

if __name__ == '__main__':
    app.run(debug=True)