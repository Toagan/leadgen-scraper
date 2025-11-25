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

def get_places_by_gps(query, lat, lon, country_code, start_index=0):
    url = "https://google.serper.dev/places"
    location_bias = f"@{lat},{lon},14z"
    
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

def scraper_worker(search_term, num_leads, match_type, region, filename):
    global job_status
    job_status["is_running"] = True
    job_status["total_leads"] = 0
    job_status["new_logs"] = []
    job_status["current_filename"] = filename
    job_status["status_message"] = f"Starting scrape for '{search_term}' in {region.upper()}..."
    
    final_query = search_term
    if match_type == 'literal':
        final_query = f'"{search_term}"'
    
    # Correctly select the target file from the map
    target_file = REGION_FILES.get(region, 'data/cities.txt')
    full_path = os.path.join(DATA_DIR, filename)

    # Load Cities
    cities = []
    try:
        with open(target_file, 'r', encoding='utf-8-sig') as f:
            for line in f:
                line = line.strip()
                if not line or line.lower().startswith("name,latitude"):
                    continue
                parts = line.split(',')
                if len(parts) >= 3:
                    cities.append({
                        "name": parts[0].strip(), 
                        "lat": parts[1].strip(), 
                        "lon": parts[2].strip()
                    })
        
        job_status["new_logs"].append(f"Loaded {len(cities)} target cities from {target_file}")

    except FileNotFoundError:
        error_msg = f"Error: City list {target_file} not found."
        print(error_msg)
        job_status["status_message"] = error_msg
        job_status["is_running"] = False
        return

    # Initialize CSV
    with open(full_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Search Term', 'City', 'Name', 'Address', 'Phone', 'Website', 'Rating', 'Place ID', 'Lat', 'Lon'])

    # Scrape Loop
    for city in cities:
        if job_status["total_leads"] >= int(num_leads): break
        if not job_status["is_running"]: break

        job_status["current_city"] = city['name']
        city_specific_query = f"{final_query} in {city['name']}"
        seen_ids = set()
        
        # Max 3 pages per city to ensure distribution
        for page in range(3): 
            if job_status["total_leads"] >= int(num_leads): break
            if not job_status["is_running"]: break
            
            data = get_places_by_gps(city_specific_query, city['lat'], city['lon'], region, page * 20)
            
            if not data or 'places' not in data or not data['places']: 
                break

            new_items_count = 0
            with open(full_path, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for p in data['places']:
                    if job_status["total_leads"] >= int(num_leads): break

                    pid = p.get('cid') or p.get('place_id')
                    
                    if pid not in seen_ids:
                        seen_ids.add(pid)
                        new_items_count += 1
                        job_status["total_leads"] += 1
                        
                        company_name = p.get('title', 'Unknown')
                        # Log visible to user
                        job_status["new_logs"].append(f"{company_name} ({city['name']})")
                        
                        writer.writerow([
                            final_query, city['name'], company_name, p.get('address', ''),
                            p.get('phoneNumber', ''), p.get('website', ''), p.get('rating', ''),
                            pid, city['lat'], city['lon']
                        ])
            
            if new_items_count == 0: break
            time.sleep(0.5) # Respectful API delay

    # Job Finished
    job_status["is_running"] = False
    if job_status["total_leads"] >= int(num_leads):
        job_status["status_message"] = "Limit reached."
    else:
        job_status["status_message"] = "Job finished."
        
    job_status["current_city"] = "Done"
    
    # Save the completed run to history
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
    
    # Generate a unique filename: marketing_agency_de_1698345.csv
    # Sanitize search term for filename
    safe_term = "".join([c if c.isalnum() else "_" for c in data.get('search_term')])
    timestamp = int(time.time())
    filename = f"{safe_term}_{data.get('region')}_{timestamp}.csv"

    thread = threading.Thread(
        target=scraper_worker, 
        args=(
            data.get('search_term'), 
            int(data.get('num_leads', 10)), 
            data.get('match_type'), 
            data.get('region'), 
            filename
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
        # Securely send file from the data_exports folder
        return send_from_directory(DATA_DIR, filename, as_attachment=True)
    except Exception as e:
        return str(e), 404

if __name__ == '__main__':
    app.run(debug=True)