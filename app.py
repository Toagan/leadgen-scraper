import os
import csv
import json
import time
import threading
import requests
import re
import pandas as pd
from datetime import datetime
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from flask import Flask, render_template, request, jsonify, send_from_directory
from dotenv import load_dotenv

# 1. Setup & Configuration
load_dotenv()
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024 

API_KEY = os.getenv("SERPER_API_KEY")

# Directories
DATA_DIR = "data_exports"
UPLOAD_FOLDER = "uploads"
for folder in [DATA_DIR, UPLOAD_FOLDER]:
    if not os.path.exists(folder): os.makedirs(folder)

HISTORY_FILE = "search_history.json"

REGION_FILES = {
    'de': 'data/cities.txt', 'us': 'data/cities_us.txt', 'uk': 'data/cities_uk.txt',
    'fr': 'data/cities_fr.txt', 'es': 'data/cities_es.txt', 'it': 'data/cities_it.txt',
    'au': 'data/cities_au.txt', 'ch': 'data/cities_ch.txt',
    'br': 'data/cities_br.txt', 'ca': 'data/cities_ca.txt', 'cn': 'data/cities_cn.txt',
    'in': 'data/cities_in.txt', 'jp': 'data/cities_jp.txt', 'ru': 'data/cities_ru.txt'
}

job_status = {"is_running": False, "current_city": "", "total_leads": 0, "api_calls": 0, "new_logs": [], "current_filename": ""}

# --- HELPERS ---
class WebsiteScraper:
    def __init__(self): self.headers = {'User-Agent': 'Mozilla/5.0 Chrome/91.0.4472.124'}
    def extract_emails(self, url):
        try:
            if not url.startswith('http'): url = 'https://' + url
            r = requests.get(url, headers=self.headers, timeout=3)
            if r.status_code != 200: return []
            emails = set(re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', BeautifulSoup(r.text, 'lxml').get_text()))
            return [e for e in emails if not e.endswith(('.png','.jpg','.gif'))][:3]
        except: return []

def extract_domain(url):
    try:
        if not url or not url.strip(): return ""
        p = urlparse(url.strip() if url.startswith(('http','https')) else 'https://'+url.strip())
        return p.netloc[4:] if p.netloc.startswith('www.') else p.netloc
    except: return ""

def save_to_history(term, region, leads, filename):
    """Saves history to a JSON file since we removed the DB."""
    reg = region if isinstance(region, str) else ", ".join(region).upper()
    entry = {
        "timestamp": time.time(),
        "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "term": term,
        "region": reg,
        "leads_found": leads,
        "filename": filename
    }
    hist = []
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE,'r') as f: 
            try: hist = json.load(f)
            except: pass
    hist.insert(0, entry)
    with open(HISTORY_FILE,'w') as f: json.dump(hist, f, indent=4)

def get_places_by_gps(q, lat, lon, gl, start=0):
    if not API_KEY: 
        print("❌ CRITICAL: SERPER_API_KEY missing!")
        return None
    url = "https://google.serper.dev/places"
    if gl=='uk': gl='gb'
    
    # Using verify=False to prevent SSL issues on some local machines
    try: 
        r = requests.post(
            url, 
            headers={'X-API-KEY': API_KEY, 'Content-Type': 'application/json'}, 
            data=json.dumps({"q":q,"gl":gl,"hl":gl,"ll":f"@{lat},{lon},14z","start":start}),
            verify=False 
        )
        if r.status_code != 200:
            print(f"⚠️ Serper API Error {r.status_code}: {r.text}")
            return None
        return r.json()
    except Exception as e: 
        print(f"⚠️ Request Failed: {e}")
        return None

# --- WORKER ---
def scraper_worker(params):
    global job_status
    job_status["is_running"] = True
    job_status["total_leads"] = 0; job_status["api_calls"] = 0
    job_status["new_logs"] = []
    job_status["current_filename"] = params['filename']
    seen = set()
    
    limit = int(params.get('limit_val', 50))
    email_scraper = WebsiteScraper() if params.get('scrape_emails') else None
    
    full_path = os.path.join(DATA_DIR, params['filename'])
    with open(full_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        h = ['Search Term','Country','City','Name','Address','Phone','Website','Rating','Place ID']
        if params.get('scrape_emails'): h.insert(7, 'Emails')
        w.writerow(h)

    countries = params['regions'] if isinstance(params['regions'], list) else [params['regions']]
    q_fmt = f'"{params["term"]}"' if params['match_type']=='literal' else params['term']

    for c_code in countries:
        if not job_status["is_running"]: break
        job_status["new_logs"].append(f"🌍 Country: {c_code.upper()}")
        f_path = REGION_FILES.get(c_code)
        
        if not f_path or not os.path.exists(f_path): 
            print(f"❌ City file not found: {f_path}")
            continue
        
        cities = []
        try:
            with open(f_path, 'r', encoding='utf-8-sig') as f:
                for line in f:
                    if not line or line.lower().startswith("name"): continue
                    p = line.strip().split(',')
                    if len(p)>=3:
                        if c_code=='de' and params['sub_region'] and p[3].strip() not in params['sub_region']: continue
                        cities.append({"name":p[0].strip(),"lat":p[1].strip(),"lon":p[2].strip()})
        except: continue

        for city in cities:
            if not job_status["is_running"]: break
            if params['limit_mode']=='leads' and job_status["total_leads"]>=limit: break
            
            job_status["current_city"] = f"{city['name']} ({c_code.upper()})"
            
            for page in range(15):
                if params['limit_mode']=='leads' and job_status["total_leads"]>=limit: break
                
                data = get_places_by_gps(f"{q_fmt} in {city['name']}", city['lat'], city['lon'], c_code, page*20)
                job_status["api_calls"]+=1
                
                if not data or 'places' not in data or not data['places']: break
                new = 0
                with open(full_path, 'a', newline='', encoding='utf-8') as f:
                    w = csv.writer(f)
                    for p in data['places']:
                        if params['limit_mode']=='leads' and job_status["total_leads"]>=limit: break
                        pid, web, rate = p.get('place_id'), p.get('website',''), p.get('rating',0)
                        
                        if pid in seen: continue
                        if params['skip_no_website'] and not web: continue
                        if rate < float(params.get('min_rating',0)): continue
                        seen.add(pid)
                        new+=1; job_status["total_leads"]+=1
                        
                        row = [q_fmt, c_code.upper(), city['name'], p.get('title'), p.get('address'), p.get('phoneNumber'), web, rate, pid]
                        if params.get('scrape_emails'):
                            ems = []
                            if web: 
                                job_status["current_city"] = f"Scanning {extract_domain(web)}..."
                                ems = email_scraper.extract_emails(web)
                            row.insert(7, "; ".join(ems))
                            if ems: job_status["new_logs"].append(f"📧 Got email for {p.get('title')}")
                        else: job_status["new_logs"].append(f"{p.get('title')}")
                        w.writerow(row)

                if new==0: break
                time.sleep(0.2)
    
    job_status["is_running"] = False
    save_to_history(params['term'], countries, job_status["total_leads"], params['filename'])

# --- ROUTES ---

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/request-feature', methods=['POST'])
def request_feature():
    data = request.json
    feature = data.get('feature_text')
    with open("feature_requests.txt", "a") as f:
        f.write(f"[{datetime.now()}] Anonymous: {feature}\n")
    return jsonify({"status": "success", "message": "Request received!"})

@app.route('/run-scrape', methods=['POST'])
def run_scrape():
    if job_status["is_running"]: return jsonify({"status": "error", "message": "Job running"})
    data = request.json
    filename = f"Maps_{data.get('search_term')}_{int(time.time())}.csv"
    params = {
        'term': data.get('search_term'), 'limit_val': data.get('limit_value'),
        'limit_mode': data.get('limit_mode'), 'match_type': data.get('match_type'),
        'regions': data.get('region'), 'sub_region': data.get('sub_region'),
        'filename': filename, 'skip_no_website': data.get('skip_no_website'),
        'min_rating': data.get('min_rating'), 'scrape_emails': data.get('scrape_emails')
    }
    thread = threading.Thread(target=scraper_worker, args=(params,))
    thread.daemon = True
    thread.start()
    return jsonify({"status": "success"})

@app.route('/status', methods=['GET'])
def status():
    response = job_status.copy()
    job_status["new_logs"] = [] 
    return jsonify(response)

@app.route('/history', methods=['GET'])
def get_history():
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'r') as f: return jsonify(json.load(f))
    return jsonify([])

@app.route('/download/<path:filename>')
def download_file(filename):
    return send_from_directory(DATA_DIR, filename, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)