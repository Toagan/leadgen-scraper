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

# 1. Setup
load_dotenv()
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024 
app.config['SECRET_KEY'] = "supersecretkey123"

API_KEY = os.getenv("SERPER_API_KEY")

# Use Absolute Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data_exports")
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")

for folder in [DATA_DIR, UPLOAD_FOLDER]:
    if not os.path.exists(folder): os.makedirs(folder)

HISTORY_FILE = os.path.join(BASE_DIR, "search_history.json")

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
    def __init__(self): self.headers = {'User-Agent': 'Mozilla/5.0 Chrome/91.0'}
    def extract_emails(self, url):
        try:
            if not url.startswith('http'): url = 'https://' + url
            r = requests.get(url, headers=self.headers, timeout=3, verify=False)
            if r.status_code != 200: return []
            emails = set(re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', BeautifulSoup(r.text, 'lxml').get_text()))
            return [e for e in emails if not e.endswith(('.png','.jpg','.gif','.svg','.webp'))][:3]
        except: return []

def extract_domain(url):
    try:
        if not url: return ""
        p = urlparse(url.strip() if url.startswith(('http','https')) else 'https://'+url.strip())
        return p.netloc[4:] if p.netloc.startswith('www.') else p.netloc
    except: return ""

def clean_filename(s):
    return re.sub(r'[\\/*?:"<>|]', "", s).replace(" ", "_")

def save_to_history(term, region, leads, filename):
    reg = region if isinstance(region, str) else ", ".join(region).upper()
    entry = {"timestamp": time.time(), "date": datetime.now().strftime("%Y-%m-%d %H:%M"), "term": term, "region": reg, "leads_found": leads, "filename": filename}
    hist = []
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE,'r') as f: 
            try: hist = json.load(f)
            except: pass
    hist.insert(0, entry)
    with open(HISTORY_FILE,'w') as f: json.dump(hist, f, indent=4)

def get_places_by_gps(q, lat, lon, gl, start=0):
    if not API_KEY: return None
    url = "https://google.serper.dev/places"
    if gl=='uk': gl='gb'
    payload = json.dumps({"q":q,"gl":gl,"hl":gl,"ll":f"@{lat},{lon},14z","start":start})
    headers = {'X-API-KEY':API_KEY,'Content-Type':'application/json'}
    try: 
        r = requests.post(url, headers=headers, data=payload, verify=False)
        return r.json() if r.status_code == 200 else None
    except: return None

# --- WORKER ---
def scraper_worker(params):
    global job_status
    
    # Use the filename PASSED from the route to ensure consistency
    filename = params['filename']
    full_path = os.path.join(DATA_DIR, filename)
    
    job_status["is_running"] = True
    job_status["total_leads"] = 0
    job_status["api_calls"] = 0
    job_status["new_logs"] = []
    job_status["current_filename"] = filename

    try:
        with open(full_path, 'w', newline='', encoding='utf-8') as f:
            w = csv.writer(f)
            h = ['Search Term','Country','City','Name','Address','Phone','Website','Rating','Place ID']
            if params.get('scrape_emails'): h.insert(7, 'Emails')
            w.writerow(h)

        limit_val = params.get('limit_val')
        # If limit is 0 or extremely high, treat as effectively infinite
        limit = int(limit_val) if limit_val else 1000000
        seen = set()
        email_scraper = WebsiteScraper() if params.get('scrape_emails') else None
        countries = params['regions'] if isinstance(params['regions'], list) else [params['regions']]
        q_fmt = f'"{params["term"]}"' if params['match_type']=='literal' else params['term']

        for c_code in countries:
            if not job_status["is_running"]: break
            job_status["new_logs"].append(f"🌍 Switching to {c_code.upper()}")
            
            f_path = REGION_FILES.get(c_code)
            if not f_path or not os.path.exists(f_path):
                job_status["new_logs"].append(f"⚠️ No city data for {c_code}")
                continue
            
            cities = []
            try:
                with open(f_path, 'r', encoding='utf-8-sig') as f:
                    for line in f:
                        if not line or line.lower().startswith("name"): continue
                        p = line.strip().split('\t')
                        if len(p)<3: p = line.strip().split(',')
                        if len(p)>=3:
                            if c_code=='de' and params['sub_region'] and len(p)>3 and p[3].strip() not in params['sub_region']: continue
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
                    
                    # Open in Append mode inside loop to save progress
                    with open(full_path, 'a', newline='', encoding='utf-8') as f:
                        w = csv.writer(f)
                        for p in data['places']:
                            if params['limit_mode']=='leads' and job_status["total_leads"]>=limit: break
                            
                            pid = p.get('place_id') or p.get('cid') or p.get('title')
                            if pid in seen: continue
                            
                            web = p.get('website','')
                            rate = p.get('rating', 0)
                            if params['skip_no_website'] and not web: continue
                            if rate < float(params.get('min_rating',0)): continue
                            
                            seen.add(pid)
                            new+=1
                            job_status["total_leads"]+=1
                            
                            row = [q_fmt, c_code.upper(), city['name'], p.get('title'), p.get('address'), p.get('phoneNumber'), web, rate, pid]
                            if params.get('scrape_emails'):
                                ems = []
                                if web: 
                                    job_status["current_city"] = f"Scanning {extract_domain(web)}..."
                                    ems = email_scraper.extract_emails(web)
                                row.insert(7, "; ".join(ems))
                                if ems: job_status["new_logs"].append(f"📧 Email found: {p.get('title')}")
                            else: 
                                job_status["new_logs"].append(f"+ {p.get('title')}")
                            w.writerow(row)
                    
                    if new==0: break
                    time.sleep(0.2)
        
        save_to_history(params['term'], countries, job_status["total_leads"], filename)
        job_status["new_logs"].append("✅ Job Finished Successfully")

    except Exception as e:
        job_status["new_logs"].append(f"❌ Critical Error: {str(e)}")
        print(f"WORKER ERROR: {e}")
    finally:
        job_status["is_running"] = False

# --- ROUTES ---
@app.route('/')
def index(): return render_template('index.html')

@app.route('/run-scrape', methods=['POST'])
def run_scrape():
    if job_status["is_running"]: return jsonify({"status": "error", "message": "Job running"})
    data = request.json
    
    # Generate clean filename HERE and pass it to worker
    safe_term = clean_filename(data.get('search_term'))
    filename = f"Maps_{safe_term}_{int(time.time())}.csv"
    
    params = {
        'term': data.get('search_term'), 
        'limit_val': data.get('limit_value'),
        'limit_mode': data.get('limit_mode'), 
        'match_type': data.get('match_type'),
        'regions': data.get('region'), 
        'sub_region': data.get('sub_region'),
        'filename': filename, # Passing pre-generated filename
        'skip_no_website': data.get('skip_no_website'),
        'min_rating': data.get('min_rating'), 
        'scrape_emails': data.get('scrape_emails')
    }
    
    thread = threading.Thread(target=scraper_worker, args=(params,))
    thread.daemon = True
    thread.start()
    return jsonify({"status": "success"})

@app.route('/status', methods=['GET'])
def status():
    r = job_status.copy(); job_status["new_logs"] = []; return jsonify(r)

@app.route('/download/<path:filename>')
def download_file(filename):
    try:
        return send_from_directory(DATA_DIR, filename, as_attachment=True)
    except Exception as e:
        return f"Error: File not found ({e})", 404

if __name__ == '__main__':
    app.run(debug=True)