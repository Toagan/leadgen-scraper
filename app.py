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
from flask import Flask, render_template, request, jsonify, send_from_directory, redirect, url_for, flash
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from dotenv import load_dotenv

# 1. Setup & Configuration
load_dotenv()
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024 
app.config['SECRET_KEY'] = 'supersecretkey123'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///users.db'

API_KEY = os.getenv("SERPER_API_KEY")

# Database & Login Setup
db = SQLAlchemy(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# Directories & Files
DATA_DIR = "data_exports"
UPLOAD_FOLDER = "uploads"
for folder in [DATA_DIR, UPLOAD_FOLDER]:
    if not os.path.exists(folder): os.makedirs(folder)
HISTORY_FILE = "search_history.json"

REGION_FILES = {
    'de': 'data/cities.txt', 'us': 'data/cities_us.txt', 'uk': 'data/cities_uk.txt',
    'fr': 'data/cities_fr.txt', 'es': 'data/cities_es.txt', 'it': 'data/cities_it.txt',
    'au': 'data/cities_au.txt', 'ch': 'data/cities_ch.txt'
}

# Global Status
job_status = {
    "is_running": False, "current_city": "", "total_leads": 0,
    "api_calls": 0, "new_logs": [], "current_filename": ""
}

# --- DB MODEL ---
class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(150), unique=True, nullable=False)
    password = db.Column(db.String(150), nullable=False)

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

# --- HELPER CLASSES ---
class WebsiteScraper:
    def __init__(self):
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36'}
    def extract_emails(self, url):
        try:
            if not url.startswith('http'): url = 'https://' + url
            response = requests.get(url, headers=self.headers, timeout=3)
            if response.status_code != 200: return []
            email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
            soup = BeautifulSoup(response.text, 'lxml')
            emails = set(re.findall(email_pattern, soup.get_text()))
            return [e for e in emails if not e.endswith(('.png', '.jpg', '.gif', '.svg', '.webp'))][:3]
        except: return []

# --- HELPER FUNCTIONS ---
def extract_domain(url_string):
    try:
        if not isinstance(url_string, str) or not url_string.strip(): return ""
        parsed = urlparse(url_string.strip() if url_string.startswith(('http', 'https')) else 'https://' + url_string.strip())
        domain = parsed.netloc
        return domain[4:] if domain.startswith('www.') else domain
    except: return ""

def save_to_history(term, region, leads_count, filename):
    region_label = region if isinstance(region, str) else ", ".join(region).upper()
    entry = {"timestamp": time.time(), "date": datetime.now().strftime("%Y-%m-%d %H:%M"), "term": term, "region": region_label, "leads_found": leads_count, "filename": filename}
    history = []
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'r') as f:
            try: history = json.load(f)
            except: history = []
    history.insert(0, entry)
    with open(HISTORY_FILE, 'w') as f: json.dump(history, f, indent=4)

def get_places_by_gps(query, lat, lon, country_code, start_index=0):
    url = "https://google.serper.dev/places"
    if country_code == 'uk': country_code = 'gb'
    payload = json.dumps({"q": query, "gl": country_code, "hl": country_code, "ll": f"@{lat},{lon},14z", "start": start_index})
    headers = {'X-API-KEY': API_KEY, 'Content-Type': 'application/json'}
    try: return requests.post(url, headers=headers, data=payload).json()
    except: return None

def get_search_results(query, page=1):
    url = "https://google.serper.dev/search"
    payload = json.dumps({"q": query, "page": page, "num": 10, "gl": "us", "hl": "en"})
    headers = {'X-API-KEY': API_KEY, 'Content-Type': 'application/json'}
    try: return requests.post(url, headers=headers, data=payload).json()
    except: return None

# --- WORKERS ---
def scraper_worker(params):
    global job_status
    job_status["is_running"] = True
    job_status["total_leads"] = 0
    job_status["api_calls"] = 0
    job_status["new_logs"] = []
    job_status["current_filename"] = params['filename']
    job_seen_ids = set()

    limit_val = int(params.get('limit_val', 50))
    limit_mode = params.get('limit_mode', 'leads')
    scrape_emails = params.get('scrape_emails', False)
    skip_no_website = params.get('skip_no_website', False)
    try: min_rating = float(params.get('min_rating', 0))
    except: min_rating = 0.0

    email_scraper = WebsiteScraper() if scrape_emails else None

    full_path = os.path.join(DATA_DIR, params['filename'])
    with open(full_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        headers = ['Search Term', 'Country', 'City', 'Name', 'Address', 'Phone', 'Website', 'Rating', 'Place ID']
        if scrape_emails: headers.insert(7, 'Scraped Emails')
        writer.writerow(headers)

    countries = params['regions'] if isinstance(params['regions'], list) else [params['regions']]
    target_states = params['sub_region'] if params['sub_region'] else []
    final_query = f'"{params["term"]}"' if params['match_type'] == 'literal' else params['term']

    for country_code in countries:
        if not job_status["is_running"]: break
        job_status["new_logs"].append(f"🌍 Starting Country: {country_code.upper()}")
        target_file = REGION_FILES.get(country_code)
        if not target_file: continue

        cities = []
        try:
            with open(target_file, 'r', encoding='utf-8-sig') as f:
                for line in f:
                    if not line or line.lower().startswith("name,latitude"): continue
                    parts = line.strip().split(',')
                    if len(parts) >= 3:
                        city_state = parts[3].strip() if len(parts) > 3 else ""
                        if country_code == 'de' and target_states and city_state not in target_states: continue
                        cities.append({"name": parts[0].strip(), "lat": parts[1].strip(), "lon": parts[2].strip()})
        except: continue

        for city in cities:
            if not job_status["is_running"]: break
            if limit_mode == 'leads' and job_status["total_leads"] >= limit_val: break
            if limit_mode == 'credits' and job_status["api_calls"] >= limit_val: break
            job_status["current_city"] = f"{city['name']} ({country_code.upper()})"
            
            for page in range(15):
                if limit_mode == 'leads' and job_status["total_leads"] >= limit_val: break
                data = get_places_by_gps(f"{final_query} in {city['name']}", city['lat'], city['lon'], country_code, page * 20)
                job_status["api_calls"] += 1
                if not data or 'places' not in data or not data['places']: break
                new_count = 0
                with open(full_path, mode='a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    for p in data['places']:
                        pid = p.get('cid') or p.get('place_id')
                        website = p.get('website', '')
                        rating = p.get('rating', 0)
                        if pid in job_seen_ids: continue
                        if skip_no_website and not website: continue
                        if rating < min_rating: continue
                        job_seen_ids.add(pid)
                        new_count += 1
                        job_status["total_leads"] += 1
                        
                        row = [final_query, country_code.upper(), city['name'], p.get('title'), p.get('address'), p.get('phoneNumber'), website, rating, pid]
                        if scrape_emails:
                            emails = []
                            if website:
                                job_status["current_city"] = f"Scraping site: {extract_domain(website)}..." 
                                emails = email_scraper.extract_emails(website)
                            row.insert(7, "; ".join(emails))
                            if emails: job_status["new_logs"].append(f"📧 Found Email: {emails[0]} for {p.get('title')}")
                        else: job_status["new_logs"].append(f"{p.get('title')} ({city['name']})")
                        writer.writerow(row)
                if new_count == 0: break
                time.sleep(0.2)
    job_status["is_running"] = False
    save_to_history(params['term'], countries, job_status["total_leads"], params['filename'])

def linkedin_worker(companies, roles, limit_val, filename):
    global job_status
    job_status["is_running"] = True
    job_status["total_leads"] = 0
    job_status["api_calls"] = 0
    job_status["new_logs"] = []
    job_status["current_filename"] = filename
    full_path = os.path.join(DATA_DIR, filename)
    with open(full_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Input URL', 'Domain', 'Role', 'Title', 'LinkedIn URL', 'Snippet'])
    for index, raw_url in enumerate(companies):
        if not job_status["is_running"] or job_status["total_leads"] >= int(limit_val): break
        domain = extract_domain(raw_url)
        if not domain: continue
        job_status["current_city"] = f"{domain}"
        for role in roles:
            query = f'site:linkedin.com/in/ "{domain}" "{role}"'
            for page in range(1, 4):
                data = get_search_results(query, page)
                job_status["api_calls"] += 1
                if not data or "organic" not in data: break
                results = data["organic"]
                if not results: break
                with open(full_path, mode='a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    for item in results:
                        job_status["total_leads"] += 1
                        job_status["new_logs"].append(f"Found: {item.get('title')} @ {domain}")
                        writer.writerow([raw_url, domain, role, item.get('title'), item.get('link'), item.get('snippet')])
                if len(results) < 10: break
                time.sleep(0.5)
    job_status["is_running"] = False
    save_to_history("LinkedIn Search", "Global", job_status["total_leads"], filename)

# --- ROUTES ---

@app.route('/')
def index():
    return render_template('index.html', user=current_user)

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        user = User.query.filter_by(email=email).first()
        if user and check_password_hash(user.password, password):
            login_user(user)
            return redirect(url_for('index'))
        else:
            flash('Invalid email or password', 'error')
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('index'))

# --- PROTECTED PAGES ---
@app.route('/subscription')
@login_required
def subscription():
    return render_template('subscription.html', user=current_user)

@app.route('/settings')
@login_required
def settings():
    return render_template('settings.html', user=current_user)

# --- PROTECTED ACTIONS ---

@app.route('/request-feature', methods=['POST'])
@login_required
def request_feature():
    data = request.json
    feature = data.get('feature_text')
    
    # Save to a simple text file (on Render this wipes daily, but works for now)
    with open("feature_requests.txt", "a") as f:
        f.write(f"[{datetime.now()}] {current_user.email}: {feature}\n")
        
    return jsonify({"status": "success", "message": "Request received!"})

@app.route('/upload-csv', methods=['POST'])
@login_required
def upload_csv():
    if 'file' not in request.files: return jsonify({"error": "No file"})
    file = request.files['file']
    if file:
        filepath = os.path.join(UPLOAD_FOLDER, file.filename)
        file.save(filepath)
        domains = set()
        try:
            if file.filename.endswith('.csv'): df = pd.read_csv(filepath)
            else: df = pd.read_excel(filepath)
            target_col = df.columns[0]
            for col in df.columns:
                if any(x in str(col).lower() for x in ['web', 'url', 'site', 'link']):
                    target_col = col; break
            for item in df[target_col].dropna():
                d = extract_domain(str(item))
                if d: domains.add(d)
            return jsonify({"status": "success", "domains": list(domains), "count": len(domains)})
        except Exception as e: return jsonify({"error": str(e)})

@app.route('/run-scrape', methods=['POST'])
@login_required
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

@app.route('/run-linkedin-scrape', methods=['POST'])
@login_required
def run_linkedin_scrape():
    if job_status["is_running"]: return jsonify({"status": "error", "message": "Job running"})
    data = request.json
    filename = f"LinkedIn_{int(time.time())}.csv"
    thread = threading.Thread(target=linkedin_worker, args=(data.get('companies'), data.get('roles'), data.get('limit'), filename))
    thread.daemon = True
    thread.start()
    return jsonify({"status": "success"})

@app.route('/status', methods=['GET'])
def status():
    # Status is public to show "Idle"
    response = job_status.copy()
    job_status["new_logs"] = [] 
    return jsonify(response)

@app.route('/history', methods=['GET'])
@login_required
def get_history():
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'r') as f: return jsonify(json.load(f))
    return jsonify([])

@app.route('/download/<path:filename>')
@login_required
def download_file(filename):
    return send_from_directory(DATA_DIR, filename, as_attachment=True)

# --- INIT ---
if __name__ == '__main__':
    with app.app_context():
        db.create_all()
        if not User.query.filter_by(email="admin@example.com").first():
            print("Creating default admin user...")
            hashed_pw = generate_password_hash("admin123", method='pbkdf2:sha256')
            new_user = User(email="admin@example.com", password=hashed_pw)
            db.session.add(new_user)
            db.session.commit()
            print("Admin created: admin@example.com / admin123")
            
    app.run(debug=True)