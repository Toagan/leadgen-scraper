import os
import csv
import json
import time
import threading
import requests
import re
import pandas as pd
import secrets
from datetime import datetime
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from flask import Flask, render_template, request, jsonify, send_from_directory, redirect, url_for, flash
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from authlib.integrations.flask_client import OAuth
from dotenv import load_dotenv

# 1. Setup & Configuration
load_dotenv()
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024 
app.config['SECRET_KEY'] = os.environ.get("SECRET_KEY", "supersecretkey123")
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///users.db'

API_KEY = os.getenv("SERPER_API_KEY")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")

# Database & Login Setup
db = SQLAlchemy(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# OAuth Setup
oauth = OAuth(app)
google = oauth.register(
    name='google',
    client_id=GOOGLE_CLIENT_ID,
    client_secret=GOOGLE_CLIENT_SECRET,
    access_token_url='https://accounts.google.com/o/oauth2/token',
    authorize_url='https://accounts.google.com/o/oauth2/auth',
    api_base_url='https://www.googleapis.com/oauth2/v1/',
    userinfo_endpoint='https://openidconnect.googleapis.com/v1/userinfo',
    client_kwargs={'scope': 'email profile'},
    jwks_uri='https://www.googleapis.com/oauth2/v3/certs'
)

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

# --- DB MODEL ---
class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(150), unique=True, nullable=False)
    password = db.Column(db.String(150), nullable=False)

@login_manager.user_loader
def load_user(user_id): return User.query.get(int(user_id))

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
    if gl=='uk': gl='gb'
    try: return requests.post("https://google.serper.dev/places", headers={'X-API-KEY':API_KEY,'Content-Type':'application/json'}, data=json.dumps({"q":q,"gl":gl,"hl":gl,"ll":f"@{lat},{lon},14z","start":start})).json()
    except: return None

def get_search_results(q, p=1):
    try: return requests.post("https://google.serper.dev/search", headers={'X-API-KEY':API_KEY,'Content-Type':'application/json'}, data=json.dumps({"q":q,"page":p,"num":10,"gl":"us","hl":"en"})).json()
    except: return None

# --- WORKERS ---
def scraper_worker(params):
    global job_status
    job_status["is_running"] = True
    job_status["total_leads"] = 0; job_status["api_calls"] = 0
    job_status["new_logs"] = []
    job_status["current_filename"] = params['filename']
    seen = set()
    
    # GUEST MODE LOGIC
    limit = int(params.get('limit_val', 50))
    if params.get('is_guest'):
        limit = 10  # Hard cap for guests
        job_status["new_logs"].append("🔒 GUEST MODE: Limiting to 10 leads.")

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
        if not f_path: continue
        
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
            # CHECK LIMITS
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
                        
                        # Stop immediately if guest limit hit inside loop
                        if params.get('is_guest') and job_status["total_leads"] >= 10:
                            job_status["new_logs"].append("🔒 Guest Limit Reached! Log in for more.")
                            break

                if new==0: break
                time.sleep(0.2)
    
    job_status["is_running"] = False
    save_to_history(params['term'], countries, job_status["total_leads"], params['filename'])

def linkedin_worker(companies, roles, limit, filename, is_guest=False):
    global job_status
    job_status["is_running"] = True; job_status["total_leads"] = 0; job_status["api_calls"] = 0; job_status["new_logs"] = []
    job_status["current_filename"] = filename
    
    if is_guest:
        limit = 10
        job_status["new_logs"].append("🔒 GUEST MODE: Limiting to 10 leads.")

    path = os.path.join(DATA_DIR, filename)
    with open(path, 'w', newline='', encoding='utf-8') as f:
        csv.writer(f).writerow(['Input URL','Domain','Role','Title','LinkedIn URL','Snippet'])
    
    for i, url in enumerate(companies):
        if not job_status["is_running"] or job_status["total_leads"]>=int(limit): break
        dom = extract_domain(url)
        if not dom: continue
        job_status["current_city"] = dom
        for r in roles:
            for p in range(1,4):
                data = get_search_results(f'site:linkedin.com/in/ "{dom}" "{r}"', p)
                job_status["api_calls"]+=1
                if not data or 'organic' not in data: break
                res = data['organic']
                if not res: break
                with open(path, 'a', newline='', encoding='utf-8') as f:
                    w = csv.writer(f)
                    for item in res:
                        job_status["total_leads"]+=1
                        job_status["new_logs"].append(f"Found {item.get('title')}")
                        w.writerow([url, dom, r, item.get('title'), item.get('link'), item.get('snippet')])
                        if is_guest and job_status["total_leads"] >= 10: break
                if len(res)<10: break
                if is_guest and job_status["total_leads"] >= 10: break
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
        else: flash('Invalid credentials', 'error')
    return render_template('login.html')

@app.route('/login/google')
def google_login():
    return google.authorize_redirect(url_for('google_callback', _external=True))

@app.route('/login/google/callback')
def google_callback():
    token = google.authorize_access_token()
    user_info = google.get('https://www.googleapis.com/oauth2/v1/userinfo').json()
    email = user_info['email']
    user = User.query.filter_by(email=email).first()
    if not user:
        random_pw = secrets.token_hex(16)
        hashed_pw = generate_password_hash(random_pw, method='pbkdf2:sha256')
        user = User(email=email, password=hashed_pw)
        db.session.add(user)
        db.session.commit()
    login_user(user)
    return redirect(url_for('index'))

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('index'))

# --- PUBLIC ACCESS ACTIONS (WITH LIMITS) ---

@app.route('/request-feature', methods=['POST'])
@login_required
def request_feature():
    data = request.json
    feature = data.get('feature_text')
    with open("feature_requests.txt", "a") as f:
        f.write(f"[{datetime.now()}] {current_user.email}: {feature}\n")
    return jsonify({"status": "success", "message": "Request received!"})

@app.route('/upload-csv', methods=['POST'])
# REMOVED @login_required to allow guest usage
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
# REMOVED @login_required
def run_scrape():
    if job_status["is_running"]: return jsonify({"status": "error", "message": "Job running"})
    data = request.json
    
    # GUEST CHECK
    is_guest = not current_user.is_authenticated
    
    filename = f"Maps_{data.get('search_term')}_{int(time.time())}.csv"
    params = {
        'term': data.get('search_term'), 'limit_val': data.get('limit_value'),
        'limit_mode': data.get('limit_mode'), 'match_type': data.get('match_type'),
        'regions': data.get('region'), 'sub_region': data.get('sub_region'),
        'filename': filename, 'skip_no_website': data.get('skip_no_website'),
        'min_rating': data.get('min_rating'), 'scrape_emails': data.get('scrape_emails'),
        'is_guest': is_guest # Pass flag to worker
    }
    thread = threading.Thread(target=scraper_worker, args=(params,))
    thread.daemon = True
    thread.start()
    return jsonify({"status": "success"})

@app.route('/run-linkedin-scrape', methods=['POST'])
# REMOVED @login_required
def run_linkedin_scrape():
    if job_status["is_running"]: return jsonify({"status": "error", "message": "Job running"})
    data = request.json
    
    is_guest = not current_user.is_authenticated
    
    fn = f"LinkedIn_{int(time.time())}.csv"
    thread = threading.Thread(target=linkedin_worker, args=(data.get('companies'), data.get('roles'), data.get('limit'), fn, is_guest))
    thread.daemon = True
    thread.start()
    return jsonify({"status": "success"})

@app.route('/status', methods=['GET'])
def status():
    response = job_status.copy()
    job_status["new_logs"] = [] 
    return jsonify(response)

@app.route('/history', methods=['GET'])
# History is public for now, or you can restrict it
def get_history():
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'r') as f: return jsonify(json.load(f))
    return jsonify([])

@app.route('/download/<path:filename>')
# Allow download for guests too
def download_file(filename):
    return send_from_directory(DATA_DIR, filename, as_attachment=True)

# --- PROTECTED PAGES ---
@app.route('/subscription')
@login_required
def subscription(): return render_template('subscription.html', user=current_user)

@app.route('/settings')
@login_required
def settings(): return render_template('settings.html', user=current_user)

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
        if not User.query.filter_by(email="admin@example.com").first():
            hp = generate_password_hash("admin123", method='pbkdf2:sha256')
            db.session.add(User(email="admin@example.com", password=hp))
            db.session.commit()
            print("Admin Created")
    app.run(debug=True)