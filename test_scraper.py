import requests
import json
import os
import csv
import time
from dotenv import load_dotenv

# 1. Load Environment Variables
load_dotenv()
API_KEY = os.getenv("SERPER_API_KEY")

if not API_KEY:
    print("❌ ERROR: SERPER_API_KEY not found in .env file.")
    exit()

# --- CONFIGURATION FOR TEST ---
SEARCH_TERM = "Marketing Agency" # We hardcode this for the test run
INPUT_FILE = "cities_test.txt"
OUTPUT_FILE = "test_leads.csv"
MAX_PAGES_PER_CITY = 3  # Safety limit for testing to save credits
RESULTS_PER_PAGE = 20   # Serper usually returns 20 for places

def get_places(query, start_index=0):
    """
    Calls the Serper Places API.
    """
    url = "https://google.serper.dev/places"
    
    # We construct the payload. 
    # Note: Serper uses the standard 'start' parameter for pagination if available,
    # or we might need to rely on the API returning everything in one go.
    # This logic attempts to paginate using 'start'.
    payload = json.dumps({
        "q": query,
        "gl": "de",   # Country: Germany
        "hl": "de",   # Language: German
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
        print(f"⚠️ API Request Failed: {e}")
        return None

def main():
    # 2. Setup CSV File
    # We open in 'w' mode to overwrite previous tests. In production, we might use 'a'.
    with open(OUTPUT_FILE, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        # Write Header
        writer.writerow(['Search Query', 'Title', 'Address', 'Rating', 'Reviews', 'Phone', 'Website', 'Place ID'])

    # 3. Read Cities
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            cities = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"❌ ERROR: {INPUT_FILE} not found.")
        return

    print(f"🚀 STARTING SCRAPE: {SEARCH_TERM}")
    print(f"📋 Loaded {len(cities)} cities from {INPUT_FILE}\n")

    # 4. Main Loop
    global_leads_count = 0
    
    for city in cities:
        full_query = f"{SEARCH_TERM} in {city}"
        print(f"📍 Processing: {city} (Query: '{full_query}')")
        
        seen_place_ids = set() # To track duplicates within this city
        
        for page in range(MAX_PAGES_PER_CITY):
            start_index = page * RESULTS_PER_PAGE
            print(f"   ↳ Page {page + 1} (Start index: {start_index})...", end=" ")
            
            data = get_places(full_query, start_index)
            
            if not data or 'places' not in data:
                print("No 'places' data found. Stopping city.")
                break

            places = data['places']
            
            if not places:
                print("Zero results returned. Stopping city.")
                break

            # Process Results
            new_items_on_page = 0
            
            with open(OUTPUT_FILE, mode='a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                
                for place in places:
                    # Deduplication Logic
                    pid = place.get('cid') or place.get('place_id') or place.get('title')
                    if pid in seen_place_ids:
                        continue
                    
                    seen_place_ids.add(pid)
                    new_items_on_page += 1
                    
                    # Write to CSV
                    writer.writerow([
                        full_query,
                        place.get('title', ''),
                        place.get('address', ''),
                        place.get('rating', ''),
                        place.get('ratingCount', ''),
                        place.get('phoneNumber', ''),
                        place.get('website', ''),
                        pid
                    ])

            print(f"✅ Found {new_items_on_page} new leads.")
            global_leads_count += new_items_on_page

            # SMART STOPPING: 
            # If we found 0 new items (all were duplicates), the API is looping or done.
            if new_items_on_page == 0:
                print("   🛑 No new unique items found. Stopping pagination for this city to save credits.")
                break
                
            # Respect Rate Limits
            time.sleep(1)

        print("-" * 40)

    print(f"\n🎉 DONE! Total leads collected: {global_leads_count}")
    print(f"💾 Data saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()