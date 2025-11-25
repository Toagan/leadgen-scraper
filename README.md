# LeadGen Master ⚡

**LeadGen Master** is a professional-grade lead extraction suite built with Python (Flask) and the Serper API. It automates the process of finding business leads from Google Maps across multiple countries, providing a real-time dashboard interface for monitoring and exporting data.

## 🚀 Features

* **Google Maps Scraping:** Leverages the Serper API to extract high-quality business data (Name, Address, Phone, Website, Rating).
* **Multi-Region Support:** Built-in city data for major markets including 🇺🇸 USA, 🇩🇪 Germany, 🇬🇧 UK, 🇦🇺 Australia, 🇨🇳 China, and more.
* **Live Dashboard:** Real-time "Terminal" style feed showing leads as they are discovered.
* **Smart Pagination:** Automatically iterates through cities and pagination results to maximize lead yield while optimizing API credit usage.
* **Search History:** Automatically saves search sessions and allows for re-downloading previous CSV exports.
* **Clean UI:** Modern, responsive interface built with TailwindCSS.

## 🛠️ Tech Stack

* **Backend:** Python, Flask, Threading
* **Frontend:** HTML5, TailwindCSS, JavaScript (Fetch API)
* **Data Source:** [Serper.dev](https://serper.dev) (Google Maps API)
* **Data Storage:** CSV (Exports) & JSON (History logs)

## 📂 Project Structure

```text
LEADGEN-MASTER/
├── data/                  # Contains city coordinates for each country
│   ├── cities_us.txt
│   ├── cities_de.txt
│   └── ...
├── data_exports/          # Generated CSV files are saved here
├── templates/
│   └── index.html         # Main Dashboard UI
├── app.py                 # Main application logic
├── search_history.json    # Database of past searches
├── requirements.txt       # Python dependencies
└── .env                   # API Key configuration
⚡ Quick Start
1. Clone the Repository
Bash

git clone [https://github.com/YOUR_USERNAME/leadgen-scraper.git](https://github.com/YOUR_USERNAME/leadgen-scraper.git)
cd leadgen-scraper
2. Set up Virtual Environment
It is recommended to use a virtual environment to keep dependencies clean.

Bash

# Mac/Linux
python3 -m venv venv
source venv/bin/activate

# Windows
python -m venv venv
venv\Scripts\activate
3. Install Dependencies
Bash

pip install -r requirements.txt
4. Configure API Key
Create a .env file in the root directory and add your Serper API key:

Plaintext

SERPER_API_KEY=your_actual_api_key_here
You can get a key from serper.dev.

5. Run the Application
Bash

python app.py
Open your browser and navigate to: http://127.0.0.1:5000

📝 Usage Guide
Select Target: Enter a keyword (e.g., "Real Estate Agents") and choose a country from the dropdown.

Set Limits: Define the maximum number of leads you want to scrape to control costs.

Select Precision:

Literal Match: Searches for the exact phrase.

Rough Match: Allows broader semantic results.

Generate: Click "Generate Leads" and watch the Live Terminal.

Download: Once the job is finished (or the limit is reached), a green Download button will appear.

🛡️ Note on API Usage
This tool uses the Serper API, which costs credits per search. The tool is optimized to stop searching immediately once your defined "Max Leads" limit is reached to prevent wasting credits.

📄 License
MIT
