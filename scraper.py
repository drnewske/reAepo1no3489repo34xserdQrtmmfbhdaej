import requests
import cloudscraper
from bs4 import BeautifulSoup
import re
import time
import json
import os
import hashlib
from datetime import datetime
from urllib.parse import urljoin
import random

# ==========================================
#        SHARED UTILITIES & CONFIG
# ==========================================

LOG_FILE = "scraper_log.json"
OUTPUT_FILE = "matches.json"

def load_json_file(filepath):
    """Safely load a JSON file."""
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return {} if 'log' in filepath else []
    return {} if 'log' in filepath else []

def save_json_file(filepath, data):
    """Safely save data to a JSON file."""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def generate_id(url):
    """Generate a unique ID based on the URL."""
    return hashlib.md5(url.strip().encode('utf-8')).hexdigest()

# ==========================================
#        SCRAPER 1: SOCCERFULL.NET
# ==========================================

class SoccerFullScraper:
    def __init__(self, log_data):
        self.base_url = "https://soccerfull.net"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.log_data = log_data
        self.new_matches = []

    def should_update(self, match_id, current_timestamp):
        """Check if we need to process this match."""
        if match_id in self.log_data:
            logged_timestamp = self.log_data[match_id].get('last_updated', '')
            # Simple check: If logged less than 24h ago, treat as same day
            if logged_timestamp and logged_timestamp[:10] == current_timestamp[:10]:
                return True, "same_day_update"
            return False, "duplicate"
        return True, "new_match"

    def get_player_html(self, video_id, server_num):
        api_url = f"{self.base_url}/ajax/change_link"
        payload = {'vl3x_server': 1, 'id': video_id, 'server': server_num}
        headers = {'X-Requested-With': 'XMLHttpRequest'}
        try:
            response = self.session.post(api_url, data=payload, headers=headers)
            response.raise_for_status()
            return response.json().get('player')
        except:
            return None

    def get_links(self, match_url):
        try:
            response = self.session.get(match_url)
            soup = BeautifulSoup(response.text, 'html.parser')
        except:
            return [], '', ''

        # Extract Date & Competition
        date = ''
        competition = ''
        try:
            info = soup.find('article', class_='infobv')
            if info:
                # Date
                for p in info.find_all('p'):
                    if 'KICK-OFF at' in p.text:
                        match = re.search(r'KICK-OFF at\s+(.*?)(?:$|\.|<)', p.text)
                        if match: date = match.group(1).strip()
                # Competition
                extras = info.find('div', id='extras')
                if extras:
                    for div in extras.find_all('div'):
                        if "League:" in div.text and div.find('a'):
                            competition = div.find('a').text.strip()
        except: pass

        # Extract Links
        links = []
        servers = soup.find_all('li', class_='video-server')
        for s in servers:
            try:
                onclick = s.get('onclick')
                if onclick:
                    m = re.search(r"server\((\d+),(\d+)\)", onclick)
                    if m:
                        server_name = s.text.strip()
                        html = self.get_player_html(m.group(2), m.group(1))
                        if html:
                            iframe = BeautifulSoup(html, 'html.parser').find('iframe')
                            if iframe and iframe.get('src'):
                                links.append({'label': server_name, 'url': iframe.get('src')})
                time.sleep(0.2)
            except: continue
            
        return links, date, competition

    def run(self):
        print(f"--- Starting SoccerFull Scraper ---")
        try:
            response = self.session.get(self.base_url)
            soup = BeautifulSoup(response.text, 'html.parser')
            items = soup.find_all('li', class_='item-movie')
        except Exception as e:
            print(f"Failed to access SoccerFull: {e}")
            return []

        # Limit to first 5 for speed, or remove slice for full scrape
        for item in items[:10]: 
            try:
                # Basic Extraction
                title_div = item.find('div', class_='title-movie')
                if not title_div: continue
                title = title_div.find('h3').text.strip()
                
                link_tag = item.find('a')
                if not link_tag: continue
                url = urljoin(self.base_url, link_tag.get('href'))
                
                # Image
                img = item.find('img', class_='movie-thumbnail')
                preview_image = img.get('data-original') or img.get('src') if img else ""

                # ID & Check
                match_id = generate_id(url)
                current_time = datetime.now().isoformat()
                should_process, reason = self.should_update(match_id, current_time)

                if not should_process:
                    print(f"[SF] Skipping duplicate: {title}")
                    continue

                # Detailed Extraction
                print(f"[SF] Processing: {title}")
                links, date, competition = self.get_links(url)

                if not links:
                    print(f"   -> No links found.")
                    continue

                # Check update logic (only save if we have more links than before)
                old_count = self.log_data.get(match_id, {}).get('link_count', 0)
                if reason == "same_day_update" and len(links) <= old_count:
                    print(f"   -> No new links (Old: {old_count}, New: {len(links)})")
                    continue

                # Build Object
                match_obj = {
                    "match_id": match_id,
                    "url": url,
                    "match": title,
                    "date": date,
                    "competition": competition,
                    "preview_image": preview_image,
                    "duration": "", # Not available on SF homepage
                    "links": links
                }
                
                self.new_matches.append(match_obj)
                
                # Update Log Memory (will be saved by Master)
                self.log_data[match_id] = {
                    'match_title': title,
                    'link_count': len(links),
                    'last_updated': current_time,
                    'source': 'soccerfull'
                }

            except Exception as e:
                print(f"Error parsing item: {e}")
                continue
                
        return self.new_matches

# ==========================================
#        SCRAPER 2: FOOTBALLORGIN.COM
# ==========================================

class FootballOrginScraper:
    def __init__(self, log_data):
        self.base_url = "https://www.footballorgin.com"
        self.log_data = log_data
        self.new_matches = []
        self.scraper = cloudscraper.create_scraper(browser={'browser': 'chrome','platform': 'windows','mobile': False})

    def make_request(self, url):
        for _ in range(3):
            try:
                time.sleep(random.uniform(1, 2))
                resp = self.scraper.get(url, timeout=20)
                if resp.status_code == 200: return resp
            except: time.sleep(2)
        return None

    def extract_video_link(self, html):
        # Look for JS variable
        m = re.search(r'"single_video_url"\s*:\s*"((?:\\"|[^"])*)"', html)
        if m:
            val = m.group(1).replace(r'\"', '"').replace(r'\/', '/')
            if '<iframe' in val:
                src = re.search(r'src\s*=\s*("|\')(.*?)\1', val)
                return src.group(2) if src else None
            return "https:" + val if val.startswith("//") else val
        return None

    def get_details(self, url):
        resp = self.make_request(url)
        if not resp: return []
        soup = BeautifulSoup(resp.text, 'html.parser')
        links = []

        # Multi-link check
        multi = soup.find('div', class_='series-listing')
        if multi:
            for a in multi.find_all('a'):
                label = a.text.strip()
                href = a.get('href')
                # If current page
                if href == url: 
                    vid = self.extract_video_link(resp.text)
                else:
                    sub_resp = self.make_request(href)
                    vid = self.extract_video_link(sub_resp.text) if sub_resp else None
                
                if vid: links.append({'label': label, 'url': vid})
        else:
            # Single link
            vid = self.extract_video_link(resp.text)
            if vid: links.append({'label': 'Full Match', 'url': vid})
            
        return links

    def run(self):
        print(f"--- Starting FootballOrgin Scraper ---")
        categories = ['full-match-replay', 'tv-show', 'news-and-interviews', 'review-show']
        
        for cat in categories:
            cat_url = f"{self.base_url}/{cat}/"
            resp = self.make_request(cat_url)
            if not resp: continue
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            articles = soup.find_all('article', class_=re.compile(r'post-item'))
            
            for item in articles:
                try:
                    title_tag = item.find('h3', class_='post-title')
                    if not title_tag: continue
                    a_tag = title_tag.find('a')
                    url = a_tag.get('href')
                    title = a_tag.text.strip()
                    
                    match_id = generate_id(url)
                    
                    # Check if exists (FootballOrgin doesn't usually update old posts, so simple check)
                    if match_id in self.log_data:
                        continue
                        
                    print(f"[FO] Processing: {title}")
                    
                    # Metadata
                    img = item.find('img', class_='blog-picture')
                    preview = img.get('data-src') or img.get('src') if img else ""
                    
                    cats = item.find('div', class_='categories-wrap')
                    comp = ", ".join([c.text for c in cats.find_all('a')]) if cats else ""
                    
                    dur = item.find('span', class_='duration-text')
                    duration = dur.text.strip() if dur else ""
                    
                    time_tag = item.find('time', class_='entry-date')
                    date = time_tag.text.split('-')[0].strip() if time_tag else ""
                    
                    # Get Links
                    links = self.get_details(url)
                    
                    if links:
                        match_obj = {
                            "match_id": match_id,
                            "url": url,
                            "match": title,
                            "date": date,
                            "competition": comp,
                            "preview_image": preview,
                            "duration": duration,
                            "links": links
                        }
                        self.new_matches.append(match_obj)
                        
                        # Update Log
                        self.log_data[match_id] = {
                            'match_title': title,
                            'link_count': len(links),
                            'last_updated': datetime.now().isoformat(),
                            'source': 'footballorgin'
                        }
                        
                except Exception as e:
                    print(f"Error parsing FO item: {e}")
                    
        return self.new_matches

# ==========================================
#        MASTER CONTROLLER
# ==========================================

def main():
    print("========================================")
    print("   UNIVERSAL SPORTS SCRAPER (v1.0)      ")
    print("========================================")

    # 1. Load History
    log_data = load_json_file(LOG_FILE)
    existing_matches = load_json_file(OUTPUT_FILE)
    
    # 2. Run Scrapers
    # Pass the log_data to both so they know what's history
    sf_scraper = SoccerFullScraper(log_data)
    sf_results = sf_scraper.run()
    
    fo_scraper = FootballOrginScraper(log_data)
    fo_results = fo_scraper.run()
    
    # 3. Merge Results
    all_new_data = sf_results + fo_results
    
    if not all_new_data:
        print("\nNo new matches found from any source.")
        return

    print(f"\nMerging Data: {len(sf_results)} from SoccerFull + {len(fo_results)} from FootballOrgin")
    
    # 4. De-duplication Logic
    # We use a dictionary keyed by match_id to merge.
    # We prefer the NEW data over the OLD data.
    
    # Start with existing matches map
    final_map = {m['match_id']: m for m in existing_matches}
    
    # Update with new matches (this adds new ones and overwrites updates)
    for match in all_new_data:
        final_map[match['match_id']] = match
        
    # Convert back to list
    final_list = list(final_map.values())
    
    # Sort: Put the items found in this run at the TOP
    new_ids = {m['match_id'] for m in all_new_data}
    newly_added = [m for m in final_list if m['match_id'] in new_ids]
    older_items = [m for m in final_list if m['match_id'] not in new_ids]
    
    ordered_list = newly_added + older_items
    
    # 5. Save Files
    save_json_file(OUTPUT_FILE, ordered_list)
    save_json_file(LOG_FILE, log_data)
    
    print("\n----------------------------------------")
    print("SUCCESS!")
    print(f"Total Matches in File: {len(ordered_list)}")
    print(f"Log Updated.")
    print("----------------------------------------")

if __name__ == "__main__":
    main()
