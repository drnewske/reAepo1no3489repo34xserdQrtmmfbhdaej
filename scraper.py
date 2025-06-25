import requests
from bs4 import BeautifulSoup
import re
import time
import json
import os
from datetime import datetime, timedelta
from urllib.parse import urljoin
import hashlib

class SoccerFullScraper:
    def __init__(self):
        self.base_url = "https://soccerfull.net"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.all_matches = []
        self.updated_matches = []
        self.log_file = "scraper_log.json"
        self.output_file = "matches.json"
        self.log_data = self.load_log()

    def load_log(self):
        """Load existing log data"""
        if os.path.exists(self.log_file):
            try:
                with open(self.log_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                return {}
        return {}

    def save_log(self):
        """Save log data to file"""
        with open(self.log_file, 'w', encoding='utf-8') as f:
            json.dump(self.log_data, f, indent=2, ensure_ascii=False)

    def generate_match_hash(self, match_title, date, competition):
        """Generate a unique hash for a match based on title, date, and competition"""
        match_string = f"{match_title}|{date}|{competition}".lower().strip()
        return hashlib.md5(match_string.encode('utf-8')).hexdigest()

    def is_same_day(self, timestamp1, timestamp2):
        """Check if two timestamps are from the same day"""
        try:
            date1 = datetime.fromisoformat(timestamp1).date()
            date2 = datetime.fromisoformat(timestamp2).date()
            return date1 == date2
        except:
            return False

    def should_update_match(self, match_hash, current_timestamp):
        """
        Determine if a match should be updated:
        - If it's from the same day, allow update
        - If it's older than 1 day, skip (duplicate)
        """
        if match_hash in self.log_data:
            logged_timestamp = self.log_data[match_hash]['timestamp']
            
            # If it's from the same day, allow update
            if self.is_same_day(current_timestamp, logged_timestamp):
                return True, "same_day_update"
            
            # If it's from a different day within 7 days, skip (duplicate)
            logged_date = datetime.fromisoformat(logged_timestamp)
            current_date = datetime.fromisoformat(current_timestamp)
            
            if (current_date - logged_date).days < 7:
                return False, "duplicate"
        
        # New match
        return True, "new_match"

    def log_match(self, match_hash, match_title, timestamp, link_count=0):
        """Log a match to prevent duplicates and track updates"""
        self.log_data[match_hash] = {
            'match_title': match_title,
            'timestamp': timestamp,
            'link_count': link_count,
            'last_updated': timestamp
        }

    def clean_old_logs(self):
        """Remove log entries older than 7 days"""
        current_time = datetime.now()
        to_remove = []
        
        for match_hash, data in self.log_data.items():
            logged_date = datetime.fromisoformat(data['timestamp'])
            if (current_time - logged_date).days >= 7:
                to_remove.append(match_hash)
        
        for match_hash in to_remove:
            del self.log_data[match_hash]

    def get_page_matches(self, page_url):
        """Extract match links and titles from a page"""
        try:
            response = self.session.get(page_url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {page_url}: {e}")
            return []

        soup = BeautifulSoup(response.text, 'html.parser')
        matches = []
        
        # Find all match items
        match_items = soup.find_all('li', class_='item-movie')
        
        for item in match_items:
            try:
                # Extract match title
                title_div = item.find('div', class_='title-movie')
                if title_div:
                    h3_tag = title_div.find('h3')
                    if h3_tag:
                        match_title = h3_tag.text.strip()
                    else:
                        continue
                else:
                    continue
                
                # Extract match URL
                link_tag = item.find('a')
                if link_tag and link_tag.get('href'):
                    match_url = urljoin(self.base_url, link_tag.get('href'))
                    matches.append({
                        'title': match_title,
                        'url': match_url
                    })
            except Exception as e:
                print(f"Error processing match item: {e}")
                continue
        
        return matches

    def get_server_parameters(self, match_url):
        """Scrapes a match page to find the parameters for each video server."""
        try:
            response = self.session.get(match_url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching match page: {e}")
            return []

        soup = BeautifulSoup(response.text, 'html.parser')
        
        servers = []
        # Find all list items with the class 'video-server'
        server_elements = soup.find_all('li', class_='video-server')

        for element in server_elements:
            server_name = element.text.strip()
            onclick_attr = element.get('onclick')
            
            if onclick_attr:
                # Use regex to reliably extract the numbers from "server(num, id)"
                match = re.search(r"server\((\d+),(\d+)\)", onclick_attr)
                if match:
                    server_num = match.group(1)
                    video_id = match.group(2)
                    servers.append({
                        "name": server_name,
                        "server_num": server_num,
                        "video_id": video_id
                    })
                    
        return servers

    def get_player_html(self, video_id, server_num):
        """Makes a POST request to the API to get the video player HTML."""
        api_url = f"{self.base_url}/ajax/change_link"
        payload = {
            'vl3x_server': 1,
            'id': video_id,
            'server': server_num
        }
        headers = {
            'X-Requested-With': 'XMLHttpRequest'
        }

        try:
            response = self.session.post(api_url, data=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
            return data.get('player')
        except requests.exceptions.RequestException as e:
            print(f"API request failed for server {server_num}, id {video_id}: {e}")
            return None
        except ValueError:
            print(f"Could not decode JSON for server {server_num}, id {video_id}.")
            return None

    def extract_iframe_src(self, player_html):
        """Parses an HTML string to find the src of an iframe."""
        if not player_html:
            return None
            
        soup = BeautifulSoup(player_html, 'html.parser')
        iframe = soup.find('iframe')
        
        if iframe:
            return iframe.get('src')
        return None

    def extract_match_details(self, match_url):
        """Extract detailed information from a match page"""
        try:
            response = self.session.get(match_url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching match details: {e}")
            return None

        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Initialize match details
        match_details = {
            'date': 'N/A',
            'competition': 'N/A',
            'links': []
        }

        # Extract match information from the infobv article
        try:
            info_article = soup.find('article', class_='infobv')
            if info_article:
                # Extract match description paragraph
                paragraphs = info_article.find_all('p')
                for p in paragraphs:
                    text = p.text.strip()
                    if 'Kick off:' in text:
                        # Extract kick-off date
                        kick_off_match = re.search(r'Kick off:\s*([^.]+)', text)
                        if kick_off_match:
                            kick_off_info = kick_off_match.group(1).strip()
                            # Extract date (everything after GMT)
                            date_match = re.search(r'GMT\s+(.+)', kick_off_info)
                            if date_match:
                                match_details['date'] = date_match.group(1).strip()

                # Extract competition from extras div
                extras_div = info_article.find('div', id='extras')
                if extras_div:
                    league_div = extras_div.find('div')
                    if league_div:
                        league_link = league_div.find('a')
                        if league_link:
                            match_details['competition'] = league_link.text.strip()
        except Exception as e:
            print(f"Error extracting match info: {e}")

        # Get server parameters and extract video links
        servers = self.get_server_parameters(match_url)
        
        for server in servers:
            try:
                player_html = self.get_player_html(server['video_id'], server['server_num'])
                if player_html:
                    video_src = self.extract_iframe_src(player_html)
                    if video_src:
                        match_details['links'].append({
                            'label': server['name'],
                            'url': video_src
                        })
                time.sleep(0.5)  # Small delay between requests
            except Exception as e:
                print(f"Error processing server {server['name']}: {e}")
                continue

        return match_details

    def has_more_links(self, new_links, old_link_count):
        """Check if new match data has more links than previously recorded"""
        return len(new_links) > old_link_count

    def scrape_homepage(self):
        """Scrape only the homepage"""
        page_url = self.base_url
        current_timestamp = datetime.now().isoformat()
        
        print(f"\n=== Scraping Homepage ===")
        print(f"URL: {page_url}")
        print(f"Timestamp: {current_timestamp}")
        
        # Clean old log entries
        self.clean_old_logs()
        
        # Get matches from the homepage
        matches = self.get_page_matches(page_url)
        
        if not matches:
            print("No matches found on homepage")
            return False
        
        print(f"Found {len(matches)} matches on homepage")
        
        # Process each match
        for i, match in enumerate(matches, 1):
            print(f"\nProcessing match {i}/{len(matches)}: {match['title']}")
            
            match_details = self.extract_match_details(match['url'])
            if match_details:
                # Generate hash for duplicate detection
                match_hash = self.generate_match_hash(
                    match['title'], 
                    match_details['date'], 
                    match_details['competition']
                )
                
                # Check if we should process this match
                should_update, reason = self.should_update_match(match_hash, current_timestamp)
                
                if not should_update and reason == "duplicate":
                    print(f"Skipping duplicate match: {match['title']}")
                    continue
                
                # Create match info in the requested format
                match_info = {
                    'match': match['title'],
                    'date': match_details['date'],
                    'competition': match_details['competition'],
                    'links': match_details['links']
                }
                
                if reason == "same_day_update":
                    # Check if there are more links than before
                    old_link_count = self.log_data[match_hash].get('link_count', 0)
                    new_link_count = len(match_details['links'])
                    
                    if self.has_more_links(match_details['links'], old_link_count):
                        print(f"🔄 Updating same-day match with more links: {match['title']} ({old_link_count} -> {new_link_count} links)")
                        self.updated_matches.append(match_info)
                        # Update log with new link count and timestamp
                        self.log_match(match_hash, match['title'], current_timestamp, new_link_count)
                    else:
                        print(f"⏭️  Same-day match has same or fewer links, skipping: {match['title']} ({new_link_count} links)")
                        continue
                else:
                    # New match
                    print(f"✅ New match: {match['title']}")
                    self.all_matches.append(match_info)
                    # Log the match
                    self.log_match(match_hash, match['title'], current_timestamp, len(match_details['links']))
                
                print(f"✓ Extracted details for {match['title']} - {len(match_details['links'])} links")
            else:
                print(f"✗ Failed to extract details for {match['title']}")
            
            time.sleep(1)  # Delay between matches to be respectful
        
        return True

    def load_existing_matches(self):
        """Load existing matches from JSON file"""
        if os.path.exists(self.output_file):
            try:
                with open(self.output_file, 'r', encoding='utf-8') as f:
                    existing_matches = json.load(f)
                    # Ensure it's a list
                    if isinstance(existing_matches, list):
                        return existing_matches
                    else:
                        print(f"Warning: {self.output_file} does not contain a valid list. Starting fresh.")
                        return []
            except (json.JSONDecodeError, FileNotFoundError) as e:
                print(f"Warning: Could not load existing matches from {self.output_file}: {e}")
                return []
        return []

    def update_existing_matches(self, existing_matches):
        """Update existing matches with new data for same-day updates"""
        if not self.updated_matches:
            return existing_matches
        
        updated_count = 0
        for updated_match in self.updated_matches:
            # Find the existing match to replace
            for i, existing_match in enumerate(existing_matches):
                if (existing_match['match'] == updated_match['match'] and 
                    existing_match['date'] == updated_match['date'] and 
                    existing_match['competition'] == updated_match['competition']):
                    
                    existing_matches[i] = updated_match
                    updated_count += 1
                    print(f"🔄 Updated existing match: {updated_match['match']}")
                    break
        
        print(f"Total matches updated in existing data: {updated_count}")
        return existing_matches

    def save_to_json(self):
        """Save all scraped data to a JSON file, merging with existing data"""
        # Load existing matches
        existing_matches = self.load_existing_matches()
        
        # Update existing matches with same-day updates
        existing_matches = self.update_existing_matches(existing_matches)
        
        # Merge new matches at the top + existing matches
        merged_matches = self.all_matches + existing_matches
        
        # Save merged data
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(merged_matches, f, indent=2, ensure_ascii=False)
        
        print(f"\n✓ Data saved to {self.output_file}")
        print(f"Total new matches added: {len(self.all_matches)}")
        print(f"Total matches updated: {len(self.updated_matches)}")
        print(f"Total existing matches preserved: {len(existing_matches)}")
        print(f"Total matches in file: {len(merged_matches)}")

    def run(self):
        """Main execution function"""
        print("SoccerFull.net Match Data Scraper (Homepage Only) - Enhanced with Same-Day Updates")
        print("=" * 80)
        
        success = self.scrape_homepage()
        
        if success and (self.all_matches or self.updated_matches):
            self.save_to_json()
        elif success:
            print("No new matches found and no updates needed (all were duplicates or failed to process)")
            # Still update the file to maintain the existing structure but don't add empty entries
            existing_matches = self.load_existing_matches()
            if existing_matches:
                print(f"Existing file maintained with {len(existing_matches)} matches")
            else:
                # Only create empty array if no existing file
                with open(self.output_file, 'w', encoding='utf-8') as f:
                    json.dump([], f)
        else:
            print("Failed to scrape homepage")
        
        # Save log data
        self.save_log()

if __name__ == "__main__":
    scraper = SoccerFullScraper()
    scraper.run()
