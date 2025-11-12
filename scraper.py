import requests
from bs4 import BeautifulSoup
import re
import time
import json
import os
from datetime import datetime
from urllib.parse import urljoin
import hashlib

class FootballOrginScraper:
    def __init__(self):
        self.base_url = "https://www.footballorgin.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.all_new_matches = [] 
        self.output_file = "matches.json"

    def generate_match_id(self, match_url):
        """Generate a unique and deterministic ID for a match based on its unique post URL."""
        return hashlib.md5(match_url.strip().encode('utf-8')).hexdigest()

    def load_existing_matches(self):
        """Load existing matches from JSON file for merging and deduplication."""
        if os.path.exists(self.output_file):
            try:
                with open(self.output_file, 'r', encoding='utf-8') as f:
                    existing_matches = json.load(f)
                    return existing_matches if isinstance(existing_matches, list) else []
            except (json.JSONDecodeError, FileNotFoundError):
                print(f"Warning: Could not read or decode {self.output_file}. Starting fresh data merge.")
                return []
        return []

    def get_page_matches(self, page_url):
        """
        Fetches and extracts match data from a single listing page.
        Returns None on error or if zero matches are found.
        """
        try:
            # Politeness delay before the request
            time.sleep(1) 
            
            response = self.session.get(page_url)
            if response.status_code >= 400:
                print(f"  --> HTTP Error {response.status_code}. Cannot scrape page.")
                return None
            
            response.raise_for_status() 
        except requests.exceptions.RequestException as e:
            print(f"Error fetching listing page {page_url}: {e}")
            return None

        soup = BeautifulSoup(response.text, 'html.parser')
        match_items = soup.find_all('article', class_=re.compile(r'post-item'))
        
        if not match_items:
            print("No articles found on the base page URL. Content may be lazy-loaded.")
            return None

        matches = []
        for item in match_items:
            try:
                # 1. Extract URL and Title
                title_tag = item.find('h3', class_='post-title')
                link_tag = title_tag.find('a') if title_tag else None
                if not link_tag or not link_tag.get('href'): continue
                
                match_url = link_tag.get('href')
                match_title = link_tag.text.strip()
                
                # 2. Extract Thumbnail URL (data-src) - Renamed to preview_image
                img_tag = item.find('img', class_='blog-picture')
                preview_image = img_tag.get('data-src') or img_tag.get('src') if img_tag else 'N/A'
                
                # 3. Extract Categories/Competition List
                categories_wrap = item.find('div', class_='categories-wrap')
                competition_list = [a.text.strip() for a in categories_wrap.find_all('a')] if categories_wrap else []
                
                # 4. Extract Duration
                duration_tag = item.find('span', class_='duration-text')
                duration = duration_tag.text.strip() if duration_tag and duration_tag.text.strip() else 'N/A'

                # 5. Extract Date
                time_tag = item.find('time', class_='entry-date')
                date_text = time_tag.text.strip().split('-')[0].strip() if time_tag else 'N/A'
                
                matches.append({
                    'match': match_title,
                    'url': match_url,
                    'preview_image': preview_image,
                    'competition_list': competition_list,
                    'duration': duration,
                    'date': date_text, 
                    'match_id': self.generate_match_id(match_url)
                })
            except Exception:
                continue
                
        return matches

    def extract_single_video_link(self, page_html):
        """
        Extracts the primary video link from the embedded JavaScript object.
        """
        match = re.search(r'var vidorev_jav_js_object=({.*?});', page_html, re.DOTALL)
        
        if match:
            raw_js_object = match.group(1).replace('\n', '').replace('\t', '')
            url_match = re.search(r'"single_video_url":"(.*?)"', raw_js_object)
            
            if url_match:
                raw_url_value = url_match.group(1)
                
                # Case 1: Direct URL (YouTube, etc.)
                if 'http' in raw_url_value:
                    clean_url = raw_url_value.replace('\\/', '/').replace('\\', '')
                    return clean_url
                
                # Case 2: Escaped IFRAME HTML (Ok.ru, etc.)
                elif 'iframe' in raw_url_value:
                    iframe_src_match = re.search(r'src=\\\"(.*?)\\\"', raw_url_value)
                    if iframe_src_match:
                        video_src = iframe_src_match.group(1).replace('\\', '')
                        return urljoin('https:', video_src)
                        
        return None

    def extract_match_details(self, match_data):
        """Extracts all video links for a match, including multi-links."""
        
        primary_url = match_data['url']
        final_links = []
        
        try:
            time.sleep(1)
            response = self.session.get(primary_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
        except requests.exceptions.RequestException:
            return {'links': []}
        
        # 1. Get the Primary Video Link
        primary_link = self.extract_single_video_link(response.text)
        
        if primary_link:
            primary_label = 'Full Match' if 'full-match' in primary_url else match_data['match']
            final_links.append({'label': primary_label, 'url': primary_link})

        # 2. Find and scrape Multi-Links
        multi_links_div = soup.find('div', class_='series-listing')
        
        if multi_links_div:
            for link_tag in multi_links_div.find_all('a'):
                href = link_tag.get('href')
                label = link_tag.text.strip()
                
                if href == primary_url:
                    continue
                    
                try:
                    time.sleep(0.5) 
                    multi_response = self.session.get(href)
                    multi_response.raise_for_status()
                    current_link = self.extract_single_video_link(multi_response.text)
                except:
                    current_link = None
                        
                if current_link and current_link not in [l['url'] for l in final_links]:
                    # FIX: Replaced non-existent 'title' key with correct 'match' key
                    clean_label = label.replace(match_data['match'], '').strip(':- ')
                    
                    final_links.append({'label': clean_label, 'url': current_link})

        # Clean up generic primary link label if more descriptive sub-links were found
        if final_links and len(final_links) > 1 and final_links[0]['label'] == final_links[0]['match']:
            final_links[0]['label'] = ''
             
        return {'links': final_links}

    def scrape_category(self, category_path):
        """
        Crawl only the base category page, as requested, with no pagination.
        """
        base_url = f"{self.base_url}/{category_path}/"
        print(f"--- Fetching: {base_url} (The only required page) ---")
        
        match_list = self.get_page_matches(base_url)
        
        if match_list is None:
            return [] 
            
        new_matches = []
        existing_urls = {match.get('url') for match in self.load_existing_matches() if match.get('url')}
        
        for match in match_list:
            if match['url'] not in existing_urls:
                new_matches.append(match)
        
        # Stop condition: The function returns after scraping the first page.
        return new_matches

    def run(self):
        """Main execution function to scrape all defined categories (first page only)."""
        print("FootballOrgin.com Scraper (V6 - Finalized and Fixed)")
        print("=" * 80)
        
        categories = [
            'full-match-replay', 
            'tv-show',
            'news-and-interviews' 
        ]
        
        existing_matches = self.load_existing_matches()
        scraped_data_list = []
        
        for category in categories:
            print(f"\n--- Starting Crawl for Category: {category} ---")
            new_matches_list = self.scrape_category(category)
            self.all_new_matches.extend(new_matches_list)
            
        print(f"\nTotal unique NEW posts found across first pages: {len(self.all_new_matches)}")
        
        # 2. Extract detailed data for new posts
        for i, match in enumerate(self.all_new_matches, 1):
            print(f"Processing details {i}/{len(self.all_new_matches)}: {match['match']}")
            
            details = self.extract_match_details(match)
            
            # Final structure consolidation
            match_info = {
                "match_id": match['match_id'],
                "url": match['url'],
                "match": match['match'],
                "date": match['date'],
                "competition": ', '.join(match['competition_list']), 
                "preview_image": match['preview_image'],
                "duration": match['duration'],
                "links": details['links']
            }
            
            scraped_data_list.append(match_info)
        
        # 3. Merge, Deduplicate, and Save
        combined_matches = scraped_data_list + existing_matches
        final_matches_map = {}
        
        for match in reversed(combined_matches):
            final_matches_map[match['match_id']] = match
        
        final_matches = list(final_matches_map.values())
        
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(final_matches, f, indent=2, ensure_ascii=False)
        
        print("-" * 80)
        print(f"\n✓ Data processing complete. Saved to {self.output_file}")
        print(f"  - New posts added this run: {len(scraped_data_list)}")
        print(f"  - Total unique posts in final file: {len(final_matches)}")

if __name__ == "__main__":
    scraper = FootballOrginScraper()
    scraper.run()
