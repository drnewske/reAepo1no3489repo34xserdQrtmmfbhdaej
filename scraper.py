import cloudscraper
from bs4 import BeautifulSoup
import re
import time
import json
import os
import hashlib
from urllib.parse import urljoin
from datetime import datetime
import random

class FootballOrginScraper:
    def __init__(self):
        self.base_url = "https://www.footballorgin.com"
        self.all_new_matches = []
        self.output_file = "matches.json"
        self.log_file = "scraper_log.json"
        self.log_data = self.load_log()

        # --- CLOUDSCRAPER SESSION (Bypasses Cloudflare) ---
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            }
        )

    def make_request(self, url, max_retries=3):
        """Make request with cloudscraper and retry logic"""
        for attempt in range(max_retries):
            try:
                # Random delay between requests (1-3 seconds)
                time.sleep(random.uniform(1, 3))
                
                response = self.scraper.get(url, timeout=30)
                
                if response.status_code == 403:
                    print(f"  --> Attempt {attempt + 1}/{max_retries}: 403 Forbidden. Retrying...")
                    time.sleep(random.uniform(3, 5))
                    continue
                
                response.raise_for_status()
                return response
                
            except Exception as e:
                print(f"  --> Attempt {attempt + 1}/{max_retries}: Error - {e}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(3, 5))
                else:
                    return None
        
        return None
    
    # --- LOG AND UTILITY METHODS ---

    def generate_match_id(self, match_url):
        return hashlib.md5(match_url.strip().encode('utf-8')).hexdigest()

    def load_log(self):
        if os.path.exists(self.log_file):
            try:
                with open(self.log_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                return {}
        return {}

    def save_log(self):
        with open(self.log_file, 'w', encoding='utf-8') as f:
            json.dump(self.log_data, f, indent=2, ensure_ascii=False)

    def log_match(self, match_id, match_title, link_count):
        current_time = datetime.now().isoformat()
        self.log_data[match_id] = {
            'match_title': match_title,
            'link_count': link_count,
            'last_updated': current_time
        }

    def should_update_match(self, match_id, current_link_count):
        if match_id not in self.log_data:
            return True, "new_match"
        
        logged_link_count = self.log_data[match_id].get('link_count', 0)
        
        if current_link_count > logged_link_count:
            return True, "link_count_increase"
            
        return False, "no_change"

    def load_existing_matches(self):
        if os.path.exists(self.output_file):
            try:
                with open(self.output_file, 'r', encoding='utf-8') as f:
                    existing_matches = json.load(f)
                    return existing_matches if isinstance(existing_matches, list) else []
            except (json.JSONDecodeError, FileNotFoundError):
                return []
        return []

    # --- CORE EXTRACTION LOGIC ---

    def get_page_matches(self, page_url):
        """Fetches and extracts match data from a single listing page"""
        print(f"  Fetching page: {page_url}")
        
        response = self.make_request(page_url)
        
        if response is None or response.status_code >= 400:
            print(f"  --> Failed to fetch page after retries.")
            return None

        soup = BeautifulSoup(response.text, 'html.parser')
        match_items = soup.find_all('article', class_=re.compile(r'post-item'))
        
        if not match_items:
            print("  --> No articles found on the page.")
            return []

        matches = []
        for item in match_items:
            try:
                title_tag = item.find('h3', class_='post-title')
                link_tag = title_tag.find('a') if title_tag else None
                if not link_tag or not link_tag.get('href'): 
                    continue
                
                match_url = link_tag.get('href')
                match_title = link_tag.text.strip()
                match_id = self.generate_match_id(match_url)
                
                img_tag = item.find('img', class_='blog-picture')
                preview_image = img_tag.get('data-src') or img_tag.get('src') if img_tag else 'N/A'
                
                categories_wrap = item.find('div', class_='categories-wrap')
                competition_list = [a.text.strip() for a in categories_wrap.find_all('a')] if categories_wrap else []
                
                duration_tag = item.find('span', class_='duration-text')
                duration = duration_tag.text.strip() if duration_tag and duration_tag.text.strip() else 'N/A'

                time_tag = item.find('time', class_='entry-date')
                date_text = time_tag.text.strip().split('-')[0].strip() if time_tag else 'N/A'
                
                matches.append({
                    'match': match_title,
                    'url': match_url,
                    'preview_image': preview_image,
                    'competition_list': competition_list,
                    'duration': duration,
                    'date': date_text, 
                    'match_id': match_id
                })
            except Exception as e:
                continue
                
        print(f"  --> Found {len(matches)} matches on this page")
        return matches

    def extract_single_video_link(self, page_html):
        """Extracts the primary video link from the embedded JavaScript object"""
        match = re.search(r'var vidorev_jav_js_object=({.*?});', page_html, re.DOTALL)
        
        if match:
            raw_js_object = match.group(1).replace('\n', '').replace('\t', '')
            url_match = re.search(r'"single_video_url":"(.*?)"', raw_js_object)
            
            if url_match:
                raw_url_value = url_match.group(1)
                
                if 'http' in raw_url_value:
                    clean_url = raw_url_value.replace('\\/', '/').replace('\\', '')
                    return clean_url
                
                elif 'iframe' in raw_url_value:
                    iframe_src_match = re.search(r'src=\\\"(.*?)\\\"', raw_url_value)
                    if iframe_src_match:
                        video_src = iframe_src_match.group(1).replace('\\', '')
                        return urljoin('https:', video_src)
                        
        return None

    def extract_match_details(self, match_data):
        """Extracts all video links for a match"""
        primary_url = match_data['url']
        final_links = []
        
        response = self.make_request(primary_url)
        if response is None:
            return {'links': []}
        
        page_html = response.text
        soup = BeautifulSoup(page_html, 'html.parser')
        
        # 1. Get the Primary Video Link
        primary_link = self.extract_single_video_link(page_html)
        
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
                
                multi_response = self.make_request(href)
                if multi_response:
                    current_link = self.extract_single_video_link(multi_response.text)
                    
                    if current_link and current_link not in [l['url'] for l in final_links]:
                        clean_label = label.replace(match_data['match'], '').strip(':- ')
                        final_links.append({'label': clean_label, 'url': current_link})

        # Clean up generic primary link label
        if final_links and len(final_links) > 1 and final_links[0]['label'] == match_data['match']:
            final_links[0]['label'] = ''
             
        return {'links': final_links}

    # --- RUNNER LOGIC ---

    def scrape_category(self, category_path):
        """Crawl only the base category page"""
        base_url = f"{self.base_url}/{category_path}/"
        print(f"\n--- Scraping Category: {category_path} ---")
        
        match_list = self.get_page_matches(base_url)
        
        if match_list is None:
            return [] 
            
        return match_list

    def run(self):
        """Main execution function"""
        print("=" * 80)
        print("FootballOrgin.com Scraper (V10 - Cloudscraper)")
        print("=" * 80)
        
        categories = [
            'full-match-replay', 
            'tv-show',
            'news-and-interviews' 
        ]
        
        existing_matches = self.load_existing_matches()
        scraped_data_list = []
        
        all_posts_found = []
        for category in categories:
            posts_found = self.scrape_category(category)
            all_posts_found.extend(posts_found)
        
        # Process only NEW posts
        posts_to_process = []
        for post in all_posts_found:
            post_id = post['match_id']
            if post_id not in self.log_data:
                posts_to_process.append(post)
        
        print(f"\n{'='*80}")
        print(f"Summary: Found {len(all_posts_found)} posts total")
        print(f"         {len(posts_to_process)} NEW posts need detailed scraping")
        print(f"{'='*80}\n")
        
        # Extract detailed data for NEW posts
        for i, match in enumerate(posts_to_process, 1):
            print(f"[NEW] Processing {i}/{len(posts_to_process)}: {match['match']}")
            
            details = self.extract_match_details(match)
            link_count = len(details['links'])

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
            
            if link_count > 0:
                scraped_data_list.append(match_info)
                self.log_match(match['match_id'], match['match'], link_count)
                print(f"  --> Extracted {link_count} video link(s)")
            else:
                print(f"  --> WARNING: No links found, skipping")

        # Merge and save
        final_matches_map = {match['match_id']: match for match in scraped_data_list}
        
        for match in existing_matches:
             if match['match_id'] not in final_matches_map:
                 final_matches_map[match['match_id']] = match

        final_matches = list(final_matches_map.values())
        
        # Put new content at the top
        new_ids = {m['match_id'] for m in scraped_data_list}
        new_list = [m for m in final_matches if m['match_id'] in new_ids]
        old_list = [m for m in final_matches if m['match_id'] not in new_ids]
        final_matches_ordered = new_list + old_list

        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(final_matches_ordered, f, indent=2, ensure_ascii=False)
        
        self.save_log()

        print(f"\n{'='*80}")
        print(f"✓ Scraping Complete!")
        print(f"  - NEW posts scraped: {len(posts_to_process)}")
        print(f"  - Total posts in {self.output_file}: {len(final_matches_ordered)}")
        print(f"{'='*80}")

if __name__ == "__main__":
    scraper = FootballOrginScraper()
    scraper.run()
