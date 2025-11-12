import requests
from bs4 import BeautifulSoup
import re
import time
import json
import os
import hashlib
from urllib.parse import urljoin
from datetime import datetime

class FootballOrginScraper:
    def __init__(self):
        self.base_url = "https://www.footballorgin.com"
        self.all_new_matches = []
        self.output_file = "matches.json"
        self.log_file = "scraper_log.json"
        self.log_data = self.load_log()

        # --- ANTI-BOT/ANTI-403 HEADERS for Requests ---
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://www.footballorgin.com/', # Crucial for 403 blocks
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    # --- LOG AND UTILITY METHODS (Same as V8) ---

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

    # --- CORE EXTRACTION LOGIC (Now requests-based) ---

    def get_page_matches(self, page_url):
        """
        Fetches and extracts match data from a single listing page using requests.
        """
        try:
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
            print("No articles found on the base page URL.")
            return None

        matches = []
        for item in match_items:
            # ... (Extraction logic remains the same) ...
            try:
                title_tag = item.find('h3', class_='post-title')
                link_tag = title_tag.find('a') if title_tag else None
                if not link_tag or not link_tag.get('href'): continue
                
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
            except Exception:
                continue
                
        return matches

    def extract_single_video_link(self, page_html):
        """Extracts the primary video link from the embedded JavaScript object."""
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
        """
        Extracts all video links for a match.
        We DO NOT visit multi-link pages, as the primary page's JS object holds the link structure.
        However, for reliability on this specific site, we'll maintain the request structure for multi-links
        to account for pages where they might swap content *without* re-embedding the whole JS object.
        """
        
        primary_url = match_data['url']
        final_links = []
        
        try:
            time.sleep(1)
            response = self.session.get(primary_url)
            response.raise_for_status()
            page_html = response.text
        except requests.exceptions.RequestException:
            return {'links': []}
        
        soup = BeautifulSoup(page_html, 'html.parser')
        
        # 1. Get the Primary Video Link
        primary_link = self.extract_single_video_link(page_html)
        
        if primary_link:
            primary_label = 'Full Match' if 'full-match' in primary_url else match_data['match']
            final_links.append({'label': primary_label, 'url': primary_link})

        # 2. Find and scrape Multi-Links (by fetching their unique URLs)
        multi_links_div = soup.find('div', class_='series-listing')
        
        if multi_links_div:
            for link_tag in multi_links_div.find_all('a'):
                href = link_tag.get('href')
                label = link_tag.text.strip()
                
                if href == primary_url:
                    continue
                    
                # Hitting each multi-link URL separately (the reliable but slower part)
                try:
                    time.sleep(0.5) 
                    multi_response = self.session.get(href)
                    multi_response.raise_for_status()
                    current_link = self.extract_single_video_link(multi_response.text)
                except:
                    current_link = None
                        
                if current_link and current_link not in [l['url'] for l in final_links]:
                    clean_label = label.replace(match_data['match'], '').strip(':- ')
                    final_links.append({'label': clean_label, 'url': current_link})

        # Clean up generic primary link label
        if final_links and len(final_links) > 1 and final_links[0]['label'] == final_links[0]['match']:
            final_links[0]['label'] = ''
             
        return {'links': final_links}


    # --- RUNNER LOGIC ---

    def scrape_category(self, category_path):
        """Crawl only the base category page and identify posts to process."""
        base_url = f"{self.base_url}/{category_path}/"
        print(f"--- Fetching: {base_url} (First page content) ---")
        
        match_list = self.get_page_matches(base_url)
        
        if match_list is None:
            return [] 
            
        return match_list

    def run(self):
        """Main execution function."""
        print("FootballOrgin.com Scraper (V9 - Requests/Anti-403 Optimized)")
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
            
        # 1. Identify ALL posts and determine if they need scraping/updating
        posts_to_process = []
        
        for post in all_posts_found:
            post_id = post['match_id']
            
            # Use the number of links found on the main post page's multi-link section to check if we need an update
            # We must load the details page to accurately count links, so the check here must rely only on the log vs. presence.
            
            # Instead of guessing the link count, we assume if the post is NEW, we process it.
            # If the post is OLD, we check the log to see if it was successfully processed before.
            
            is_logged = post_id in self.log_data
            
            if not is_logged:
                 posts_to_process.append((post, "new_match"))
            else:
                 # To check for a "link_count_increase", we MUST scrape the details page first.
                 # Let's perform a lightweight check to determine if the post needs a detail scrape.
                 # Since we cannot accurately predict link count without visiting, we skip update checks for now
                 # and focus only on NEW posts for speed, unless a dedicated 'link_check' function is added.
                 posts_to_process.append((post, "update_check")) # Force check old posts too
        
        # --- REFINEMENT: Only scrape new posts for speed, unless updates are CRITICAL. ---
        # Since scraping details is the bottleneck, let's stick to NEW posts only for maximum speed.
        posts_to_process_new = []
        for post in all_posts_found:
            post_id = post['match_id']
            if post_id not in self.log_data:
                posts_to_process_new.append((post, "new_match"))
            
        posts_to_process = posts_to_process_new
        
        print(f"\nFound {len(all_posts_found)} posts in total. {len(posts_to_process)} posts needing detailed scrape (NEW).")
        
        # 2. Extract detailed data for NEW posts
        for i, (match, reason) in enumerate(posts_to_process, 1):
            print(f"[{reason.upper()}] Processing details {i}/{len(posts_to_process)}: {match['match']}")
            
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
            else:
                print(f"-> WARNING: Skipped post {match['match']} due to zero links found on detail page.")

        # 3. Merge, Deduplicate, and Save
        
        # 1. Add all new/updated posts first
        final_matches_map = {match['match_id']: match for match in scraped_data_list}
        
        # 2. Add remaining older/existing posts
        for match in existing_matches:
             if match['match_id'] not in final_matches_map:
                 final_matches_map[match['match_id']] = match

        final_matches = list(final_matches_map.values())
        
        # Reorder: Ensure new content is at the top
        new_ids = {m['match_id'] for m in scraped_data_list}
        new_list = [m for m in final_matches if m['match_id'] in new_ids]
        old_list = [m for m in final_matches if m['match_id'] not in new_ids]
        final_matches_ordered = new_list + old_list

        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(final_matches_ordered, f, indent=2, ensure_ascii=False)
        
        self.save_log()

        print("-" * 80)
        print(f"\n✓ Data processing complete. Saved to {self.output_file}")
        print(f"  - Posts scraped in detail this run: {len(posts_to_process)}")
        print(f"  - Total unique posts in final file: {len(final_matches_ordered)}")

if __name__ == "__main__":
    scraper = FootballOrginScraper()
    scraper.run()
