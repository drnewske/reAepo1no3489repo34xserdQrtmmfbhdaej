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
from pprint import pprint  # Can be left in, it's not used by run()

class FootballOrginScraper:
    def __init__(self):
        self.base_url = "https://www.footballorgin.com"
        self.output_file = "matches.json"
        self.log_file = "scraper_log.json"
        
        # self.log_data is now loaded in the run() method
        self.log_data = {} 

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
                    print(f"    --> Attempt {attempt + 1}/{max_retries}: 403 Forbidden. Retrying...")
                    time.sleep(random.uniform(3, 5))
                    continue
                
                response.raise_for_status()
                return response
                
            except Exception as e:
                print(f"    --> Attempt {attempt + 1}/{max_retries}: Error - {e}")
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

    def load_existing_matches(self):
        if os.path.exists(self.output_file):
            try:
                with open(self.output_file, 'r', encoding='utf-8') as f:
                    existing_matches = json.load(f)
                    return existing_matches if isinstance(existing_matches, list) else []
            except (json.JSONDecodeError, FileNotFoundError):
                return []
        return []


    # --- CORE EXTRACTION LOGIC (Patterns 1, 2, 3) ---

    def get_page_matches(self, page_url):
        """Fetches and extracts match data from a single listing page"""
        print(f"  Fetching page: {page_url}")
        
        response = self.make_request(page_url)
        
        if response is None or response.status_code >= 400:
            print(f"  --> Failed to fetch page after retries.")
            return None

        soup = BeautifulSoup(response.text, 'html.parser')
        
        match_items_container = soup.find('div', class_='blog-items')
        if not match_items_container:
            print(f"  --> No 'blog-items' container found on page: {page_url}")
            return []
            
        match_items = match_items_container.find_all('article', class_=re.compile(r'post-item'))
        
        if not match_items:
            print(f"  --> No articles found on page: {page_url}")
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
                print(f"    --> Error parsing an article item: {e}")
                continue
                
        print(f"  --> Found {len(matches)} matches on this page")
        return matches

    def extract_single_video_link(self, page_html):
        """
        Extracts the primary video link from the embedded 'vidorev_jav_js_object'
        JavaScript variable. This is our resilient Pattern 2.
        """
        
        # 1. Find the 'single_video_url' key and capture its value.
        js_match = re.search(
            r'"single_video_url"\s*:\s*"((?:\\"|[^"])*)"',
            page_html
        )
        
        if not js_match:
            print("        --> ERROR: Could not find 'single_video_url' in page's JavaScript.")
            return None

        # 2. The value is either an iframe string or a direct URL string
        raw_value = js_match.group(1)
        
        # 3. Unescape the string (e.g., \" -> ", \/ -> /)
        try:
            unescaped_value = raw_value.replace(r'\"', '"').replace(r'\/', '/')
        except Exception:
            return None # Should not happen with simple replace

        # 4. Check if the value is an iframe or a direct link
        
        if unescaped_value.strip().startswith('<iframe'):
            # Case 1: It's an iframe string. We must parse 'src' from it.
            src_match = re.search(
                r'src\s*=\s*("|\')(.*?)\1',  # Handles src="..." and src='...'
                unescaped_value,
                re.IGNORECASE
            )
            
            if not src_match:
                print(f"        --> ERROR: Found an iframe but could not parse 'src': {unescaped_value[:100]}...")
                return None
            
            video_src = src_match.group(2) # The URL
        
        elif unescaped_value.strip().startswith('http'):
            # Case 2: It's a direct URL. This *is* our link.
            video_src = unescaped_value
        
        else:
            # It's an unknown format
            print(f"        --> ERROR: Unknown 'single_video_url' format: {unescaped_value[:100]}...")
            return None
        
        # 5. Final cleanup (ensure 'https:')
        if video_src.startswith("//"):
            return "https:" + video_src
        
        return video_src

    def extract_match_details(self, match_data):
        """
        Extracts all video links for a match.
        It implements Pattern 3 by checking for 'series-listing' first.
        """
        primary_url = match_data['url']
        final_links = []
        
        print(f"    Fetching details for: {primary_url}")
        response = self.make_request(primary_url)
        if response is None:
            print("    --> Failed to fetch page.")
            return {'links': []}
            
        page_html = response.text
        soup = BeautifulSoup(page_html, 'html.parser')
        
        # Pattern 3: Check for the "Multi-Links" box first.
        multi_links_div = soup.find('div', class_='series-listing')
        
        if multi_links_div:
            # Case A: Multi-video post (e.g., "1st half", "2nd half")
            print("    --> Multi-link post found. Scraping all parts.")
            links_to_scrape = []
            for link_tag in multi_links_div.find_all('a'):
                links_to_scrape.append({
                    'label': link_tag.text.strip(),
                    'href': link_tag.get('href')
                })
            
            for i, link_info in enumerate(links_to_scrape):
                href = link_info['href']
                label = link_info['label']
                print(f"        - Processing Part {i+1}: {label} ({href.split('/')[-2] or 'base'})")

                if href == primary_url:
                    current_page_html = page_html
                else:
                    part_response = self.make_request(href)
                    if part_response:
                        current_page_html = part_response.text
                    else:
                        print(f"        --> Failed to fetch part: {href}")
                        continue
                
                video_url = self.extract_single_video_link(current_page_html)
                
                if video_url:
                    final_links.append({'label': label, 'url': video_url})
                else:
                    print(f"        --> No video URL found for part: {label}")
                    
        else:
            # Case B: Single-video post (Pattern 2 only)
            print("    --> Single video post found.")
            video_url = self.extract_single_video_link(page_html)
            
            if video_url:
                label = "Full Show" if "tv-show" in primary_url else "Full Match"
                final_links.append({'label': label, 'url': video_url})

        return {'links': final_links}

    def scrape_category(self, category_path):
        """Crawl only the base category page"""
        base_url = f"{self.base_url}/{category_path}/"
        print(f"\n--- Scraping Category: {category_path} ---")
        
        match_list = self.get_page_matches(base_url)
        
        if match_list is None:
            return []  
            
        return match_list

    # --- TEST RUNNER (Kept here in case you need it again) ---
    def run_test(self):
        """
        TEST version: Scrapes the first 3 items from one category
        and prints the full results to the terminal.
        """
        print("=" * 80)
        print("FootballOrgin.com Scraper (TEST MODE - Print to Terminal)")
        print("=" * 80)
        
        test_category = 'full-match-replay'
        all_posts_found = self.scrape_category(test_category)
        
        if not all_posts_found:
            print("No posts found. Exiting test.")
            return

        posts_to_process = all_posts_found[:3] 
        
        print(f"\n{'='*80}")
        print(f"Found {len(all_posts_found)} posts in '{test_category}'.")
        print(f"Processing the first {len(posts_to_process)} for this test...")
        print(f"{'='*80}\n")
        
        scraped_data_list = []
        
        for i, match in enumerate(posts_to_process, 1):
            print(f"--- Processing {i}/{len(posts_to_process)}: {match['match']} ---")
            
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
                print(f"  --> Extracted {link_count} video link(s). Result:")
                pprint(match_info, indent=2, width=120)
            else:
                print(f"  --> WARNING: No links found, skipping")
            
            print("-" * 80) # Separator for clarity

        print(f"\n{'='*80}")
        print(f"✓ Test Complete!")
        print(f"  - Processed {len(posts_to_process)} posts.")
        print(f"  - (File saving is disabled in this test version)")
        print(f"{'='*80}")

    # --- FINAL PRODUCTION RUNNER ---
    def run(self):
        """Main execution function"""
        print("=" * 80)
        print("FootballOrgin.com Scraper (V12 - Updated Categories)")
        print("=" * 80)
        
        # We need to load the log for the full run
        self.log_data = self.load_log()
        
        # --- THIS IS THE UPDATED LIST ---
        # It uses the paths from the URLs you provided
        categories = [
            'full-match-replay',
            'tv-show',
            'news-and-interviews',
            'review-show',
            'tv-show/bbc-match-of-the-day-motd'
        ]
        # ---------------------------------
        
        existing_matches = self.load_existing_matches()
        scraped_data_list = []
        
        all_posts_found = []
        for category in categories:
            posts_found = self.scrape_category(category)
            all_posts_found.extend(posts_found)
        
        # De-duplicate posts found in multiple categories
        # This handles your exact concern about a post being in multiple categories
        unique_posts_map = {post['match_id']: post for post in all_posts_found}
        all_posts_unique = list(unique_posts_map.values())

        # Process only NEW posts (checks the "earmark" log)
        posts_to_process = []
        for post in all_posts_unique:
            post_id = post['match_id']
            if post_id not in self.log_data:
                posts_to_process.append(post)
        
        print(f"\n{'='*80}") 
        print(f"Summary: Found {len(all_posts_unique)} unique posts across {len(categories)} categories")
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
                print(f"    --> Extracted {link_count} video link(s)")
            else:
                print(f"    --> WARNING: No links found, skipping")

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
        print(f"  - NEW posts scraped: {len(scraped_data_list)}")
        print(f"  - Total posts in {self.output_file}: {len(final_matches_ordered)}")
        print(f"{'='*80}")


if __name__ == "__main__":
    scraper = FootballOrginScraper()
    
    # --- THIS IS THE FINAL VERSION ---
    # It will run the full script and save to files.
    scraper.run()

    # To run the test version again, comment the line
    # above and uncomment the line below:
    # scraper.run_test()
