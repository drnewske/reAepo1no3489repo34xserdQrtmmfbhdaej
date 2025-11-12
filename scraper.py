from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import re
import time
import json
import os
import hashlib

class FootballOrginScraper:
    def __init__(self):
        self.base_url = "https://www.footballorgin.com"
        self.all_new_matches = [] 
        self.output_file = "matches.json"
        self.browser = None
        self.context = None
        self.page = None

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

    def init_browser(self, playwright):
        """Initialize browser with stealth settings"""
        self.browser = playwright.chromium.launch(headless=True)
        self.context = self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        self.page = self.context.new_page()

    def get_page_content(self, url):
        """Get page content using Playwright"""
        try:
            self.page.goto(url, wait_until='domcontentloaded', timeout=30000)
            time.sleep(2)  # Wait for any dynamic content
            return self.page.content()
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return None

    def get_page_matches(self, page_url):
        """
        Fetches and extracts match data from a single listing page.
        Returns None on error or if zero matches are found.
        """
        html_content = self.get_page_content(page_url)
        
        if not html_content:
            print(f"  --> Cannot scrape page.")
            return None

        soup = BeautifulSoup(html_content, 'html.parser')
        match_items = soup.find_all('article', class_=re.compile(r'post-item'))
        
        if not match_items:
            print("No articles found on the base page URL. Content may be lazy-loaded.")
            return None

        matches = []
        for item in match_items:
            try:
                title_tag = item.find('h3', class_='post-title')
                link_tag = title_tag.find('a') if title_tag else None
                if not link_tag or not link_tag.get('href'): continue
                
                match_url = link_tag.get('href')
                match_title = link_tag.text.strip()
                
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
                
                if 'http' in raw_url_value:
                    clean_url = raw_url_value.replace('\\/', '/').replace('\\', '')
                    return clean_url
                
                elif 'iframe' in raw_url_value:
                    iframe_src_match = re.search(r'src=\\\"(.*?)\\\"', raw_url_value)
                    if iframe_src_match:
                        video_src = iframe_src_match.group(1).replace('\\', '')
                        from urllib.parse import urljoin
                        return urljoin('https:', video_src)
                        
        return None

    def extract_match_details(self, match_data):
        """Extracts all video links for a match, including multi-links."""
        
        primary_url = match_data['url']
        final_links = []
        
        page_html = self.get_page_content(primary_url)
        if not page_html:
            return {'links': []}
        
        soup = BeautifulSoup(page_html, 'html.parser')
        
        primary_link = self.extract_single_video_link(page_html)
        
        if primary_link:
            primary_label = 'Full Match' if 'full-match' in primary_url else match_data['match']
            final_links.append({'label': primary_label, 'url': primary_link})

        multi_links_div = soup.find('div', class_='series-listing')
        
        if multi_links_div:
            for link_tag in multi_links_div.find_all('a'):
                href = link_tag.get('href')
                label = link_tag.text.strip()
                
                if href == primary_url:
                    continue
                    
                multi_html = self.get_page_content(href)
                if not multi_html:
                    continue
                    
                current_link = self.extract_single_video_link(multi_html)
                        
                if current_link and current_link not in [l['url'] for l in final_links]:
                    clean_label = label.replace(match_data['match'], '').strip(':- ')
                    final_links.append({'label': clean_label, 'url': current_link})

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
        
        return new_matches

    def run(self):
        """Main execution function to scrape all defined categories (first page only)."""
        print("FootballOrgin.com Scraper (V7 - Playwright)")
        print("=" * 80)
        
        with sync_playwright() as playwright:
            self.init_browser(playwright)
            
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
            
            for i, match in enumerate(self.all_new_matches, 1):
                print(f"Processing details {i}/{len(self.all_new_matches)}: {match['match']}")
                
                details = self.extract_match_details(match)
                
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
            
            self.browser.close()
        
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
