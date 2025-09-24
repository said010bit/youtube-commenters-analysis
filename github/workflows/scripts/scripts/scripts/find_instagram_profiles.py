import argparse
import pandas as pd
import requests
import time
import re
import os
from urllib.parse import quote

class InstagramProfileFinder:
    def __init__(self, google_api_key, search_engine_id):
        self.google_api_key = google_api_key
        self.search_engine_id = search_engine_id
        self.base_url = "https://www.googleapis.com/customsearch/v1"
    
    def search_instagram_profile(self, username, real_name=None):
        """Search for Instagram profile using Google Custom Search"""
        search_queries = [
            f'site:instagram.com "{username}"',
            f'site:instagram.com "{username}" bio',
        ]
        
        if real_name and real_name != username:
            search_queries.append(f'site:instagram.com "{real_name}"')
        
        for query in search_queries:
            try:
                params = {
                    'key': self.google_api_key,
                    'cx': self.search_engine_id,
                    'q': query,
                    'num': 5
                }
                
                response = requests.get(self.base_url, params=params)
                response.raise_for_status()
                
                data = response.json()
                
                if 'items' in data:
                    for item in data['items']:
                        url = item['link']
                        title = item['title']
                        snippet = item.get('snippet', '')
                        
                        # Validate Instagram URL
                        if self.is_valid_instagram_profile(url, username):
                            return {
                                'instagram_url': url,
                                'instagram_username': self.extract_instagram_username(url),
                                'title': title,
                                'snippet': snippet,
                                'search_query': query,
                                'verification_method': 'Google Custom Search'
                            }
                
                # Rate limiting
                time.sleep(0.1)
                
            except Exception as e:
                print(f"Error searching for {username}: {e}")
                continue
        
        return None
    
    def is_valid_instagram_profile(self, url, username):
        """Validate if the Instagram URL is likely the correct profile"""
        if not url.startswith('https://www.instagram.com/'):
            return False
        
        # Extract username from URL
        ig_username = self.extract_instagram_username(url)
        if not ig_username:
            return False
        
        # Simple similarity check
        username_lower = username.lower()
        ig_username_lower = ig_username.lower()
        
        # Exact match
        if username_lower == ig_username_lower:
            return True
        
        # Username contains IG username or vice versa
        if username_lower in ig_username_lower or ig_username_lower in username_lower:
            return True
        
        # Remove common separators and check
        clean_username = re.sub(r'[._-]', '', username_lower)
        clean_ig_username = re.sub(r'[._-]', '', ig_username_lower)
        
        if clean_username == clean_ig_username:
            return True
        
        return False
    
    def extract_instagram_username(self, url):
        """Extract username from Instagram URL"""
        match = re.search(r'instagram\.com/([^/?]+)', url)
        return match.group(1) if match else None

def main():
    parser = argparse.ArgumentParser(description='Find Instagram profiles for YouTube commenters')
    parser.add_argument('--input-file', required=True, help='Input CSV file with ranked commenters')
    parser.add_argument('--output-file', required=True, help='Output CSV file')
    
    args = parser.parse_args()
    
    google_api_key = os.getenv('GOOGLE_SEARCH_API_KEY')
    search_engine_id = os.getenv('GOOGLE_SEARCH_ENGINE_ID')
    
    if not google_api_key or not search_engine_id:
        print("Warning: Google Search API credentials not set. Skipping Instagram search.")
        # Create empty results file
        df = pd.read_csv(args.input_file)
        df['instagram_profile'] = 'API Not Configured'
        df['instagram_username'] = ''
        df['verification_method'] = ''
        df['search_query_used'] = ''
        df.to_csv(args.output_file, index=False)
        return
    
    finder = InstagramProfileFinder(google_api_key, search_engine_id)
    
    # Load ranked commenters
    df = pd.read_csv(args.input_file)
    
    results = []
    
    for i, row in df.iterrows():
        username = row['author_name']
        print(f"Searching Instagram for: {username} ({i+1}/{len(df)})")
        
        # Search for Instagram profile
        instagram_data = finder.search_instagram_profile(username)
        
        result = {
            'rank': row['rank'],
            'youtube_display_name': username,
            'youtube_channel_url': row['author_channel_url'],
            'total_comments': row['total_comments'],
            'total_likes': row['total_likes'],
            'engagement_score': row['engagement_score'],
            'sample_comments': row['sample_comments']
        }
        
        if instagram_data:
            result.update({
                'instagram_profile': instagram_data['instagram_url'],
                'instagram_username': instagram_data['instagram_username'],
                'verification_method': instagram_data['verification_method'],
                'search_query_used': instagram_data['search_query']
            })
        else:
            result.update({
                'instagram_profile': 'Not Found',
                'instagram_username': '',
                'verification_method': '',
                'search_query_used': ''
            })
        
        results.append(result)
        
        # Rate limiting
        time.sleep(0.2)
    
    # Save results
    results_df = pd.DataFrame(results)
    results_df.to_csv(args.output_file, index=False)
    
    found_count = len(results_df[results_df['instagram_profile'] != 'Not Found'])
    print(f"Found Instagram profiles for {found_count}/{len(results_df)} commenters")

if __name__ == "__main__":
    main()
