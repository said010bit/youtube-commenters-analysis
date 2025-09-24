import argparse
import pandas as pd
import requests
import time
import os
from urllib.parse import quote

def search_instagram_profile(username, api_key, search_engine_id):
    """Search for Instagram profile using Google Custom Search"""
    try:
        # Clean username
        clean_username = username.replace(' ', '').replace('@', '').lower()
        
        # Search query
        query = f"site:instagram.com {clean_username}"
        url = f"https://www.googleapis.com/customsearch/v1"
        
        params = {
            'key': api_key,
            'cx': search_engine_id,
            'q': query,
            'num': 3
        }
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            
            if 'items' in data:
                for item in data['items']:
                    link = item['link']
                    title = item.get('title', '')
                    
                    # Check if it's a valid Instagram profile
                    if 'instagram.com/' in link and not any(x in link for x in ['/p/', '/reel/', '/tv/']):
                        # Extract Instagram username from URL
                        ig_username = link.split('instagram.com/')[-1].split('/')[0].split('?')[0]
                        
                        # Simple similarity check
                        if (clean_username in ig_username.lower() or 
                            ig_username.lower() in clean_username or
                            len(set(clean_username) & set(ig_username.lower())) > len(clean_username) * 0.5):
                            
                            return {
                                'instagram_url': link,
                                'instagram_username': ig_username,
                                'match_confidence': 'high' if clean_username in ig_username.lower() else 'medium',
                                'search_title': title
                            }
        
        return None
        
    except Exception as e:
        print(f"Error searching for {username}: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description='Find Instagram profiles for YouTube commenters')
    parser.add_argument('--input-file', required=True, help='Input CSV file with ranked commenters')
    parser.add_argument('--output-file', required=True, help='Output CSV file')
    
    args = parser.parse_args()
    
    # Get API credentials
    api_key = os.getenv('GOOGLE_SEARCH_API_KEY')
    search_engine_id = os.getenv('GOOGLE_SEARCH_ENGINE_ID')
    
    if not api_key or not search_engine_id:
        print("Error: Google Search API credentials not found!")
        return
    
    # Load data
    if not os.path.exists(args.input_file):
        print(f"Error: Input file {args.input_file} not found!")
        return
    
    try:
        df = pd.read_csv(args.input_file)
        print(f"Loaded {len(df)} commenters from {args.input_file}")
    except Exception as e:
        print(f"Error loading data: {e}")
        return
    
    if df.empty:
        print("No data to process!")
        pd.DataFrame().to_csv(args.output_file, index=False)
        return
    
    # Search for Instagram profiles
    results = []
    
    for i, row in df.iterrows():
        username = row['author_name']
        print(f"Searching Instagram for: {username} ({i+1}/{len(df)})")
        
        instagram_data = search_instagram_profile(username, api_key, search_engine_id)
        
        result = row.to_dict()
        
        if instagram_data:
            result.update(instagram_data)
            print(f"  Found: {instagram_data['instagram_url']}")
        else:
            result.update({
                'instagram_url': '',
                'instagram_username': '',
                'match_confidence': '',
                'search_title': ''
            })
            print(f"  No Instagram profile found")
        
        results.append(result)
        
        # Rate limiting
        time.sleep(0.1)
        
        # Stop if we hit API limits
        if i >= 50:  # Limit to first 50 to avoid hitting API limits
            print("Stopping at 50 searches to avoid API limits")
            break
    
    # Save results
    results_df = pd.DataFrame(results)
    os.makedirs(os.path.dirname(args.output_file), exist_ok=True)
    results_df.to_csv(args.output_file, index=False)
    
    found_count = len([r for r in results if r.get('instagram_url')])
    print(f"Found Instagram profiles for {found_count}/{len(results)} commenters")

if __name__ == "__main__":
    main()
