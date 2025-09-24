import argparse
import os
import pandas as pd
from googleapiclient.discovery import build
from datetime import datetime, timedelta
import pytz
import re

def get_channel_description(youtube, channel_id):
    """Get channel description and extract Instagram links"""
    try:
        request = youtube.channels().list(
            part="snippet",
            id=channel_id
        )
        response = request.execute()
        
        if response['items']:
            description = response['items'][0]['snippet'].get('description', '')
            
            # Extract Instagram links from description
            instagram_patterns = [
                r'instagram\.com/([a-zA-Z0-9_.]+)',
                r'@([a-zA-Z0-9_.]+)',
                r'ig:\s*([a-zA-Z0-9_.]+)',
                r'insta:\s*([a-zA-Z0-9_.]+)'
            ]
            
            instagram_links = []
            for pattern in instagram_patterns:
                matches = re.findall(pattern, description, re.IGNORECASE)
                for match in matches:
                    if 'instagram.com' in pattern:
                        instagram_links.append(f"https://instagram.com/{match}")
                    else:
                        instagram_links.append(f"https://instagram.com/{match}")
            
            return {
                'description': description[:500],  # Begränsa längd för CSV
                'instagram_links': instagram_links
            }
    except Exception as e:
        print(f"Error getting channel description for {channel_id}: {e}")
    
    return {'description': '', 'instagram_links': []}

def get_channel_videos(youtube, channel_url, max_results=10):
    """Get recent videos from a YouTube channel"""
    try:
        # Extract channel ID from URL
        if '/channel/' in channel_url:
            channel_id = channel_url.split('/channel/')[-1]
        elif '/@' in channel_url:
            # Handle @username format
            username = channel_url.split('/@')[-1]
            search_response = youtube.search().list(
                q=username,
                type='channel',
                part='id',
                maxResults=1
            ).execute()
            
            if search_response['items']:
                channel_id = search_response['items'][0]['id']['channelId']
            else:
                print(f"Could not find channel for username: {username}")
                return []
        else:
            print(f"Invalid channel URL format: {channel_url}")
            return []
        
        # Get channel's uploads playlist
        channel_response = youtube.channels().list(
            id=channel_id,
            part='contentDetails'
        ).execute()
        
        if not channel_response['items']:
            print(f"Channel not found: {channel_id}")
            return []
        
        uploads_playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        
        # Get videos from uploads playlist
        playlist_response = youtube.playlistItems().list(
            playlistId=uploads_playlist_id,
            part='snippet',
            maxResults=max_results
        ).execute()
        
        videos = []
        for item in playlist_response['items']:
            videos.append({
                'video_id': item['snippet']['resourceId']['videoId'],
                'title': item['snippet']['title'],
                'published_at': item['snippet']['publishedAt']
            })
        
        print(f"Found {len(videos)} videos from channel")
        return videos
        
    except Exception as e:
        print(f"Error getting channel videos: {e}")
        return []

def get_video_comments(youtube, video_id, max_results=100):
    """Get comments from a specific video"""
    comments = []
    
    try:
        request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId=video_id,
            maxResults=min(max_results, 100),
            order="relevance"
        )
        
        while request and len(comments) < max_results:
            response = request.execute()
            
            for item in response['items']:
                comment = item['snippet']['topLevelComment']
                author_channel_id = comment['snippet'].get('authorChannelId', {}).get('value', '')
                
                # Get channel description for this commenter
                channel_data = get_channel_description(youtube, author_channel_id) if author_channel_id else {'description': '', 'instagram_links': []}
                
                comment_data = {
                    'comment_id': comment['id'],
                    'video_id': video_id,
                    'author_name': comment['snippet']['authorDisplayName'],
                    'author_channel_id': author_channel_id,
                    'author_channel_url': f"https://youtube.com/channel/{author_channel_id}" if author_channel_id else '',
                    'comment_text': comment['snippet']['textDisplay'],
                    'like_count': comment['snippet']['likeCount'],
                    'published_at': comment['snippet']['publishedAt'],
                    'channel_description': channel_data['description'],
                    'instagram_from_description': ', '.join(channel_data['instagram_links'])
                }
                
                comments.append(comment_data)
                
                # Add replies if they exist
                if 'replies' in item:
                    for reply in item['replies']['comments']:
                        reply_author_channel_id = reply['snippet'].get('authorChannelId', {}).get('value', '')
                        reply_channel_data = get_channel_description(youtube, reply_author_channel_id) if reply_author_channel_id else {'description': '', 'instagram_links': []}
                        
                        reply_data = {
                            'comment_id': reply['id'],
                            'video_id': video_id,
                            'author_name': reply['snippet']['authorDisplayName'],
                            'author_channel_id': reply_author_channel_id,
                            'author_channel_url': f"https://youtube.com/channel/{reply_author_channel_id}" if reply_author_channel_id else '',
                            'comment_text': reply['snippet']['textDisplay'],
                            'like_count': reply['snippet']['likeCount'],
                            'published_at': reply['snippet']['publishedAt'],
                            'channel_description': reply_channel_data['description'],
                            'instagram_from_description': ', '.join(reply_channel_data['instagram_links'])
                        }
                        
                        comments.append(reply_data)
                
                if len(comments) >= max_results:
                    break
            
            # Get next page
            request = youtube.commentThreads().list_next(request, response) if len(comments) < max_results else None
            
    except Exception as e:
        print(f"Error getting comments for video {video_id}: {e}")
    
    return comments

def filter_comments_by_date(comments, days_back):
    """Filter comments by date"""
    if days_back <= 0:
        return comments
    
    cutoff_date = datetime.now(pytz.UTC) - timedelta(days=days_back)
    filtered_comments = []
    
    for comment in comments:
        try:
            comment_date = datetime.fromisoformat(comment['published_at'].replace('Z', '+00:00'))
            if comment_date >= cutoff_date:
                filtered_comments.append(comment)
        except Exception as e:
            print(f"Error parsing date for comment {comment['comment_id']}: {e}")
            # Include comment if date parsing fails
            filtered_comments.append(comment)
    
    return filtered_comments

def main():
    parser = argparse.ArgumentParser(description='Extract YouTube comments')
    parser.add_argument('--channel-url', required=True, help='YouTube channel URL')
    parser.add_argument('--video-count', type=int, default=10, help='Number of recent videos to analyze')
    parser.add_argument('--output-file', required=True, help='Output CSV file')
    parser.add_argument('--days-back', type=int, default=30, help='Only include comments from the last N days')
    
    args = parser.parse_args()
    
    # Get API key
    api_key = os.getenv('YOUTUBE_API_KEY')
    if not api_key:
        print("Error: YOUTUBE_API_KEY environment variable not set!")
        return
    
    # Initialize YouTube API
    youtube = build('youtube', 'v3', developerKey=api_key)
    
    # Get channel videos
    print(f"Getting recent videos from channel: {args.channel_url}")
    videos = get_channel_videos(youtube, args.channel_url, args.video_count)
    
    if not videos:
        print("No videos found!")
        return
    
    # Extract comments from all videos
    all_comments = []
    
    for i, video in enumerate(videos):
        print(f"Processing video {i+1}/{len(videos)}: {video['title']}")
        comments = get_video_comments(youtube, video['video_id'])
        all_comments.extend(comments)
        print(f"  Found {len(comments)} comments")
    
    print(f"Total comments extracted: {len(all_comments)}")
    
    # Filter by date
    if args.days_back > 0:
        filtered_comments = filter_comments_by_date(all_comments, args.days_back)
        print(f"Comments after date filter ({args.days_back} days): {len(filtered_comments)}")
        all_comments = filtered_comments
    
    # Save to CSV
    if all_comments:
        df = pd.DataFrame(all_comments)
        os.makedirs(os.path.dirname(args.output_file), exist_ok=True)
        df.to_csv(args.output_file, index=False)
        print(f"Saved {len(all_comments)} comments to {args.output_file}")
    else:
        print("No comments to save!")
        # Create empty file
        pd.DataFrame().to_csv(args.output_file, index=False)

if __name__ == "__main__":
    main()
