import os
import argparse
import pandas as pd
from googleapiclient.discovery import build
from datetime import datetime, timedelta
import re

class YouTubeCommentExtractor:
    def __init__(self, api_key):
        self.youtube = build('youtube', 'v3', developerKey=api_key)
        
    def extract_channel_id(self, channel_url):
        """Extract channel ID from various YouTube URL formats"""
        patterns = [
            r'youtube\.com/channel/([a-zA-Z0-9_-]+)',
            r'youtube\.com/c/([a-zA-Z0-9_-]+)',
            r'youtube\.com/@([a-zA-Z0-9_-]+)',
            r'youtube\.com/user/([a-zA-Z0-9_-]+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, channel_url)
            if match:
                username = match.group(1)
                if pattern.startswith(r'youtube\.com/channel/'):
                    return username
                else:
                    return self.get_channel_id_by_username(username)
        return None
    
    def get_channel_id_by_username(self, username):
        """Convert username to channel ID"""
        try:
            response = self.youtube.search().list(
                part='snippet',
                q=username,
                type='channel',
                maxResults=1
            ).execute()
            
            if response['items']:
                return response['items'][0]['snippet']['channelId']
        except Exception as e:
            print(f"Error getting channel ID: {e}")
        return None
    
    def get_recent_videos(self, channel_id, max_videos=10):
        """Get recent videos from channel"""
        try:
            response = self.youtube.search().list(
                part='snippet',
                channelId=channel_id,
                type='video',
                order='date',
                maxResults=max_videos
            ).execute()
            
            videos = []
            for item in response['items']:
                videos.append({
                    'video_id': item['id']['videoId'],
                    'title': item['snippet']['title'],
                    'published_at': item['snippet']['publishedAt']
                })
            return videos
        except Exception as e:
            print(f"Error getting videos: {e}")
            return []
    
    def extract_comments(self, video_id, days_back=30):
        """Extract comments from a video"""
        comments = []
        cutoff_date = datetime.now() - timedelta(days=days_back) if days_back > 0 else None
        
        try:
            response = self.youtube.commentThreads().list(
                part='snippet,replies',
                videoId=video_id,
                maxResults=100,
                order='relevance'
            ).execute()
            
            while response:
                for item in response['items']:
                    comment = item['snippet']['topLevelComment']['snippet']
                    comment_date = datetime.fromisoformat(comment['publishedAt'].replace('Z', '+00:00'))
                    
                    if cutoff_date and comment_date < cutoff_date:
                        continue
                    
                    comments.append({
                        'video_id': video_id,
                        'comment_id': item['snippet']['topLevelComment']['id'],
                        'author_name': comment['authorDisplayName'],
                        'author_channel_id': comment.get('authorChannelId', {}).get('value', ''),
                        'author_channel_url': comment.get('authorChannelUrl', ''),
                        'comment_text': comment['textDisplay'],
                        'like_count': comment['likeCount'],
                        'published_at': comment['publishedAt'],
                        'reply_count': item['snippet']['totalReplyCount']
                    })
                    
                    # Extract replies if any
                    if 'replies' in item:
                        for reply in item['replies']['comments']:
                            reply_snippet = reply['snippet']
                            reply_date = datetime.fromisoformat(reply_snippet['publishedAt'].replace('Z', '+00:00'))
                            
                            if cutoff_date and reply_date < cutoff_date:
                                continue
                                
                            comments.append({
                                'video_id': video_id,
                                'comment_id': reply['id'],
                                'author_name': reply_snippet['authorDisplayName'],
                                'author_channel_id': reply_snippet.get('authorChannelId', {}).get('value', ''),
                                'author_channel_url': reply_snippet.get('authorChannelUrl', ''),
                                'comment_text': reply_snippet['textDisplay'],
                                'like_count': reply_snippet['likeCount'],
                                'published_at': reply_snippet['publishedAt'],
                                'reply_count': 0,
                                'is_reply': True,
                                'parent_comment_id': item['snippet']['topLevelComment']['id']
                            })
                
                if 'nextPageToken' in response:
                    response = self.youtube.commentThreads().list(
                        part='snippet,replies',
                        videoId=video_id,
                        maxResults=100,
                        pageToken=response['nextPageToken'],
                        order='relevance'
                    ).execute()
                else:
                    break
                    
        except Exception as e:
            print(f"Error extracting comments for video {video_id}: {e}")
        
        return comments

def main():
    parser = argparse.ArgumentParser(description='Extract YouTube comments')
    parser.add_argument('--channel-url', required=True, help='YouTube channel URL')
    parser.add_argument('--video-count', type=int, default=10, help='Number of videos to analyze')
    parser.add_argument('--days-back', type=int, default=30, help='Days back to analyze (0 for all time)')
    
    args = parser.parse_args()
    
    api_key = os.getenv('YOUTUBE_API_KEY')
    if not api_key:
        raise ValueError("YOUTUBE_API_KEY environment variable not set")
    
    extractor = YouTubeCommentExtractor(api_key)
    
    # Extract channel ID
    channel_id = extractor.extract_channel_id(args.channel_url)
    if not channel_id:
        raise ValueError("Could not extract channel ID from URL")
    
    print(f"Analyzing channel: {channel_id}")
    
    # Get recent videos
    videos = extractor.get_recent_videos(channel_id, args.video_count)
    print(f"Found {len(videos)} videos")
    
    # Extract comments from all videos
    all_comments = []
    for video in videos:
        print(f"Extracting comments from: {video['title']}")
        comments = extractor.extract_comments(video['video_id'], args.days_back)
        
        # Add video info to comments
        for comment in comments:
            comment['video_title'] = video['title']
            comment['video_published_at'] = video['published_at']
        
        all_comments.extend(comments)
    
    # Save to CSV
    os.makedirs('data', exist_ok=True)
    df = pd.DataFrame(all_comments)
    df.to_csv('data/youtube_commenters_raw.csv', index=False)
    
    print(f"Extracted {len(all_comments)} comments")
    print(f"Unique commenters: {df['author_name'].nunique()}")

if __name__ == "__main__":
    main()
