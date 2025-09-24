import argparse
import pandas as pd
import numpy as np

def rank_commenters(df, limit=100):
    """Rank commenters by activity and engagement"""
    
    # Group by commenter
    commenter_stats = df.groupby(['author_name', 'author_channel_id', 'author_channel_url']).agg({
        'comment_id': 'count',  # Total comments
        'like_count': ['sum', 'mean'],  # Total and average likes
        'comment_text': lambda x: ' | '.join(x[:3]),  # Sample comments
        'video_id': 'nunique',  # Videos commented on
        'published_at': ['min', 'max']  # First and last comment dates
    }).reset_index()
    
    # Flatten column names
    commenter_stats.columns = [
        'author_name', 'author_channel_id', 'author_channel_url',
        'total_comments', 'total_likes', 'avg_likes_per_comment',
        'sample_comments', 'videos_commented_on', 'first_comment', 'last_comment'
    ]
    
    # Calculate engagement score
    commenter_stats['engagement_score'] = (
        commenter_stats['total_comments'] * 0.4 +
        commenter_stats['total_likes'] * 0.3 +
        commenter_stats['videos_commented_on'] * 0.2 +
        commenter_stats['avg_likes_per_comment'] * 0.1
    )
    
    # Rank by engagement score
    commenter_stats = commenter_stats.sort_values('engagement_score', ascending=False)
    
    # Add rank
    commenter_stats['rank'] = range(1, len(commenter_stats) + 1)
    
    # Limit results
    return commenter_stats.head(limit)

def main():
    parser = argparse.ArgumentParser(description='Rank YouTube commenters')
    parser.add_argument('--input-file', required=True, help='Input CSV file')
    parser.add_argument('--output-file', required=True, help='Output CSV file')
    parser.add_argument('--limit', type=int, default=100, help='Number of top commenters')
    
    args = parser.parse_args()
    
    # Load data
    df = pd.read_csv(args.input_file)
    
    # Rank commenters
    ranked_df = rank_commenters(df, args.limit)
    
    # Save results
    ranked_df.to_csv(args.output_file, index=False)
    
    print(f"Ranked {len(ranked_df)} commenters")
    print(f"Top 5 commenters:")
    for i, row in ranked_df.head().iterrows():
        print(f"{row['rank']}. {row['author_name']} - {row['total_comments']} comments, {row['total_likes']} likes")

if __name__ == "__main__":
    main()
