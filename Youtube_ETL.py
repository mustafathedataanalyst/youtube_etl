import pandas as pd
from googleapiclient.discovery import build
from datetime import datetime
import s3fs


def run_youtube_etl():
    api_key = "AIzaSyAJYYLOvWZEf0zhgTmL4fnwkYwCAccVV3c"
    channel_id = "UCh9nVJoWXmFb7sLApWGcLPQ"

    youtube = build("youtube", "v3", developerKey=api_key)


    def get_channel_stats(youtube, channel_id):
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
        response = request.execute()
        data = {
            'channel_name': response['items'][0]['snippet']['title'],
            'subscribers': response['items'][0]['statistics']['subscriberCount'],
            'views': response['items'][0]['statistics']['viewCount'],
            'video_count': response['items'][0]['statistics']['videoCount']
        }
        return data


    def get_video_details(youtube, video_ids):
        video_details = []
        request = youtube.videos().list(
            part="snippet,statistics",
            id=','.join(video_ids)
        )
        response = request.execute()
        for item in response['items']:
            video_data = {
                'title': item['snippet']['title'],
                'view_count': item['statistics']['viewCount'],
                'comment_count': item['statistics'].get('commentCount', 0)  # Some videos may have comments disabled
            }
            video_details.append(video_data)
        return video_details


    def get_all_videos(youtube, playlist_id):
        videos = []
        next_page_token = None

        while True:
            request = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()
            videos.extend(item['contentDetails']['videoId'] for item in response['items'])

            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break

        return videos



    channel_data = get_channel_stats(youtube, channel_id)


    uploads_playlist_id = youtube.channels().list(
        part="contentDetails",
        id=channel_id
    ).execute()['items'][0]['contentDetails']['relatedPlaylists']['uploads']


    video_ids = get_all_videos(youtube, uploads_playlist_id)


    chunk_size = 50
    video_details = []
    for i in range(0, len(video_ids), chunk_size):
        video_chunk = video_ids[i:i + chunk_size]
        video_details.extend(get_video_details(youtube, video_chunk))


    channel_df = pd.DataFrame([channel_data])
    video_df = pd.DataFrame(video_details)


    #channel_df.to_csv('s3://youtube-etl-bucket-de/channel_stats.csv', index=False)
    #video_df.to_csv('s3://youtube-etl-bucket-de/video_details.csv', index=False)
    channel_df.to_csv(r'C:\Users\attaul.mustafa\Desktop\Youtube\channel_stats.csv', index=False)
    video_df.to_csv(r'C:\Users\attaul.mustafa\Desktop\Youtube\video_details.csv', index=False)

    print("Data has been saved to channel_stats.csv and video_details.csv")
