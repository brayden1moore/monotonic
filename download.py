import os
import time
import json
import boto3
import random
import yt_dlp
import requests
import tempfile
import itertools
import scrapetube
import urllib.parse
from datetime import datetime
from flask import Flask, send_file, Response
from concurrent.futures import ThreadPoolExecutor
import threading

opts = {
    'format': 'bestaudio',
    'extractaudio': True,
    'audioformat': 'mp3',
    'outtmpl': f'temp/%(id)s.mp3',
}

with open('videos.json', 'r') as f: 
    video_dict = json.load(f)

videos = list(video_dict.keys())

def upload_to_bucket(id):    
    session = boto3.session.Session()
    client = session.client('s3',
                          region_name='sfo3',
                          endpoint_url='https://sfo3.digitaloceanspaces.com',
                          aws_access_key_id='DO00PTMRLEA3CJ6KGGQJ',
                          aws_secret_access_key='F5nqO8l6nhOBfmcJDjjEuznff0cTaMOJVINyuMsb6M0')
    
    file_path = f'temp/{id}.mp3'
    object_key = f'monotonic-radio/{id}.mp3'
    
    try:
        client.upload_file(file_path, 'scudbucket', object_key,
                          ExtraArgs={'StorageClass': 'STANDARD'})
        print(f"Successfully uploaded {object_key}")
        return True
    except Exception as e:
        print(f"Error uploading: {e}")
        return False
    

def get_files_in_bucket():
    client = boto3.client('s3',
                         region_name='sfo3',
                         endpoint_url='https://sfo3.digitaloceanspaces.com',
                         aws_access_key_id='DO00PTMRLEA3CJ6KGGQJ',
                         aws_secret_access_key='F5nqO8l6nhOBfmcJDjjEuznff0cTaMOJVINyuMsb6M0')
    
    files = []
    response = client.list_objects_v2(Bucket='scudbucket', Prefix='monotonic-radio/')
    
    if 'Contents' in response:
        for obj in response['Contents']:
            if obj['Key'].endswith('.mp3'):
                files.append(obj['Key'])
    
    return files
    

def process_video(video, opts, current_files):
    id = video['videoId']
    title = video['title']['runs'][0]['text']
    url = f'https://www.youtube.com/watch?v={id}'
    
    need_download = f'monotonic-radio/{id}.mp3' not in current_files
    
    with yt_dlp.YoutubeDL(opts) as ydl:
        meta = ydl.extract_info(url, download=need_download)
        duration = meta['duration']
        description = meta['description']
    
    info_dict = {
        'id': id,
        'title': title,
        'duration': duration,
        'description': description
    }
    
    if need_download and os.path.exists(f'temp/{id}.mp3'):
        upload_to_bucket(id)
    
    return id, info_dict


def get_videos(current_files):
    monotonic = scrapetube.get_channel("UCjQ-C6-2HdjE35Y-aXYIlLQ")
    scudhouse = scrapetube.get_channel("UCy0m3MTgem2Oks21uo6YdMg")
    videos = list(itertools.chain(monotonic, scudhouse))
    
    video_dict = {}
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_video, video, opts, current_files) for video in videos]
        
        for future in futures:
            id, info_dict = future.result()
            video_dict[id] = info_dict
    
    with open('videos.json', 'w') as f:
        json.dump(video_dict, f)
    
    return video_dict


get_videos(get_files_in_bucket())