import os
import time
import json
import boto3
import random
import yt_dlp
import requests
import tempfile
import itertools
import subprocess
import scrapetube
import urllib.parse
from datetime import datetime
from flask import Flask, send_file, Response
from concurrent.futures import ThreadPoolExecutor
import threading

import subprocess
import mutagen.mp3
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

with open('archives.json', 'r') as f: 
    archive_dict = json.load(f)

def upload_to_bucket(id):    
    session = boto3.session.Session()
    client = session.client('s3',
                          region_name='sfo3',
                          endpoint_url='https://sfo3.digitaloceanspaces.com',
                          aws_access_key_id='DO00PTMRLEA3CJ6KGGQJ',
                          aws_secret_access_key='F5nqO8l6nhOBfmcJDjjEuznff0cTaMOJVINyuMsb6M0')
    
    file_path = f'archives/{id}.mp3'
    object_key = f'monotonic-radio/{id}.mp3'
    
    try:
        client.upload_file(file_path, 'scudbucket', object_key,
                          ExtraArgs={'StorageClass': 'STANDARD','ACL':'public-read'})
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
                files.append(obj['Key'].replace('monotonic-radio/','').replace('.mp3',''))
    
    return files

def get_mp3_bitrate_and_duration(filepath):
    cmd = [
        'ffprobe', '-v', 'quiet', '-print_format', 'json',
        '-show_streams', filepath
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    data = json.loads(result.stdout)
    try:
        bitrate = float(data['streams'][0]['bit_rate'])
    except:
        bitrate = float(data['streams'][0]['sample_rate'])
    duration = float(data['streams'][0]['duration'])
    return bitrate, duration

total_duration = 0
for key, val in archive_dict.items():
    bitrate, duration = get_mp3_bitrate_and_duration(f'archives/{key}.mp3')
    archive_dict[key]['bitrate'] = bitrate
    archive_dict[key]['duration'] = duration
    total_duration += duration

def make_playlist(archives, iterations):
    playlist = []
    for i in range(iterations):
        for j in archives:
            
            shuffled_archives = archives.copy()
            random.Random(iterations).shuffle(shuffled_archives)
            
            if len(playlist) > 0:
                attempt = 0
                while shuffled_archives[0] == playlist[-1] and attempt < 100:
                    random.Random(iterations + attempt + 1).shuffle(shuffled_archives)
                    attempt += 1

            playlist.extend(shuffled_archives)
    
    print((total_duration / 60 / 60) * iterations)
    return playlist

playlist = make_playlist(list(archive_dict.keys()), 5)

def upload():
    files = get_files_in_bucket()
    for key, val in archive_dict.items():
        if key not in files:
            upload_to_bucket(key)

with open('playlist.json', 'w') as f:
    json.dump(playlist, f)

with open('archives.json', 'w') as f:
    json.dump(archive_dict, f)

upload()