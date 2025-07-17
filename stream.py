import os
import time
import json
import boto3
import random
import requests
import tempfile
import itertools
import urllib.parse
from datetime import datetime
from flask import Flask, send_file, Response
from concurrent.futures import ThreadPoolExecutor
import threading

with open('videos.json', 'r') as f: 
    video_dict = json.load(f)

total_duration = 0
for k,v in video_dict.items():
    total_duration += v['duration']

videos = list(video_dict.keys())

app = Flask(__name__)

from flask import Flask, Response, render_template_string

def clear_temp():
    temp_dir = os.listdir('temp')
    for file in temp_dir:
        os.remove(f'temp/{file}')

def get_current_video():

    # get time since start and set iteration seed
    current_time = datetime.now()
    beginning_time = datetime(year=2025, month=7, day=11, hour=9)
    elapsed_seconds = (current_time - beginning_time).total_seconds()
    iterations = elapsed_seconds // total_duration

    # shuffle with seed
    current_list = videos
    random.Random(iterations).shuffle(videos)
    while current_list[-1] == videos[0]:
        random.Random(iterations).shuffle(videos)

    # get video and time into video
    video_list = list(video_dict.keys())
    time_into_iteration = elapsed_seconds - (total_duration * iterations)
    time_sum = 0
    video_elapsed = 0
    video = ''
    next_video = ''
    mp3_path = ''

    for k,v in video_dict.items():
        time_sum += v['duration']

        prev_video = video_list[video_list.index(k) - 1]
        video = k
        next_video = video_list[video_list.index(k) + 1]

        title = v['title']
        mp3_path = f'temp/{k}.mp3'

        if time_sum + v['duration'] > time_into_iteration:
            video_elapsed = time_into_iteration - time_sum
            break

    if os.path.exists(f'temp/{video}.mp3') == False:
        download_from_bucket(video)
    if os.path.exists(f'temp/{next_video}.mp3') == False:
        download_from_bucket(next_video)
    if os.path.exists(f'temp/{prev_video}.mp3'):
        os.remove(f'temp/{prev_video}.mp3')

    return title, video, mp3_path, video_elapsed


def download_from_bucket(id):
    url = f"https://scudbucket.sfo3.cdn.digitaloceanspaces.com/monotonic-radio/{id}.mp3"
    doc = requests.get(url)
    with open(f'temp/{id}.mp3', 'wb') as f:
        f.write(doc.content)


def generate_stream():
    current_video, id, mp3_path, video_elapsed = get_current_video()
    
    bytes_per_second = 16000
    start_byte = int(video_elapsed * bytes_per_second)
    
    file_size = os.path.getsize(mp3_path)
    start_byte = start_byte % file_size 
    
    with open(mp3_path, 'rb') as f:
        f.seek(start_byte)
        
        while True:
            chunk = f.read(1024)
            if not chunk:
                f.seek(0)
                chunk = f.read(1024)
                if not chunk:
                    break
            yield chunk
            time.sleep(1024/bytes_per_second/10)


@app.route('/')
def stream_mp3():
    return Response(
        generate_stream(),
        mimetype='audio/mpeg',
        headers={
            'Cache-Control': 'no-cache, no-store, must-revalidate',
            'Accept-Ranges': 'none',  
            'Content-Type': 'audio/mpeg'
        }
    )


@app.route('/info')
def get_info():
    current_video, id, mp3_path, video_elapsed = get_current_video()
    return {
        'now_playing': current_video,
        'video_description': video_dict[id]['description'],
        'duration': video_dict[id]['duration'],
        'elapsed': video_elapsed
    }

clear_temp()
get_current_video()

if __name__ == '__main__':
    app.run(debug=True)
