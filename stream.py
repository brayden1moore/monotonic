import os
import time
import json
import boto3
import random
import requests
import tempfile
import itertools
import urllib.parse
from bs4 import BeautifulSoup
from datetime import datetime
from flask import Flask, send_file, Response, redirect, render_template 
from concurrent.futures import ThreadPoolExecutor
import threading

with open('videos.json', 'r') as f: 
    video_dict = json.load(f)

total_duration = 0
for k,v in video_dict.items():
    total_duration += v['duration']

videos = list(video_dict.keys())

app = Flask(__name__, template_folder='templates', static_folder='assets')

from flask import Flask, Response, render_template_string

def clear_temp():
    if not os.path.exists('temp'):
        os.mkdir('temp')
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
    shuffled_videos = videos.copy()
    random.Random(iterations).shuffle(shuffled_videos)
    while shuffled_videos[-1] == videos[0]:
        random.Random(iterations).shuffle(videos)

    # get video and time into video
    video_list = list(video_dict.keys())
    time_into_iteration = elapsed_seconds - (total_duration * iterations)
    time_sum = 0
    video_elapsed = 0
    video = ''
    next_video = ''
    mp3_path = ''

    for k, v in video_dict.items():

        if time_sum + v['duration'] > time_into_iteration:
            video_elapsed = time_into_iteration - time_sum
            video = k
            title = v['title']
            mp3_path = f'temp/{k}.mp3'
            
            video_index = video_list.index(k)
            if video_index < len(video_list) - 1:
                next_video = video_list[video_index + 1]
            else:
                next_video = video_list[0]
            
            prev_video = video_list[video_index - 1]
            break
        
        time_sum += v['duration']

    if os.path.exists(f'temp/{video}.mp3') == False:
        download_from_bucket(video)
    if os.path.exists(f'temp/{next_video}.mp3') == False:
        download_from_bucket(next_video)
    if os.path.exists(f'temp/{prev_video}.mp3'):
        os.remove(f'temp/{prev_video}.mp3')

    return title, video, mp3_path, video_elapsed


from functools import lru_cache
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@lru_cache(maxsize=128)
def download_from_bucket(id):
    try:
        url = f"https://scudbucket.sfo3.cdn.digitaloceanspaces.com/monotonic-radio/{id}.mp3"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        filepath = f'temp/{id}.mp3'
        with open(filepath, 'wb') as f:
            f.write(response.content)
        return True
    except requests.RequestException as e:
        logger.error(f"Failed to download {id}: {e}")
        return False

def preload_files():
    while True:
        _, current_id, _, _ = get_current_video()
        video_list = list(video_dict.keys())
        current_index = video_list.index(current_id)
        for i in range(1, 4):
            next_index = (current_index + i) % len(video_list)
            next_id = video_list[next_index]
            if not os.path.exists(f'temp/{next_id}.mp3'):
                download_from_bucket(next_id)
        time.sleep(30)  

preloader = threading.Thread(target=preload_files, daemon=True)
preloader.start()

def get_monotonic_live_link():
    url = 'https://monotonic.studio/live'
    webpage = requests.get(url).text
    soup = BeautifulSoup(webpage, 'html.parser')
    iframes = soup.find_all('iframe')
    for i in iframes:
        if 'meshcast' in i['src']:
            return i['src']
        else:
            return None

def generate_stream():
    CHUNK_SIZE = 4096 
    BUFFER_SIZE = 16384 
    INITIAL_CHUNKS = 3
    
    try:
        while True:
            current_video, id, mp3_path, video_elapsed = get_current_video()
            
            if not os.path.exists(mp3_path):
                logger.warning(f"File not found: {mp3_path}")
                time.sleep(1)
                continue
                
            bytes_per_second = 16000
            start_byte = int(video_elapsed * bytes_per_second)
            
            with open(mp3_path, 'rb') as f:
                f.seek(start_byte)
                chunk_count = 0
                
                while True:
                    new_video, new_id, _, _ = get_current_video()
                    if new_id != id:
                        break
                        
                    chunk = f.read(CHUNK_SIZE)
                    if not chunk:
                        break
                        
                    yield chunk

                    if chunk_count < INITIAL_CHUNKS:
                        chunk_count += 1
                    else:
                        time.sleep(CHUNK_SIZE / bytes_per_second)
                    
    except Exception as e:
        logger.error(f"Streaming error: {e}")


from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

def create_session():
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.3,
        status_forcelist=[500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

session = create_session()

@app.route('/stream')
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

@app.route('/')
def hello():
    return render_template('index.html', now_playing=get_current_video()[0])
    #return redirect("http://www.monotonic.studio/live", code=302)

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
    app.run(debug=True, port=8888)
