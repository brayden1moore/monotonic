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
    iterations = int(elapsed_seconds // total_duration)
    
    shuffled_videos = videos.copy()
    random.Random(iterations).shuffle(shuffled_videos)
    
    attempt = 0
    while shuffled_videos[-1] == shuffled_videos[0] and attempt < 100:
        random.Random(iterations + attempt + 1).shuffle(shuffled_videos)
        attempt += 1
    
    time_into_iteration = elapsed_seconds - (total_duration * iterations)
    time_sum = 0
    
    for i, video_id in enumerate(shuffled_videos):
        v = video_dict[video_id]
        
        if time_sum + v['duration'] > time_into_iteration:
            video_elapsed = time_into_iteration - time_sum
            mp3_path = f'temp/{video_id}.mp3'
            
            next_video = shuffled_videos[(i + 1) % len(shuffled_videos)]
            prev_video = shuffled_videos[i - 1]
            
            if not os.path.exists(mp3_path):
                logger.warning(f"Current video {video_id} not downloaded yet")
            if not os.path.exists(f'temp/{next_video}.mp3'):
                logger.warning(f"Next video {next_video} not downloaded yet")
            
            if os.path.exists(f'temp/{prev_video}.mp3'):
                try:
                    os.remove(f'temp/{prev_video}.mp3')
                except Exception as e:
                    logger.error(f"Failed to remove {prev_video}: {e}")
            
            return v['title'], video_id, mp3_path, video_elapsed
        
        time_sum += v['duration']
    
    logger.error("Failed to find current video in iteration")
    return "", "", "", 0


from functools import lru_cache
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def download_from_bucket(id, max_retries=3):
    filepath = f'temp/{id}.mp3'
    if os.path.exists(filepath):
        logger.info(f"File {id} already exists, skipping download")
        return True
    
    for attempt in range(max_retries):
        try:
            url = f"https://scudbucket.sfo3.cdn.digitaloceanspaces.com/monotonic-radio/{id}.mp3"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                f.write(response.content)
            logger.info(f"Successfully downloaded {id}")
            return True
        except requests.RequestException as e:
            logger.error(f"Attempt {attempt+1} failed to download {id}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt) 
    
    return False

def preload_files():
    while True:
        try:
            _, current_id, _, _ = get_current_video()
            
            if not os.path.exists(f'temp/{current_id}.mp3'):
                logger.info(f"Preloader downloading current video: {current_id}")
                download_from_bucket(current_id)
            
            video_list = list(video_dict.keys())
            current_index = video_list.index(current_id)
            for i in range(1, 4):  # Next 3 videos
                next_index = (current_index + i) % len(video_list)
                next_id = video_list[next_index]
                if not os.path.exists(f'temp/{next_id}.mp3'):
                    logger.info(f"Preloader downloading: {next_id}")
                    download_from_bucket(next_id)
            
            time.sleep(10) 
        except Exception as e:
            logger.error(f"Preloader error: {e}")
            time.sleep(5)

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
        
import mutagen.mp3

def get_mp3_bitrate(filepath):
    audio = mutagen.mp3.MP3(filepath)
    return audio.info.bitrate / 8

import subprocess

def generate_stream():
    try:
        while True:
            current_video, id, mp3_path, video_elapsed = get_current_video()
            
            if not os.path.exists(mp3_path):
                logger.warning(f"File not found: {mp3_path}")
                time.sleep(1)
                continue
            
            cmd = [
                'ffmpeg',
                '-ss', str(video_elapsed),
                '-i', mp3_path,
                '-f', 'mp3',
                '-c', 'copy', 
                'pipe:1'
            ]
            
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            
            try:
                while True:
                    new_video, new_id, _, _ = get_current_video()
                    if new_id != id:
                        process.kill()
                        break
                    
                    chunk = process.stdout.read(8192)
                    if not chunk:
                        break
                    
                    yield chunk
                    
            finally:
                process.kill()
                
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
