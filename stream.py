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
from flask_cors import CORS

with open('videos.json', 'r') as f: 
    video_dict = json.load(f)

total_duration = 0
for k,v in video_dict.items():
    total_duration += v['duration']

videos = list(video_dict.keys())

app = Flask(__name__, template_folder='templates', static_folder='assets')
CORS(app)

from flask import Flask, Response, render_template_string

def clear_temp():
    if not os.path.exists('temp'):
        os.mkdir('temp')
    temp_dir = os.listdir('temp')
    for file in temp_dir:
        os.remove(f'temp/{file}')

def get_shuffled_playlist():
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
    
    return shuffled_videos

import subprocess
import mutagen.mp3


def get_mp3_bitrate(filepath):
    logging.info(filepath)
    cmd = [
        'ffprobe', '-v', 'quiet', '-print_format', 'json',
        '-show_streams', filepath
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    data = json.loads(result.stdout)
    logging.info(data)
    try:
        bitrate = int(data['streams'][0]['bit_rate'])
    except:
        bitrate = int(data['streams'][0]['sample_rate'])
    return bitrate / 8


def get_current_video(need_bitrate=False):
    current_time = datetime.now()
    beginning_time = datetime(year=2025, month=7, day=11, hour=9)
    elapsed_seconds = (current_time - beginning_time).total_seconds()
    iterations = int(elapsed_seconds // total_duration)
    
    shuffled_videos = get_shuffled_playlist()

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
                download_from_bucket(video_id)
            if not os.path.exists(f'temp/{next_video}.mp3'):
                logger.warning(f"Next video {video_id} not downloaded yet")
                download_from_bucket(next_video)
            
            if os.path.exists(f'temp/{prev_video}.mp3'):
                try:
                    os.remove(f'temp/{prev_video}.mp3')
                except Exception as e:
                    logger.error(f"Failed to remove {prev_video}: {e}")
            
            if need_bitrate:
                bitrate = get_mp3_bitrate(mp3_path)
            else: 
                bitrate = 0

            return v['title'], video_id, mp3_path, video_elapsed, bitrate
        
        time_sum += v['duration']
    
    return "", "", "", 0


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
            get_current_video()
            time.sleep(10)
            
        except Exception as e:
            logger.error(f"Preloader crashed: {e}", exc_info=True)
            time.sleep(5)

preloader = threading.Thread(target=preload_files, daemon=True)
preloader.start()
    
def generate_stream():
    CHUNK_SIZE = 8192 
    BUFFER_SIZE = 16384 * 16
    INITIAL_CHUNKS = 16
    MIN_BUFFER_CHUNKS = 8  # Refill when buffer gets low
    last_completed_id = None
    
    while True:
        current_video, id, mp3_path, video_elapsed, bitrate = get_current_video(need_bitrate=True)
        
        if id == last_completed_id:
            time.sleep(0.5)
            continue
            
        if not os.path.exists(mp3_path):
            logger.warning(f"File not found: {mp3_path}")
            time.sleep(1)
            continue
        
        start_byte = int(video_elapsed * bitrate)
        
        with open(mp3_path, 'rb') as f:
            f.seek(start_byte)
            
            # Pre-buffer
            buffer = []
            bytes_buffered = 0
            while bytes_buffered < BUFFER_SIZE:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                buffer.append(chunk)
                bytes_buffered += len(chunk)
            
            chunk_count = 0
            file_ended = len(buffer[-1]) < CHUNK_SIZE if buffer else True
            
            while True:
                new_video, new_id, _, _, _ = get_current_video(need_bitrate=False)
                if new_id != id:
                    break
                
                if not buffer:
                    if file_ended:
                        last_completed_id = id
                    break
                
                chunk_start = time.time()
                
                # Get chunk from buffer
                chunk = buffer.pop(0)
                yield chunk
                
                # Refill buffer proactively
                if len(buffer) < MIN_BUFFER_CHUNKS and not file_ended:
                    new_chunk = f.read(CHUNK_SIZE)
                    if new_chunk:
                        buffer.append(new_chunk)
                        if len(new_chunk) < CHUNK_SIZE:
                            file_ended = True
                    else:
                        file_ended = True
                
                if chunk_count < INITIAL_CHUNKS:
                    chunk_count += 1
                else:
                    chunk_duration = CHUNK_SIZE / bitrate
                    elapsed = time.time() - chunk_start
                    sleep_time = chunk_duration - elapsed
                    if sleep_time > 0:
                        time.sleep(sleep_time)

@app.route('/stream')
def stream_mp3():
    return Response(
        generate_stream(),
        mimetype='audio/mpeg',
        headers={
            'Cache-Control': 'no-cache, no-store, must-revalidate',
            'Accept-Ranges': 'none',   
            'Content-Type': 'audio/mpeg',
        }
    )

def get_thumbnail(id):
    thumbnail = video_dict[id]['thumbnail']
    try:
        response = requests.head(thumbnail, timeout=0.5)
        if response.status_code == 404:
            thumbnail = 'assets/mtr.jpg'
    except (requests.RequestException, requests.Timeout):
        thumbnail = 'assets/mtr.jpg'
    return thumbnail

@app.route('/')
def hello():
    current_video, id, mp3_path, video_elapsed, bitrate = get_current_video()
    return render_template('index.html', now_playing=current_video, thumbnail=get_thumbnail(id))
    #return redirect("http://www.monotonic.studio/live", code=302)

@app.route('/info')
def get_info():
    current_video, id, mp3_path, video_elapsed, bitrate = get_current_video(need_bitrate=True)

    return {
        'now_playing': current_video,
        'video_description': video_dict[id]['description'],
        'duration': video_dict[id]['duration'],
        'elapsed': round(video_elapsed),
        'bitrate': bitrate,
        'link': f'https://www.youtube.com/watch?v={id}',
        'thumbnail':get_thumbnail(id)
    }

get_current_video()

if __name__ == '__main__':
    app.run(debug=True, port=8888)
