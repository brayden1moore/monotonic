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
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

with open('archives.json', 'r') as f: 
    archive_dict = json.load(f)

with open('playlist.json', 'r') as f: 
    playlist = json.load(f)

total_duration = 0
for k,v in archive_dict.items():
    total_duration += v['duration']

app = Flask(__name__, template_folder='templates', static_folder='assets')
CORS(app)

from flask import Flask, Response, render_template_string

def get_current():
    # get current iteration
    current_time = datetime.now()
    beginning_time = datetime(year=2025, month=3, day=20, hour=6)
    elapsed_seconds = (current_time - beginning_time).total_seconds()
    iterations = int(elapsed_seconds // total_duration)
    time_into_iteration = elapsed_seconds - (total_duration * iterations)

    # get place in current iteration
    time_sum = 0
    for i, archive_id in enumerate(playlist):
        v = archive_dict[archive_id]
        
        if time_sum + v['duration'] > time_into_iteration:
            archive_elapsed = time_into_iteration - time_sum
            byterate = v['bitrate'] / 8
            mp3_path = f'temp/{archive_id}.mp3'
            
            next_archive = playlist[(i + 1) % len(playlist)]
            next_next_archive = playlist[(i + 2) % len(playlist)]
            prev_archive = playlist[i - 1]

            # clear out previous
            if os.path.exists(f'temp/{prev_archive}.mp3'):
                try:
                    os.remove(f'temp/{prev_archive}.mp3')
                except Exception as e:
                    logger.error(f"Failed to remove {prev_archive}: {e}")

            # download current, next, and double-next
            if not os.path.exists(mp3_path):
                logger.warning(f"Current video {archive_id} not downloaded yet")
                download_from_bucket(archive_id)
            if not os.path.exists(f'temp/{next_archive}.mp3'):
                logger.warning(f"Next video {next_archive} not downloaded yet")
                download_from_bucket(next_archive)
            if not os.path.exists(f'temp/{next_next_archive}.mp3'):
                logger.warning(f"Next video {next_next_archive} not downloaded yet")
                download_from_bucket(next_next_archive)           

            return v['title'], archive_id, mp3_path, archive_elapsed, byterate
        
        time_sum += v['duration']

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
            get_current()
            time.sleep(10)
            
        except Exception as e:
            logger.error(f"Preloader crashed: {e}", exc_info=True)
            time.sleep(5)

preloader = threading.Thread(target=preload_files, daemon=True)
preloader.start()
    
def generate_stream():
    logging.info('generating stream started')
    CHUNK_SIZE = 8192 
    BUFFER_SIZE = 16384 * 16
    INITIAL_CHUNKS = 16
    MIN_BUFFER_CHUNKS = 8
    
    # Track actual stream time separately from system time
    stream_absolute_start = time.time()
    total_stream_bytes = 0
    
    while True:
        current_video, id, mp3_path, video_elapsed, bitrate = get_current()
        
        if not os.path.exists(mp3_path):
            logger.warning(f"File not found: {mp3_path}")
            time.sleep(0.5)
            continue
        
        # Calculate where we should be based on our stream time, not system time
        stream_elapsed = time.time() - stream_absolute_start
        
        start_byte = int(video_elapsed * bitrate)
        logger.info(f"Audio {id}: seeking to byte {start_byte} ({video_elapsed:.1f}s)")
        
        with open(mp3_path, 'rb') as f:
            f.seek(start_byte)
            
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
            video_start_time = time.time()
            video_bytes_sent = 0
            
            while True:
                new_video, new_id, _, _, _ = get_current()
                if new_id != id:
                    logger.info(f"Switch: {id} -> {new_id}")
                    break
                
                if not buffer:
                    break
                
                chunk = buffer.pop(0)
                yield chunk
                
                video_bytes_sent += len(chunk)
                total_stream_bytes += len(chunk)
                
                if len(buffer) < MIN_BUFFER_CHUNKS and not file_ended:
                    new_chunk = f.read(CHUNK_SIZE)
                    if new_chunk:
                        buffer.append(new_chunk)
                        file_ended = len(new_chunk) < CHUNK_SIZE
                    else:
                        file_ended = True
                
                if chunk_count >= INITIAL_CHUNKS:
                    expected_time = video_bytes_sent / bitrate
                    actual_time = time.time() - video_start_time
                    sleep_time = expected_time - actual_time
                    
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                else:
                    chunk_count += 1

import subprocess

def generate_stream_ffmpeg():
    logging.info('generating stream started')
    last_live_check = 0
    LIVE_CHECK_INTERVAL = 5
    
    while True:
        current_time = time.time()
        if current_time - last_live_check >= LIVE_CHECK_INTERVAL:
            live_info = check_for_live()
            last_live_check = current_time
        else:
            live_info = None 
        
        if live_info:
            logger.info(f"Switching to live stream: {live_info.get('name')}")
            
            mpv_command = [
                "mpv",
                "--no-video",
                "--no-terminal",
                "--o=-",
                "--of=mp3",
                "--oac=libmp3lame",
                "--oacopts=b=128k",
                "http://monotonicradio.com:8000/stream.m3u"
            ]
            
            process = subprocess.Popen(
                mpv_command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=8192
            )
            
            try:
                chunk_count = 0
                while True:
                    # Check every ~25 chunks 
                    chunk_count += 1
                    if chunk_count % 25 == 0:
                        if not check_for_live():
                            logger.info("Live stream ended, switching back to playlist")
                            process.terminate()
                            break
                    
                    chunk = process.stdout.read(8192)
                    if not chunk:
                        logger.warning("Live stream ended unexpectedly")
                        break
                    
                    yield chunk
                    
            finally:
                process.terminate()
                process.wait()
                
        else:
            # Regular playlist streaming
            current_video, id, mp3_path, video_elapsed, bitrate = get_current()
            
            if not os.path.exists(mp3_path):
                logger.warning(f"File not found: {mp3_path}")
                time.sleep(0.5)
                continue
            
            logger.info(f"Audio {id}: starting from {video_elapsed:.1f}s")
            
            cmd = [
                'ffmpeg',
                '-ss', str(video_elapsed),
                '-i', mp3_path,
                '-f', 'mp3',
                '-b:a', '128k',
                '-ar', '44100',
                '-'
            ]
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                bufsize=8192
            )
            
            try:
                chunk_count = 0
                while True:
                    # Check for live every ~25 chunks
                    chunk_count += 1
                    if chunk_count % 25 == 0:
                        if check_for_live():
                            logger.info("Live stream detected, switching")
                            process.terminate()
                            break
                    
                    # Check if we should switch to next video
                    new_video, new_id, _, _, _ = get_current()
                    if new_id != id:
                        logger.info(f"Switch: {id} -> {new_id}")
                        process.terminate()
                        break
                    
                    chunk = process.stdout.read(8192)
                    if not chunk:
                        break
                        
                    yield chunk
                    
            finally:
                process.terminate()
                process.wait()

def generate_live_stream(url):
    
    mpv_command = [
        "mpv",
        "--no-video",
        "--no-terminal",
        "--o=-",               
        "--of=mp3",             
        "--oac=libmp3lame",    
        "--oacopts=b=128k",    
        url
    ]
    
    print("Starting MPV direct stream...")
    
    try:
        process = subprocess.Popen(
            mpv_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=10**8
        )
        
        while True:
            chunk = process.stdout.read(8192)
            
            if not chunk:
                if process.poll() is not None:
                    stderr = process.stderr.read().decode('utf-8', errors='ignore')
                break
            
            yield chunk
                
    except Exception as e:
        print(f"Streaming error: {e}")
    finally:
        if 'process' in locals():
            process.kill()

def check_for_live():
    resp = requests.get("http://monotonicradio.com:8000/status-json.xsl").json()

    if not resp.get('icestats', {}).get('source'):
        return None
    else:
        info = resp['icestats']['source']
        yt_link = info.get('server_url')
        if yt_link:
            yt_link = yt_link.replace('/watch','/embed')

        genres = info.get('genre') or ''
        genres = [i.strip() for i in genres.split(',')]

        return {
            'genres':genres,
            'yt_link':yt_link,
            'name': info.get('server_name'),
            'description': info.get('server_description')
        }
    

@app.route('/stream')
def stream_ffmpeg_mp3():
    return Response(
        generate_stream_ffmpeg(),
        mimetype='audio/mpeg',
        headers={
            'Cache-Control': 'no-cache, no-store, must-revalidate',
            'Pragma': 'no-cache',
            'Expires': '0',
            'Content-Type': 'audio/mpeg',
            'X-Content-Type-Options': 'nosniff',
            'Transfer-Encoding': 'chunked',
            'Accept-Ranges': 'none', 
            'Connection': 'keep-alive'
        },
        direct_passthrough=True 
    )

@app.route('/stream-old')
def stream_mp3():
    live_info = check_for_live()
    if live_info:
        return Response(
            generate_live_stream('http://monotonicradio.com:8000/stream.m3u'),
            mimetype='audio/mpeg',
            headers={
                'Cache-Control': 'no-cache, no-store, must-revalidate',
                'Pragma': 'no-cache',
                'Expires': '0',
                'Content-Type': 'audio/mpeg',
                'X-Content-Type-Options': 'nosniff'
            }
        )
    else :
        current_video, id, mp3_path, video_elapsed, bitrate = get_current()
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
    thumbnail = archive_dict[id]['thumbnail']
    if 'assets/thumbnail' not in thumbnail:
        if os.path.exists(f'assets/thumbnail/{id}.webp'):
            thumbnail = f'assets/thumbnail/{id}.webp'
        else:
            try:
                response = requests.head(thumbnail, timeout=0.5)
                if response.status_code == 404:
                    thumbnail = 'assets/mtr.jpg'
            except (requests.RequestException, requests.Timeout):
                thumbnail = 'assets/mtr.jpg'
    return thumbnail

@app.route('/')
def hello():
    current, id, mp3_path, video_elapsed, byterate = get_current()
    genres = ', '.join(archive_dict[id]['genres'])
    return render_template('index.html', now_playing=current, genres=genres, description=archive_dict[id]['description'].replace('\n','<br>'), thumbnail=get_thumbnail(id))
    #return redirect("http://www.monotonic.studio/live", code=302)

@app.route('/shmoodguy')
def shmoodguy():
    return redirect("https://www.ticketmaster.com/user/orders", code=302)

@app.route('/info')
def get_info():
    live_info = check_for_live()
    if live_info:
        return {
            'now_playing': live_info['name'],
            'video_description': live_info['description'],
            'genres': live_info['genres'],
            'youtube_link': live_info['yt_link'],
            'duration': None,
            'elapsed': None,
            'byterate': None,
            'thumbnail': None,
            'source':'live'
        }
    
    else:
        current, id, mp3_path, video_elapsed, byterate = get_current()

        return {
            'now_playing': current,
            'video_description': archive_dict[id]['description'],
            'duration': archive_dict[id]['duration'],
            'genres': archive_dict[id]['genres'],
            'elapsed': round(video_elapsed),
            'byterate': byterate,
            'thumbnail': get_thumbnail(id),
            'source':'archive'
        }


for id, _ in archive_dict.items():
    download_from_bucket(id)

get_current()

if __name__ == '__main__':
    app.run(debug=True, port=8888)
