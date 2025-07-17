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
from flask import Flask, send_file, Response, redirect
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


def get_monotonic_live_link():
    url = 'https://monotonic.studio/live'
    try:
        webpage = requests.get(url).text
        soup = BeautifulSoup(webpage, 'html.parser')
        iframes = soup.find_all('iframe')
        for i in iframes:
            if 'meshcast' in i['src']:
                return i['src']
    except Exception as e:
        print(f"Error checking live link: {e}")
    return None


def stream_live_audio(live_url):
    try:
        response = requests.get(live_url, stream=True)
        response.raise_for_status()
        
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                yield chunk
                if not get_monotonic_live_link():
                    break
    except Exception as e:
        print(f"Error streaming live audio: {e}")


def generate_stream():
    live_link = get_monotonic_live_link()
    if live_link:
        yield from stream_live_audio(live_link)
        return
    
    current_video, id, mp3_path, video_elapsed = get_current_video()
    bytes_per_second = 16000
    start_byte = int(video_elapsed * bytes_per_second)
    file_size = os.path.getsize(mp3_path)
    start_byte = start_byte % file_size
    current_file = open(mp3_path, 'rb')
    current_file.seek(start_byte)
    
    last_live_check = time.time()
    live_check_interval = 5.0 
    
    try:
        while True:
            current_time = time.time()
            if current_time - last_live_check >= live_check_interval:
                live_link = get_monotonic_live_link()
                if live_link:
                    current_file.close()
                    yield from stream_live_audio(live_link)
                    return
                last_live_check = current_time
            
            chunk = current_file.read(1024)
            if not chunk:
                current_file.close()
                current_video, id, mp3_path, video_elapsed = get_current_video()
                current_file = open(mp3_path, 'rb')
                current_file.seek(0)
                chunk = current_file.read(1024)
                if not chunk:
                    break
            
            yield chunk
            time.sleep(1024/bytes_per_second/10)
    finally:
        current_file.close()


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
    return redirect("http://www.monotonic.studio/live", code=302)

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
    app.run(debug=True, port=888)
