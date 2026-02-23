import os
import time
import json
import boto3
import string
import random
import requests
import subprocess
import threading
import queue
import logging
import collections 
from datetime import datetime
from flask import Flask, request, Response, redirect, render_template, session as flask_session
from flask_cors import CORS
from werkzeug.utils import secure_filename

# ============================================================================
# CONFIGURATION & SETUP
# ============================================================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__, template_folder='templates', static_folder='assets')
app.secret_key = os.environ.get('SECRET_KEY', 'orange-trench')
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB
CORS(app)

try:
    ARCHIVE_PATH = '/var/lib/mtr/archives'
    os.makedirs(ARCHIVE_PATH, exist_ok=True)
except:
    ARCHIVE_PATH = 'archives'

ALLOWED_EXTENSIONS = {'mp3', 'png', 'jpg', 'jpeg', 'gif', 'webp'}
LIVE_STREAM_URL = "http://monotonicradio.com:8000/stream.m3u"
LIVE_STATUS_URL = "http://monotonicradio.com:8000/status-json.xsl"
BEGINNING_TIME = datetime(year=2025, month=3, day=20, hour=6)

try: 
    with open('config.json', 'r') as f:
        config = json.load(f)
except:
    config = {
        'AWS_ID':os.environ['AWS_ID'],
        'AWS_P':os.environ['AWS_P']
    }

# Load archives data from individual JSON files

def download_from_bucket(archive_id, max_retries=3):
    """Download MP3 file from CDN with retry logic"""
    filepath = f'{ARCHIVE_PATH}/{archive_id}'
    if os.path.exists(filepath):
        logger.info(f"File {archive_id} already exists, skipping download")
        return True

    url = f"https://scudbucket.sfo3.cdn.digitaloceanspaces.com/monotonic-radio/{archive_id}"
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                f.write(response.content)
            logger.info(f"Successfully downloaded {archive_id}")
            return True
            
        except requests.RequestException as e:
            logger.error(f"Attempt {attempt + 1} failed to download {archive_id}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
    
    return False


def upload_to_bucket(file_path, filename):    
    session = boto3.session.Session()
    client = session.client('s3',
                          region_name='sfo3',
                          endpoint_url='https://sfo3.digitaloceanspaces.com',
                          aws_access_key_id=config['AWS_ID'],
                          aws_secret_access_key=config['AWS_P'])
    
    object_key = f'monotonic-radio/{filename}'
    
    try:
        client.upload_file(file_path, 'scudbucket', object_key,
                          ExtraArgs={'StorageClass': 'STANDARD','ACL':'public-read'})
        print(f"Successfully uploaded {object_key}")
        return True
    except Exception as e:
        print(f"Error uploading: {e}")
        return False

archive_dict = {}
missing_files = []
archives = []
total_duration = 0
def refresh_archive_dict():
    archive_data = os.listdir('data')
    global archive_dict, missing_files, archives, total_duration
    for archive_file in archive_data:
        if archive_file.endswith('.json'):
            with open(f'data/{archive_file}', 'r') as f:
                data = json.load(f)
                data['genre_string'] = ', '.join(data['genres'])
                archive_id = data['id']
                data['download'] = 'https://scudbucket.sfo3.cdn.digitaloceanspaces.com/monotonic-radio/' + data['filename']
                logger.info(f"{ARCHIVE_PATH}/{data['filename']}")
                if os.path.exists(f"{ARCHIVE_PATH}/{data['filename']}"):
                    archive_dict[archive_id] = data
                else:
                    download_from_bucket(data['filename'])
                    archive_dict[archive_id] = data
    logger.warning(f'MISSING {len(missing_files)} FILES')
    for i in missing_files:
        logger.warning(f'   -{i}')

    archives = sorted(archive_dict.keys())
    total_duration = sum(v['duration'] for v in archive_dict.values())
refresh_archive_dict()

# Make users
users = {
    os.environ.get('ADMIN_PASS', 'test'): {
        'shows':['a','c','r'],
    },
    os.environ.get('AB_PASS', 'testmiles'): {
        'shows':['a']
    },
    os.environ.get('RFN_PASS', 'testflynn'): {
        'shows':['r']
    }
}

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def cleanup_process(process):
    """Safely terminate and clean up a subprocess"""
    try:
        process.terminate()
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            logger.warning("Process didn't terminate, killing it")
            process.kill()
            process.wait()
    except Exception as e:
        logger.error(f"Error cleaning up process: {e}")


def get_thumbnail(archive_id):
    """Get thumbnail path for an archive, with fallback"""
    thumbnail = archive_dict[archive_id]['thumbnail']
    
    if 'assets/thumbnail' not in thumbnail:
        local_path = f'assets/thumbnails/{archive_id}.webp'
        if os.path.exists(local_path):
            return local_path
        
        # Check if remote thumbnail exists
        try:
            response = requests.head(thumbnail, timeout=0.5)
            if response.status_code == 404:
                return 'assets/mtr.jpg'
        except (requests.RequestException, requests.Timeout):
            return 'assets/mtr.jpg'
    
    return thumbnail


def check_for_live():
    """Check if live stream is currently active"""
    try:
        resp = requests.get(LIVE_STATUS_URL, timeout=2).json()
        
        source = resp.get('icestats', {}).get('source')
        if not source:
            return None
        
        yt_link = source.get('server_url')
        if yt_link:
            yt_link = yt_link.replace('/watch', '/embed')
        
        genres = source.get('genre', '')
        genres_list = [g.strip() for g in genres.split(',')]
        
        return {
            'genres': genres_list,
            'yt_link': yt_link,
            'name': source.get('server_name'),
            'description': source.get('server_description')
        }
    except Exception as e:
        logger.error(f"Error checking live stream: {e}")
        return None


def get_mp3_metadata(filepath):
    """Extract duration and bitrate from MP3 file"""
    cmd = [
        'ffprobe', '-v', 'quiet', '-print_format', 'json',
        '-show_streams', filepath
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    data = json.loads(result.stdout)
    
    stream = data['streams'][0]
    bitrate = float(stream.get('bit_rate', stream.get('sample_rate', 128000)))
    duration = float(stream['duration'])
    
    return bitrate, duration


def save_new_archive(archive_data):
    """Save new archive to data directory and reload archives"""
    global archive_dict, archives, total_duration
    
    archive_id = archive_data['id']
    filepath = f'data/{archive_id}.json'
    
    # Save to file
    with open(filepath, 'w') as f:
        json.dump(archive_data, f, indent=4)
    
    # Update in-memory data
    archive_dict[archive_id] = archive_data
    archives = sorted(archive_dict.keys())
    total_duration = sum(v['duration'] for v in archive_dict.values())
    
    logger.info(f"Added new archive: {archive_id} (total: {len(archive_dict)})")
    return True

# ============================================================================
# PLAYLIST & PLAYBACK LOGIC
# ============================================================================

def get_current():
    """Get currently playing track based on elapsed time - deterministic calculation"""
    current_time = datetime.now()
    elapsed_seconds = (current_time - BEGINNING_TIME).total_seconds()
    
    # Calculate which iteration we're in
    iteration = int(elapsed_seconds // total_duration)
    time_into_iteration = elapsed_seconds % total_duration
    
    # Generate shuffled order for this iteration
    shuffled = archives.copy()
    random.Random(iteration).shuffle(shuffled)
    
    # Avoid back-to-back repeats across iterations
    if iteration > 0:
        prev_shuffled = archives.copy()
        random.Random(iteration - 1).shuffle(prev_shuffled)
        
        attempt = 0
        while shuffled[0] == prev_shuffled[-1] and attempt < 100:
            random.Random(iteration * 1000 + attempt).shuffle(shuffled)
            attempt += 1
    
    # Find which track we're currently in
    time_sum = 0
    for i, archive_id in enumerate(shuffled):
        v = archive_dict[archive_id]
        
        if time_sum + v['duration'] > time_into_iteration:
            # Found the current track
            archive_elapsed = time_into_iteration - time_sum
            byterate = v['bitrate'] / 8
            mp3_path = ARCHIVE_PATH + '/' + v['filename']
            
            return v['title'], archive_id, mp3_path, archive_elapsed, byterate, v['duration']
        
        time_sum += v['duration']
    
    logger.warning("Reached end of iteration without finding track")
    return None

# ============================================================================
# STREAMING LOGIC
# ============================================================================

def stream_live(live_info, chunk_size, chunks_between_checks):
    """Stream live content using mpv"""
    logger.info(f"Switching to live stream: {live_info.get('name')}")
    
    mpv_command = [
        "mpv",
        "--no-video",
        "--no-terminal",
        "--o=-",
        "--of=mp3",
        "--oac=libmp3lame",
        "--oacopts=b=128k",
        LIVE_STREAM_URL
    ]
    
    process = subprocess.Popen(
        mpv_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=chunk_size
    )
    
    try:
        chunk_count = 0
        while True:
            chunk_count += 1
            
            if chunk_count % chunks_between_checks == 0:
                if not check_for_live():
                    logger.info("Live stream ended, switching back to playlist")
                    break
            
            chunk = process.stdout.read(chunk_size)
            if not chunk:
                logger.warning("Live stream ended unexpectedly")
                break
            
            yield chunk
    finally:
        cleanup_process(process)

def stream_playlist(chunk_size, chunks_between_checks, skip_track_id=None):
    """Stream archived content using ffmpeg"""
    current_result = get_current()
    if not current_result:
        logger.error("get_current() returned None")
        time.sleep(1)
        return

    current_video, track_id, mp3_path, video_elapsed, bitrate, duration = current_result

    if track_id == skip_track_id:
        logger.info(f"Still on finished track {track_id}, waiting for next...")
        time.sleep(2)
        current_result = get_current()
        if not current_result:
            return
        current_video, track_id, mp3_path, video_elapsed, bitrate, duration = current_result
    
    if not os.path.exists(mp3_path):
        logger.warning(f"File not found: {mp3_path}")
        time.sleep(0.5)
        return
    
    logger.info(f"Audio {track_id}: starting from {video_elapsed:.1f}s")
    
    cmd = [
        'ffmpeg',
        '-re',
        '-ss', str(video_elapsed) if video_elapsed > 0 else '0', 
        '-i', mp3_path,
        '-f', 'mp3',
        '-b:a', '128k',
        '-ar', '44100',
        '-'
    ]
    
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=chunk_size
    )
    
    try:
        chunk_count = 0
        bytes_read = 0
        while True:
            chunk_count += 1
            
            if chunk_count % chunks_between_checks == 0:
                # Check for live stream 
                if check_for_live():
                    logger.info("Live stream detected, switching")
                    break
                
                # Check if track changed
                new_result = get_current()
                if new_result:
                    new_video, new_id, _, _, _, _ = new_result
                    if new_id != track_id:
                        logger.info(f"Track switch: {track_id} -> {new_id}")
                        break
            
            chunk = process.stdout.read(chunk_size)
            if not chunk:
                # Log why it ended
                stderr_output = process.stderr.read().decode('utf-8', errors='ignore')
                logger.info(f"End of track {track_id} - read {bytes_read} bytes")
                if stderr_output:
                    logger.error(f"FFmpeg stderr: {stderr_output[-500:]}")  # Last 500 chars
                break
            
            bytes_read += len(chunk)
            yield chunk
    finally:
        cleanup_process(process)

CHUNK_SIZE = 8192
CHUNKS_BETWEEN_CHECKS = 25
BUFFER_SECONDS = 4
def stream_simple():
    first_open = True
    track_over = False
    need_to_switch_to_archive = False

    while True:

        if check_for_live():
            logger.info("Live stream detected, switching")  
            need_to_switch_to_archive = True # flag so the next block knows to reopen the file
            
            mpv_command = [
                "mpv",
                "--no-video",
                "--no-terminal",
                "--o=-",
                "--of=mp3",
                "--oac=libmp3lame",
                "--oacopts=b=128k",
                LIVE_STREAM_URL
            ]
            
            process = subprocess.Popen(
                mpv_command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=CHUNK_SIZE
            )
            
            chunk_count = 0
            while True:
                chunk_count += 1
                
                if chunk_count % CHUNKS_BETWEEN_CHECKS == 0:
                    if not check_for_live():
                        logger.info("Live stream ended, switching back to playlist")
                        break
                
                chunk = process.stdout.read(CHUNK_SIZE)
                if not chunk:
                    logger.warning("Live stream ended unexpectedly")
                    break
                
                yield chunk
            
            break

        else:
            current, track_id, mp3_path, elapsed, byterate, duration = get_current()
            start_chunk = round(elapsed * byterate)
            
            logger.info(current)
            logger.info(f'START CHUNK: {start_chunk}')
            logger.info(f'elapsed: {elapsed}')
            logger.info(f'duration: {duration}')
            logger.info(f'total chunks: {round(duration * byterate)}')
            
            last_check_for_current = time.time()

            if first_open or track_over or need_to_switch_to_archive:

                need_to_switch_to_archive = False
                with open(mp3_path, 'rb') as f:
                    f.seek(start_chunk)
                    
                    # pre-yield a buffer burst to fill client's buffer
                    prebuffer = f.read(int(byterate * BUFFER_SECONDS))
                    yield prebuffer
                    
                    # now pace the rest at real playback speed
                    live_detected = False
                    while chunk := f.read(1024):
                        yield chunk
                        time.sleep(1024 / byterate)

                        if (time.time() - last_check_for_current >= 5):
                            _, _, _, elapsed_check, _, _ = get_current()
                            last_check_for_current = time.time()
                            if (duration - elapsed_check) <= 1:
                                track_over = True
                                break
                            if check_for_live():
                                live_detected = True
                                break
                    
                    if live_detected:
                        break
                        
        time.sleep(3)

class StreamBroadcaster:
    def __init__(self):
        self.clients = set()
        self.lock = threading.Lock()
        self.buffer = collections.deque(maxlen=10)
    
    def _generate_master_stream(self):
        """The ONE stream that feeds everyone"""
        CHUNK_SIZE = 8192
        CHUNKS_BETWEEN_CHECKS = 25
        last_track_id = None
        
        while True:
            try:
                live_info = check_for_live()
                if live_info:
                    stream_generator = stream_live(live_info, CHUNK_SIZE, CHUNKS_BETWEEN_CHECKS)
                    logger.info('Switching to Live')
                else:
                    stream_generator = stream_simple()
                    logger.info('Switching to Archive')

                for chunk in stream_generator:
                    self.buffer.append(chunk)
                    with self.lock:
                        dead_clients = set()
                        for client_queue in self.clients:
                            try:
                                client_queue.put_nowait(chunk)
                            except:
                                dead_clients.add(client_queue)
                        self.clients -= dead_clients

                result = get_current()
                if result:
                    last_track_id = result[1] 

            except Exception as e:
                logger.error(f"Broadcast error: {e}", exc_info=True)
                time.sleep(1)
    
    def start(self):
        """Start broadcasting in background thread"""
        thread = threading.Thread(target=self._generate_master_stream, daemon=True)
        thread.start()
    
    def add_client(self):
        client_queue = queue.Queue(maxsize=100)
        with self.lock:
            for chunk in self.buffer:          # seed with recent audio
                client_queue.put_nowait(chunk)
            self.clients.add(client_queue)
        return client_queue
    
    def remove_client(self, client_queue):
        """Client disconnected"""
        with self.lock:
            self.clients.discard(client_queue)


# ============================================================================
# FLASK ROUTES
# ============================================================================

@app.route('/')
def index():
    """Main page showing current track"""
    result = get_current()
    if not result:
        return "Stream not ready", 503
    
    current, archive_id, mp3_path, video_elapsed, byterate, duration = result
    genres = ', '.join(archive_dict[archive_id]['genres'])
    description = archive_dict[archive_id]['description'].replace('\n', '<br>')
    
    all_episodes = sorted(archive_dict.values(), key=lambda d: d['date'], reverse=True)
    pages = (len(all_episodes) // 9) + 1

    return render_template(
        'index.html',
        now_playing=current,
        genres=genres,
        description=description,
        thumbnail=get_thumbnail(archive_id),
        episodes=all_episodes,
        all_episodes=all_episodes,
        pages = pages
    )


@app.route('/stream')
def stream():
   
   '''
    client_queue = broadcaster.add_client()
    
    def generate():
        try:
            while True:
                chunk = client_queue.get(timeout=30)
                yield chunk
        except:
            pass
        finally:
            broadcaster.remove_client(client_queue)'''
   return Response(
        stream_simple(),
        mimetype='audio/mpeg',
        headers={
            'Cache-Control': 'no-cache, no-store, must-revalidate',
            'Pragma': 'no-cache',
            'Expires': '0',
            'X-Accel-Buffering': 'no'
        }
    )

@app.route('/info')
def get_info():
    """API endpoint for current track info"""
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
            'source': 'live'
        }
    
    result = get_current()
    if not result:
        return {'error': 'Stream not ready'}, 503
    
    current, archive_id, mp3_path, video_elapsed, byterate, duration = result
    
    return {
        'now_playing': current,
        'video_description': archive_dict[archive_id]['description'],
        'duration': archive_dict[archive_id]['duration'],
        'genres': archive_dict[archive_id]['genres'],
        'elapsed': round(video_elapsed),
        'byterate': byterate,
        'thumbnail': get_thumbnail(archive_id),
        'id':archive_id,
        'download':f'https://scudbucket.sfo3.cdn.digitaloceanspaces.com/monotonic-radio/{mp3_path.split('/')[-1]}',
        'source': 'archive'
    }


@app.route('/login', methods=['GET', 'POST'])
def login():
    """Admin login page"""
    if request.method == 'POST':
        password = request.form.get('kundalini')
        
        if password in list(users.keys()):
            flask_session['authenticated'] = True
            flask_session['user_shows'] = users[password]['shows']
            logger.warning(flask_session.get('user_shows'))
            page = request.args.get('page', 'upload')
            return redirect(f'/{page}')
        else:
            return render_template('login.html', error='Invalid password')
    
    return render_template('login.html')

def get_user_episodes(user_shows):
    user_episodes = []
    for id, val in archive_dict.items():
        if val['show'] in user_shows:
            val['genre_string'] = ', '.join(val['genres'])
            user_episodes.append(val)    
    user_episodes = sorted(user_episodes, key=lambda d: d['date'], reverse=True)
    return user_episodes

@app.route('/upload', methods=['GET', 'POST'])
def upload():
    """Admin upload page for new archives"""
    if not flask_session.get('authenticated'):
        return redirect('/login?page=upload')
    
    user_shows = flask_session.get('user_shows', [])
    user_episodes = get_user_episodes(user_shows)

    episode_to_edit = request.args.get('episode')
    if request.method == 'GET':
        if episode_to_edit:
            editing = archive_dict[episode_to_edit]
            if 'date' not in editing.keys():
                editing['date'] = ''
            return render_template('upload.html', shows=user_shows, episodes=user_episodes, editing=editing)
        else:
            return render_template('upload.html', shows=user_shows, episodes=user_episodes)
    
    # Handle POST
    show = request.form.get('show')
    title = request.form.get('title')
    tracklist = request.form.get('tracklist')
    genres = request.form.get('genres')
    mp3_file = request.files.get('mp3')
    thumbnail_file = request.files.get('thumbnail')
    show_date = request.form.get('date')

    editing_id = request.form.get('id')
    
    # Validate user has permission for this show
    if show not in user_shows:
        return render_template('upload.html', shows=user_shows, error='You do not have permission to upload to this show', episodes=user_episodes)
    
    # Validate all fields
    if not all([show, title, tracklist, genres, mp3_file, thumbnail_file, show_date]):
        if not editing_id:
            return render_template('upload.html', shows=user_shows, error='All fields are required', episodes=user_episodes)
    
    # Validate and save MP3
    if not allowed_file(mp3_file.filename) and not editing_id:
        return render_template('upload.html', shows=user_shows, error='Invalid MP3 file', episodes=user_episodes)
    
    if mp3_file:
        mp3_filename = secure_filename(mp3_file.filename)
        mp3_path = os.path.join(ARCHIVE_PATH, mp3_filename)
        mp3_file.save(mp3_path)
        upload_to_bucket(mp3_path, mp3_filename)
        
        # Extract metadata
        try:
            bitrate, duration = get_mp3_metadata(mp3_path)
        except Exception as e:
            logger.error(f"Error extracting MP3 metadata: {e}")
            return render_template('upload.html', shows=user_shows, error='Failed to read MP3 metadata', episodes=user_episodes)
    else:
        mp3_path = archive_dict[editing_id]['filepath']
        mp3_filename = archive_dict[editing_id]['filename']
        duration = archive_dict[editing_id]['duration']
        bitrate = archive_dict[editing_id]['bitrate']
    
    if thumbnail_file:
        # Validate and save thumbnail
        if not allowed_file(thumbnail_file.filename):
            return render_template('upload.html', error='Invalid thumbnail file', episodes=user_episodes)
        
        thumb_filename = secure_filename(thumbnail_file.filename)
        thumb_path = os.path.join('assets', 'thumbnails', thumb_filename)
        thumbnail_file.save(thumb_path)
    else:
        thumb_path = archive_dict[editing_id]['thumbnail']
    
    # Process genres
    genres_list = [g.strip() for g in genres.split(',')]
    
    # Create archive entry
    id = editing_id or ''.join(random.choices(string.ascii_letters + string.digits, k=16))
    archive_data = {
        'id': id,
        'title': title,
        'genres': genres_list,
        'description': tracklist,
        'show': show,
        'bitrate': bitrate,
        'duration': duration,
        'thumbnail': thumb_path,
        'filepath': mp3_path,
        'filename': mp3_filename,
        'date': show_date
    }
    filename = f"{id}.json"
    with open(f'data/{filename}', 'w') as f:
        json.dump(archive_data, f)

    refresh_archive_dict()
    user_episodes = get_user_episodes(user_shows)
    
    logger.info(f"New upload: {title} ({duration}s, {bitrate} bps)")
    
    if episode_to_edit:
        editing = archive_dict[episode_to_edit]
        return render_template('upload.html', shows=user_shows, episodes=user_episodes, editing=editing, success="Updated successfully!")
    else:
        return render_template('upload.html', shows=user_shows, episodes=user_episodes, success="Uploaded successfully!")

# ============================================================================
# STARTUP
# ============================================================================

# Warm up get_current
get_current()

if __name__ == '__main__':
    app.run(debug=True, port=8888, threaded=True)
