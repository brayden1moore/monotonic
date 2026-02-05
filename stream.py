import os
import time
import json
import random
import requests
import subprocess
import threading
import queue
import logging
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

ARCHIVE_PATH = '/var/lib/mtr/archives'
os.makedirs(ARCHIVE_PATH, exist_ok=True)

ALLOWED_EXTENSIONS = {'mp3', 'png', 'jpg', 'jpeg', 'gif', 'webp'}
LIVE_STREAM_URL = "http://monotonicradio.com:8000/stream.m3u"
LIVE_STATUS_URL = "http://monotonicradio.com:8000/status-json.xsl"
BEGINNING_TIME = datetime(year=2025, month=3, day=20, hour=6)

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

archive_dict = {}
missing_files = []
archive_data = os.listdir('data')
for archive_file in archive_data:
    if archive_file.endswith('.json'):
        with open(f'data/{archive_file}', 'r') as f:
            data = json.load(f)
            archive_id = data['id']
            logger.info(f"{ARCHIVE_PATH}/{data['filename']}")
            if os.path.exists(f"{ARCHIVE_PATH}/{data['filename']}"):
                archive_dict[archive_id] = data
            else:
                download_from_bucket(data['filename'])
logger.warning(f'MISSING {len(missing_files)} FILES')
for i in missing_files:
    logger.warning(f'   -{i}')

# Create sorted list of archive IDs for deterministic ordering
archives = sorted(archive_dict.keys())

# Calculate total duration
total_duration = sum(v['duration'] for v in archive_dict.values())

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
            mp3_path = ARCHIVE_PATH + '/' + v['filepath']
            
            return v['title'], archive_id, mp3_path, archive_elapsed, byterate
        
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


def stream_playlist(chunk_size, chunks_between_checks):
    """Stream archived content using ffmpeg"""
    current_result = get_current()
    if not current_result:
        logger.error("get_current() returned None")
        time.sleep(1)
        return
    
    current_video, track_id, mp3_path, video_elapsed, bitrate = current_result
    
    if not os.path.exists(mp3_path):
        logger.warning(f"File not found: {mp3_path}")
        time.sleep(0.5)
        return
    
    logger.info(f"Audio {track_id}: starting from {video_elapsed:.1f}s")
    
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
        bufsize=chunk_size
    )
    
    try:
        chunk_count = 0
        while True:
            chunk_count += 1
            
            if chunk_count % chunks_between_checks == 0:
                # Check for live stream (higher priority)
                if check_for_live():
                    logger.info("Live stream detected, switching")
                    break
                
                # Check if track changed
                new_result = get_current()
                if new_result:
                    new_video, new_id, _, _, _ = new_result
                    if new_id != track_id:
                        logger.info(f"Track switch: {track_id} -> {new_id}")
                        break
            
            chunk = process.stdout.read(chunk_size)
            if not chunk:
                logger.info(f"End of track {track_id}")
                break
            
            yield chunk
    finally:
        cleanup_process(process)

class StreamBroadcaster:
    """Manages a single stream broadcast to multiple clients"""
    
    def __init__(self):
        self.clients = {}
        self.client_id_counter = 0
        self.lock = threading.Lock()
        self.broadcast_thread = None
        self.running = False
    
    def start_broadcast(self):
        """Start the broadcast thread if not already running"""
        if self.broadcast_thread and self.broadcast_thread.is_alive():
            return
        
        self.running = True
        self.broadcast_thread = threading.Thread(target=self._broadcast_loop, daemon=True)
        self.broadcast_thread.start()
        logger.info("Broadcast thread started")
    
    def _broadcast_loop(self):
        """Main broadcast loop - generates stream and distributes to clients"""
        CHUNK_SIZE = 8192
        CHUNKS_BETWEEN_CHECKS = 25
        LIVE_CHECK_INTERVAL = 3
        
        last_live_check = 0
        
        while self.running:
            with self.lock:
                if not self.clients:
                    time.sleep(0.1)
                    continue
            
            try:
                current_time = time.time()
                
                # Check for live stream periodically
                if current_time - last_live_check >= LIVE_CHECK_INTERVAL:
                    live_info = check_for_live()
                    last_live_check = current_time
                else:
                    live_info = None
                
                # Generate appropriate stream
                if live_info:
                    stream_generator = stream_live(live_info, CHUNK_SIZE, CHUNKS_BETWEEN_CHECKS)
                else:
                    stream_generator = stream_playlist(CHUNK_SIZE, CHUNKS_BETWEEN_CHECKS)
                
                # Broadcast chunks to all clients
                for chunk in stream_generator:
                    with self.lock:
                        dead_clients = []
                        for client_id, client_queue in self.clients.items():
                            try:
                                client_queue.put_nowait(chunk)
                            except queue.Full:
                                logger.warning(f"Client {client_id} queue full, disconnecting")
                                dead_clients.append(client_id)
                        
                        for client_id in dead_clients:
                            del self.clients[client_id]
            
            except Exception as e:
                logger.error(f"Broadcast error: {e}", exc_info=True)
                time.sleep(1)
    
    def add_client(self):
        """Register a new client"""
        with self.lock:
            client_id = self.client_id_counter
            self.client_id_counter += 1
            client_queue = queue.Queue(maxsize=100)
            self.clients[client_id] = client_queue
            
            logger.info(f"Client {client_id} connected. Total clients: {len(self.clients)}")
            return client_id, client_queue
    
    def remove_client(self, client_id):
        """Unregister a client"""
        with self.lock:
            if client_id in self.clients:
                del self.clients[client_id]
                logger.info(f"Client {client_id} disconnected. Total clients: {len(self.clients)}")


# Initialize broadcast
broadcaster = StreamBroadcaster()
broadcaster.start_broadcast()

# ============================================================================
# FLASK ROUTES
# ============================================================================

@app.route('/')
def index():
    """Main page showing current track"""
    result = get_current()
    if not result:
        return "Stream not ready", 503
    
    current, archive_id, mp3_path, video_elapsed, byterate = result
    genres = ', '.join(archive_dict[archive_id]['genres'])
    description = archive_dict[archive_id]['description'].replace('\n', '<br>')
    
    return render_template(
        'index.html',
        now_playing=current,
        genres=genres,
        description=description,
        thumbnail=get_thumbnail(archive_id)
    )

@app.route('/stream')
def stream():
    """Stream endpoint - broadcasts audio to clients"""
    client_id, client_queue = broadcaster.add_client()
    
    def generate():
        try:
            while True:
                try:
                    chunk = client_queue.get(timeout=10)
                    yield chunk
                except queue.Empty:
                    logger.warning(f"Client {client_id} timeout")
                    break
        finally:
            broadcaster.remove_client(client_id)
    
    return Response(
        generate(),
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
    
    current, archive_id, mp3_path, video_elapsed, byterate = result
    
    return {
        'now_playing': current,
        'video_description': archive_dict[archive_id]['description'],
        'duration': archive_dict[archive_id]['duration'],
        'genres': archive_dict[archive_id]['genres'],
        'elapsed': round(video_elapsed),
        'byterate': byterate,
        'thumbnail': get_thumbnail(archive_id),
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

@app.route('/upload', methods=['GET', 'POST'])
def upload():
    """Admin upload page for new archives"""
    if not flask_session.get('authenticated'):
        return redirect('/login?page=upload')
    
    user_shows = flask_session.get('user_shows', [])
    user_episodes = []
    for id, val in archive_dict.items():
        if val['show'] in user_shows:
            user_episodes.append(val)    

    if request.method == 'GET':
        return render_template('upload.html', shows=user_shows, episodes=user_episodes)
    
    # Handle POST
    show = request.form.get('show')
    title = request.form.get('title')
    tracklist = request.form.get('tracklist')
    genres = request.form.get('genres')
    mp3_file = request.files.get('mp3')
    thumbnail_file = request.files.get('thumbnail')
    
    # Validate user has permission for this show
    if show not in user_shows:
        return render_template('upload.html', shows=user_shows, error='You do not have permission to upload to this show', episodes=user_episodes)
    
    # Validate all fields
    if not all([show, title, tracklist, genres, mp3_file, thumbnail_file]):
        return render_template('upload.html', shows=user_shows, error='All fields are required', episodes=user_episodes)
    
    # Validate and save MP3
    if not allowed_file(mp3_file.filename):
        return render_template('upload.html', shows=user_shows, error='Invalid MP3 file', episodes=user_episodes)
    
    mp3_filename = secure_filename(mp3_file.filename)
    mp3_path = os.path.join('archives', mp3_filename)
    mp3_file.save(mp3_path)
    
    # Validate and save thumbnail
    if not allowed_file(thumbnail_file.filename):
        return render_template('upload.html', error='Invalid thumbnail file', episodes=user_episodes)
    
    thumb_filename = secure_filename(thumbnail_file.filename)
    thumb_path = os.path.join('assets', 'thumbnails', thumb_filename)
    thumbnail_file.save(thumb_path)
    
    # Extract metadata
    try:
        bitrate, duration = get_mp3_metadata(mp3_path)
    except Exception as e:
        logger.error(f"Error extracting MP3 metadata: {e}")
        return render_template('upload.html', shows=user_shows, error='Failed to read MP3 metadata', episodes=user_episodes)
    
    # Process genres
    genres_list = [g.strip() for g in genres.split(',')]
    
    # Create archive entry
    id = secure_filename(title)
    archive_data = {
        'id': id,
        'title': title,
        'genres': genres_list,
        'description': tracklist,
        'bitrate': bitrate,
        'duration': duration,
        'show': show,
        'thumbnail': thumb_path,
        'filepath': mp3_path
    }
    filename = f"{id}.json"
    with open(f'data/{filename}', 'w') as f:
        json.dump(archive_data, f)
    
    logger.info(f"New upload: {title} ({duration}s, {bitrate} bps)")
    
    return render_template('upload.html', shows=user_shows, success='Upload successful!', episodes=user_episodes)

# ============================================================================
# STARTUP
# ============================================================================

# Warm up get_current
get_current()

if __name__ == '__main__':
    app.run(debug=True, port=8888, threaded=True)
