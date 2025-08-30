#!/usr/bin/env python3
"""
Enhanced YouTube Data Collector & Video Rating Tool
Integrates Invidious API as primary source with YouTube API fallback
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import json
import time
import random
from typing import Dict, List, Optional, Tuple
import re
import requests
import numpy as np
from PIL import Image
import io

# Import with fallbacks for Streamlit Cloud compatibility
try:
    from streamlit_autorefresh import st_autorefresh
    AUTOREFRESH_AVAILABLE = True
except ImportError:
    AUTOREFRESH_AVAILABLE = False
    def st_autorefresh(interval=30000, key=None, limit=None, debounce=True):
        return 0

try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    YOUTUBE_API_AVAILABLE = True
except ImportError:
    YOUTUBE_API_AVAILABLE = False
    
try:
    import gspread
    from google.oauth2.service_account import Credentials
    SHEETS_AVAILABLE = True
except ImportError:
    SHEETS_AVAILABLE = False
    
try:
    import isodate
    ISODATE_AVAILABLE = True
except ImportError:
    ISODATE_AVAILABLE = False
    def parse_duration_simple(duration_str):
        import re
        match = re.search(r'PT(?:(\d+)M)?(?:(\d+)S)?', duration_str)
        if match:
            minutes = int(match.group(1) or 0)
            seconds = int(match.group(2) or 0)
            return minutes * 60 + seconds
        return 0

# Page config
st.set_page_config(
    page_title="Enhanced YouTube Collection & Rating Tool",
    page_icon="🎬",
    layout="wide"
)

# Enhanced CSS styling
st.markdown("""
<style>
    .main-header {
        text-align: center;
        padding: 2rem 0;
        margin-bottom: 2rem;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border-radius: 15px;
    }
    .instance-health {
        background: #2d3748;
        color: #e2e8f0;
        padding: 1rem;
        border-radius: 8px;
        margin: 0.5rem 0;
        border-left: 4px solid #4299e1;
    }
    .health-good { border-left-color: #48bb78; }
    .health-warning { border-left-color: #ed8936; }
    .health-error { border-left-color: #f56565; }
    .api-status {
        padding: 0.5rem 1rem;
        border-radius: 6px;
        margin: 0.25rem 0;
        font-size: 0.9rem;
    }
    .api-primary { background: #48bb78; color: white; }
    .api-fallback { background: #ed8936; color: white; }
    .api-failed { background: #f56565; color: white; }
</style>
""", unsafe_allow_html=True)

# Initialize session state
def init_session_state():
    defaults = {
        'collected_videos': [],
        'is_collecting': False,
        'is_rating': False,
        'is_batch_collecting': False,
        'collector_stats': {
            'checked': 0, 'found': 0, 'rejected': 0, 
            'api_calls_youtube': 0, 'api_calls_invidious': 0,
            'invidious_successes': 0, 'youtube_fallbacks': 0,
            'has_captions': 0, 'no_captions': 0
        },
        'rater_stats': {
            'rated': 0, 'moved_to_tobe': 0, 'rejected': 0, 
            'api_calls': 0
        },
        'logs': [],
        'used_queries': set(),
        'system_status': {'type': None, 'message': ''},
        'batch_progress': {'current': 0, 'total': 0, 'results': []},
        'invidious_instance_stats': {}
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

init_session_state()

# Check compatibility and show warnings
if not AUTOREFRESH_AVAILABLE:
    st.warning("streamlit-autorefresh not available. Auto-refresh features disabled.")

if not YOUTUBE_API_AVAILABLE:
    st.warning("Google API client not available. YouTube API features disabled.")
    
if not SHEETS_AVAILABLE:
    st.error("Google Sheets integration not available. Please check requirements.txt")
    st.stop()
    
if not ISODATE_AVAILABLE:
    st.warning("isodate not available. Using basic duration parsing.")

# Categories configuration
CATEGORIES = {
    'heartwarming': {
        'name': 'Heartwarming Content',
        'emoji': '❤️',
        'description': 'Genuine emotional moments that create positive feelings'
    },
    'funny': {
        'name': 'Funny Content', 
        'emoji': '😂',
        'description': 'Humorous content that entertains and amuses'
    },
    'traumatic': {
        'name': 'Traumatic Events',
        'emoji': '⚠️', 
        'description': 'Serious events with significant impact'
    }
}

# Status management functions
def show_status_alert():
    """Display system status alerts prominently"""
    if st.session_state.system_status['type']:
        if st.session_state.system_status['type'] == 'error':
            st.error(f"🚫 {st.session_state.system_status['message']}")
        elif st.session_state.system_status['type'] == 'warning':
            st.warning(f"⚠️ {st.session_state.system_status['message']}")
        elif st.session_state.system_status['type'] == 'info':
            st.info(f"ℹ️ {st.session_state.system_status['message']}")
        elif st.session_state.system_status['type'] == 'success':
            st.success(f"✅ {st.session_state.system_status['message']}")

def set_status(status_type: str, message: str):
    st.session_state.system_status = {'type': status_type, 'message': message}

def clear_status():
    st.session_state.system_status = {'type': None, 'message': ''}


class InvidiousCollector:
    """Enhanced Invidious API collector with robust error handling"""
    
    def __init__(self):
        # Official instances from docs.invidious.io
        self.instances = [
            'https://inv.nadeko.net',
            'https://yewtu.be',
            'https://invidious.nerdvpn.de',
            'https://invidious.f5.si',
        ]
        
        # Health tracking
        self.instance_health = {}
        self.current_instance_index = 0
        self.failed_instances = set()
        
        # Request configuration
        self.request_timeout = 10
        self.max_retries = 3
        self.retry_delay_base = 1
        self.last_request_time = 0
        self.min_request_interval = 0.5
        
        # Initialize health monitoring
        self._initialize_instance_health()
        
        # Enhanced search queries
        self.search_queries = {
            'heartwarming': [
                'soldier surprise homecoming', 'dog reunion owner', 'random acts kindness',
                'baby first time hearing', 'proposal reaction emotional', 'surprise gift reaction',
                'homeless man helped', 'teacher surprised students', 'reunion after years'
            ],
            'funny': [
                'unexpected moments caught', 'comedy sketches viral', 'hilarious reactions',
                'funny animals doing', 'epic fail video', 'instant karma funny',
                'comedy gold moments', 'prank goes wrong', 'funny kids saying'
            ],
            'traumatic': [
                'shocking moments caught', 'dramatic rescue operation', 'natural disaster footage',
                'intense police chase', 'survival story real', 'near death experience',
                'wildfire escape footage', 'building evacuation emergency', 'storm damage aftermath'
            ]
        }
    
    def _initialize_instance_health(self):
        """Initialize health tracking for all instances"""
        for instance in self.instances:
            self.instance_health[instance] = {
                'status': 'unknown',
                'last_check': None,
                'response_time': None,
                'consecutive_failures': 0,
                'last_success': None,
                'total_requests': 0,
                'successful_requests': 0,
                'last_error': None
            }
    
    def add_log(self, message: str, log_type: str = "INFO"):
        """Add detailed log entry"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] COLLECTOR {log_type}: {message}"
        st.session_state.logs.insert(0, log_entry)
        st.session_state.logs = st.session_state.logs[:100]
    
    def get_healthy_instance(self):
        """Get next healthy instance with circuit breaker logic"""
        attempts = 0
        max_attempts = len(self.instances) * 2
        
        while attempts < max_attempts:
            instance = self.instances[self.current_instance_index]
            health = self.instance_health[instance]
            
            if (instance not in self.failed_instances and 
                health['consecutive_failures'] < 3):
                return instance
            
            self.current_instance_index = (self.current_instance_index + 1) % len(self.instances)
            attempts += 1
        
        # All instances failing - return least failed
        best_instance = min(self.instances, 
                          key=lambda x: self.instance_health[x]['consecutive_failures'])
        return best_instance
    
    def _mark_instance_unhealthy(self, instance_url, error_msg):
        """Mark instance as unhealthy and update failure tracking"""
        health = self.instance_health[instance_url]
        health.update({
            'status': 'unhealthy',
            'last_check': datetime.now(),
            'consecutive_failures': health['consecutive_failures'] + 1,
            'last_error': error_msg
        })
        
        if health['consecutive_failures'] >= 3:
            self.failed_instances.add(instance_url)
    
    def check_instance_health(self, instance_url, timeout=5):
        """Check instance health using /api/v1/stats endpoint"""
        try:
            stats_url = f"{instance_url}/api/v1/stats"
            start_time = time.time()
            
            response = requests.get(stats_url, timeout=timeout, headers={
                'User-Agent': 'Mozilla/5.0 (compatible; InvidiousCollector/1.0)'
            })
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                try:
                    stats_data = response.json()
                    if isinstance(stats_data, dict) and 'version' in stats_data:
                        self.instance_health[instance_url].update({
                            'status': 'healthy',
                            'last_check': datetime.now(),
                            'response_time': response_time,
                            'consecutive_failures': 0,
                            'last_success': datetime.now(),
                            'last_error': None
                        })
                        self.failed_instances.discard(instance_url)
                        return True, stats_data
                    else:
                        self._mark_instance_unhealthy(instance_url, "Invalid stats response format")
                        return False, "Invalid stats response format"
                except json.JSONDecodeError:
                    self._mark_instance_unhealthy(instance_url, "Invalid JSON in stats response")
                    return False, "Invalid JSON in stats response"
            else:
                self._mark_instance_unhealthy(instance_url, f"HTTP {response.status_code}")
                return False, f"HTTP {response.status_code}"
                
        except Exception as e:
            self._mark_instance_unhealthy(instance_url, str(e))
            return False, str(e)
    
    def make_api_request(self, endpoint, params=None):
        """Make API request with comprehensive error handling"""
        if params is None:
            params = {}
        
        # Rate limiting
        current_time = time.time()
        if current_time - self.last_request_time < self.min_request_interval:
            time.sleep(self.min_request_interval - (current_time - self.last_request_time))
        
        for attempt in range(self.max_retries):
            instance = self.get_healthy_instance()
            url = f"{instance}{endpoint}"
            
            try:
                self.instance_health[instance]['total_requests'] += 1
                self.last_request_time = time.time()
                st.session_state.collector_stats['api_calls_invidious'] += 1
                
                response = requests.get(url, params=params, timeout=self.request_timeout, 
                                      headers={'User-Agent': 'Mozilla/5.0 (compatible; InvidiousCollector/1.0)'})
                
                if response.status_code == 200:
                    try:
                        json_data = response.json()
                        
                        if isinstance(json_data, (dict, list)) and json_data is not None:
                            self.instance_health[instance]['successful_requests'] += 1
                            self.instance_health[instance]['consecutive_failures'] = 0
                            self.failed_instances.discard(instance)
                            st.session_state.collector_stats['invidious_successes'] += 1
                            return json_data, None
                        else:
                            self._mark_instance_unhealthy(instance, "Empty or invalid response data")
                            continue
                            
                    except json.JSONDecodeError as e:
                        self._mark_instance_unhealthy(instance, f"Invalid JSON response: {str(e)}")
                        continue
                
                elif response.status_code == 500:
                    self._mark_instance_unhealthy(instance, f"Server error: {response.status_code}")
                    continue
                    
                elif response.status_code == 429:
                    time.sleep(2 ** attempt)
                    continue
                    
                else:
                    self._mark_instance_unhealthy(instance, f"HTTP {response.status_code}")
                    continue
                    
            except requests.RequestException as e:
                self._mark_instance_unhealthy(instance, str(e))
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay_base * (2 ** attempt))
                continue
        
        return None, "All Invidious instances failed"
    
    def search_videos(self, query, max_results=25):
        """Search videos using Invidious API"""
        params = {
            'q': query,
            'type': 'video',
            'sort_by': 'relevance',
            'max_results': max_results
        }
        
        data, error = self.make_api_request("/api/v1/search", params)
        if error:
            return []
        
        if isinstance(data, list):
            valid_results = []
            for item in data:
                if isinstance(item, dict) and item.get('videoId'):
                    valid_results.append(item)
            return valid_results
        elif isinstance(data, dict) and 'items' in data:
            return data.get('items', [])
        else:
            return []
    
    def fetch_video_metadata(self, video_id):
        """Fetch video metadata with format validation"""
        data, error = self.make_api_request(f"/api/v1/videos/{video_id}")
        
        if error:
            return None, error
            
        if not isinstance(data, dict):
            return None, "Invalid metadata format"
            
        required_fields = ['videoId', 'title']
        missing_fields = [field for field in required_fields if not data.get(field)]
        
        if missing_fields:
            return None, f"Missing required fields: {', '.join(missing_fields)}"
        
        return data, None
    
    def validate_all_instances(self):
        """Validate all Invidious instances before starting collection"""
        healthy_instances = 0
        for instance in self.instances:
            is_healthy, result = self.check_instance_health(instance)
            if is_healthy:
                healthy_instances += 1
                self.add_log(f"Instance {instance.replace('https://', '')} is healthy", "SUCCESS")
            else:
                self.add_log(f"Instance {instance.replace('https://', '')} failed: {result}", "WARNING")
        
        if healthy_instances == 0:
            return False, "No healthy Invidious instances available"
        
        return True, f"{healthy_instances}/{len(self.instances)} instances healthy"
    
    def test_search_capability(self, test_query="test"):
        """Test search functionality on healthy instances"""
        for attempt in range(3):
            results = self.search_videos(test_query, max_results=1)
            if results and len(results) > 0:
                return True, "Search functionality working"
            time.sleep(1)
        
        return False, "Search functionality not working on any instance"
    
    def get_instance_stats(self):
        """Get comprehensive instance health statistics"""
        stats = {}
        for instance, health in self.instance_health.items():
            success_rate = 0
            if health['total_requests'] > 0:
                success_rate = health['successful_requests'] / health['total_requests']
            
            status_class = 'health-good'
            if health['consecutive_failures'] > 0:
                status_class = 'health-warning'
            if health['consecutive_failures'] >= 3:
                status_class = 'health-error'
            
            stats[instance] = {
                'status': health['status'],
                'success_rate': success_rate,
                'consecutive_failures': health['consecutive_failures'],
                'response_time': health.get('response_time', 'N/A'),
                'last_check': health.get('last_check'),
                'last_error': health.get('last_error', 'None'),
                'status_class': status_class
            }
        
        return stats


class RateLimitedSheetsExporter:
    """Google Sheets exporter with rate limiting"""
    
    def __init__(self, credentials_dict: Dict):
        self.creds = Credentials.from_service_account_info(
            credentials_dict,
            scopes=['https://www.googleapis.com/auth/spreadsheets',
                   'https://www.googleapis.com/auth/drive']
        )
        self.client = gspread.authorize(self.creds)
        self.request_count = 0
        self.last_request_time = 0
        self.requests_per_minute_limit = 200
    
    def _rate_limit_sheets_request(self):
        """Rate limit Google Sheets requests"""
        current_time = time.time()
        
        if current_time - self.last_request_time > 60:
            self.request_count = 0
            self.last_request_time = current_time
        
        if self.request_count >= self.requests_per_minute_limit:
            wait_time = 60 - (current_time - self.last_request_time)
            if wait_time > 0:
                time.sleep(wait_time)
                self.request_count = 0
                self.last_request_time = time.time()
        
        self.request_count += 1
    
    def get_spreadsheet_by_id(self, spreadsheet_id: str):
        self._rate_limit_sheets_request()
        return self.client.open_by_key(spreadsheet_id)
    
    def export_to_sheets_enhanced(self, videos: List[Dict], spreadsheet_id: str = None):
        """Export videos with enhanced metadata to raw_links sheet"""
        try:
            if not videos:
                return None
            
            spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            worksheet_name = "raw_links"
            
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
            except gspread.exceptions.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=30)
            
            # Enhanced headers for additional metadata
            enhanced_headers = [
                'video_id', 'title', 'url', 'category', 'search_query',
                'duration_seconds', 'view_count', 'like_count', 'comment_count',
                'published_at', 'channel_title', 'tags', 'collected_at',
                'full_description', 'collection_source', 'collection_instance_used'
            ]
            
            existing_data = worksheet.get_all_values()
            
            if not existing_data or len(existing_data) <= 1:
                worksheet.clear()
                self._rate_limit_sheets_request()
                worksheet.append_row(enhanced_headers)
            
            # Add videos with rate limiting
            for video in videos:
                enhanced_row = self._prepare_enhanced_row(video, enhanced_headers)
                self._rate_limit_sheets_request()
                worksheet.append_row(enhanced_row)
            
            return spreadsheet.url
            
        except Exception as e:
            st.error(f"Sheets export error: {str(e)}")
            return None
    
    def _prepare_enhanced_row(self, video: Dict, headers: List[str]) -> List[str]:
        """Prepare enhanced row with all metadata fields"""
        row = []
        for header in headers:
            value = video.get(header, '')
            
            if header == 'tags' and isinstance(value, list):
                value = ','.join(value)
            elif isinstance(value, (list, dict)):
                value = json.dumps(value)
            
            row.append(str(value) if value else '')
        
        return row


class SimpleVideoCollector:
    """Simplified video collector focused on working functionality"""
    
    def __init__(self, youtube_api_key: str = None, sheets_exporter=None):
        self.invidious_collector = InvidiousCollector()
        self.youtube_api_key = youtube_api_key
        self.sheets_exporter = sheets_exporter
        self.existing_sheet_ids = set()
        self.discarded_urls = set()
    
    def add_log(self, message: str, log_type: str = "INFO"):
        """Add log entry"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] COLLECTOR {log_type}: {message}"
        st.session_state.logs.insert(0, log_entry)
        st.session_state.logs = st.session_state.logs[:100]
    
    def validate_video_simple(self, video_data: Dict, target_category: str) -> Tuple[bool, str]:
        """Simple video validation"""
        if not isinstance(video_data, dict):
            return False, f"Invalid video data format: expected dict, got {type(video_data)}"
        
        video_id = video_data.get('videoId')
        title = video_data.get('title', '')
        
        if not video_id:
            return False, "No video ID found"
        
        if not isinstance(title, str):
            return False, "Invalid title format"
        
        # Duration check
        duration_seconds = 0
        duration_raw = video_data.get('lengthSeconds', 0)
        
        try:
            if isinstance(duration_raw, (int, float)):
                duration_seconds = int(duration_raw)
            elif isinstance(duration_raw, str) and duration_raw.isdigit():
                duration_seconds = int(duration_raw)
            else:
                return False, f"Invalid duration format: {duration_raw}"
        except (ValueError, TypeError):
            return False, "Could not parse duration"
        
        if duration_seconds < 90 or duration_seconds > 600:
            return False, f"Duration out of range: {duration_seconds}s (need 90-600s)"
        
        # View count check
        try:
            view_count_raw = video_data.get('viewCount', 0)
            if isinstance(view_count_raw, (int, float)):
                view_count = int(view_count_raw)
            elif isinstance(view_count_raw, str):
                view_count = int(view_count_raw.replace(',', '').replace(' ', ''))
            else:
                return False, f"Invalid view count format: {view_count_raw}"
            
            if view_count < 10000:
                return False, f"View count too low: {view_count:,}"
        except (ValueError, AttributeError):
            return False, "Could not parse view count"
        
        # Category check
        title_lower = title.lower()
        category_keywords = {
            'heartwarming': ['heartwarming', 'touching', 'emotional', 'reunion', 'surprise'],
            'funny': ['funny', 'comedy', 'humor', 'hilarious', 'laugh'],
            'traumatic': ['accident', 'disaster', 'emergency', 'rescue', 'shocking']
        }
        
        keywords = category_keywords.get(target_category, [])
        if not any(kw in title_lower for kw in keywords):
            return False, f"No {target_category} keywords in title"
        
        return True, "Valid"
    
    def collect_videos_simple(self, target_count: int, category: str, progress_callback=None):
        """Simple video collection"""
        collected = []
        
        # Pre-validate instances
        instance_check, instance_msg = self.invidious_collector.validate_all_instances()
        if not instance_check:
            self.add_log(f"Instance validation failed: {instance_msg}", "ERROR")
            return []
        
        self.add_log(f"Starting collection: {target_count} videos, category: {category}", "INFO")
        
        attempts = 0
        max_attempts = 50
        videos_checked = set()
        
        while len(collected) < target_count and attempts < max_attempts:
            query = random.choice(self.invidious_collector.search_queries[category])
            self.add_log(f"Searching '{category}': {query}", "INFO")
            
            search_results = self.invidious_collector.search_videos(query, max_results=20)
            
            if not search_results:
                attempts += 1
                continue
            
            for item in search_results:
                if len(collected) >= target_count:
                    break
                
                video_id = item.get('videoId')
                if not video_id or video_id in videos_checked:
                    continue
                
                videos_checked.add(video_id)
                st.session_state.collector_stats['checked'] += 1
                
                # Get detailed metadata
                metadata, error = self.invidious_collector.fetch_video_metadata(video_id)
                if error or not metadata:
                    continue
                
                # Validate
                is_valid, reason = self.validate_video_simple(metadata, category)
                
                if is_valid:
                    video_record = {
                        'video_id': video_id,
                        'title': str(metadata.get('title', '')),
                        'url': f"https://youtube.com/watch?v={video_id}",
                        'category': category,
                        'search_query': query,
                        'duration_seconds': int(metadata.get('lengthSeconds', 0)),
                        'view_count': int(metadata.get('viewCount', 0)),
                        'like_count': int(metadata.get('likeCount', 0)),
                        'comment_count': int(metadata.get('commentCount', 0)),
                        'published_at': str(metadata.get('publishedText', '')),
                        'channel_title': str(metadata.get('author', '')),
                        'tags': ','.join(metadata.get('keywords', [])),
                        'collected_at': datetime.now().isoformat(),
                        'full_description': str(metadata.get('description', '')),
                        'collection_source': 'invidious',
                        'collection_instance_used': str(self.invidious_collector.current_instance_index)
                    }
                    
                    collected.append(video_record)
                    st.session_state.collected_videos.append(video_record)
                    st.session_state.collector_stats['found'] += 1
                    
                    self.add_log(f"Added: {video_record['title'][:50]}", "SUCCESS")
                    
                    if progress_callback:
                        progress_callback(len(collected), target_count)
                else:
                    st.session_state.collector_stats['rejected'] += 1
                    self.add_log(f"Rejected: {reason}", "WARNING")
                
                time.sleep(0.5)  # Rate limiting
            
            attempts += 1
            time.sleep(1)
        
        return collected


def main():
    # Configure autorefresh based on activity
    if AUTOREFRESH_AVAILABLE:
        if st.session_state.is_collecting:
            count = st_autorefresh(interval=3000, key="collector")
        else:
            count = st_autorefresh(interval=30000, key="idle_monitor")
    
    st.markdown("""
    <div class="main-header">
        <h1>Enhanced YouTube Collection Tool</h1>
        <p><strong>Invidious-powered collection with robust error handling</strong></p>
    </div>
    """, unsafe_allow_html=True)
    
    # Sidebar configuration
    with st.sidebar:
        st.header("Configuration")
        
        st.subheader("API Configuration")
        youtube_api_key = st.text_input("YouTube API Key (Optional)", type="password")
        
        st.subheader("Google Sheets Configuration")
        sheets_creds_text = st.text_area("Service Account JSON", height=150)
        sheets_creds = None
        
        if sheets_creds_text:
            try:
                sheets_creds = json.loads(sheets_creds_text)
                st.success("Valid JSON")
            except json.JSONDecodeError as e:
                st.error(f"Invalid JSON: {str(e)}")
        
        spreadsheet_url = st.text_input("Google Sheet URL")
        match = re.search(r'/d/([a-zA-Z0-9-_]+)', spreadsheet_url)
        spreadsheet_id = match.group(1) if match else spreadsheet_url
    
    # Show status alerts
    show_status_alert()
    
    # Main interface
    st.subheader("Data Collector")
    
    with st.sidebar:
        st.subheader("Collection Settings")
        category = st.selectbox("Content Category", 
                               options=['heartwarming', 'funny', 'traumatic'])
        target_count = st.number_input("Target Video Count", min_value=1, max_value=100, value=10)
        auto_export = st.checkbox("Auto-export to Google Sheets", value=True)
    
    # Statistics display
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Videos Found", st.session_state.collector_stats['found'])
    with col2:
        st.metric("Videos Checked", st.session_state.collector_stats['checked'])
    with col3:
        st.metric("Videos Rejected", st.session_state.collector_stats['rejected'])
    with col4:
        st.metric("API Calls", st.session_state.collector_stats['api_calls_invidious'])
    
    # API Status Dashboard
    st.subheader("Invidious Instance Status")
    
    invidious_collector = InvidiousCollector()
    instance_stats = invidious_collector.get_instance_stats()
    
    for instance, stats in instance_stats.items():
        instance_name = instance.replace('https://', '')
        status_text = f"{instance_name}: {stats['status'].title()}"
        
        if stats['consecutive_failures'] == 0:
            st.markdown(f'<div class="api-status api-primary">{status_text}</div>', unsafe_allow_html=True)
        elif stats['consecutive_failures'] < 3:
            st.markdown(f'<div class="api-status api-fallback">{status_text} ({stats["consecutive_failures"]} failures)</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="api-status api-failed">{status_text} (Circuit breaker open)</div>', unsafe_allow_html=True)
    
    # Control buttons
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        if st.button("Start Collection", 
                    disabled=st.session_state.is_collecting,
                    type="primary"):
            clear_status()
            
            if not sheets_creds and auto_export:
                set_status('error', "COLLECTION ABORTED: Google Sheets credentials required")
            else:
                st.session_state.is_collecting = True
                
                try:
                    exporter = None
                    if sheets_creds:
                        exporter = RateLimitedSheetsExporter(sheets_creds)
                    
                    collector = SimpleVideoCollector(youtube_api_key, exporter)
                    
                    set_status('info', "COLLECTION STARTED: Validating instances...")
                    
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    def update_progress(current, total):
                        progress = current / total
                        progress_bar.progress(progress)
                        status_text.text(f"Collecting: {current}/{total} videos")
                    
                    with st.spinner(f"Collecting {target_count} videos..."):
                        videos = collector.collect_videos_simple(
                            target_count=target_count,
                            category=category,
                            progress_callback=update_progress
                        )
                    
                    if videos and len(videos) > 0:
                        set_status('success', f"COLLECTION COMPLETED: Found {len(videos)} videos")
                    else:
                        set_status('warning', "COLLECTION COMPLETED: No videos found")
                    
                    # Export
                    if auto_export and sheets_creds and videos and len(videos) > 0:
                        try:
                            sheet_url = exporter.export_to_sheets_enhanced(videos, spreadsheet_id=spreadsheet_id)
                            
                            if sheet_url:
                                st.success("Exported to Google Sheets!")
                                st.markdown(f"[View Spreadsheet]({sheet_url})")
                                set_status('success', f"EXPORT SUCCESS: {len(videos)} videos exported")
                            else:
                                set_status('error', "EXPORT FAILED: Could not export to sheets")
                                
                        except Exception as e:
                            set_status('error', f"EXPORT FAILED: {str(e)}")
                
                except Exception as e:
                    set_status('error', f"COLLECTION FAILED: {str(e)}")
                finally:
                    st.session_state.is_collecting = False
            
            st.rerun()
    
    with col2:
        if st.button("Stop Collection", disabled=not st.session_state.is_collecting):
            set_status('warning', "COLLECTION STOPPED: Process terminated by user")
            st.session_state.is_collecting = False
            st.rerun()
    
    with col3:
        if st.button("Reset Stats"):
            st.session_state.collected_videos = []
            st.session_state.collector_stats = {
                'checked': 0, 'found': 0, 'rejected': 0, 
                'api_calls_youtube': 0, 'api_calls_invidious': 0,
                'invidious_successes': 0, 'youtube_fallbacks': 0,
                'has_captions': 0, 'no_captions': 0
            }
            st.session_state.logs = []
            clear_status()
            st.rerun()
    
    # Display collected videos
    if st.session_state.collected_videos:
        st.subheader("Collected Videos")
        df = pd.DataFrame(st.session_state.collected_videos)
        
        display_columns = ['title', 'category', 'view_count', 'duration_seconds', 'collection_source']
        available_columns = [col for col in display_columns if col in df.columns]
        
        st.dataframe(
            df[available_columns],
            use_container_width=True,
            hide_index=True
        )
    
    # Activity log
    with st.expander("Activity Log", expanded=False):
        if st.session_state.logs:
            for log in st.session_state.logs[-20:]:
                if "SUCCESS" in log:
                    st.success(log)
                elif "ERROR" in log:
                    st.error(log)
                elif "WARNING" in log:
                    st.warning(log)
                else:
                    st.info(log)
        else:
            st.info("No activity logged yet")


if __name__ == "__main__":
    main()
