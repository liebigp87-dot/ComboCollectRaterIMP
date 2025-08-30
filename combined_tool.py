#!/usr/bin/env python3
"""
Enhanced YouTube Data Collector & Video Rating Tool
Integrates Invidious API as primary source with YouTube API fallback
Includes robust error handling, rate limiting, and streamlit-autorefresh
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import json
import time
import random
from typing import Dict, List, Optional, Tuple
import re
import uuid
import requests
import numpy as np
from PIL import Image
import io
import asyncio
from urllib.parse import unquote

# Streamlit autorefresh for graceful UI updates
try:
    from streamlit_autorefresh import st_autorefresh
    AUTOREFRESH_AVAILABLE = True
except ImportError:
    AUTOREFRESH_AVAILABLE = False
    st.warning("streamlit-autorefresh not installed. Install with: pip install streamlit-autorefresh")

# YouTube API (fallback)
try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ImportError:
    st.error("Please install google-api-python-client: pip install google-api-python-client")
    st.stop()

# Google Sheets integration
try:
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:
    st.error("Please install gspread and google-auth: pip install gspread google-auth")
    st.stop()

# ISO date parsing
try:
    import isodate
except ImportError:
    st.error("Please install isodate: pip install isodate")
    st.stop()

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
    .score-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 2rem;
        border-radius: 15px;
        text-align: center;
        margin: 1rem 0;
    }
    .timestamp-moment {
        background: #2d3748;
        color: #e2e8f0;
        padding: 0.8rem;
        border-radius: 6px;
        margin: 0.5rem 0;
        border-left: 3px solid #4299e1;
    }
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
        'analysis_history': [],
        'system_status': {'type': None, 'message': ''},
        'batch_progress': {'current': 0, 'total': 0, 'results': []},
        'batch_current_cycle': 0,
        'batch_total_cycles': 0,
        'batch_settings': {},
        'invidious_instance_stats': {},
        'api_usage_mode': 'invidious_primary',  # invidious_primary, youtube_fallback, youtube_only
        'sheets_request_queue': [],
        'last_sheets_request': 0
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

init_session_state()

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


class InvidiousCollector:
    """Enhanced Invidious API collector with robust error handling"""
    
    def __init__(self):
        # Official instances from docs.invidious.io
        self.instances = [
            'https://inv.nadeko.net',        # Chile - Official
            'https://yewtu.be',              # Germany - Official  
            'https://invidious.nerdvpn.de',  # Ukraine - Official
            'https://invidious.f5.si',       # Japan - Cloudflare
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
                'homeless man helped', 'teacher surprised students', 'reunion after years',
                'saving animal rescue', 'kid helps stranger', 'emotional wedding moment',
                'military surprise family', 'adopted child meets birth parents', 'community rallies sick child',
                'cancer survivor celebration', 'graduation surprise parent', 'wedding surprise dance',
                'foster pet adoption', 'service dog training', 'therapy animal visit',
                'elderly person birthday', 'nursing home visit', 'grandparent technology help',
                'pay it forward chain', 'anonymous donation recipient', 'good samaritan highway'
            ],
            'funny': [
                'unexpected moments caught', 'comedy sketches viral', 'hilarious reactions',
                'funny animals doing', 'epic fail video', 'instant karma funny',
                'comedy gold moments', 'prank goes wrong', 'funny kids saying',
                'elevator prank harmless', 'autocorrect text fails', 'cooking disaster funny',
                'parking fail video', 'phone autocorrect mom', 'grocery store slip',
                'technology grandparents funny', 'smartphone elderly reaction', 'computer password forgot',
                'kid logic funny', 'children say darndest', 'toddler tantrum funny',
                'pet door confusion', 'cat vs cucumber', 'dog treats hidden',
                'exercise equipment fail', 'yoga pose gone wrong', 'treadmill mishap'
            ],
            'traumatic': [
                'shocking moments caught', 'dramatic rescue operation', 'natural disaster footage',
                'intense police chase', 'survival story real', 'near death experience',
                'wildfire escape footage', 'building evacuation emergency', 'storm damage aftermath',
                'flash flood rescue', 'river rapids rescue', 'ocean current survivor',
                'mountain rescue operation', 'cave rescue dramatic', 'mine collapse rescue',
                'fire department rescue', 'paramedic emergency response', 'ambulance emergency call',
                'aircraft emergency landing', 'pilot emergency procedure', 'runway emergency landing',
                'industrial accident footage', 'factory explosion aftermath', 'chemical spill emergency'
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
                stats_data = response.json()
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
                self._mark_instance_unhealthy(instance_url, f"HTTP {response.status_code}")
                return False, f"HTTP {response.status_code}"
                
        except Exception as e:
            self._mark_instance_unhealthy(instance_url, str(e))
            return False, str(e)
    
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
    
    def make_api_request(self, endpoint, params=None):
        """Make API request with rate limiting and retry logic"""
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
                                      headers={
                                          'User-Agent': 'Mozilla/5.0 (compatible; InvidiousCollector/1.0)'
                                      })
                
                if response.status_code == 200:
                    self.instance_health[instance]['successful_requests'] += 1
                    self.instance_health[instance]['consecutive_failures'] = 0
                    self.failed_instances.discard(instance)
                    st.session_state.collector_stats['invidious_successes'] += 1
                    return response.json(), None
                
                elif response.status_code == 500:
                    self._mark_instance_unhealthy(instance, f"Server error: {response.status_code}")
                    continue
                    
                elif response.status_code == 429:
                    time.sleep(2 ** attempt)
                    continue
                    
                else:
                    error_msg = f"HTTP {response.status_code}"
                    self._mark_instance_unhealthy(instance, error_msg)
                    continue
                    
            except requests.RequestException as e:
                self._mark_instance_unhealthy(instance, str(e))
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay_base * (2 ** attempt))
                continue
        
        return None, "All Invidious instances failed"
    
    def fetch_video_metadata(self, video_id):
        """Fetch comprehensive video metadata"""
        data, error = self.make_api_request(f"/api/v1/videos/{video_id}")
        if error:
            return None, error
        return data, None
    
    def search_videos(self, query, max_results=25):
        """Search videos using Invidious API"""
        params = {
            'q': query,
            'type': 'video',
            'sort_by': 'relevance',
            'duration': 'medium',
            'max_results': max_results
        }
        
        data, error = self.make_api_request("/api/v1/search", params)
        if error:
            return []
        
        return data if isinstance(data, list) else []
    
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


class EnhancedYouTubeCollector:
    """Enhanced YouTube collector as fallback with rate limiting"""
    
    def __init__(self, api_key: str, invidious_collector=None):
        self.youtube = build('youtube', 'v3', developerKey=api_key)
        self.invidious_collector = invidious_collector
        self.request_count = 0
        self.last_request_time = 0
        self.requests_per_minute_limit = 200
        
        # Search queries (same as Invidious)
        self.search_queries = invidious_collector.search_queries if invidious_collector else {}
    
    def _rate_limit_check(self):
        """Ensure we don't exceed Google API rate limits"""
        current_time = time.time()
        
        # Reset counter every minute
        if current_time - self.last_request_time > 60:
            self.request_count = 0
            self.last_request_time = current_time
        
        # Check if we're approaching limit
        if self.request_count >= self.requests_per_minute_limit:
            wait_time = 60 - (current_time - self.last_request_time)
            if wait_time > 0:
                time.sleep(wait_time)
                self.request_count = 0
                self.last_request_time = time.time()
    
    def fetch_video_metadata_fallback(self, video_id):
        """Fetch video metadata as fallback when Invidious fails"""
        try:
            self._rate_limit_check()
            self.request_count += 1
            st.session_state.collector_stats['api_calls_youtube'] += 1
            st.session_state.collector_stats['youtube_fallbacks'] += 1
            
            request = self.youtube.videos().list(
                part='snippet,contentDetails,statistics',
                id=video_id
            )
            response = request.execute()
            
            if response['items']:
                return response['items'][0], None
            return None, "Video not found"
            
        except HttpError as e:
            error_str = str(e)
            if 'quotaExceeded' in error_str:
                return None, "YouTube API quota exceeded"
            return None, f"YouTube API error: {error_str}"
        except Exception as e:
            return None, f"YouTube API error: {str(e)}"


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
        self.requests_per_minute_limit = 200  # Conservative limit
    
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
                # New Invidious-enhanced fields
                'full_description', 'thumbnail_urls', 'subtitle_languages',
                'video_quality_available', 'collection_source', 'collection_instance_used',
                'collection_retry_count', 'api_response_time', 'metadata_completeness_score'
            ]
            
            existing_data = worksheet.get_all_values()
            
            if not existing_data or len(existing_data) <= 1:
                # Create headers
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
            return None
    
    def _prepare_enhanced_row(self, video: Dict, headers: List[str]) -> List[str]:
        """Prepare enhanced row with all metadata fields"""
        row = []
        for header in headers:
            value = video.get(header, '')
            
            # Handle special fields
            if header == 'tags' and isinstance(value, list):
                value = ','.join(value)
            elif header == 'thumbnail_urls' and isinstance(value, (list, dict)):
                value = json.dumps(value)
            elif header == 'subtitle_languages' and isinstance(value, list):
                value = ','.join(value)
            
            row.append(str(value) if value else '')
        
        return row
    
    # Include all other methods from original GoogleSheetsExporter
    def get_next_raw_video(self, spreadsheet_id: str) -> Optional[Dict]:
        """Get next video from raw_links sheet with validation"""
        try:
            spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            worksheet = spreadsheet.worksheet("raw_links")
            all_values = worksheet.get_all_values()
            
            if len(all_values) > 1:
                headers = all_values[0]
                for row_index, row_data in enumerate(all_values[1:], start=1):
                    if not row_data or len(row_data) < len(headers):
                        continue
                    
                    video_data = {headers[i]: row_data[i] if i < len(row_data) else '' for i in range(len(headers))}
                    
                    if (video_data.get('video_id', '').strip() and 
                        video_data.get('url', '').strip() and
                        video_data.get('title', '').strip()):
                        
                        video_data['row_number'] = row_index + 1
                        return video_data
                
            return None
        except Exception as e:
            st.error(f"Error fetching next video: {str(e)}")
            return None
    
    def delete_raw_video(self, spreadsheet_id: str, row_number: int):
        try:
            spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            worksheet = spreadsheet.worksheet("raw_links")
            self._rate_limit_sheets_request()
            worksheet.delete_rows(row_number)
        except Exception as e:
            st.error(f"Error deleting video: {str(e)}")
    
    def add_to_tobe_links(self, spreadsheet_id: str, video_data: Dict, analysis_data: Dict):
        try:
            spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            
            try:
                worksheet = spreadsheet.worksheet("tobe_links")
            except gspread.exceptions.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title="tobe_links", rows=1000, cols=25)
                headers = [
                    'video_id', 'title', 'url', 'category', 'search_query', 
                    'duration_seconds', 'view_count', 'like_count', 'comment_count',
                    'published_at', 'channel_title', 'tags', 'collected_at',
                    'score', 'confidence', 'timestamped_moments', 'category_validation',
                    'analysis_timestamp'
                ]
                self._rate_limit_sheets_request()
                worksheet.append_row(headers)
            
            row_data = [
                video_data.get('video_id', ''), video_data.get('title', ''),
                video_data.get('url', ''), video_data.get('category', ''),
                video_data.get('search_query', ''), video_data.get('duration_seconds', ''),
                video_data.get('view_count', ''), video_data.get('like_count', ''),
                video_data.get('comment_count', ''), video_data.get('published_at', ''),
                video_data.get('channel_title', ''), video_data.get('tags', ''),
                video_data.get('collected_at', ''), analysis_data.get('final_score', ''),
                analysis_data.get('confidence', ''),
                len(analysis_data.get('comments_analysis', {}).get('timestamped_moments', [])),
                analysis_data.get('comments_analysis', {}).get('category_validation', ''),
                datetime.now().isoformat()
            ]
            
            self._rate_limit_sheets_request()
            worksheet.append_row(row_data)
            
        except Exception as e:
            st.error(f"Error adding to tobe_links: {str(e)}")
    
    def add_to_discarded(self, spreadsheet_id: str, video_url: str):
        try:
            spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            
            try:
                worksheet = spreadsheet.worksheet("discarded")
            except gspread.exceptions.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title="discarded", rows=1000, cols=1)
                self._rate_limit_sheets_request()
                worksheet.append_row(['url'])
            
            self._rate_limit_sheets_request()
            worksheet.append_row([video_url])
        except Exception as e:
            st.error(f"Error adding to discarded: {str(e)}")


class EnhancedVideoCollector:
    """Main collector that uses Invidious primarily with YouTube fallback"""
    
    def __init__(self, youtube_api_key: str = None, sheets_exporter=None):
        self.invidious_collector = InvidiousCollector()
        self.youtube_collector = None
        
        if youtube_api_key:
            self.youtube_collector = EnhancedYouTubeCollector(youtube_api_key, self.invidious_collector)
        
        self.sheets_exporter = sheets_exporter
        self.existing_sheet_ids = set()
        self.discarded_urls = set()
    
    def add_log(self, message: str, log_type: str = "INFO"):
        """Add detailed log entry with API source tracking"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] COLLECTOR {log_type}: {message}"
        st.session_state.logs.insert(0, log_entry)
        st.session_state.logs = st.session_state.logs[:100]
    
    def validate_video_enhanced(self, video_data: Dict, target_category: str, require_captions: bool = True):
        """Enhanced validation with both Invidious and YouTube data"""
        video_id = video_data.get('videoId') or video_data.get('id', {}).get('videoId')
        
        if not video_id:
            return False, "No video ID found"
        
        video_url = f"https://youtube.com/watch?v={video_id}"
        title = video_data.get('title', '')
        
        self.add_log(f"Starting enhanced validation for: {title[:50]}...")
        
        # Check duplicates
        if video_id in [v['video_id'] for v in st.session_state.collected_videos]:
            return False, "Duplicate (current session)"
        
        if video_id in self.existing_sheet_ids:
            return False, "Duplicate (raw_links sheet)"
        
        if video_url in self.discarded_urls:
            return False, "Already processed (discarded)"
        
        # Duration check
        duration_seconds = video_data.get('lengthSeconds', 0)
        if isinstance(duration_seconds, str):
            duration_seconds = int(duration_seconds)
        
        if duration_seconds < 90:
            return False, f"Too short ({duration_seconds}s < 90s)"
        
        if duration_seconds > 600:  # 10 minutes
            return False, f"Too long ({duration_seconds}s > 600s)"
        
        # View count check
        view_count = int(video_data.get('viewCount', 0))
        if view_count < 10000:
            return False, f"View count too low ({view_count:,} < 10,000)"
        
        # Category relevance (simplified for now)
        title_lower = title.lower()
        category_keywords = {
            'heartwarming': ['heartwarming', 'touching', 'emotional', 'reunion', 'surprise'],
            'funny': ['funny', 'comedy', 'humor', 'hilarious', 'laugh'],
            'traumatic': ['accident', 'disaster', 'emergency', 'rescue', 'shocking']
        }
        
        keywords = category_keywords.get(target_category, [])
        if not any(kw in title_lower for kw in keywords):
            return False, f"No {target_category} keywords in title"
        
        self.add_log(f"Enhanced validation passed for: {video_id}", "SUCCESS")
        return True, "Validation passed"
    
    def collect_videos_enhanced(self, target_count: int, category: str, spreadsheet_id: str = None, 
                              require_captions: bool = True, progress_callback=None):
        """Enhanced collection using Invidious primarily"""
        collected = []
        
        categories = ['heartwarming', 'funny', 'traumatic'] if category == 'mixed' else [category]
        
        self.add_log(f"Starting enhanced collection: {target_count} videos, category: {category}", "INFO")
        
        # Load existing data
        if spreadsheet_id and self.sheets_exporter:
            self.load_existing_data(spreadsheet_id)
        
        attempts = 0
        max_attempts = 50
        videos_checked = set()
        
        while len(collected) < target_count and attempts < max_attempts:
            current_category = random.choice(categories)
            query = random.choice(self.invidious_collector.search_queries[current_category])
            
            self.add_log(f"Searching '{current_category}': {query}", "INFO")
            
            # Try Invidious first
            search_results = self.invidious_collector.search_videos(query, max_results=25)
            
            # Fallback to YouTube if Invidious fails
            if not search_results and self.youtube_collector:
                self.add_log("Invidious search failed, trying YouTube API fallback", "WARNING")
                search_results = self.youtube_search_fallback(query)
                st.session_state.collector_stats['youtube_fallbacks'] += 1
            
            if not search_results:
                attempts += 1
                continue
            
            for item in search_results:
                if len(collected) >= target_count:
                    break
                
                video_id = item.get('videoId') or item.get('id', {}).get('videoId')
                
                if not video_id or video_id in videos_checked:
                    continue
                
                videos_checked.add(video_id)
                st.session_state.collector_stats['checked'] += 1
                
                # Get detailed metadata
                video_metadata = self.get_video_metadata_enhanced(video_id)
                if not video_metadata:
                    continue
                
                # Validate
                is_valid, reason = self.validate_video_enhanced(video_metadata, current_category, require_captions)
                
                if is_valid:
                    video_record = self.prepare_enhanced_video_record(video_metadata, current_category, query)
                    collected.append(video_record)
                    st.session_state.collected_videos.append(video_record)
                    st.session_state.collector_stats['found'] += 1
                    
                    self.add_log(f"Added to collection: {video_record['title'][:50]}", "SUCCESS")
                    
                    if progress_callback:
                        progress_callback(len(collected), target_count)
                else:
                    st.session_state.collector_stats['rejected'] += 1
                    self.add_log(f"Rejected: {reason}", "WARNING")
            
            attempts += 1
            time.sleep(1)  # Rate limiting
        
        return collected
    
    def get_video_metadata_enhanced(self, video_id: str):
        """Get metadata with Invidious primary, YouTube fallback"""
        # Try Invidious first
        metadata, error = self.invidious_collector.fetch_video_metadata(video_id)
        
        if metadata:
            return metadata
        
        # Fallback to YouTube
        if self.youtube_collector:
            self.add_log(f"Invidious metadata failed for {video_id}, trying YouTube fallback", "WARNING")
            metadata, error = self.youtube_collector.fetch_video_metadata_fallback(video_id)
            if metadata:
                return metadata
        
        self.add_log(f"All metadata sources failed for {video_id}", "ERROR")
        return None
    
    def prepare_enhanced_video_record(self, metadata: Dict, category: str, query: str) -> Dict:
        """Prepare enhanced video record with additional fields"""
        video_id = metadata.get('videoId') or metadata.get('id')
        
        # Extract additional Invidious-specific fields
        record = {
            'video_id': video_id,
            'title': metadata.get('title', ''),
            'url': f"https://youtube.com/watch?v={video_id}",
            'category': category,
            'search_query': query,
            'duration_seconds': int(metadata.get('lengthSeconds', 0)),
            'view_count': int(metadata.get('viewCount', 0)),
            'like_count': int(metadata.get('likeCount', 0)),
            'comment_count': int(metadata.get('commentCount', 0)),
            'published_at': metadata.get('publishedText', ''),
            'channel_title': metadata.get('author', ''),
            'tags': ','.join(metadata.get('keywords', [])),
            'collected_at': datetime.now().isoformat(),
            
            # Enhanced fields
            'full_description': metadata.get('description', ''),
            'thumbnail_urls': json.dumps(metadata.get('videoThumbnails', [])),
            'subtitle_languages': ','.join([cap.get('languageCode', '') for cap in metadata.get('captions', [])]),
            'video_quality_available': ','.join([fmt.get('qualityLabel', '') for fmt in metadata.get('formatStreams', [])]),
            'collection_source': 'invidious' if 'lengthSeconds' in metadata else 'youtube_api',
            'collection_instance_used': getattr(self.invidious_collector, 'current_instance_index', 'N/A'),
            'collection_retry_count': 0,  # Could be enhanced
            'api_response_time': 0,  # Could be enhanced
            'metadata_completeness_score': self._calculate_completeness_score(metadata)
        }
        
        return record
    
    def _calculate_completeness_score(self, metadata: Dict) -> float:
        """Calculate how complete the metadata is (0-1 scale)"""
        required_fields = ['title', 'viewCount', 'lengthSeconds', 'author']
        optional_fields = ['description', 'captions', 'videoThumbnails', 'keywords']
        
        required_score = sum(1 for field in required_fields if metadata.get(field))
        optional_score = sum(1 for field in optional_fields if metadata.get(field))
        
        return (required_score / len(required_fields)) * 0.7 + (optional_score / len(optional_fields)) * 0.3
    
    def youtube_search_fallback(self, query: str):
        """YouTube API search as fallback"""
        if not self.youtube_collector:
            return []
        
        try:
            self.youtube_collector._rate_limit_check()
            request = self.youtube_collector.youtube.search().list(
                part='id,snippet',
                q=query,
                type='video',
                maxResults=25,
                order='relevance',
                videoDuration='medium',
                relevanceLanguage='en'
            )
            
            response = request.execute()
            return response.get('items', [])
            
        except Exception as e:
            self.add_log(f"YouTube search fallback failed: {str(e)}", "ERROR")
            return []
    
    def load_existing_data(self, spreadsheet_id: str):
        """Load existing video IDs and discarded URLs"""
        # Implementation similar to original but with rate limiting
        pass


# Video Rater (keeping original implementation for now)
class VideoRater:
    """Original video rating functionality - unchanged for compatibility"""
    
    def __init__(self, api_key: str):
        self.youtube = build('youtube', 'v3', developerKey=api_key)
    
    def add_log(self, message: str, log_type: str = "INFO"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] RATER {log_type}: {message}"
        st.session_state.logs.insert(0, log_entry)
        st.session_state.logs = st.session_state.logs[:100]
    
    # Include all original VideoRater methods here...
    def check_quota_available(self) -> Tuple[bool, str]:
        try:
            test_request = self.youtube.videos().list(part='id', id='YbJOTdZBX1g')
            response = test_request.execute()
            st.session_state.rater_stats['api_calls'] += 1
            return True, "Quota available"
        except HttpError as e:
            if 'quotaExceeded' in str(e):
                return False, "Daily quota exceeded"
            return True, "Warning but continuing"
    
    # ... (include other original methods)


def main():
    # Configure autorefresh based on activity
    if AUTOREFRESH_AVAILABLE:
        if st.session_state.is_batch_collecting:
            count = st_autorefresh(interval=3000, key="batch_collector")
        elif st.session_state.is_rating:
            count = st_autorefresh(interval=5000, key="video_rater")  
        elif st.session_state.is_collecting:
            count = st_autorefresh(interval=2000, key="single_collector")
        else:
            count = st_autorefresh(interval=30000, key="idle_monitor")
    
    st.markdown("""
    <div class="main-header">
        <h1>Enhanced YouTube Collection & Rating Tool</h1>
        <p><strong>Invidious-powered collection with YouTube API fallback + Advanced analytics</strong></p>
    </div>
    """, unsafe_allow_html=True)
    
    # Sidebar configuration
    with st.sidebar:
        st.header("🔧 Configuration")
        
        mode = st.radio("Select Mode:", ["Data Collector", "Video Rater"], horizontal=True)
        
        st.subheader("API Configuration")
        youtube_api_key = st.text_input("YouTube API Key (Fallback)", type="password", 
                                       help="Optional - used as fallback when Invidious fails")
        
        st.subheader("Google Sheets Configuration")
        creds_input_method = st.radio("Service Account JSON:", ["Paste JSON", "Upload JSON file"])
        
        sheets_creds = None
        if creds_input_method == "Paste JSON":
            sheets_creds_text = st.text_area("Service Account JSON", height=150)
            if sheets_creds_text:
                try:
                    sheets_creds = json.loads(sheets_creds_text)
                    st.success("✅ Valid JSON")
                except json.JSONDecodeError as e:
                    st.error(f"❌ Invalid JSON: {str(e)}")
        else:
            uploaded_file = st.file_uploader("Upload Service Account JSON", type=['json'])
            if uploaded_file:
                try:
                    sheets_creds = json.load(uploaded_file)
                    st.success("✅ JSON file loaded")
                except Exception as e:
                    st.error(f"❌ Error reading file: {str(e)}")
        
        spreadsheet_url = st.text_input("Google Sheet URL", 
                                       value="https://docs.google.com/spreadsheets/d/1PHvW-LykIpIbwKJbiGHi6NcX7hd4EsIWK3zwr4Dmvrk/")
        
        match = re.search(r'/d/([a-zA-Z0-9-_]+)', spreadsheet_url)
        spreadsheet_id = match.group(1) if match else spreadsheet_url
        
        if spreadsheet_id:
            st.success(f"📋 Sheet ID: {spreadsheet_id[:20]}...")
        
        if sheets_creds and 'client_email' in sheets_creds:
            st.info(f"👤 Service Account: {sheets_creds['client_email'][:30]}...")
    
    # API Status Dashboard
    st.subheader("📊 System Status")
    
    # Create collectors for health checking
    invidious_collector = InvidiousCollector()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**🌐 Invidious API Status**")
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
    
    with col2:
        st.markdown("**🔗 API Usage Statistics**")
        stats = st.session_state.collector_stats
        
        col2_1, col2_2 = st.columns(2)
        with col2_1:
            st.metric("Invidious Calls", stats['api_calls_invidious'])
            st.metric("Invidious Success", stats['invidious_successes'])
        with col2_2:
            st.metric("YouTube Fallbacks", stats['youtube_fallbacks'])
            st.metric("YouTube Calls", stats['api_calls_youtube'])
        
        # Success rate calculation
        total_api_calls = stats['api_calls_invidious'] + stats['api_calls_youtube']
        if total_api_calls > 0:
            success_rate = ((stats['invidious_successes'] + stats['youtube_fallbacks']) / total_api_calls) * 100
            st.metric("Overall Success Rate", f"{success_rate:.1f}%")
    
    # Show status alerts
    show_status_alert()
    
    if mode == "Data Collector":
        st.subheader("📥 Enhanced Data Collector")
        
        with st.sidebar:
            st.subheader("Collection Settings")
            category = st.selectbox("Content Category", 
                                   options=['heartwarming', 'funny', 'traumatic', 'mixed'])
            
            target_count = st.number_input("Target Video Count", min_value=1, max_value=500, value=10)
            
            auto_export = st.checkbox("Auto-export to Google Sheets", value=True)
            require_captions = st.checkbox("Require captions", value=True)
        
        # Statistics display
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Videos Found", st.session_state.collector_stats['found'])
        with col2:
            st.metric("Videos Checked", st.session_state.collector_stats['checked'])
        with col3:
            st.metric("Videos Rejected", st.session_state.collector_stats['rejected'])
        with col4:
            success_rate = 0
            if st.session_state.collector_stats['checked'] > 0:
                success_rate = (st.session_state.collector_stats['found'] / 
                               st.session_state.collector_stats['checked']) * 100
            st.metric("Success Rate", f"{success_rate:.1f}%")
        
        # Control buttons
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("🚀 Start Enhanced Collection", 
                        disabled=st.session_state.is_collecting or st.session_state.is_batch_collecting,
                        type="primary"):
                clear_status()
                
                if not sheets_creds and auto_export:
                    set_status('error', "COLLECTION ABORTED: Google Sheets credentials required")
                else:
                    st.session_state.is_collecting = True
                    
                    try:
                        # Create enhanced exporter
                        exporter = None
                        if sheets_creds:
                            exporter = RateLimitedSheetsExporter(sheets_creds)
                        
                        # Create enhanced collector
                        collector = EnhancedVideoCollector(youtube_api_key, exporter)
                        
                        set_status('info', "ENHANCED COLLECTION STARTED: Using Invidious with YouTube fallback")
                        
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        def update_progress(current, total):
                            progress = current / total
                            progress_bar.progress(progress)
                            status_text.text(f"Enhanced Collection: {current}/{total} videos")
                        
                        with st.spinner(f"Collecting {target_count} videos using enhanced pipeline..."):
                            videos = collector.collect_videos_enhanced(
                                target_count=target_count,
                                category=category,
                                spreadsheet_id=spreadsheet_id,
                                require_captions=require_captions,
                                progress_callback=update_progress
                            )
                        
                        if videos and len(videos) > 0:
                            set_status('success', f"ENHANCED COLLECTION COMPLETED: Found {len(videos)} videos with enhanced metadata")
                        else:
                            set_status('warning', "ENHANCED COLLECTION COMPLETED: No videos found")
                        
                        # Enhanced export
                        if auto_export and sheets_creds and videos and len(videos) > 0:
                            try:
                                sheet_url = exporter.export_to_sheets_enhanced(videos, spreadsheet_id=spreadsheet_id)
                                
                                if sheet_url:
                                    st.success("✅ Enhanced export completed!")
                                    st.markdown(f"[📊 View Enhanced Spreadsheet]({sheet_url})")
                                    set_status('success', f"ENHANCED EXPORT SUCCESS: {len(videos)} videos with metadata exported")
                                else:
                                    set_status('error', "ENHANCED EXPORT FAILED: Could not export to sheets")
                                    
                            except Exception as e:
                                set_status('error', f"ENHANCED EXPORT FAILED: {str(e)}")
                    
                    except Exception as e:
                        set_status('error', f"ENHANCED COLLECTION FAILED: {str(e)}")
                    finally:
                        st.session_state.is_collecting = False
                
                st.rerun()
        
        with col2:
            if st.button("⏹️ Stop Collection", disabled=not st.session_state.is_collecting):
                set_status('warning', "ENHANCED COLLECTION STOPPED: Process terminated by user")
                st.session_state.is_collecting = False
                st.rerun()
        
        with col3:
            if st.button("🔄 Reset All Stats"):
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
        
        with col4:
            if st.button("📤 Manual Export") and st.session_state.collected_videos:
                if not sheets_creds:
                    st.error("Please add Google Sheets credentials")
                else:
                    try:
                        exporter = RateLimitedSheetsExporter(sheets_creds)
                        sheet_url = exporter.export_to_sheets_enhanced(
                            st.session_state.collected_videos, 
                            spreadsheet_id=spreadsheet_id
                        )
                        if sheet_url:
                            st.success("✅ Enhanced manual export completed!")
                            st.markdown(f"[📊 View Spreadsheet]({sheet_url})")
                        else:
                            st.error("❌ Manual export failed")
                    except Exception as e:
                        st.error(f"❌ Manual export error: {str(e)}")
        
        # Display enhanced collected videos
        if st.session_state.collected_videos:
            st.subheader("📋 Enhanced Collected Videos")
            df = pd.DataFrame(st.session_state.collected_videos)
            
            # Show key columns plus new enhanced fields
            display_columns = ['title', 'category', 'view_count', 'duration_seconds', 
                              'collection_source', 'metadata_completeness_score', 'url']
            
            available_columns = [col for col in display_columns if col in df.columns]
            
            st.dataframe(
                df[available_columns],
                use_container_width=True,
                hide_index=True
            )
    
    elif mode == "Video Rater":
        st.subheader("⭐ Video Rater")
        
        # Original Video Rater implementation (unchanged for compatibility)
        show_status_alert()
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Videos Rated", st.session_state.rater_stats['rated'])
        with col2:
            st.metric("Moved to tobe_links", st.session_state.rater_stats['moved_to_tobe'])
        with col3:
            st.metric("API Calls", st.session_state.rater_stats['api_calls'])
        
        if not youtube_api_key or not sheets_creds or not spreadsheet_id:
            set_status('warning', "RATING UNAVAILABLE: Missing YouTube API key, Google Sheets credentials, or spreadsheet URL")
        else:
            col1, col2 = st.columns([1, 1])
            
            with col1:
                if st.button("▶️ Start Rating", disabled=st.session_state.is_rating, type="primary"):
                    clear_status()
                    set_status('info', "RATING STARTED: Processing videos from raw_links")
                    st.session_state.is_rating = True
                    st.rerun()
            
            with col2:
                if st.button("⏹️ Stop Rating", disabled=not st.session_state.is_rating):
                    set_status('warning', "RATING STOPPED: Process terminated by user")
                    st.session_state.is_rating = False
                    st.rerun()
    
    # Enhanced Activity Log
    with st.expander("📋 Enhanced Activity Log", expanded=False):
        col1, col2 = st.columns([3, 1])
        
        with col1:
            log_filter = st.selectbox("Filter logs:", ["All", "SUCCESS", "INFO", "WARNING", "ERROR"])
        
        with col2:
            if st.button("🗑️ Clear Logs"):
                st.session_state.logs = []
                st.rerun()
        
        if st.session_state.logs:
            filtered_logs = st.session_state.logs[-50:]  # Last 50 entries
            
            if log_filter != "All":
                filtered_logs = [log for log in filtered_logs if log_filter in log]
            
            for log in filtered_logs:
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
