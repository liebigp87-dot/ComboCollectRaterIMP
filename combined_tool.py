#!/usr/bin/env python3
"""
Complete YouTube Collection & Rating Tool with Auto-Refresh
Combines collection from YouTube API and rating with comment analysis
Includes streamlit-autorefresh for non-blocking continuous operation
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
import logging
from streamlit_autorefresh import st_autorefresh

try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ImportError:
    st.error("Please install google-api-python-client: pip install google-api-python-client")
    st.stop()

try:
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:
    st.error("Please install gspread and google-auth: pip install gspread google-auth")
    st.stop()

try:
    import isodate
except ImportError:
    st.error("Please install isodate: pip install isodate")
    st.stop()

# Page config
st.set_page_config(
    page_title="YouTube Collection & Rating Tool",
    page_icon="🎬",
    layout="wide"
)

# CSS styling
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
    .category-card {
        background: #f0f2f6;
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
    }
    .score-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 2rem;
        border-radius: 15px;
        text-align: center;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'collected_videos' not in st.session_state:
    st.session_state.collected_videos = []
if 'is_collecting' not in st.session_state:
    st.session_state.is_collecting = False
if 'is_rating' not in st.session_state:
    st.session_state.is_rating = False
if 'collector_stats' not in st.session_state:
    st.session_state.collector_stats = {'checked': 0, 'found': 0, 'rejected': 0, 'api_calls': 0}
if 'rater_stats' not in st.session_state:
    st.session_state.rater_stats = {'rated': 0, 'moved_to_tobe': 0, 'rejected': 0, 'api_calls': 0}
if 'logs' not in st.session_state:
    st.session_state.logs = []
if 'system_status' not in st.session_state:
    st.session_state.system_status = {'type': None, 'message': ''}
if 'auto_refresh_enabled' not in st.session_state:
    st.session_state.auto_refresh_enabled = False
if 'refresh_interval' not in st.session_state:
    st.session_state.refresh_interval = 5000
if 'batch_mode' not in st.session_state:
    st.session_state.batch_mode = False
if 'batch_queue' not in st.session_state:
    st.session_state.batch_queue = []
if 'batch_progress' not in st.session_state:
    st.session_state.batch_progress = 0
if 'batch_total' not in st.session_state:
    st.session_state.batch_total = 0

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

class GoogleSheetsExporter:
    """Handle Google Sheets export and import"""
    
    def __init__(self, credentials_dict: Dict):
        self.creds = Credentials.from_service_account_info(
            credentials_dict,
            scopes=['https://www.googleapis.com/auth/spreadsheets',
                   'https://www.googleapis.com/auth/drive']
        )
        self.client = gspread.authorize(self.creds)
    
    def get_spreadsheet_by_id(self, spreadsheet_id: str):
        try:
            return self.client.open_by_key(spreadsheet_id)
        except Exception as e:
            raise e
    
    def get_next_raw_video(self, spreadsheet_id: str) -> Optional[Dict]:
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
                    'score', 'confidence', 'timestamped_moments', 'analysis_timestamp'
                ]
                worksheet.append_row(headers)
            
            row_data = [
                video_data.get('video_id', ''),
                video_data.get('title', ''),
                video_data.get('url', ''),
                video_data.get('category', ''),
                video_data.get('search_query', ''),
                video_data.get('duration_seconds', ''),
                video_data.get('view_count', ''),
                video_data.get('like_count', ''),
                video_data.get('comment_count', ''),
                video_data.get('published_at', ''),
                video_data.get('channel_title', ''),
                video_data.get('tags', ''),
                video_data.get('collected_at', ''),
                analysis_data.get('final_score', ''),
                analysis_data.get('confidence', ''),
                len(analysis_data.get('comments_analysis', {}).get('timestamped_moments', [])),
                datetime.now().isoformat()
            ]
            
            worksheet.append_row(row_data)
        except Exception as e:
            st.error(f"Error adding to tobe_links: {str(e)}")
    
    def export_to_sheets(self, videos: List[Dict], spreadsheet_id: str = None):
        try:
            if not videos:
                return None
                
            spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            
            try:
                worksheet = spreadsheet.worksheet("raw_links")
            except gspread.exceptions.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title="raw_links", rows=1000, cols=20)
            
            df = pd.DataFrame(videos)
            existing_data = worksheet.get_all_values()
            
            if not existing_data or len(existing_data) <= 1:
                worksheet.clear()
                headers = list(df.columns)
                worksheet.append_row(headers)
            
            for _, row in df.iterrows():
                values = [str(v) if pd.notna(v) else '' for v in row.tolist()]
                worksheet.append_row(values)
            
            return spreadsheet.url
            
        except Exception as e:
            st.error(f"Google Sheets export failed: {str(e)}")
            return None

class YouTubeCollector:
    """YouTube video collection functionality"""
    
    def __init__(self, api_key: str, sheets_exporter=None):
        self.youtube = build('youtube', 'v3', developerKey=api_key)
        self.sheets_exporter = sheets_exporter
        self.existing_sheet_ids = set()
        
        self.search_queries = {
            'heartwarming': [
                'soldier surprise homecoming', 'dog reunion owner', 'random acts kindness',
                'baby first time hearing', 'proposal reaction emotional', 'surprise gift reaction',
                'homeless man helped', 'teacher surprised students', 'reunion after years'
            ],
            'funny': [
                'funny fails compilation', 'unexpected moments caught', 'comedy sketches viral',
                'hilarious reactions', 'funny animals doing', 'epic fail video',
                'instant karma funny', 'comedy gold moments', 'prank goes wrong'
            ],
            'traumatic': [
                'shocking moments caught', 'dramatic rescue operation', 'natural disaster footage',
                'intense police chase', 'survival story real', 'near death experience',
                'unbelievable close call', 'extreme weather footage', 'emergency response dramatic'
            ]
        }
    
    def add_log(self, message: str, log_type: str = "INFO"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] COLLECTOR {log_type}: {message}"
        st.session_state.logs.insert(0, log_entry)
        st.session_state.logs = st.session_state.logs[:100]
    
    def check_quota_available(self) -> Tuple[bool, str]:
        try:
            test_request = self.youtube.videos().list(part='id', id='YbJOTdZBX1g')
            response = test_request.execute()
            st.session_state.collector_stats['api_calls'] += 1
            return True, "Quota available"
        except HttpError as e:
            error_str = str(e)
            if 'quotaExceeded' in error_str:
                return False, "Daily quota exceeded"
            elif 'forbidden' in error_str.lower():
                return False, "API key invalid"
            else:
                return True, "Warning but continuing"
        except Exception:
            return True, "Could not verify quota"
    
    def search_videos(self, query: str, max_results: int = 25) -> List[Dict]:
        try:
            st.session_state.collector_stats['api_calls'] += 1
            six_months_ago = (datetime.now() - timedelta(days=180)).isoformat() + 'Z'
            
            request = self.youtube.search().list(
                part='id,snippet',
                q=query,
                type='video',
                maxResults=max_results,
                order='relevance',
                publishedAfter=six_months_ago,
                videoDuration='medium',
                relevanceLanguage='en'
            )
            
            response = request.execute()
            return response.get('items', [])
        except HttpError:
            return []
    
    def get_video_details(self, video_id: str) -> Optional[Dict]:
        try:
            st.session_state.collector_stats['api_calls'] += 1
            request = self.youtube.videos().list(
                part='snippet,contentDetails,statistics',
                id=video_id
            )
            response = request.execute()
            
            if response['items']:
                return response['items'][0]
            return None
        except HttpError:
            return None
    
    def collect_videos(self, target_count: int, category: str, spreadsheet_id: str = None, require_captions: bool = True):
        collected = []
        
        if category == 'mixed':
            categories = ['heartwarming', 'funny', 'traumatic']
        else:
            categories = [category]
        
        self.add_log(f"Starting collection for category: {category}, target: {target_count} videos", "INFO")
        
        category_index = 0
        attempts = 0
        max_attempts = 30
        
        while len(collected) < target_count and attempts < max_attempts:
            current_category = categories[category_index % len(categories)]
            query = random.choice(self.search_queries[current_category])
            
            self.add_log(f"Searching: {query}", "INFO")
            search_results = self.search_videos(query)
            
            if not search_results:
                attempts += 1
                category_index += 1
                continue
            
            for item in search_results:
                if len(collected) >= target_count:
                    break
                
                video_id = item['id']['videoId']
                st.session_state.collector_stats['checked'] += 1
                
                details = self.get_video_details(video_id)
                if details:
                    try:
                        duration = isodate.parse_duration(details['contentDetails']['duration'])
                        duration_seconds = duration.total_seconds()
                        
                        if duration_seconds >= 90 and duration_seconds <= 600:
                            video_record = {
                                'video_id': video_id,
                                'title': details['snippet']['title'],
                                'url': f"https://youtube.com/watch?v={video_id}",
                                'category': current_category,
                                'search_query': query,
                                'duration_seconds': int(duration_seconds),
                                'view_count': int(details['statistics'].get('viewCount', 0)),
                                'like_count': int(details['statistics'].get('likeCount', 0)),
                                'comment_count': int(details['statistics'].get('commentCount', 0)),
                                'published_at': details['snippet']['publishedAt'],
                                'channel_title': details['snippet']['channelTitle'],
                                'tags': ','.join(details['snippet'].get('tags', [])),
                                'collected_at': datetime.now().isoformat()
                            }
                            
                            collected.append(video_record)
                            st.session_state.collected_videos.append(video_record)
                            st.session_state.collector_stats['found'] += 1
                            self.add_log(f"Added: {video_record['title'][:50]}...", "SUCCESS")
                    except Exception:
                        st.session_state.collector_stats['rejected'] += 1
                
                time.sleep(0.3)
            
            category_index += 1
            attempts += 1
            time.sleep(1.5)
        
        return collected

class VideoRater:
    """Video rating functionality with comment analysis"""
    
    def __init__(self, api_key: str):
        self.youtube = build('youtube', 'v3', developerKey=api_key)
    
    def add_log(self, message: str, log_type: str = "INFO"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] RATER {log_type}: {message}"
        st.session_state.logs.insert(0, log_entry)
        st.session_state.logs = st.session_state.logs[:100]
    
    def analyze_sentiment(self, text):
        text_lower = text.lower()
        positive_words = ['amazing', 'incredible', 'beautiful', 'love', 'great', 'good', 'nice', 'happy']
        negative_words = ['terrible', 'awful', 'worst', 'hate', 'bad', 'fake', 'boring']
        
        pos_count = sum(1 for word in positive_words if word in text_lower)
        neg_count = sum(1 for word in negative_words if word in text_lower)
        
        if pos_count > neg_count:
            return 'positive'
        elif neg_count > pos_count:
            return 'negative'
        else:
            return 'neutral'
    
    def fetch_comments(self, video_id, max_results=500):
        comments = []
        sentiment_data = {'positive': 0, 'negative': 0, 'neutral': 0, 'total': 0}
        
        try:
            url = f"https://www.googleapis.com/youtube/v3/commentThreads"
            params = {
                'part': 'snippet',
                'videoId': video_id,
                'maxResults': 100,
                'order': 'relevance',
                'key': self.youtube._developerKey
            }
            
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                
                for item in data.get('items', []):
                    comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay']
                    
                    if comment_text not in comments and len(comment_text.strip()) > 5:
                        comments.append(comment_text)
                        sentiment = self.analyze_sentiment(comment_text)
                        sentiment_data[sentiment] += 1
                        sentiment_data['total'] += 1
                        
                        if len(comments) >= max_results:
                            break
        except Exception as e:
            self.add_log(f"Comment fetch error: {str(e)}", "WARNING")
        
        return {
            'comments': comments,
            'sentiment_analysis': sentiment_data,
            'total_fetched': len(comments)
        }
    
    def analyze_comments_for_category(self, comments, category_key):
        if not comments:
            return {
                'category_validation': 0.0,
                'emotional_alignment': 0.0,
                'authenticity_support': 0.0,
                'engagement_quality': 0.0,
                'timestamped_moments': []
            }
        
        all_text = ' '.join(comments).lower()
        
        if category_key == 'heartwarming':
            positive_emotions = ['crying', 'tears', 'emotional', 'beautiful', 'touching', 'moving', 'wholesome']
            authenticity_words = ['real', 'genuine', 'authentic', 'natural']
            fake_indicators = ['fake', 'staged', 'acting', 'scripted']
            
            positive_count = sum(1 for word in positive_emotions if word in all_text)
            auth_count = sum(1 for word in authenticity_words if word in all_text)
            fake_count = sum(1 for word in fake_indicators if word in all_text)
            
            validation = min(positive_count / max(len(comments) * 0.05, 1), 1.0)
            emotional = min(positive_count / max(len(comments) * 0.03, 1), 1.0)
            authenticity = max(0.1, min(auth_count / max(fake_count + 1, 1) * 0.5, 1.0))
            
            return {
                'category_validation': validation,
                'emotional_alignment': emotional,
                'authenticity_support': authenticity,
                'engagement_quality': min(positive_count / max(len(comments), 1), 1.0),
                'timestamped_moments': []
            }
        
        return {
            'category_validation': 0.5,
            'emotional_alignment': 0.5,
            'authenticity_support': 0.5,
            'engagement_quality': 0.5,
            'timestamped_moments': []
        }
    
    def calculate_category_score(self, video_data, comments_data, category_key):
        comments_analysis = self.analyze_comments_for_category(comments_data['comments'], category_key)
        
        weights = {
            'comment_validation': 0.35,
            'comment_emotional': 0.25,
            'comment_authenticity': 0.20,
            'content_match': 0.15,
            'engagement': 0.05
        }
        
        base_score = 3.0
        
        component_scores = {
            'comment_validation': comments_analysis['category_validation'],
            'comment_emotional': comments_analysis['emotional_alignment'],
            'comment_authenticity': comments_analysis['authenticity_support'],
            'content_match': 0.5,
            'engagement': 0.5
        }
        
        weighted_score = sum(component_scores[key] * weights[key] for key in weights)
        final_score = base_score + weighted_score * 7.0
        
        confidence = min(0.3 + (len(comments_data['comments']) / 500) * 0.7, 1.0)
        
        return {
            'final_score': min(final_score, 10.0),
            'confidence': confidence,
            'comments_analysis': comments_analysis
        }

def show_status_alert():
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

def main():
    st.markdown("""
    <div class="main-header">
        <h1>YouTube Collection & Rating Tool</h1>
        <p><strong>Collect YouTube videos and rate them with AI analysis</strong></p>
    </div>
    """, unsafe_allow_html=True)
    
    # Sidebar configuration
    with st.sidebar:
        st.header("Configuration")
        
        mode = st.radio("Select Mode:", ["Data Collector", "Video Rater"], horizontal=True)
        
        st.subheader("API Configuration")
        youtube_api_key = st.text_input("YouTube API Key", type="password")
        
        st.subheader("Google Sheets")
        sheets_creds_text = st.text_area("Service Account JSON", height=150)
        sheets_creds = None
        if sheets_creds_text:
            try:
                sheets_creds = json.loads(sheets_creds_text)
                st.success("Valid JSON")
            except json.JSONDecodeError:
                st.error("Invalid JSON")
        
        spreadsheet_url = st.text_input("Google Sheet URL")
        match = re.search(r'/d/([a-zA-Z0-9-_]+)', spreadsheet_url)
        spreadsheet_id = match.group(1) if match else spreadsheet_url
        
        # Auto-refresh settings
        st.divider()
        st.subheader("⚡ Auto-Refresh")
        
        auto_refresh_enabled = st.checkbox(
            "Enable Auto-Refresh",
            value=st.session_state.auto_refresh_enabled,
            help="Automatically refresh for continuous operation"
        )
        
        if auto_refresh_enabled:
            refresh_options = {
                "2 seconds": 2000,
                "5 seconds": 5000,
                "10 seconds": 10000,
                "30 seconds": 30000,
                "1 minute": 60000
            }
            
            selected_interval = st.selectbox(
                "Refresh Interval",
                options=list(refresh_options.keys()),
                index=1
            )
            st.session_state.refresh_interval = refresh_options[selected_interval]
            st.session_state.auto_refresh_enabled = True
        else:
            st.session_state.auto_refresh_enabled = False
    
    # Auto-refresh implementation
    refresh_count = 0
    if st.session_state.auto_refresh_enabled:
        refresh_count = st_autorefresh(
            interval=st.session_state.refresh_interval,
            key="youtube_tool_refresh"
        )
        
        if refresh_count > 0:
            st.sidebar.info(f"🔄 Refresh #{refresh_count}")
    
    # Show status alerts
    show_status_alert()
    
    # Main content based on mode
    if mode == "Data Collector":
        st.subheader("Data Collector")
        
        with st.sidebar:
            st.subheader("Collection Settings")
            category = st.selectbox("Content Category", options=['heartwarming', 'funny', 'traumatic', 'mixed'])
            target_count = st.number_input("Target Video Count", min_value=1, max_value=500, value=10)
            
            # Batch mode
            batch_mode = st.checkbox("Batch Collection Mode", help="Collect multiple batches automatically")
            if batch_mode:
                batch_count = st.number_input("Number of Batches", min_value=1, max_value=10, value=3)
                videos_per_batch = st.number_input("Videos per Batch", min_value=1, max_value=50, value=10)
        
        # Statistics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Videos Found", st.session_state.collector_stats['found'])
        with col2:
            st.metric("Videos Checked", st.session_state.collector_stats['checked'])
        with col3:
            st.metric("API Calls", st.session_state.collector_stats['api_calls'])
        
        # Control buttons
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("Start Collection", disabled=st.session_state.is_collecting, type="primary"):
                if not youtube_api_key or not sheets_creds:
                    set_status('error', "Missing API key or credentials")
                else:
                    st.session_state.is_collecting = True
                    
                    if batch_mode:
                        # Setup batch queue
                        st.session_state.batch_mode = True
                        st.session_state.batch_queue = [
                            {'category': category, 'count': videos_per_batch}
                            for _ in range(batch_count)
                        ]
                        st.session_state.batch_total = batch_count
                        st.session_state.batch_progress = 0
                        set_status('info', f"Batch collection started: {batch_count} batches")
                    else:
                        set_status('info', "Collection started")
                    
                    st.rerun()
        
        with col2:
            if st.button("Stop", disabled=not st.session_state.is_collecting):
                st.session_state.is_collecting = False
                st.session_state.batch_mode = False
                st.session_state.batch_queue = []
                set_status('warning', "Collection stopped")
                st.rerun()
        
        with col3:
            if st.button("Reset Stats"):
                st.session_state.collected_videos = []
                st.session_state.collector_stats = {'checked': 0, 'found': 0, 'rejected': 0, 'api_calls': 0}
                st.session_state.logs = []
                clear_status()
                st.rerun()
        
        # Process collection on refresh
        if st.session_state.is_collecting and refresh_count > 0:
            if st.session_state.batch_mode and st.session_state.batch_queue:
                # Process one batch
                batch = st.session_state.batch_queue[0]
                
                try:
                    exporter = GoogleSheetsExporter(sheets_creds) if sheets_creds else None
                    collector = YouTubeCollector(youtube_api_key, exporter)
                    
                    videos = collector.collect_videos(
                        target_count=batch['count'],
                        category=batch['category'],
                        spreadsheet_id=spreadsheet_id
                    )
                    
                    if videos and exporter:
                        sheet_url = exporter.export_to_sheets(videos, spreadsheet_id)
                        if sheet_url:
                            st.session_state.batch_progress += 1
                            st.session_state.batch_queue.pop(0)
                            
                            if not st.session_state.batch_queue:
                                st.session_state.is_collecting = False
                                st.session_state.batch_mode = False
                                set_status('success', f"Batch collection complete: {st.session_state.batch_total} batches")
                            else:
                                set_status('info', f"Batch {st.session_state.batch_progress}/{st.session_state.batch_total} complete")
                
                except Exception as e:
                    set_status('error', f"Collection error: {str(e)}")
                    st.session_state.is_collecting = False
            
            elif not st.session_state.batch_mode:
                # Single collection
                try:
                    exporter = GoogleSheetsExporter(sheets_creds) if sheets_creds else None
                    collector = YouTubeCollector(youtube_api_key, exporter)
                    
                    videos = collector.collect_videos(
                        target_count=target_count,
                        category=category,
                        spreadsheet_id=spreadsheet_id
                    )
                    
                    if videos and exporter:
                        exporter.export_to_sheets(videos, spreadsheet_id)
                        st.session_state.is_collecting = False
                        set_status('success', f"Collection complete: {len(videos)} videos")
                
                except Exception as e:
                    set_status('error', f"Collection error: {str(e)}")
                    st.session_state.is_collecting = False
        
        # Display collected videos
        if st.session_state.collected_videos:
            st.subheader("Collected Videos")
            df = pd.DataFrame(st.session_state.collected_videos)
            st.dataframe(df[['title', 'category', 'view_count', 'duration_seconds']], use_container_width=True, hide_index=True)
    
    elif mode == "Video Rater":
        st.subheader("Video Rater")
        
        # Statistics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Videos Rated", st.session_state.rater_stats['rated'])
        with col2:
            st.metric("Moved to tobe_links", st.session_state.rater_stats['moved_to_tobe'])
        with col3:
            st.metric("API Calls", st.session_state.rater_stats['api_calls'])
        
        # Control buttons
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("Start Rating", disabled=st.session_state.is_rating, type="primary"):
                if not youtube_api_key or not sheets_creds:
                    set_status('error', "Missing API key or credentials")
                else:
                    st.session_state.is_rating = True
                    set_status('info', "Rating started")
                    st.rerun()
        
        with col2:
            if st.button("Stop Rating", disabled=not st.session_state.is_rating):
                st.session_state.is_rating = False
                set_status('warning', "Rating stopped")
                st.rerun()
        
        # Process rating on refresh
        if st.session_state.is_rating and refresh_count > 0:
            try:
                rater = VideoRater(youtube_api_key)
                exporter = GoogleSheetsExporter(sheets_creds)
                
                next_video = exporter.get_next_raw_video(spreadsheet_id)
                
                if next_video:
                    video_id = next_video.get('video_id')
                    category = next_video.get('category', 'heartwarming')
                    
                    # Fetch comments
                    comments_data = rater.fetch_comments(video_id)
                    
                    # Calculate score
                    analysis = rater.calculate_category_score(next_video, comments_data, category)
                    score = analysis['final_score']
                    
                    # Delete from raw_links
                    exporter.delete_raw_video(spreadsheet_id, next_video['row_number'])
                    
                    # If score >= 6.5, add to tobe_links
                    if score >= 6.5:
                        exporter.add_to_tobe_links(spreadsheet_id, next_video, analysis)
                        st.session_state.rater_stats['moved_to_tobe'] += 1
                        rater.add_log(f"Video scored {score:.1f} - moved to tobe_links", "SUCCESS")
                    else:
                        rater.add_log(f"Video scored {score:.1f} - removed", "INFO")
                    
                    st.session_state.rater_stats['rated'] += 1
                    
                    # Display current video info
                    with st.container():
                        st.markdown(f"**Current:** {next_video.get('title', 'Unknown')[:50]}...")
                        st.markdown(f"**Score:** {score:.1f}/10")
                        
                        if score >= 6.5:
                            st.success(f"✅ Moved to tobe_links")
                        else:
                            st.info(f"ℹ️ Below threshold")
                else:
                    st.session_state.is_rating = False
                    set_status('info', "No more videos to rate")
            
            except Exception as e:
                set_status('error', f"Rating error: {str(e)}")
                st.session_state.is_rating = False
    
    # Activity log
    with st.expander("Activity Log", expanded=False):
        if st.session_state.logs:
            for log in st.session_state.logs[:20]:
                if "SUCCESS" in log:
                    st.success(log)
                elif "ERROR" in log:
                    st.error(log)
                elif "WARNING" in log:
                    st.warning(log)
                else:
                    st.info(log)
        else:
            st.info("No activity yet")

if __name__ == "__main__":
    main()
