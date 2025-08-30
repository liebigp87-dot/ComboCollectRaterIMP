#!/usr/bin/env python3
"""
Combined YouTube Data Collector & Video Rating Tool
Collects YouTube videos and rates them for content suitability
Version without streamlit-autorefresh - uses native Streamlit features
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
        background: #262730;
        color: #fafafa;
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 2px 10px rgba(0,0,0,0.3);
        border: 1px solid #404040;
    }
    .score-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 2rem;
        border-radius: 15px;
        text-align: center;
        margin: 1rem 0;
    }
    .component-card {
        background: #262730;
        color: #fafafa;
        padding: 1.2rem;
        border-radius: 8px;
        border-left: 4px solid #667eea;
        margin: 0.5rem 0;
        box-shadow: 0 2px 8px rgba(0,0,0,0.3);
    }
    .timestamp-moment {
        background: #2d3748;
        color: #e2e8f0;
        padding: 0.8rem;
        border-radius: 6px;
        margin: 0.5rem 0;
        border-left: 3px solid #4299e1;
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
    st.session_state.collector_stats = {'checked': 0, 'found': 0, 'rejected': 0, 'api_calls': 0, 'has_captions': 0, 'no_captions': 0}
if 'rater_stats' not in st.session_state:
    st.session_state.rater_stats = {'rated': 0, 'moved_to_tobe': 0, 'rejected': 0, 'api_calls': 0}
if 'logs' not in st.session_state:
    st.session_state.logs = []
if 'used_queries' not in st.session_state:
    st.session_state.used_queries = set()
if 'analysis_history' not in st.session_state:
    st.session_state.analysis_history = []
if 'system_status' not in st.session_state:
    st.session_state.system_status = {'type': None, 'message': ''}
if 'batch_mode' not in st.session_state:
    st.session_state.batch_mode = False
if 'batch_progress' not in st.session_state:
    st.session_state.batch_progress = {'current': 0, 'total': 0, 'results': []}

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
    """Set system status message"""
    st.session_state.system_status = {'type': status_type, 'message': message}

def clear_status():
    """Clear system status message"""
    st.session_state.system_status = {'type': None, 'message': ''}

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
        """Initialize with service account credentials"""
        self.creds = Credentials.from_service_account_info(
            credentials_dict,
            scopes=['https://www.googleapis.com/auth/spreadsheets',
                   'https://www.googleapis.com/auth/drive']
        )
        self.client = gspread.authorize(self.creds)
    
    def get_spreadsheet_by_id(self, spreadsheet_id: str):
        """Get spreadsheet by ID"""
        try:
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            return spreadsheet
        except Exception as e:
            raise e
    
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
        """Delete video from raw_links sheet"""
        try:
            spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            worksheet = spreadsheet.worksheet("raw_links")
            worksheet.delete_rows(row_number)
        except Exception as e:
            st.error(f"Error deleting video: {str(e)}")
    
    def add_to_tobe_links(self, spreadsheet_id: str, video_data: Dict, analysis_data: Dict):
        """Add video to tobe_links sheet with analysis data"""
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
                analysis_data.get('comments_analysis', {}).get('category_validation', ''),
                datetime.now().isoformat()
            ]
            
            worksheet.append_row(row_data)
        except Exception as e:
            st.error(f"Error adding to tobe_links: {str(e)}")
    
    def add_to_discarded(self, spreadsheet_id: str, video_url: str):
        """Add video URL to discarded table"""
        try:
            spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            
            try:
                worksheet = spreadsheet.worksheet("discarded")
            except gspread.exceptions.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title="discarded", rows=1000, cols=1)
                worksheet.append_row(['url'])
            
            worksheet.append_row([video_url])
        except Exception as e:
            st.error(f"Error adding to discarded: {str(e)}")
    
    def load_discarded_urls(self, spreadsheet_id: str) -> set:
        """Load existing URLs from discarded sheet"""
        try:
            spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            try:
                worksheet = spreadsheet.worksheet("discarded")
                all_values = worksheet.get_all_values()
                
                if len(all_values) > 1:
                    discarded_urls = {row[0] for row in all_values[1:] if row and row[0]}
                    return discarded_urls
            except gspread.exceptions.WorksheetNotFound:
                pass
            return set()
        except Exception as e:
            st.error(f"Error loading discarded URLs: {str(e)}")
            return set()
    
    def add_time_comments(self, spreadsheet_id: str, video_id: str, video_url: str, comments_analysis: Dict):
        """Add timestamped and category-matched comments to time_comments table"""
        try:
            spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            
            try:
                worksheet = spreadsheet.worksheet("time_comments")
            except gspread.exceptions.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title="time_comments", rows=1000, cols=10)
                
                headers = [
                    'video_id', 'video_url', 'comment_text', 'timestamp', 
                    'category_matched', 'relevance_score', 'sentiment'
                ]
                worksheet.append_row(headers)
            
            moments = comments_analysis.get('timestamped_moments', [])
            
            for moment in moments:
                row_data = [
                    video_id,
                    video_url,
                    moment.get('comment', ''),
                    moment.get('timestamp', ''),
                    moment.get('category_matches', 0),
                    moment.get('relevance_score', 0),
                    moment.get('sentiment', '')
                ]
                worksheet.append_row(row_data)
                
        except Exception as e:
            st.error(f"Error adding to time_comments: {str(e)}")
    
    def export_to_sheets(self, videos: List[Dict], spreadsheet_id: str = None, spreadsheet_name: str = "YouTube_Collection_Data"):
        """Export videos to raw_links sheet"""
        try:
            if not videos:
                return None
                
            if spreadsheet_id:
                spreadsheet = self.get_spreadsheet_by_id(spreadsheet_id)
            else:
                try:
                    spreadsheet = self.client.open(spreadsheet_name)
                except gspread.exceptions.SpreadsheetNotFound:
                    spreadsheet = self.client.create(spreadsheet_name)
            
            worksheet_name = "raw_links"
            
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
            except gspread.exceptions.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=20)
            
            df = pd.DataFrame(videos)
            existing_data = worksheet.get_all_values()
            
            if existing_data and len(existing_data) > 1:
                for _, row in df.iterrows():
                    values = [str(v) if pd.notna(v) else '' for v in row.tolist()]
                    worksheet.append_row(values)
            else:
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
        self.existing_queries = set()
        self.discarded_urls = set()
        
        self.search_queries = {
            'heartwarming': [
                'soldier surprise homecoming', 'dog reunion owner', 'random acts kindness',
                'baby first time hearing', 'proposal reaction emotional', 'surprise gift reaction',
                'homeless man helped', 'teacher surprised students', 'reunion after years',
                'saving animal rescue', 'kid helps stranger', 'emotional wedding moment',
                'surprise visit family', 'grateful reaction wholesome', 'community helps neighbor',
                'dad meets baby', 'emotional support moment', 'stranger pays bill',
                'found lost pet', 'surprise donation reaction', 'elderly couple sweet',
                'child generous sharing', 'unexpected hero saves', 'touching tribute video',
                'faith humanity restored', 'emotional thank you',
                'surprise birthday elderly', 'veteran honored ceremony', 'wholesome interaction strangers'
            ],
            'funny': [
                'funny fails compilation', 'unexpected moments caught', 'comedy sketches viral',
                'hilarious reactions', 'funny animals doing', 'epic fail video',
                'instant karma funny', 'comedy gold moments', 'prank goes wrong',
                'funny kids saying', 'dad jokes reaction', 'wedding fails funny',
                'sports bloopers hilarious', 'funny news bloopers', 'pet fails compilation',
                'funny work moments', 'hilarious misunderstanding', 'comedy timing perfect',
                'funny voice over', 'unexpected plot twist', 'funny security camera',
                'hilarious interview moments', 'comedy accident harmless', 'funny dancing fails',
                'laughing contagious video', 'funny sleep talking', 'comedy scare pranks',
                'funny workout fails', 'hilarious costume fails', 'funny zoom fails'
            ],
            'traumatic': [
                'shocking moments caught', 'dramatic rescue operation', 'natural disaster footage',
                'intense police chase', 'survival story real', 'near death experience',
                'unbelievable close call', 'extreme weather footage', 'emergency response dramatic',
                'accident caught camera', 'dangerous situation survived', 'storm chaser footage',
                'rescue mission dramatic', 'wildfire evacuation footage', 'flood rescue dramatic',
                'earthquake footage real', 'tornado close encounter', 'avalanche survival story',
                'lightning strike caught', 'road rage incident', 'building collapse footage',
                'helicopter rescue dramatic', 'cliff rescue operation', 'shark encounter real',
                'volcano eruption footage', 'mudslide caught camera', 'train near miss',
                'bridge collapse footage', 'explosion caught camera', 'emergency landing footage'
            ]
        }
