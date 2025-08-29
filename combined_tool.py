#!/usr/bin/env python3
"""
Enhanced YouTube Collection & Rating Tool with Invidious Integration
Complete implementation with new scoring weights and controlled comment analysis
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
import logging
from dataclasses import dataclass

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
    page_title="Enhanced YouTube Collection & Rating Tool",
    page_icon="🎬",
    layout="wide"
)

# Initialize session state with new fields
if 'collected_videos' not in st.session_state:
    st.session_state.collected_videos = []
if 'is_collecting' not in st.session_state:
    st.session_state.is_collecting = False
if 'is_rating' not in st.session_state:
    st.session_state.is_rating = False
if 'collector_stats' not in st.session_state:
    st.session_state.collector_stats = {'checked': 0, 'found': 0, 'rejected': 0, 'api_calls': 0}
if 'rater_stats' not in st.session_state:
    st.session_state.rater_stats = {'rated': 0, 'moved_to_tobe': 0, 'rejected': 0, 'api_calls': 0, 'comments_analyzed': 0}
if 'logs' not in st.session_state:
    st.session_state.logs = []
if 'system_status' not in st.session_state:
    st.session_state.system_status = {'type': None, 'message': ''}

@dataclass
class InvidiousInstance:
    """Represents an Invidious instance with health tracking"""
    url: str
    last_success: Optional[datetime] = None
    failure_count: int = 0
    response_time: float = 0.0
    is_available: bool = True

class InvidiousAPI:
    """Enhanced Invidious API client with robust error handling"""
    
    DEFAULT_INSTANCES = [
        "https://vid.puffyan.us",
        "https://invidious.nerdvpn.de", 
        "https://inv.nadeko.net",
        "https://invidious.protokolla.fi",
        "https://yt.artemislena.eu",
        "https://invidious.tiekoetter.com",
        "https://iv.ggtyler.dev"
    ]
    
    def __init__(self, instances: Optional[List[str]] = None, max_retries: int = 3):
        self.instances = [InvidiousInstance(url) for url in (instances or self.DEFAULT_INSTANCES)]
        self.max_retries = max_retries
        self.logger = logging.getLogger(__name__)
        self.session = requests.Session()
        
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        })
        
        self._initial_health_check()
    
    def add_log(self, message: str, log_type: str = "INFO"):
        """Add detailed log entry"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] INVIDIOUS {log_type}: {message}"
        st.session_state.logs.insert(0, log_entry)
        st.session_state.logs = st.session_state.logs[:100]
    
    def _initial_health_check(self):
        """Test all instances and mark unavailable ones"""
        self.add_log(f"Testing {len(self.instances)} Invidious instances...", "INFO")
        
        for instance in self.instances:
            try:
                start_time = time.time()
                response = self.session.get(f"{instance.url}/api/v1/stats", timeout=10)
                
                if response.status_code == 200:
                    instance.response_time = time.time() - start_time
                    instance.last_success = datetime.now()
                    instance.is_available = True
                    self.add_log(f"✅ {instance.url} - OK ({instance.response_time:.2f}s)", "SUCCESS")
                else:
                    instance.is_available = False
                    self.add_log(f"❌ {instance.url} - HTTP {response.status_code}", "WARNING")
                    
            except Exception as e:
                instance.is_available = False
                instance.failure_count += 1
                self.add_log(f"❌ {instance.url} - {str(e)[:100]}", "WARNING")
        
        available = sum(1 for i in self.instances if i.is_available)
        self.add_log(f"Health check complete: {available}/{len(self.instances)} instances available", "INFO")
    
    def _get_best_instance(self) -> Optional[InvidiousInstance]:
        """Get the best available instance"""
        available_instances = [i for i in self.instances if i.is_available]
        
        if not available_instances:
            self.add_log("No available instances, attempting recovery...", "WARNING")
            self._initial_health_check()
            available_instances = [i for i in self.instances if i.is_available]
            
            if not available_instances:
                return None
        
        available_instances.sort(key=lambda x: (x.failure_count, x.response_time))
        return available_instances[0]
    
    def _make_request(self, endpoint: str, params: Dict = None, timeout: int = 15) -> Optional[Dict]:
        """Make robust API request with automatic failover"""
        params = params or {}
        
        for attempt in range(self.max_retries):
            instance = self._get_best_instance()
            
            if not instance:
                self.add_log("No available Invidious instances", "ERROR")
                return None
            
            try:
                url = f"{instance.url}/api/v1/{endpoint}"
                start_time = time.time()
                
                response = self.session.get(url, params=params, timeout=timeout)
                response_time = time.time() - start_time
                
                if response.status_code == 200:
                    instance.last_success = datetime.now()
                    instance.response_time = response_time
                    instance.failure_count = max(0, instance.failure_count - 1)
                    
                    return response.json()
                
                elif response.status_code == 429:
                    self.add_log(f"Rate limited on {instance.url}, trying next instance", "WARNING")
                    instance.failure_count += 1
                    continue
                    
                elif response.status_code >= 500:
                    self.add_log(f"Server error {response.status_code} on {instance.url}", "WARNING")
                    instance.failure_count += 1
                    instance.is_available = False
                    continue
                    
                else:
                    self.add_log(f"Client error {response.status_code}: {response.text[:200]}", "ERROR")
                    return None
                    
            except requests.exceptions.Timeout:
                self.add_log(f"Timeout on {instance.url} (attempt {attempt + 1})", "WARNING")
                instance.failure_count += 1
                
            except requests.exceptions.ConnectionError:
                self.add_log(f"Connection error on {instance.url}", "WARNING")
                instance.is_available = False
                instance.failure_count += 1
                
            except Exception as e:
                self.add_log(f"Unexpected error on {instance.url}: {str(e)}", "ERROR")
                instance.failure_count += 1
            
            if attempt < self.max_retries - 1:
                delay = (2 ** attempt) + random.uniform(0, 1)
                time.sleep(delay)
        
        return None
    
    def search_videos(self, query: str, max_results: int = 25, sort_by: str = "relevance") -> List[Dict]:
        """Search for videos using Invidious API"""
        params = {
            'q': query,
            'type': 'video',
            'sort_by': sort_by,
            'page': 1
        }
        
        all_results = []
        page = 1
        
        while len(all_results) < max_results and page <= 5:
            params['page'] = page
            
            response = self._make_request('search', params)
            if not response or 'error' in response:
                break
            
            videos = [item for item in response if item.get('type') == 'video']
            if not videos:
                break
                
            all_results.extend(videos)
            
            if len(videos) < 20:
                break
                
            page += 1
            time.sleep(0.5)
        
        return all_results[:max_results]
    
    def get_video_details(self, video_id: str) -> Optional[Dict]:
        """Get detailed video information"""
        return self._make_request(f'videos/{video_id}')

# Import the rate-limited exporter
from rate_limited_sheets_exporter import RateLimitedGoogleSheetsExporter

# Use the rate-limited version as the main exporter
EnhancedGoogleSheetsExporter = RateLimitedGoogleSheetsExporter
    
    def export_to_enhanced_raw_links(self, videos: List[Dict], spreadsheet_id: str) -> Optional[str]:
        """Export videos to raw_links with enhanced metadata columns"""
        try:
            if not videos:
                return None
                
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            
            try:
                worksheet = spreadsheet.worksheet("raw_links")
            except gspread.exceptions.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title="raw_links", rows=1000, cols=25)
            
            # Enhanced headers with new Invidious data
            enhanced_headers = [
                'video_id', 'title', 'url', 'category', 'search_query', 
                'duration_seconds', 'view_count', 'like_count', 'comment_count',
                'published_at', 'channel_title', 'tags', 'collected_at',
                # New enhanced fields from Invidious
                'video_quality_available', 'caption_languages', 'estimated_age_rating',
                'content_warning_flags', 'channel_subscriber_count', 'like_dislike_ratio',
                'invidious_metadata_complete', 'description_preview', 'channel_verified'
            ]
            
            existing_data = worksheet.get_all_values()
            
            if not existing_data or len(existing_data) <= 1:
                # Create new sheet with enhanced headers
                worksheet.clear()
                worksheet.append_row(enhanced_headers)
            
            # Prepare enhanced data rows
            for video in videos:
                row_data = [
                    video.get('video_id', ''),
                    video.get('title', ''),
                    video.get('url', ''),
                    video.get('category', ''),
                    video.get('search_query', ''),
                    video.get('duration_seconds', ''),
                    video.get('view_count', ''),
                    video.get('like_count', ''),
                    video.get('comment_count', ''),
                    video.get('published_at', ''),
                    video.get('channel_title', ''),
                    video.get('tags', ''),
                    video.get('collected_at', ''),
                    # Enhanced fields
                    video.get('video_quality_available', ''),
                    video.get('caption_languages', ''),
                    video.get('estimated_age_rating', ''),
                    video.get('content_warning_flags', ''),
                    video.get('channel_subscriber_count', ''),
                    video.get('like_dislike_ratio', ''),
                    video.get('invidious_metadata_complete', 'true'),
                    video.get('description_preview', ''),
                    video.get('channel_verified', '')
                ]
                worksheet.append_row(row_data)
            
            return spreadsheet.url
            
        except Exception as e:
            st.error(f"Enhanced sheets export failed: {str(e)}")
            return None
    
    def get_next_raw_video(self, spreadsheet_id: str) -> Optional[Dict]:
        """Get next video from enhanced raw_links sheet"""
        try:
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            worksheet = spreadsheet.worksheet("raw_links")
            all_values = worksheet.get_all_values()
            
            if len(all_values) > 1:
                headers = all_values[0]
                first_row = all_values[1]
                
                video_data = {headers[i]: first_row[i] if i < len(first_row) else '' for i in range(len(headers))}
                
                if (video_data.get('video_id', '').strip() and 
                    video_data.get('url', '').strip() and
                    video_data.get('title', '').strip()):
                    
                    video_data['row_number'] = 2
                    return video_data
                
            return None
        except Exception as e:
            st.error(f"Error fetching next video: {str(e)}")
            return None
    
    def add_to_enhanced_tobe_links(self, spreadsheet_id: str, video_data: Dict, analysis_data: Dict):
        """Add video to enhanced tobe_links sheet"""
        try:
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            
            try:
                worksheet = spreadsheet.worksheet("tobe_links")
            except gspread.exceptions.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title="tobe_links", rows=1000, cols=30)
                
                # Enhanced headers combining raw_links + analysis data + new scoring components
                headers = [
                    'video_id', 'title', 'url', 'category', 'search_query', 
                    'duration_seconds', 'view_count', 'like_count', 'comment_count',
                    'published_at', 'channel_title', 'tags', 'collected_at',
                    # Enhanced metadata from raw_links
                    'video_quality_available', 'caption_languages', 'estimated_age_rating',
                    'content_warning_flags', 'channel_subscriber_count', 'like_dislike_ratio',
                    'invidious_metadata_complete', 'description_preview', 'channel_verified',
                    # New scoring components
                    'final_score', 'confidence', 'comment_analysis_score', 'engagement_metrics_score',
                    'channel_authority_score', 'content_match_score', 'technical_quality_score',
                    'comments_analyzed_count', 'timestamped_moments', 'analysis_timestamp'
                ]
                worksheet.append_row(headers)
            
            # Prepare enhanced row data
            row_data = [
                # Original data
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
                # Enhanced metadata from collection
                video_data.get('video_quality_available', ''),
                video_data.get('caption_languages', ''),
                video_data.get('estimated_age_rating', ''),
                video_data.get('content_warning_flags', ''),
                video_data.get('channel_subscriber_count', ''),
                video_data.get('like_dislike_ratio', ''),
                video_data.get('invidious_metadata_complete', ''),
                video_data.get('description_preview', ''),
                video_data.get('channel_verified', ''),
                # Analysis results with new scoring
                analysis_data.get('final_score', ''),
                analysis_data.get('confidence', ''),
                analysis_data.get('component_scores', {}).get('comment_analysis', ''),
                analysis_data.get('component_scores', {}).get('engagement_metrics', ''),
                analysis_data.get('component_scores', {}).get('channel_authority', ''),
                analysis_data.get('component_scores', {}).get('content_match', ''),
                analysis_data.get('component_scores', {}).get('technical_quality', ''),
                analysis_data.get('comments_analyzed_count', ''),
                len(analysis_data.get('comments_analysis', {}).get('timestamped_moments', [])),
                datetime.now().isoformat()
            ]
            
            worksheet.append_row(row_data)
        except Exception as e:
            st.error(f"Error adding to enhanced tobe_links: {str(e)}")
    
    def delete_raw_video(self, spreadsheet_id: str, row_number: int):
        """Delete video from raw_links sheet"""
        try:
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            worksheet = spreadsheet.worksheet("raw_links")
            worksheet.delete_rows(row_number)
        except Exception as e:
            st.error(f"Error deleting video: {str(e)}")

class EnhancedVideoCollector:
    """Enhanced video collector using Invidious API with comprehensive metadata"""
    
    def __init__(self, invidious_instances: Optional[List[str]] = None):
        self.invidious_api = InvidiousAPI(invidious_instances)
        self.logger = logging.getLogger(__name__)
        
        # Enhanced search queries with more variety
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
                'faith humanity restored', 'emotional thank you', 'surprise birthday elderly'
            ],
            'funny': [
                'unexpected moments caught', 'comedy sketches viral', 'hilarious reactions',
                'funny animals doing', 'epic fail video', 'instant karma funny',
                'comedy gold moments', 'prank goes wrong', 'funny kids saying',
                'dad jokes reaction', 'wedding fails funny', 'sports hilarious moments',
                'funny news moments', 'pet fails video', 'funny work moments',
                'hilarious misunderstanding', 'comedy timing perfect', 'funny voice over',
                'unexpected plot twist', 'hilarious interview moments', 'comedy accident harmless'
            ],
            'traumatic': [
                'dramatic rescue operation', 'natural disaster footage', 'intense police chase',
                'survival story real', 'near death experience', 'unbelievable close call',
                'extreme weather footage', 'emergency response dramatic', 'accident caught camera',
                'dangerous situation survived', 'storm chaser footage', 'rescue mission dramatic',
                'wildfire evacuation footage', 'flood rescue dramatic', 'earthquake footage real',
                'tornado close encounter', 'avalanche survival story', 'lightning strike caught',
                'building collapse footage', 'helicopter rescue dramatic', 'cliff rescue operation'
            ]
        }
    
    def add_log(self, message: str, log_type: str = "INFO"):
        """Add detailed log entry"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] COLLECTOR {log_type}: {message}"
        st.session_state.logs.insert(0, log_entry)
        st.session_state.logs = st.session_state.logs[:100]
    
    def extract_enhanced_metadata(self, invidious_video: Dict, search_query: str, category: str) -> Dict:
        """Extract comprehensive metadata from Invidious video data"""
        try:
            # Get additional details if needed
            video_id = invidious_video.get('videoId', '')
            if not video_id:
                return {}
            
            detailed_data = self.invidious_api.get_video_details(video_id)
            if detailed_data:
                invidious_video.update(detailed_data)
            
            # Extract video quality information
            video_qualities = []
            if 'adaptiveFormats' in invidious_video:
                qualities = set()
                for fmt in invidious_video['adaptiveFormats']:
                    if fmt.get('type', '').startswith('video'):
                        quality_label = fmt.get('qualityLabel', '')
                        if quality_label:
                            qualities.add(quality_label)
                video_qualities = sorted(list(qualities), 
                                       key=lambda x: int(x.replace('p', '')) if x.replace('p', '').isdigit() else 0, 
                                       reverse=True)
            
            # Extract caption languages
            caption_languages = []
            if 'captions' in invidious_video:
                caption_languages = [cap.get('languageCode', '') for cap in invidious_video['captions']]
            
            # Calculate like/dislike ratio
            like_count = invidious_video.get('likeCount', 0)
            dislike_count = invidious_video.get('dislikeCount', 0)
            like_dislike_ratio = 0
            if like_count > 0 or dislike_count > 0:
                like_dislike_ratio = like_count / max(like_count + dislike_count, 1)
            
            # Estimate age rating based on content
            title_desc = (invidious_video.get('title', '') + ' ' + 
                         invidious_video.get('description', '')).lower()
            age_rating = self._estimate_age_rating(title_desc, category)
            
            # Detect content warnings
            content_warnings = self._detect_content_warnings(title_desc, category)
            
            # Extract description preview
            description = invidious_video.get('description', '')
            description_preview = description[:200] + '...' if len(description) > 200 else description
            
            return {
                'video_id': video_id,
                'title': invidious_video.get('title', ''),
                'url': f"https://youtube.com/watch?v={video_id}",
                'category': category,
                'search_query': search_query,
                'duration_seconds': invidious_video.get('lengthSeconds', 0),
                'view_count': invidious_video.get('viewCount', 0),
                'like_count': like_count,
                'comment_count': invidious_video.get('commentCount', 0),
                'published_at': str(invidious_video.get('published', 0)),
                'channel_title': invidious_video.get('author', ''),
                'tags': ','.join(invidious_video.get('keywords', [])),
                'collected_at': datetime.now().isoformat(),
                # Enhanced metadata
                'video_quality_available': ','.join(video_qualities),
                'caption_languages': ','.join(caption_languages),
                'estimated_age_rating': age_rating,
                'content_warning_flags': ','.join(content_warnings),
                'channel_subscriber_count': invidious_video.get('subCountText', ''),
                'like_dislike_ratio': f"{like_dislike_ratio:.3f}",
                'invidious_metadata_complete': 'true',
                'description_preview': description_preview,
                'channel_verified': str(invidious_video.get('authorVerified', False))
            }
            
        except Exception as e:
            self.add_log(f"Error extracting enhanced metadata: {str(e)}", "ERROR")
            return {}
    
    def _estimate_age_rating(self, content_text: str, category: str) -> str:
        """Estimate age rating based on content and category"""
        default_ratings = {
            'heartwarming': 'G',
            'funny': 'PG',
            'traumatic': 'PG-13'
        }
        
        base_rating = default_ratings.get(category, 'PG')
        
        concerning_terms = ['violence', 'blood', 'death', 'accident', 'disaster', 'explicit']
        if any(term in content_text for term in concerning_terms):
            if base_rating == 'G':
                base_rating = 'PG'
            elif base_rating == 'PG':
                base_rating = 'PG-13'
        
        return base_rating
    
    def _detect_content_warnings(self, content_text: str, category: str) -> List[str]:
        """Detect potential content warnings"""
        warnings = []
        
        warning_patterns = {
            'violence': ['violence', 'violent', 'blood', 'injury'],
            'disturbing': ['disturbing', 'graphic', 'shocking'],
            'adult_content': ['explicit', 'mature', 'adult only'],
            'fake_content': ['fake', 'staged', 'scripted', 'actors']
        }
        
        for warning_type, keywords in warning_patterns.items():
            if any(keyword in content_text for keyword in keywords):
                warnings.append(warning_type)
        
        return warnings
    
    def validate_enhanced_video(self, video_data: Dict, category: str) -> Tuple[bool, str]:
        """Enhanced validation using new metadata"""
        
        # Basic validations
        duration = video_data.get('duration_seconds', 0)
        if not isinstance(duration, int):
            try:
                duration = int(duration)
            except:
                duration = 0
                
        if duration < 90:
            return False, f"Too short: {duration}s < 90s"
        
        view_count = video_data.get('view_count', 0)
        if not isinstance(view_count, int):
            try:
                view_count = int(view_count)
            except:
                view_count = 0
                
        if view_count < 10000:
            return False, f"Low views: {view_count:,} < 10,000"
        
        # Content warnings check
        content_warnings = video_data.get('content_warning_flags', '').split(',')
        serious_warnings = ['violence', 'disturbing', 'adult_content']
        if any(warning in content_warnings for warning in serious_warnings if warning):
            return False, f"Content warnings: {content_warnings}"
        
        # Category relevance check
        title_desc = (video_data.get('title', '') + ' ' + 
                     video_data.get('description_preview', '')).lower()
        
        category_keywords = {
            'heartwarming': ['heartwarming', 'touching', 'emotional', 'reunion', 'surprise'],
            'funny': ['funny', 'comedy', 'humor', 'hilarious', 'joke'],
            'traumatic': ['accident', 'disaster', 'emergency', 'rescue', 'shocking']
        }
        
        keywords = category_keywords.get(category, [])
        if not any(keyword in title_desc for keyword in keywords):
            return False, f"No {category} keywords found"
        
        return True, "Valid"
    
    def collect_videos_enhanced(self, query: str, target_count: int, category: str) -> List[Dict]:
        """Enhanced video collection with comprehensive metadata"""
        collected = []
        
        self.add_log(f"Starting enhanced collection: {query} (category: {category})", "INFO")
        
        try:
            raw_results = self.invidious_api.search_videos(query, max_results=target_count * 3)
            
            for raw_video in raw_results:
                if len(collected) >= target_count:
                    break
                
                try:
                    # Extract comprehensive metadata
                    video_data = self.extract_enhanced_metadata(raw_video, query, category)
                    
                    if not video_data:
                        continue
                    
                    # Enhanced validation
                    is_valid, reason = self.validate_enhanced_video(video_data, category)
                    
                    if is_valid:
                        collected.append(video_data)
                        st.session_state.collector_stats['found'] += 1
                        self.add_log(f"✅ Enhanced collection: {video_data['title'][:50]}...", "SUCCESS")
                        self.add_log(f"   Quality: {video_data['video_quality_available']}", "INFO")
                        self.add_log(f"   Captions: {video_data['caption_languages']}", "INFO")
                    else:
                        st.session_state.collector_stats['rejected'] += 1
                        self.add_log(f"❌ Rejected: {reason}", "INFO")
                
                except Exception as e:
                    self.add_log(f"Error processing video: {str(e)}", "ERROR")
                    continue
                
                st.session_state.collector_stats['checked'] += 1
                time.sleep(0.5)
        
        except Exception as e:
            self.add_log(f"Enhanced collection failed: {str(e)}", "ERROR")
        
        self.add_log(f"Enhanced collection complete: {len(collected)} videos", "SUCCESS")
        return collected

class EnhancedVideoRater:
    """Enhanced video rater with new scoring weights and controlled comment analysis"""
    
    def __init__(self, api_key: str):
        self.youtube = build('youtube', 'v3', developerKey=api_key)
        self.logger = logging.getLogger(__name__)
        
        # New scoring weights (confirmed)
        self.scoring_weights = {
            'comment_analysis': 0.60,      # YouTube API comments (reduced from ~0.80)
            'engagement_metrics': 0.15,    # Enhanced with Invidious data
            'channel_authority': 0.05,     # New from Invidious subscriber data
            'content_match': 0.15,         # Existing title/description analysis
            'technical_quality': 0.05      # New HD/caption bonuses
        }
        
        # Channel authority thresholds
        self.channel_thresholds = {
            'mega': 10000000,      # 10M+ subscribers
            'large': 1000000,      # 1M+ subscribers  
            'medium': 100000,      # 100K+ subscribers
            'small': 10000,        # 10K+ subscribers
            'micro': 1000          # 1K+ subscribers
        }
    
    def add_log(self, message: str, log_type: str = "INFO"):
        """Add detailed log entry"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] RATER {log_type}: {message}"
        st.session_state.logs.insert(0, log_entry)
        st.session_state.logs = st.session_state.logs[:100]
    
    def fetch_controlled_comments(self, video_id: str, target_count: int = 400) -> Dict:
        """Fetch exactly target_count comments with additional API call if needed"""
        comments = []
        sentiment_data = {'positive': 0, 'negative': 0, 'neutral': 0, 'total': 0}
        
        try:
            # First API call - relevance order
            url = "https://www.googleapis.com/youtube/v3/commentThreads"
            params = {
                'part': 'snippet',
                'videoId': video_id,
                'maxResults': min(100, target_count),
                'order': 'relevance',
                'key': self.youtube._developerKey
            }
            
            response = requests.get(url, params=params)
            st.session_state.rater_stats['api_calls'] += 1
            
            if response.status_code == 200:
                data = response.json()
                for item in data.get('items', []):
                    comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay']
                    if len(comment_text.strip()) > 5 and comment_text not in comments:
                        comments.append(comment_text)
                        sentiment = self.analyze_sentiment(comment_text)
                        sentiment_data[sentiment] += 1
                        sentiment_data['total'] += 1
            
            # Second API call if we need more comments - time order
            if len(comments) < target_count:
                remaining_needed = target_count - len(comments)
                params.update({
                    'maxResults': min(100, remaining_needed),
                    'order': 'time'
                })
                
                response = requests.get(url, params=params)
                st.session_state.rater_stats['api_calls'] += 1
                
                if response.status_code == 200:
                    data = response.json()
                    for item in data.get('items', []):
                        if len(comments) >= target_count:
                            break
                        comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay']
                        if len(comment_text.strip()) > 5 and comment_text not in comments:
                            comments.append(comment_text)
                            sentiment = self.analyze_sentiment(comment_text)
                            sentiment_data[sentiment] += 1
                            sentiment_data['total'] += 1
            
            # Third API call if still need more comments
            if len(comments) < target_count:
                remaining_needed = target_count - len(comments)
                params.update({
                    'maxResults': min(100, remaining_needed),
                    'order': 'relevance'
                })
                
                # Use different parameters to get different results
                params['pageToken'] = data.get('nextPageToken', '')
                if params['pageToken']:
                    response = requests.get(url, params=params)
                    st.session_state.rater_stats['api_calls'] += 1
                    
                    if response.status_code == 200:
                        data = response.json()
                        for item in data.get('items', []):
                            if len(comments) >= target_count:
                                break
                            comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay']
                            if len(comment_text.strip()) > 5 and comment_text not in comments:
                                comments.append(comment_text)
                                sentiment = self.analyze_sentiment(comment_text)
                                sentiment_data[sentiment] += 1
                                sentiment_data['total'] += 1
            
            # Limit to exact target count
            comments = comments[:target_count]
            st.session_state.rater_stats['comments_analyzed'] += len(comments)
            
            self.add_log(f"Controlled comment fetch: {len(comments)}/{target_count} comments", "INFO")
            
            return {
                'comments': comments,
                'sentiment_analysis': sentiment_data,
                'total_fetched': len(comments),
                'target_reached': len(comments) == target_count
            }
            
        except Exception as e:
            self.add_log(f"Controlled comment fetch error: {str(e)}", "ERROR")
            return {
                'comments': [],
                'sentiment_analysis': sentiment_data,
                'total_fetched': 0,
                'target_reached': False
            }
    
    def analyze_sentiment(self, text: str) -> str:
        """Basic sentiment analysis"""
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
    
    def calculate_channel_authority_score(self, video_data: Dict) -> float:
        """Calculate channel authority score based on subscriber count"""
        try:
            subscriber_text = video_data.get('channel_subscriber_count', '0')
            
            # Parse subscriber count (could be in format like "1.2M", "500K", etc.)
            subscriber_count = 0
            if isinstance(subscriber_text, str) and subscriber_text:
                subscriber_text = subscriber_text.upper().replace(',', '')
                
                if 'M' in subscriber_text:
                    subscriber_count = float(subscriber_text.replace('M', '')) * 1000000
                elif 'K' in subscriber_text:
                    subscriber_count = float(subscriber_text.replace('K', '')) * 1000
                else:
                    try:
                        subscriber_count = float(subscriber_text)
                    except:
                        subscriber_count = 0
            
            # Score based on thresholds
            if subscriber_count >= self.channel_thresholds['mega']:
                return 1.0
            elif subscriber_count >= self.channel_thresholds['large']:
                return 0.8
            elif subscriber_count >= self.channel_thresholds['medium']:
                return 0.6
            elif subscriber_count >= self.channel_thresholds['small']:
                return 0.4
            elif subscriber_count >= self.channel_thresholds['micro']:
                return 0.2
            else:
                return 0.1  # Minimum score for unknown/small channels
                
        except Exception as e:
            self.add_log(f"Error calculating channel authority: {str(e)}", "ERROR")
            return 0.1
    
    def calculate_engagement_metrics_score(self, video_data: Dict) -> float:
        """Calculate enhanced engagement metrics score"""
        try:
            view_count = int(video_data.get('view_count', 0))
            like_count = int(video_data.get('like_count', 0))
            comment_count = int(video_data.get('comment_count', 0))
            
            if view_count == 0:
                return 0.0
            
            # Like ratio (likes per 1000 views)
            like_ratio = (like_count / view_count) * 1000
            
            # Comment ratio (comments per 1000 views)
            comment_ratio = (comment_count / view_count) * 1000
            
            # Combined engagement score
            engagement_score = min((like_ratio + comment_ratio * 2) / 20, 1.0)  # Scale to 0-1
            
            # Bonus from Invidious like/dislike ratio
            like_dislike_ratio_str = video_data.get('like_dislike_ratio', '0')
            try:
                like_dislike_ratio = float(like_dislike_ratio_str)
                engagement_score += like_dislike_ratio * 0.2  # Up to 0.2 bonus
            except:
                pass
            
            return min(engagement_score, 1.0)
            
        except Exception as e:
            self.add_log(f"Error calculating engagement metrics: {str(e)}", "ERROR")
            return 0.0
    
    def calculate_technical_quality_score(self, video_data: Dict) -> float:
        """Calculate technical quality score from Invidious metadata"""
        try:
            score = 0.0
            
            # HD availability bonus
            video_qualities = video_data.get('video_quality_available', '').split(',')
            hd_qualities = [q for q in video_qualities if q and int(q.replace('p', '')) >= 720]
            if hd_qualities:
                score += 0.5  # HD availability bonus
            
            # Caption languages bonus
            caption_languages = video_data.get('caption_languages', '').split(',')
            caption_languages = [lang for lang in caption_languages if lang.strip()]
            
            if len(caption_languages) >= 3:
                score += 0.3  # Multiple languages
            elif len(caption_languages) >= 1:
                score += 0.2  # At least one language
            
            # Channel verification bonus
            if video_data.get('channel_verified', '').lower() == 'true':
                score += 0.1
            
            # Age rating appropriateness
            age_rating = video_data.get('estimated_age_rating', '')
            if age_rating in ['G', 'PG']:  # Family-friendly content
                score += 0.1
            
            return min(score, 1.0)
            
        except Exception as e:
            self.add_log(f"Error calculating technical quality: {str(e)}", "ERROR")
            return 0.0
    
    def analyze_comments_for_category(self, comments: List[str], category: str) -> Dict:
        """Enhanced comment analysis with timestamped moments"""
        if not comments:
            return {
                'category_validation': 0.0,
                'emotional_alignment': 0.0,
                'authenticity_support': 0.0,
                'engagement_quality': 0.0,
                'timestamped_moments': [],
                'breakdown': {}
            }
        
        all_text = ' '.join(comments).lower()
        timestamped_moments = self.extract_timestamped_moments(comments, category)
        
        if category == 'heartwarming':
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
                'timestamped_moments': timestamped_moments,
                'breakdown': {
                    'positive_emotions': positive_count,
                    'authenticity_indicators': auth_count,
                    'fake_indicators': fake_count,
                    'timestamped_moments_found': len(timestamped_moments)
                }
            }
        
        elif category == 'funny':
            humor_words = ['laugh', 'funny', 'hilarious', 'lol', 'haha', 'comedy', 'joke']
            entertainment_words = ['entertaining', 'fun', 'enjoy', 'smile']
            boring_words = ['boring', 'not funny', 'stupid', 'lame']
            
            humor_count = sum(1 for word in humor_words if word in all_text)
            entertain_count = sum(1 for word in entertainment_words if word in all_text)
            boring_count = sum(1 for word in boring_words if word in all_text)
            
            validation = min(humor_count / max(len(comments) * 0.03, 1), 1.0)
            emotional = min(humor_count / max(len(comments) * 0.02, 1), 1.0)
            authenticity = 0.5 if humor_count == boring_count else min(0.8, max(0.2, humor_count / max(boring_count + 1, 1) * 0.4))
            
            return {
                'category_validation': validation,
                'emotional_alignment': emotional,
                'authenticity_support': authenticity,
                'engagement_quality': min(humor_count / max(len(comments), 1), 1.0),
                'timestamped_moments': timestamped_moments,
                'breakdown': {
                    'humor_reactions': humor_count,
                    'entertainment_validation': entertain_count,
                    'negative_reactions': boring_count,
                    'timestamped_moments_found': len(timestamped_moments)
                }
            }
        
        elif category == 'traumatic':
            empathy_words = ['prayers', 'sorry', 'sad', 'terrible', 'awful', 'devastating']
            concern_words = ['hope everyone ok', 'what happened', 'is everyone safe']
            inappropriate_words = ['lol', 'funny', 'cool', 'awesome']
            
            empathy_count = sum(1 for word in empathy_words if word in all_text)
            concern_count = sum(1 for phrase in concern_words if phrase in all_text)
            inappropriate_count = sum(1 for word in inappropriate_words if word in all_text)
            
            appropriate_total = empathy_count + concern_count
            
            validation = min(appropriate_total / max(len(comments) * 0.05, 1), 1.0)
            if inappropriate_count > appropriate_total:
                validation *= 0.3
                
            emotional = min(empathy_count / max(len(comments) * 0.03, 1), 1.0)
            authenticity = min(0.8, max(0.2, appropriate_total / max(inappropriate_count + 1, 1) * 0.3))
            
            return {
                'category_validation': validation,
                'emotional_alignment': emotional,
                'authenticity_support': authenticity,
                'engagement_quality': min(appropriate_total / max(len(comments), 1), 1.0),
                'timestamped_moments': timestamped_moments,
                'breakdown': {
                    'empathetic_responses': empathy_count,
                    'concern_responses': concern_count,
                    'inappropriate_responses': inappropriate_count,
                    'timestamped_moments_found': len(timestamped_moments)
                }
            }
        
        return {
            'category_validation': 0.5,
            'emotional_alignment': 0.5,
            'authenticity_support': 0.5,
            'engagement_quality': 0.5,
            'timestamped_moments': timestamped_moments,
            'breakdown': {}
        }
    
    def extract_timestamped_moments(self, comments: List[str], category: str) -> List[Dict]:
        """Extract timestamped comments for clippable moments"""
        timestamp_pattern = r'(?:at\s+)?(\d{1,2}):(\d{2})|(\d+:\d+)'
        moments = []
        
        category_keywords = {
            'heartwarming': ['crying', 'tears', 'emotional', 'touching', 'beautiful', 'best part', 'favorite moment'],
            'funny': ['laugh', 'hilarious', 'funny', 'lol', 'comedy', 'joke', 'humor'],
            'traumatic': ['shocking', 'unbelievable', 'devastating', 'terrible', 'awful', 'important', 'crucial moment']
        }
        
        clip_indicators = ['clip this', 'short', 'viral', 'best part', 'highlight', 'moment', 'scene', 'timestamp', 'here']
        
        for comment in comments:
            timestamps = re.findall(timestamp_pattern, comment)
            if timestamps:
                comment_lower = comment.lower()
                
                relevance_score = 0
                
                category_matches = sum(1 for kw in category_keywords.get(category, []) if kw in comment_lower)
                relevance_score += category_matches * 2
                
                clip_matches = sum(1 for ind in clip_indicators if ind in comment_lower)
                relevance_score += clip_matches * 1.5
                
                if len(comment) > 50:
                    relevance_score += 1
                
                strong_words = ['amazing', 'incredible', 'unbelievable', 'perfect', 'exactly', 'omg', 'wow']
                emotion_matches = sum(1 for word in strong_words if word in comment_lower)
                relevance_score += emotion_matches
                
                if relevance_score > 0:
                    for timestamp_match in timestamps:
                        timestamp = ':'.join(filter(None, timestamp_match))
                        
                        time_parts = timestamp.split(':')
                        seconds = int(time_parts[0]) * 60 + int(time_parts[1]) if len(time_parts) == 2 else 0
                        
                        moments.append({
                            'timestamp': timestamp,
                            'seconds': seconds,
                            'comment': comment,
                            'relevance_score': relevance_score,
                            'category_matches': category_matches,
                            'clip_potential': clip_matches > 0,
                            'sentiment': self.analyze_sentiment(comment)
                        })
        
        return sorted(moments, key=lambda x: (-x['relevance_score'], x['seconds']))
    
    def calculate_enhanced_score(self, video_data: Dict, comments_data: Dict, category: str) -> Dict:
        """Calculate final score using enhanced 5-component system"""
        
        # Component 1: Comment Analysis (60%)
        comments_analysis = self.analyze_comments_for_category(comments_data['comments'], category)
        comment_score = (
            comments_analysis['category_validation'] * 0.4 +
            comments_analysis['emotional_alignment'] * 0.3 +
            comments_analysis['authenticity_support'] * 0.3
        )
        
        # Component 2: Engagement Metrics (15%)
        engagement_score = self.calculate_engagement_metrics_score(video_data)
        
        # Component 3: Channel Authority (5%)
        authority_score = self.calculate_channel_authority_score(video_data)
        
        # Component 4: Content Match (15%)
        title_desc_text = (video_data.get('title', '') + ' ' + 
                          video_data.get('description_preview', '')).lower()
        
        category_keywords = {
            'heartwarming': ['heartwarming', 'touching', 'emotional', 'reunion', 'surprise', 'family', 'love'],
            'funny': ['funny', 'comedy', 'humor', 'hilarious', 'joke', 'laugh', 'entertaining'],
            'traumatic': ['accident', 'tragedy', 'disaster', 'emergency', 'breaking news', 'shocking']
        }
        
        keyword_matches = sum(1 for kw in category_keywords.get(category, []) if kw in title_desc_text)
        content_match_score = min(keyword_matches * 0.2, 1.0)
        
        # Component 5: Technical Quality (5%)
        technical_score = self.calculate_technical_quality_score(video_data)
        
        # Calculate weighted final score
        component_scores = {
            'comment_analysis': comment_score,
            'engagement_metrics': engagement_score,
            'channel_authority': authority_score,
            'content_match': content_match_score,
            'technical_quality': technical_score
        }
        
        weighted_score = sum(
            component_scores[component] * self.scoring_weights[component] 
            for component in self.scoring_weights
        )
        
        # Scale to 10-point system with category base scores
        category_base_scores = {
            'heartwarming': 3.0,
            'funny': 2.5,
            'traumatic': 4.0
        }
        
        base_score = category_base_scores.get(category, 3.0)
        final_score = base_score + (weighted_score * 7.0)
        
        # Apply bonuses and penalties
        if comments_analysis['category_validation'] > 0.8:
            final_score += 1.0
        if comments_analysis['authenticity_support'] < 0.2:
            final_score *= 0.6
        
        # Calculate confidence
        confidence = 0.3
        if len(comments_data['comments']) >= 300:
            confidence += 0.3
        if len(comments_data['comments']) >= 400:
            confidence += 0.2
        if comments_analysis['category_validation'] > 0.6:
            confidence += 0.2
        
        return {
            'final_score': min(final_score, 10.0),
            'confidence': min(confidence, 1.0),
            'component_scores': component_scores,
            'comments_analysis': comments_analysis,
            'comments_analyzed_count': len(comments_data['comments']),
            'target_comments_reached': comments_data.get('target_reached', False)
        }

# Streamlit UI Implementation
def display_rate_limit_stats(exporter):
    """Display rate limiting statistics in Streamlit sidebar"""
    try:
        stats = exporter.get_rate_limit_stats()
        
        with st.sidebar.expander("Google Sheets Rate Limits", expanded=False):
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Total Requests", stats['total_requests'])
                st.metric("This Minute", stats['requests_this_minute'])
            
            with col2:
                st.metric("Max/Second", stats['rate_limit_per_second'])
                st.metric("Last Request", stats['last_request_time'])
            
            # Progress bar for minute limit
            minute_progress = min(stats['requests_this_minute'] / 240.0, 1.0)  # 240 = 80% of 300 limit
            st.progress(minute_progress, f"Minute usage: {stats['requests_this_minute']}/240 (safe limit)")
            
            if stats['requests_this_minute'] > 200:
                st.warning("Approaching rate limit - processing will slow down")
            elif stats['requests_this_minute'] > 240:
                st.error("Rate limit safety threshold exceeded - waiting for reset")
    except Exception as e:
        st.error(f"Error displaying rate limit stats: {str(e)}")

def show_status_alert():
    """Display system status alerts"""
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

def main():
    st.markdown("""
    <div style="text-align: center; padding: 2rem 0; margin-bottom: 2rem; 
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                color: white; border-radius: 15px;">
        <h1>Enhanced YouTube Collection & Rating Tool</h1>
        <p><strong>Invidious API + YouTube API with Advanced Scoring</strong></p>
    </div>
    """, unsafe_allow_html=True)
    
    # Sidebar configuration
    with st.sidebar:
        st.header("Configuration")
        
        mode = st.radio("Select Mode:", ["Data Collector", "Video Rater"], horizontal=True)
        
        st.subheader("API Configuration")
        youtube_api_key = st.text_input("YouTube API Key", type="password", help="For Video Rater comment analysis")
        
        st.subheader("Google Sheets Configuration")
        creds_input_method = st.radio("Service Account JSON:", ["Paste JSON", "Upload JSON file"])
        
        sheets_creds = None
        if creds_input_method == "Paste JSON":
            sheets_creds_text = st.text_area("Service Account JSON", height=150)
            if sheets_creds_text:
                try:
                    sheets_creds = json.loads(sheets_creds_text)
                    st.success("Valid JSON")
                except json.JSONDecodeError as e:
                    st.error(f"Invalid JSON: {str(e)}")
        else:
            uploaded_file = st.file_uploader("Upload Service Account JSON", type=['json'])
            if uploaded_file:
                try:
                    sheets_creds = json.load(uploaded_file)
                    st.success("JSON file loaded")
                except Exception as e:
                    st.error(f"Error reading file: {str(e)}")
        
        spreadsheet_url = st.text_input(
            "Google Sheet URL",
            value="https://docs.google.com/spreadsheets/d/1PHvW-LykIpIbwKJbiGHi6NcX7hd4EsIWK3zwr4Dmvrk/",
            help="URL or ID of your Google Sheets document"
        )
        
        match = re.search(r'/d/([a-zA-Z0-9-_]+)', spreadsheet_url)
        spreadsheet_id = match.group(1) if match else spreadsheet_url
        
        if spreadsheet_id:
            st.success(f"Sheet ID: {spreadsheet_id[:20]}...")
        
        if sheets_creds and 'client_email' in sheets_creds:
            st.info(f"Service Account: {sheets_creds['client_email'][:30]}...")
            
            # Show rate limit monitoring if exporter exists
            if 'rate_limited_exporter' in st.session_state:
                display_rate_limit_stats(st.session_state.rate_limited_exporter)
    
    # Main content
    if mode == "Data Collector":
        st.subheader("🔗 Enhanced Data Collector (Invidious API)")
        
        show_status_alert()
        
        with st.sidebar:
            st.subheader("Collection Settings")
            category = st.selectbox("Content Category", options=['heartwarming', 'funny', 'traumatic', 'mixed'])
            target_count = st.number_input("Target Video Count", min_value=1, max_value=500, value=10)
            auto_export = st.checkbox("Auto-export to Google Sheets", value=True)
        
        # Statistics display with enhanced fields
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Videos Found", st.session_state.collector_stats['found'])
        with col2:
            st.metric("Videos Checked", st.session_state.collector_stats['checked'])
        with col3:
            st.metric("Videos Rejected", st.session_state.collector_stats['rejected'])
        with col4:
            st.metric("API Calls", st.session_state.collector_stats['api_calls'])
        
        # Control buttons
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("Start Enhanced Collection", disabled=st.session_state.is_collecting, type="primary"):
                clear_status()
                
                if not sheets_creds and auto_export:
                    set_status('error', "COLLECTION ABORTED: Google Sheets credentials required for auto-export")
                else:
                    st.session_state.is_collecting = True
                    
                    try:
                        # Create enhanced collector (Invidious only)
                        collector = EnhancedVideoCollector()
                        
                        # Initialize rate-limited exporter
                        if sheets_creds:
                            exporter = RateLimitedGoogleSheetsExporter(sheets_creds)
                            st.session_state.rate_limited_exporter = exporter
                        else:
                            exporter = None
                        
                        set_status('info', f"ENHANCED COLLECTION STARTED: Using Invidious API for {category} videos")
                        
                        # Collection phase with enhanced metadata
                        with st.spinner(f"Collecting {target_count} enhanced videos for {category}..."):
                            videos = []
                            
                            if category == 'mixed':
                                categories = ['heartwarming', 'funny', 'traumatic']
                                videos_per_category = target_count // 3
                                remainder = target_count % 3
                                
                                for i, cat in enumerate(categories):
                                    cat_target = videos_per_category + (1 if i < remainder else 0)
                                    
                                    # Use random query for each category
                                    queries = collector.search_queries[cat]
                                    query = random.choice(queries)
                                    
                                    cat_videos = collector.collect_videos_enhanced(query, cat_target, cat)
                                    videos.extend(cat_videos)
                            else:
                                query = random.choice(collector.search_queries[category])
                                videos = collector.collect_videos_enhanced(query, target_count, category)
                        
                        if len(videos) > 0:
                            set_status('success', f"ENHANCED COLLECTION COMPLETED: Found {len(videos)} videos with metadata")
                            
                            # Add to session state
                            st.session_state.collected_videos.extend(videos)
                            
                            # Export phase
                            if auto_export and sheets_creds:
                                try:
                                    sheet_url = exporter.export_to_enhanced_raw_links(videos, spreadsheet_id)
                                    
                                    if sheet_url:
                                        st.success("✅ Exported to Enhanced Google Sheets!")
                                        st.markdown(f"[Open Spreadsheet]({sheet_url})")
                                        set_status('success', f"EXPORT SUCCESS: {len(videos)} enhanced videos exported")
                                    else:
                                        set_status('error', "EXPORT FAILED: Could not get spreadsheet URL")
                                        
                                except Exception as e:
                                    set_status('error', f"EXPORT FAILED: {str(e)}")
                        else:
                            set_status('warning', "ENHANCED COLLECTION COMPLETED: No videos found")
                    
                    except Exception as e:
                        set_status('error', f"ENHANCED COLLECTION FAILED: {str(e)}")
                    finally:
                        st.session_state.is_collecting = False
                
                st.rerun()
        
        with col2:
            if st.button("Stop Collection", disabled=not st.session_state.is_collecting):
                set_status('warning', "ENHANCED COLLECTION STOPPED: Process terminated by user")
                st.session_state.is_collecting = False
                st.rerun()
        
        with col3:
            if st.button("Reset Stats"):
                st.session_state.collected_videos = []
                st.session_state.collector_stats = {'checked': 0, 'found': 0, 'rejected': 0, 'api_calls': 0}
                st.session_state.logs = []
                clear_status()
                st.rerun()
        
        # Display collected videos with enhanced metadata
        if st.session_state.collected_videos:
            st.subheader("Enhanced Collected Videos")
            
            # Create enhanced dataframe
            enhanced_df = pd.DataFrame(st.session_state.collected_videos)
            
            # Display key columns
            display_columns = ['title', 'category', 'view_count', 'duration_seconds', 'video_quality_available', 
                             'caption_languages', 'channel_subscriber_count', 'like_dislike_ratio']
            
            available_columns = [col for col in display_columns if col in enhanced_df.columns]
            
            st.dataframe(
                enhanced_df[available_columns],
                use_container_width=True,
                hide_index=True
            )
            
            # Show enhanced metadata summary
            with st.expander("Enhanced Metadata Summary", expanded=False):
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    hd_videos = sum(1 for video in st.session_state.collected_videos 
                                   if '720p' in video.get('video_quality_available', '') or 
                                      '1080p' in video.get('video_quality_available', ''))
                    st.metric("HD Videos", hd_videos)
                
                with col2:
                    captioned_videos = sum(1 for video in st.session_state.collected_videos 
                                         if video.get('caption_languages', ''))
                    st.metric("With Captions", captioned_videos)
                
                with col3:
                    avg_engagement = np.mean([float(video.get('like_dislike_ratio', '0')) 
                                            for video in st.session_state.collected_videos])
                    st.metric("Avg Like Ratio", f"{avg_engagement:.3f}")
    
    elif mode == "Video Rater":
        st.subheader("Enhanced Video Rater (YouTube API + New Scoring)")
        
        show_status_alert()
        
        # Enhanced statistics display
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Videos Rated", st.session_state.rater_stats['rated'])
        with col2:
            st.metric("Moved to tobe_links", st.session_state.rater_stats['moved_to_tobe'])
        with col3:
            st.metric("Comments Analyzed", st.session_state.rater_stats['comments_analyzed'])
        with col4:
            st.metric("API Calls", st.session_state.rater_stats['api_calls'])
        
        # Show enhanced scoring weights
        with st.sidebar:
            st.subheader("Enhanced Scoring Weights")
            st.info("""
            **New 5-Component System:**
            - Comment Analysis: 60%
            - Engagement Metrics: 15%
            - Content Match: 15%
            - Channel Authority: 5%
            - Technical Quality: 5%
            """)
            
            st.subheader("Comment Control")
            st.info("Max 400 comments per video for consistent analysis")
        
        if not youtube_api_key or not sheets_creds or not spreadsheet_id:
            set_status('warning', "RATING UNAVAILABLE: Missing YouTube API key, Google Sheets credentials, or spreadsheet URL")
        else:
            # Rating control
            col1, col2 = st.columns([1, 1])
            
            with col1:
                if st.button("Start Enhanced Rating", disabled=st.session_state.is_rating, type="primary"):
                    clear_status()
                    set_status('info', "ENHANCED RATING STARTED: Processing videos with new scoring system")
                    st.session_state.is_rating = True
                    st.rerun()
            
            with col2:
                if st.button("Stop Rating", disabled=not st.session_state.is_rating):
                    set_status('warning', "ENHANCED RATING STOPPED: Process terminated by user")
                    st.session_state.is_rating = False
                    st.rerun()
            
            if st.session_state.is_rating:
                try:
                    rater = EnhancedVideoRater(youtube_api_key)
                    
                    # Initialize rate-limited exporter for rater
                    if sheets_creds:
                        exporter = RateLimitedGoogleSheetsExporter(sheets_creds)
                        st.session_state.rate_limited_exporter = exporter
                    else:
                        exporter = None
                    
                    # Continuous enhanced rating loop
                    while st.session_state.is_rating:
                        # Get next video from enhanced raw_links
                        next_video = exporter.get_next_raw_video(spreadsheet_id)
                        
                        if not next_video:
                            set_status('info', "ENHANCED RATING COMPLETED: No more videos in raw_links")
                            st.session_state.is_rating = False
                            st.rerun()
                            break
                        
                        # Display enhanced video info
                        video_category = next_video.get('category', 'heartwarming')
                        
                        video_container = st.container()
                        
                        with video_container:
                            st.markdown("### Currently Processing (Enhanced):")
                            st.markdown(f"**Title:** {next_video.get('title', 'Unknown Title')}")
                            st.markdown(f"**Channel:** {next_video.get('channel_title', 'Unknown')}")
                            st.markdown(f"**Category:** {video_category}")
                            
                            # Enhanced metadata display
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric("Views", f"{int(next_video.get('view_count', 0)):,}")
                            with col2:
                                st.metric("Likes", f"{int(next_video.get('like_count', 0)):,}")
                            with col3:
                                quality = next_video.get('video_quality_available', 'Unknown')
                                st.metric("Quality", quality if quality else 'N/A')
                            with col4:
                                subs = next_video.get('channel_subscriber_count', 'Unknown')
                                st.metric("Subscribers", subs if subs else 'N/A')
                        
                        # Enhanced analysis with controlled comments
                        with st.spinner("Enhanced video analysis (400 comments max)..."):
                            video_id = next_video.get('video_id')
                            
                            if video_id:
                                try:
                                    # Fetch controlled comments (400 max)
                                    comments_data = rater.fetch_controlled_comments(video_id, target_count=400)
                                    
                                    # Calculate enhanced score with new weights
                                    analysis = rater.calculate_enhanced_score(next_video, comments_data, video_category)
                                    
                                    # Display enhanced score breakdown
                                    score = analysis['final_score']
                                    confidence = analysis['confidence']
                                    
                                    col1, col2 = st.columns([2, 1])
                                    
                                    with col2:
                                        st.markdown(f"""
                                        <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                                                   color: white; padding: 2rem; border-radius: 15px; text-align: center;">
                                            <h2>Enhanced Score</h2>
                                            <h1 style="font-size: 3rem;">{score:.1f}/10</h1>
                                            <p>Confidence: {confidence:.0%}</p>
                                            <p>Comments: {analysis['comments_analyzed_count']}/400</p>
                                        </div>
                                        """, unsafe_allow_html=True)
                                    
                                    with col1:
                                        st.subheader("Score Breakdown")
                                        
                                        # Component scores with weights
                                        components = analysis['component_scores']
                                        weights = rater.scoring_weights
                                        
                                        for component, score_val in components.items():
                                            weight = weights[component]
                                            weighted_contribution = score_val * weight
                                            
                                            st.write(f"**{component.replace('_', ' ').title()}**")
                                            st.write(f"Score: {score_val:.3f} × Weight: {weight:.0%} = {weighted_contribution:.3f}")
                                            st.progress(score_val)
                                        
                                        # Timestamped moments
                                        moments = analysis['comments_analysis'].get('timestamped_moments', [])
                                        if moments:
                                            st.subheader("Top Timestamped Moments")
                                            for moment in moments[:3]:
                                                st.markdown(f"""
                                                <div style="background: #2d3748; color: #e2e8f0; padding: 0.8rem;
                                                           border-radius: 6px; margin: 0.5rem 0; border-left: 3px solid #4299e1;">
                                                    <strong>{moment['timestamp']}</strong><br>
                                                    <em>"{moment['comment'][:80]}{'...' if len(moment['comment']) > 80 else ''}"</em>
                                                </div>
                                                """, unsafe_allow_html=True)
                                    
                                    # Process the video automatically
                                    video_url = next_video.get('url', '')
                                    
                                    # Delete from raw_links first
                                    exporter.delete_raw_video(spreadsheet_id, next_video['row_number'])
                                    
                                    # If score >= 6.5, add to enhanced tobe_links
                                    if score >= 6.5:
                                        exporter.add_to_enhanced_tobe_links(spreadsheet_id, next_video, analysis)
                                        
                                        st.session_state.rater_stats['moved_to_tobe'] += 1
                                        st.success(f"Enhanced Score: {score:.1f}/10 - Moved to tobe_links!")
                                        
                                        rater.add_log(f"Enhanced rating: {next_video.get('title', '')[:50]} scored {score:.1f} - moved to tobe_links", "SUCCESS")
                                    else:
                                        st.info(f"Enhanced Score: {score:.1f}/10 - Below threshold, removed from raw_links.")
                                        rater.add_log(f"Enhanced rating: {next_video.get('title', '')[:50]} scored {score:.1f} - removed", "INFO")
                                    
                                    st.session_state.rater_stats['rated'] += 1
                                    
                                    # Brief pause before next video
                                    time.sleep(2)
                                    video_container.empty()
                                
                                except Exception as e:
                                    set_status('error', f"ENHANCED RATING ERROR: {str(e)}")
                                    rater.add_log(f"Enhanced rating error: {str(e)}", "ERROR")
                                    
                                    # Still remove from raw_links to avoid infinite loop
                                    exporter.delete_raw_video(spreadsheet_id, next_video['row_number'])
                                    time.sleep(1)
                            else:
                                set_status('error', "RATING ERROR: Video has no ID - skipping")
                                exporter.delete_raw_video(spreadsheet_id, next_video['row_number'])
                        
                        # Continue loop
                        if st.session_state.is_rating:
                            time.sleep(0.5)
                            st.rerun()
                
                except Exception as e:
                    set_status('error', f"ENHANCED RATING SYSTEM FAILURE: {str(e)}")
                    st.session_state.is_rating = False
    
    # Enhanced Activity log with filtering
    with st.expander("Enhanced Activity Log", expanded=False):
        col1, col2 = st.columns([3, 1])
        
        with col1:
            log_levels = ["All", "SUCCESS", "INFO", "WARNING", "ERROR"]
            selected_level = st.selectbox("Filter logs:", log_levels)
        
        with col2:
            if st.button("Clear Logs"):
                st.session_state.logs = []
                st.rerun()
        
        # Display filtered logs
        logs_to_show = st.session_state.logs[:30]  # Show last 30 logs
        
        if selected_level != "All":
            logs_to_show = [log for log in logs_to_show if selected_level in log]
        
        if logs_to_show:
            for log in logs_to_show:
                if "SUCCESS" in log:
                    st.success(log)
                elif "ERROR" in log:
                    st.error(log)
                elif "WARNING" in log:
                    st.warning(log)
                else:
                    st.info(log)
        else:
            st.info("No logs to display")


if __name__ == "__main__":
    main()