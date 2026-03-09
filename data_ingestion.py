"""
Multi-Source Data Pipeline for Perception Engine
Architectural Choice: Separation of concerns with dedicated handlers for each data source
allows independent scaling and failure isolation. Using async/await for non-blocking I/O.
"""
import pandas as pd
import numpy as np
import requests
import ccxt
from datetime import datetime, timedelta
import asyncio
import aiohttp
from typing import Dict, List, Optional, Any, Tuple
import logging
import time
from dataclasses import dataclass
import json

from firebase_setup import firebase_manager

logger = logging.getLogger(__name__)

@dataclass
class DataSourceConfig:
    """Configuration for data sources with retry logic"""
    name: str
    endpoint: str
    api_key: Optional[str] = None
    rate_limit: int = 10  # requests per minute
    timeout: int = 30
    retry_attempts: int = 3
    retry_delay: int = 5  # seconds

class PerceptionEngine:
    """Main data ingestion and synthesis engine with resilience"""
    
    def __init__(self):
        self.firestore = None
        self.realtime_db = None
        self.session = None
        self.data_cache = {}
        self.cache_ttl = 300  # 5 minutes in seconds
        
        # Initialize data source configurations
        self.sources = {
            'dex_screener': DataSourceConfig(
                name='dex_screener',
                endpoint='https://api.dexscreener.com/latest/dex/tokens/',
                rate_limit=60
            ),
            'base_rpc': DataSourceConfig(
                name='base_rpc',
                endpoint='https://mainnet.base.org',
                rate_limit=100
            ),
            'coingecko': DataSourceConfig(
                name='coingecko',
                endpoint='https://api.coingecko.com/api/v3/',
                rate_limit=50
            )
        }
        
        # Rate limiting tracking
        self.request_timestamps = {source: [] for source in self.sources}
        
    async def initialize(self) -> bool:
        """Async initialization with dependency setup"""
        try:
            # Initialize Firebase
            if not firebase_manager.initialize():
                logger.error("Failed to initialize Firebase")
                return False
            
            self.firestore = firebase_manager.get_firestore()
            self.realtime_db = firebase_manager.get_realtime_db()
            
            # Create aiohttp session for async requests
            self.session = aiohttp.ClientSession()
            
            logger.info("PerceptionEngine initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize PerceptionEngine: {e}")
            return False
    
    def _check_rate_limit(self, source_name: str) -> bool:
        """Check if we're within rate limits for a source"""
        now = time.time()
        timestamps = self.request_timestamps[source_name]
        
        # Remove timestamps older than 1 minute
        timestamps = [ts for ts in timestamps if now - ts < 60]
        self.request_timestamps[source_name] = timestamps
        
        # Check if under limit
        source_config = self.sources.get(source_name)
        if not source_config:
            return False
            
        if len(timestamps) < source_config.rate_limit:
            timestamps.append(now)
            return True
        return False
    
    async def _fetch_with_retry(self, source: DataSourceConfig, url: str, params: Dict = None) -> Optional[Dict]:
        """Fetch data with exponential backoff retry logic"""
        headers = {}
        if source.api_key:
            headers['Authorization'] = f'Bearer {source.api_key}'
        
        for attempt in range(source.retry_attempts):
            try:
                # Check rate limit
                if not self._check_rate_limit(source.name):
                    wait_time = 60 / source.rate_limit
                    logger.warning(f"Rate limit reached for {source.name}, waiting {wait_time}s")
                    await asyncio.sleep(wait_time)
                    continue
                
                async with self.session.get(
                    url, 
                    params=params, 
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=source.timeout)
                ) as response:
                    
                    if response.status == 200:
                        data = await response.json()
                        return data
                    elif response.status == 429:  # Rate limited
                        retry_after = int(response.headers.get('Retry-After', 30))
                        logger.warning(f"Rate limited by {source.name}, retrying in {retry_after}s")
                        await asyncio.sleep(retry_after)
                        continue
                    else:
                        logger.error(f"HTTP {response.status} from {source.name}")
                        
            except asyncio.TimeoutError:
                logger.warning(f"Timeout fetching from {source.name}, attempt {attempt + 1}")
            except Exception as e:
                logger.error(f"Error fetching from {source.name}: {e}")
            
            # Exponential backoff
            if attempt < source.retry_attempts - 1:
                delay = source.retry_delay * (2 ** attempt)
                logger.info(f"Retrying {source.name} in {delay}s")
                await asyncio.sleep(delay)
        
        logger.error(f"Failed to fetch from {source.name} after {source.retry_attempts} attempts")
        return None
    
    async def fetch_dex_data(self, token_address: Optional[str] = None) -> Dict:
        """Fetch DEX data from DexScreener with caching"""
        cache_key = f"dex_data_{token_address or 'global'}"
        
        # Check cache first
        if cache_key in self.data_cache:
            cached_time, cached_data = self.data_cache[cache_key]
            if time.time() - cached_time < self.cache_ttl:
                return cached_data
        
        source = self.sources['dex_screener']
        url = source.endpoint
        
        if token_address:
            url = f"{url}{token_address}"
        
        data = await self._fetch_with_retry(source, url)
        
        if data:
            # Update cache
            self.data_cache[cache_key] = (time.time(), data)
            
            # Store in Firebase
            await self._store_data('data_streams/on_chain/dex', data)
            
        return data or {}
    
    async def fetch_base_token_metrics(self) -> Dict:
        """Fetch token metrics from Base chain"""
        # For now, use Coingecko as Base RPC alternative
        source = self.sources['coingecko']
        
        # Top Base tokens by market cap
        params = {
            'vs_currency': 'usd',
            'order': 'market_cap_desc',
            'per_page': 50,
            'page': 1,
            'sparkline': False
        }
        
        url = f"{source.endpoint}coins/markets"
        data = await self._fetch_with_retry(source, url, params)
        
        if data:
            # Filter for Base ecosystem tokens (simplified logic)
            base_tokens = [
                token for token in data 
                if 'base' in token.get('id', '').lower() or 
                   'base' in str(token.get('platforms', {})).lower()
            ][:10]  # Top 10
            
            metrics = {
                'timestamp': datetime.now().isoformat(),
                'tokens': base_tokens,
                'total_market_cap': sum(t.get('market_cap', 0) for t in base_tokens),
                'avg_24h_change': np.mean([t.get('price_change_percentage_24h', 0) for t in base_tokens])
            }
            
            await self._store_data('data_streams/on_chain/token_metrics', metrics)
            return metrics
        
        return {}
    
    def _calculate_liquidity_velocity(self, pool_data: List[Dict]) -> float:
        """Measure rate of liquidity change across pools with validation"""
        try:
            if not pool_data or len(pool_data) < 2:
                return 0.0
            
            # Extract liquidity values
            liquidity_values = []
            for pool in pool_data:
                if isinstance(pool, dict):
                    # Handle different possible field names
                    liquidity = pool.get('liquidityUSD') or pool.get('liquidity') or pool.get('liquidity_usd')
                    if liquidity and isinstance(liquidity, (int, float)):
                        liquidity_values.append(float(liquidity))
            
            if len(liquidity_values) < 2:
                return 0.0
            
            # Calculate percentage changes
            df = pd.Series(liquidity_values)
            pct_changes = df.pct_change().dropna()
            
            if len(pct_changes) == 0:
                return 0.0
            
            # Calculate velocity as std dev of changes adjusted for sample size
            velocity = pct_changes.std() * np.sqrt(len(pct_changes))
            
            # Ensure reasonable bounds
            velocity = float(np.clip(velocity, 0, 10))
            
            return velocity
            
        except Exception as e:
            logger.error(f"Error calculating liquidity velocity: {e}")
            return 0.0
    
    def _analyze_sentiment_quality(self, social_data: Dict) -> float:
        """Basic sarcasm/authenticity detection with edge case handling"""
        try:
            if not social_data or 'tweets' not in social_data:
                return 0.5  # Neutral default
            
            tweets = social_data['tweets']
            if not isinstance(tweets, list) or len(tweets) == 0:
                return 0.5
            
            suspicious_patterns = [
                r'100x', r'guaranteed', r'to the moon', r'diamond hands',
                r'this is financial advice', r'trust me bro',
                r'DYOR', r'NOT A FINANCIAL ADVISOR',
                r'\$[A-Z]{2,5}.*(moon|rocket|🚀|🌕)',
                r'(buy|load).*(now|immediately|ASAP)'
            ]
            
            import re
            suspicious_count = 0
            total_tweets = len(tweets)
            
            for tweet in tweets:
                if not isinstance(tweet, str):
                    continue
                    
                tweet_lower = tweet.lower()
                
                # Check for multiple suspicious patterns
                pattern_matches = sum(
                    1 for pattern in suspicious_patterns 
                    if re.search(pattern, tweet_lower, re.IGNORECASE)
                )
                
                if pattern_matches >= 2:  # Multiple red flags
                    suspicious_count += 1
                elif pattern_matches == 1:
                    suspicious_count += 0.5  # Partial flag
            
            if total_tweets == 0:
                return 0.5
            
            quality_score = 1 - (suspicious_count / total_tweets)
            
            # Clamp to [0.1, 1.0] range
            quality_score = max(0.1, min(1.0, quality_score))
            
            return round(quality_score, 3)
            
        except Exception as e:
            logger.error(f"Error