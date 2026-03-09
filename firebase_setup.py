"""
Firebase Foundation for Project Alchemy
Architectural Choice: Firebase provides real-time synchronization and offline persistence
which is critical for momentum trading where milliseconds matter.
Using Firestore for structured data and Realtime DB for streaming metrics.
"""
import firebase_admin
from firebase_admin import credentials, firestore
from firebase_admin import db as realtime_db
import os
import json
from typing import Dict, Any
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FirebaseManager:
    """Robust Firebase initialization and schema management with error recovery"""
    
    def __init__(self, credential_path: str = "firebase_credentials.json"):
        self.credential_path = credential_path
        self.app = None
        self.firestore_db = None
        self.realtime_db = None
        self._initialized = False
        
    def initialize(self) -> bool:
        """Initialize Firebase with fallback strategies"""
        try:
            # Check if credentials file exists
            if not os.path.exists(self.credential_path):
                logger.error(f"Firebase credentials file not found: {self.credential_path}")
                return False
            
            # Initialize with credentials
            cred = credentials.Certificate(self.credential_path)
            self.app = firebase_admin.initialize_app(cred, {
                'databaseURL': 'https://project-alchemy-default-rtdb.firebaseio.com/'
            })
            
            # Initialize database instances
            self.firestore_db = firestore.client()
            self.realtime_db = realtime_db.reference('/')
            
            # Create initial schema
            self._create_schema()
            
            self._initialized = True
            logger.info("Firebase initialized successfully")
            return True
            
        except FileNotFoundError as e:
            logger.error(f"Credentials file not found: {e}")
            return False
        except ValueError as e:
            logger.error(f"Invalid Firebase configuration: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error initializing Firebase: {e}")
            return False
    
    def _create_schema(self) -> None:
        """Create initial database schema with collections and documents"""
        try:
            # Firestore collections schema
            schema = {
                'data_streams': {
                    'on_chain': {
                        'description': 'On-chain metrics and whale tracking',
                        'created_at': datetime.now().isoformat()
                    },
                    'social': {
                        'description': 'Social sentiment and narrative data',
                        'created_at': datetime.now().isoformat()
                    },
                    'derivative': {
                        'description': 'Perp DEX and funding rate analytics',
                        'created_at': datetime.now().isoformat()
                    },
                    'ecosystem': {
                        'description': 'GitHub activity and developer metrics',
                        'created_at': datetime.now().isoformat()
                    }
                },
                'signals': {
                    'active_signals': {},
                    'signal_history': {}
                },
                'trades': {
                    'pending_trades': {},
                    'trade_history': {}
                },
                'model_state': {
                    'pattern_weights': {},
                    'performance_metrics': {}
                },
                'system_health': {
                    'last_updated': datetime.now().isoformat(),
                    'component_status': {}
                }
            }
            
            # Write schema to Firestore
            for collection_name, collection_data in schema.items():
                doc_ref = self.firestore_db.collection('schema').document(collection_name)
                doc_ref.set(collection_data)
            
            # Initialize Realtime DB structure
            self.realtime_db.update({
                'heartbeat': datetime.now().isoformat(),
                'system_status': 'initializing'
            })
            
            logger.info("Database schema created successfully")
            
        except Exception as e:
            logger.error(f"Error creating schema: {e}")
            raise
    
    def get_firestore(self):
        """Get Firestore instance with validation"""
        if not self._initialized:
            raise RuntimeError("Firebase not initialized. Call initialize() first.")
        return self.firestore_db
    
    def get_realtime_db(self):
        """Get Realtime Database instance with validation"""
        if not self._initialized:
            raise RuntimeError("Firebase not initialized. Call initialize() first.")
        return self.realtime_db
    
    def update_heartbeat(self) -> None:
        """Update system heartbeat for monitoring"""
        if self._initialized and self.realtime_db:
            try:
                self.realtime_db.update({
                    'heartbeat': datetime.now().isoformat(),
                    'last_update': datetime.now().timestamp()
                })
            except Exception as e:
                logger.warning(f"Failed to update heartbeat: {e}")

# Singleton instance for global access
firebase_manager = FirebaseManager()