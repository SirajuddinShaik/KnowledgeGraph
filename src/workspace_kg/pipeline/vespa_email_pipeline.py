#!/usr/bin/env python3
"""
Vespa Email Processing Pipeline with Progress Tracking

Complete pipeline that:
1. Maintains JSON progress file to track processed emails
2. Fetches only unprocessed emails from Vespa 
3. Extracts entities and relationships with email source tracking
4. Merges and stores data in Kuzu database
5. Updates progress file after successful processing
6. Supports resume from previous runs

This pipeline provides stateful end-to-end processing from Vespa to Knowledge Graph.
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set
from pathlib import Path
import hashlib
from dotenv import load_dotenv
# Load environment variables from .env file
load_dotenv()

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

# Import Vespa integration
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from workspace_kg.utils.vespa_integration import VespaConnector, VespaConfig, VespaDataProcessor

# Import knowledge graph components
from workspace_kg.components.entity_extractor import EntityExtractor
from workspace_kg.utils.merge_pipeline import MergePipeline
from workspace_kg.utils.prompt_factory import DataType

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EmailProgressTracker:
    """Tracks email processing progress using JSON file"""
    
    def __init__(self, progress_file: str = "data/email_processing_progress.json"):
        self.progress_file = Path(progress_file)
        self.progress_data = {
            "metadata": {
                "created_at": None,
                "last_updated": None,
                "total_emails_discovered": 0,
                "total_emails_processed": 0,
                "total_emails_failed": 0,
                "processing_sessions": 0
            },
            "processed_emails": {},  # email_id -> processing_info
            "failed_emails": {},     # email_id -> error_info
            "current_session": {
                "session_id": None,
                "started_at": None,
                "emails_in_session": 0,
                "entities_extracted": 0,
                "relationships_extracted": 0
            }
        }
        
        # Load existing progress
        self._load_progress()
    
    def _load_progress(self) -> None:
        """Load progress from JSON file"""
        try:
            if self.progress_file.exists():
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    loaded_data = json.load(f)
                    # Merge with default structure to handle schema updates
                    self._merge_progress_data(loaded_data)
                logger.info(f"📖 Loaded progress from {self.progress_file}")
                logger.info(f"   📊 Previously processed: {len(self.progress_data['processed_emails'])} emails")
                logger.info(f"   ❌ Previously failed: {len(self.progress_data['failed_emails'])} emails")
            else:
                logger.info(f"📄 No existing progress file found, starting fresh")
                self.progress_data["metadata"]["created_at"] = datetime.now().isoformat()
                
        except Exception as e:
            logger.error(f"❌ Error loading progress file: {e}")
            logger.info("🔄 Starting with fresh progress data")
    
    def _merge_progress_data(self, loaded_data: Dict[str, Any]) -> None:
        """Merge loaded data with default structure"""
        try:
            # Update metadata
            if "metadata" in loaded_data:
                self.progress_data["metadata"].update(loaded_data["metadata"])
            
            # Update processed emails
            if "processed_emails" in loaded_data:
                self.progress_data["processed_emails"] = loaded_data["processed_emails"]
            
            # Update failed emails
            if "failed_emails" in loaded_data:
                self.progress_data["failed_emails"] = loaded_data["failed_emails"]
                
        except Exception as e:
            logger.error(f"❌ Error merging progress data: {e}")
    
    def _save_progress(self) -> None:
        """Save progress to JSON file"""
        try:
            # Ensure directory exists
            self.progress_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Update metadata
            self.progress_data["metadata"]["last_updated"] = datetime.now().isoformat()
            self.progress_data["metadata"]["total_emails_processed"] = len(self.progress_data["processed_emails"])
            self.progress_data["metadata"]["total_emails_failed"] = len(self.progress_data["failed_emails"])
            
            # Save to file
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(self.progress_data, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            logger.error(f"❌ Error saving progress file: {e}")
    
    def start_session(self) -> str:
        """Start a new processing session"""
        session_id = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.progress_data["current_session"] = {
            "session_id": session_id,
            "started_at": datetime.now().isoformat(),
            "emails_in_session": 0,
            "entities_extracted": 0,
            "relationships_extracted": 0
        }
        self.progress_data["metadata"]["processing_sessions"] += 1
        logger.info(f"🚀 Started processing session: {session_id}")
        return session_id
    
    def is_email_processed(self, email_id: str) -> bool:
        """Check if email has been successfully processed"""
        return email_id in self.progress_data["processed_emails"]
    
    def is_email_failed(self, email_id: str) -> bool:
        """Check if email has previously failed processing"""
        return email_id in self.progress_data["failed_emails"]
    
    def get_processed_email_ids(self) -> Set[str]:
        """Get set of all processed email IDs"""
        return set(self.progress_data["processed_emails"].keys())
    
    def get_failed_email_ids(self) -> Set[str]:
        """Get set of all failed email IDs"""
        return set(self.progress_data["failed_emails"].keys())
    
    def mark_email_processed(self, email_id: str, extraction_result: Dict[str, Any]) -> None:
        """Mark email as successfully processed"""
        self.progress_data["processed_emails"][email_id] = {
            "processed_at": datetime.now().isoformat(),
            "session_id": self.progress_data["current_session"]["session_id"],
            "entity_count": extraction_result.get("entity_count", 0),
            "relationship_count": extraction_result.get("relationship_count", 0),
            "processing_time_ms": extraction_result.get("processing_time_ms", 0)
        }
        
        # Update session stats
        self.progress_data["current_session"]["emails_in_session"] += 1
        self.progress_data["current_session"]["entities_extracted"] += extraction_result.get("entity_count", 0)
        self.progress_data["current_session"]["relationships_extracted"] += extraction_result.get("relationship_count", 0)
        
        # Remove from failed if it was there
        if email_id in self.progress_data["failed_emails"]:
            del self.progress_data["failed_emails"][email_id]
        
        logger.debug(f"✅ Marked email {email_id} as processed")
    
    def mark_email_failed(self, email_id: str, error: str) -> None:
        """Mark email as failed processing"""
        self.progress_data["failed_emails"][email_id] = {
            "failed_at": datetime.now().isoformat(),
            "session_id": self.progress_data["current_session"]["session_id"],
            "error": str(error)
        }
        logger.debug(f"❌ Marked email {email_id} as failed: {error}")
    
    def filter_unprocessed_emails(self, emails: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter out already processed emails"""
        processed_ids = self.get_processed_email_ids()
        unprocessed = []
        
        for email in emails:
            email_id = email.get('id', '')
            if email_id and email_id not in processed_ids:
                unprocessed.append(email)
        
        logger.info(f"📋 Filtered emails: {len(emails)} total → {len(unprocessed)} unprocessed")
        return unprocessed
    
    def get_progress_summary(self) -> Dict[str, Any]:
        """Get comprehensive progress summary"""
        total_discovered = (
            len(self.progress_data["processed_emails"]) + 
            len(self.progress_data["failed_emails"])
        )
        
        return {
            "total_emails_discovered": total_discovered,
            "total_emails_processed": len(self.progress_data["processed_emails"]),
            "total_emails_failed": len(self.progress_data["failed_emails"]),
            "processing_sessions": self.progress_data["metadata"]["processing_sessions"],
            "created_at": self.progress_data["metadata"]["created_at"],
            "last_updated": self.progress_data["metadata"]["last_updated"],
            "current_session": self.progress_data["current_session"],
            "success_rate": (
                len(self.progress_data["processed_emails"]) / total_discovered * 100
                if total_discovered > 0 else 0
            )
        }
    
    def save(self) -> None:
        """Save current progress to file"""
        self._save_progress()
    
    def reset_failed_emails(self) -> int:
        """Reset failed emails to allow retry. Returns count of reset emails."""
        count = len(self.progress_data["failed_emails"])
        self.progress_data["failed_emails"] = {}
        logger.info(f"🔄 Reset {count} failed emails for retry")
        return count

class VespaEmailPipelineConfig:
    """Configuration for Vespa Email Pipeline"""
    
    def __init__(self):
        # Vespa configuration
        self.vespa_endpoint = os.getenv('VESPA_ENDPOINT', 'http://localhost:8080')
        self.vespa_schema = os.getenv('VESPA_SCHEMA', 'mail')
        self.vespa_namespace = os.getenv('VESPA_NAMESPACE', 'namespace')
        self.vespa_cluster = os.getenv('VESPA_CLUSTER', 'my_content')
        
        # Processing configuration
        self.batch_size = int(os.getenv('VESPA_BATCH_SIZE', '50'))
        self.max_emails = int(os.getenv('VESPA_MAX_EMAILS', '1000'))
        self.parallel_extractions = int(os.getenv('PARALLEL_EXTRACTIONS', '5'))
        
        # Entity extraction configuration
        self.llm_model = os.getenv('LLM_MODEL_NAME', 'gemini-2.5-flash')
        self.auto_detect_data_type = os.getenv('AUTO_DETECT_DATA_TYPE', 'false').lower() == 'true'
        
        # Database configuration
        self.kuzu_url = os.getenv('KUZU_URL', 'http://localhost:7000')
        
        # Progress tracking configuration
        self.progress_file = os.getenv('EMAIL_PROGRESS_FILE', 'data/email_processing_progress.json')
        self.save_extracted_data = os.getenv('SAVE_EXTRACTED_DATA', 'true').lower() == 'true'
        self.output_dir = os.getenv('PIPELINE_OUTPUT_DIR', 'data/pipeline_outputs')
        
        # Retry configuration
        self.retry_failed_emails = os.getenv('RETRY_FAILED_EMAILS', 'false').lower() == 'true'
        self.max_retries = int(os.getenv('MAX_RETRIES', '3'))
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return {
            'vespa_endpoint': self.vespa_endpoint,
            'vespa_schema': self.vespa_schema,
            'vespa_namespace': self.vespa_namespace,
            'vespa_cluster': self.vespa_cluster,
            'batch_size': self.batch_size,
            'max_emails': self.max_emails,
            'parallel_extractions': self.parallel_extractions,
            'llm_model': self.llm_model,
            'auto_detect_data_type': self.auto_detect_data_type,
            'kuzu_url': self.kuzu_url,
            'progress_file': self.progress_file,
            'save_extracted_data': self.save_extracted_data,
            'output_dir': self.output_dir,
            'retry_failed_emails': self.retry_failed_emails,
            'max_retries': self.max_retries
        }

class VespaEmailPipeline:
    """Main pipeline class for processing emails from Vespa to Knowledge Graph with progress tracking"""
    
    def __init__(self, config: Optional[VespaEmailPipelineConfig] = None):
        self.config = config or VespaEmailPipelineConfig()
        self.progress_tracker = EmailProgressTracker(self.config.progress_file)
        self.vespa_connector = None
        self.entity_extractor = None
        self.merge_pipeline = None
        
        # Statistics tracking
        self.stats = {
            'start_time': None,
            'end_time': None,
            'emails_fetched': 0,
            'emails_skipped': 0,
            'emails_processed': 0,
            'emails_failed': 0,
            'entities_extracted': 0,
            'relationships_extracted': 0,
            'entities_merged': 0,
            'relationships_merged': 0,
            'errors': [],
            'processing_time_seconds': 0
        }
        
        # Create output directory
        Path(self.config.output_dir).mkdir(parents=True, exist_ok=True)
    
    async def initialize(self) -> bool:
        """Initialize all pipeline components"""
        try:
            logger.info("🚀 Initializing Vespa Email Pipeline with Progress Tracking")
            
            # Show progress summary at start
            progress_summary = self.progress_tracker.get_progress_summary()
            logger.info("📊 Previous Progress Summary:")
            logger.info(f"   📧 Total emails discovered: {progress_summary['total_emails_discovered']}")
            logger.info(f"   ✅ Successfully processed: {progress_summary['total_emails_processed']}")
            logger.info(f"   ❌ Failed processing: {progress_summary['total_emails_failed']}")
            logger.info(f"   🎯 Success rate: {progress_summary['success_rate']:.1f}%")
            logger.info(f"   🔄 Processing sessions: {progress_summary['processing_sessions']}")
            
            # Initialize Vespa connector
            vespa_config = VespaConfig(
                endpoint=self.config.vespa_endpoint,
                application_name="unstructured_data",
                schema_name=self.config.vespa_schema,
                namespace=self.config.vespa_namespace
            )
            self.vespa_connector = VespaConnector(vespa_config)
            
            # Initialize entity extractor with email source tracking
            self.entity_extractor = EntityExtractor(
                model=self.config.llm_model,
                data_type=DataType.EMAIL,
                auto_detect_data_type=self.config.auto_detect_data_type
            )
            
            # Initialize merge pipeline
            self.merge_pipeline = MergePipeline(self.config.kuzu_url)
            await self.merge_pipeline.initialize()
            
            logger.info("✅ Pipeline initialization completed")
            
            return True
            
        except Exception as e:
            error_msg = f"❌ Pipeline initialization failed: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            return False
    
    async def fetch_unprocessed_emails(self) -> List[Dict[str, Any]]:
        """Fetch emails from Vespa, filtering out already processed ones"""
        try:
            logger.info("📧 Fetching emails from Vespa")
            logger.info(f"🔗 Vespa endpoint: {self.config.vespa_endpoint}")
            logger.info(f"📋 Schema: {self.config.vespa_schema}")
            logger.info(f"📊 Max emails: {self.config.max_emails}")
            logger.info(f"📦 Batch size: {self.config.batch_size}")
            
            async with self.vespa_connector as connector:
                # Test connection first
                if not await connector.test_connection():
                    raise Exception("Failed to connect to Vespa")
                
                # Use visit API to fetch documents
                processor = VespaDataProcessor(connector)
                vespa_documents = await processor.get_all_documents_via_visit(
                    schema=self.config.vespa_schema,
                    max_documents=self.config.max_emails,
                    wanted_document_count=self.config.batch_size
                )
                
                # Convert to format expected by entity extractor
                all_emails = processor.prepare_for_entity_extraction(vespa_documents)
                
                # Filter out already processed emails
                unprocessed_emails = self.progress_tracker.filter_unprocessed_emails(all_emails)
                
                self.stats['emails_fetched'] = len(all_emails)
                self.stats['emails_skipped'] = len(all_emails) - len(unprocessed_emails)
                
                logger.info(f"✅ Email fetch summary:")
                logger.info(f"   📧 Total emails found: {len(all_emails)}")
                logger.info(f"   ⏭️ Already processed (skipped): {self.stats['emails_skipped']}")
                logger.info(f"   🆕 New emails to process: {len(unprocessed_emails)}")
                
                # Save fetched emails if configured
                if self.config.save_extracted_data:
                    await self._save_fetched_emails(unprocessed_emails)
                
                return unprocessed_emails
                
        except Exception as e:
            error_msg = f"❌ Failed to fetch emails from Vespa: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            return []
    
    async def extract_entities_batch(self, emails: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract entities and relationships from email batch with source tracking, permissions, and progress updates"""
        try:
            logger.info(f"🔍 Extracting entities from {len(emails)} emails")
            logger.info(f"⚡ Using {self.config.parallel_extractions} parallel extractions")
            
            # Extract entities and relationships with email source tracking
            extraction_results = await self.entity_extractor.extract_entities_batch(emails)
            
            # Process results and update progress
            successful_results = []
            for result in extraction_results:
                email_id = result.get('item_id', '')
                
                if result.get('error'):
                    # Mark as failed
                    self.progress_tracker.mark_email_failed(email_id, result['error'])
                    self.stats['emails_failed'] += 1
                    logger.warning(f"❌ Failed to extract from email {email_id}: {result['error']}")
                else:
                    # Add permissions to entities and relationships
                    result = await self._add_permissions_to_extraction_result(result, emails)
                    
                    # Mark as processed
                    self.progress_tracker.mark_email_processed(email_id, result)
                    successful_results.append(result)
                    self.stats['emails_processed'] += 1
                    logger.debug(f"✅ Successfully extracted from email {email_id}")
            
            # Update statistics
            total_entities = sum(result.get('entity_count', 0) for result in successful_results)
            total_relationships = sum(result.get('relationship_count', 0) for result in successful_results)
            
            self.stats['entities_extracted'] = total_entities
            self.stats['relationships_extracted'] = total_relationships
            
            logger.info(f"✅ Extraction completed:")
            logger.info(f"   📧 Emails successfully processed: {len(successful_results)}")
            logger.info(f"   ❌ Emails failed: {self.stats['emails_failed']}")
            logger.info(f"   🏷️ Entities extracted: {total_entities}")
            logger.info(f"   🔗 Relationships extracted: {total_relationships}")
            
            # Verify email source tracking and permissions
            await self._verify_email_source_tracking(successful_results)
            await self._verify_permissions_tracking(successful_results)
            
            # Save extraction results if configured
            if self.config.save_extracted_data:
                await self._save_extraction_results(successful_results)
            
            # Save progress after each batch
            self.progress_tracker.save()
            
            return successful_results
            
        except Exception as e:
            error_msg = f"❌ Entity extraction failed: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            return []
    
    async def merge_to_database(self, extraction_results: List[Dict[str, Any]]) -> bool:
        """Merge extracted data to Kuzu database"""
        try:
            logger.info("💾 Merging extracted data to database")
            
            # Process each extraction result
            total_entities_merged = 0
            total_relationships_merged = 0
            
            for result in extraction_results:
                if result.get('entities') or result.get('relationships'):
                    # Prepare batch for merge pipeline (use extracted data format)
                    batch = {
                        'item_id': result.get('item_id', 'unknown'),
                        'entities': result.get('entities', []),
                        'relationships': result.get('relationships', [])
                    }
                    
                    # Process through merge pipeline
                    merge_result = await self.merge_pipeline.merge_handler.process_batch(batch)
                    
                    if merge_result.get('status') == 'success':
                        # Fix: Use the correct field names from merge_handler response
                        total_entities_merged += merge_result.get('entities_processed', 0)
                        total_relationships_merged += merge_result.get('relations_processed', 0)
                    else:
                        error_msg = f"Merge failed for batch {result.get('item_id')}: {merge_result}"
                        logger.warning(error_msg)
                        self.stats['errors'].append(error_msg)
            
            # Update statistics
            self.stats['entities_merged'] = total_entities_merged
            self.stats['relationships_merged'] = total_relationships_merged
            
            logger.info(f"✅ Database merge completed:")
            logger.info(f"   🏷️ Entities merged: {total_entities_merged}")
            logger.info(f"   🔗 Relationships merged: {total_relationships_merged}")
            
            return True
            
        except Exception as e:
            error_msg = f"❌ Database merge failed: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            return False
    
    async def run_complete_pipeline(self) -> Dict[str, Any]:
        """Run the complete Vespa to Knowledge Graph pipeline with progress tracking"""
        try:
            self.stats['start_time'] = datetime.now()
            session_id = self.progress_tracker.start_session()
            
            logger.info("🚀 Starting Vespa Email Pipeline with Progress Tracking")
            logger.info("=" * 80)
            
            # Step 1: Initialize pipeline
            if not await self.initialize():
                return {"status": "failed", "error": "Pipeline initialization failed"}
            
            # Step 2: Fetch unprocessed emails
            unprocessed_emails = await self.fetch_unprocessed_emails()
            if not unprocessed_emails:
                logger.info("✅ No new emails to process")
                return {
                    "status": "completed",
                    "message": "No new emails to process",
                    "progress_summary": self.progress_tracker.get_progress_summary()
                }
            
            # Step 3: Extract entities and relationships
            extraction_results = await self.extract_entities_batch(unprocessed_emails)
            if not extraction_results:
                return {"status": "failed", "error": "Entity extraction failed"}
            
            # Step 4: Merge to database
            merge_success = await self.merge_to_database(extraction_results)
            if not merge_success:
                return {"status": "failed", "error": "Database merge failed"}
            
            # Step 5: Final progress save and cleanup
            self.stats['end_time'] = datetime.now()
            self.stats['processing_time_seconds'] = (
                self.stats['end_time'] - self.stats['start_time']
            ).total_seconds()
            
            self.progress_tracker.save()
            
            if self.merge_pipeline:
                await self.merge_pipeline.cleanup()
            
            # Generate final report
            progress_summary = self.progress_tracker.get_progress_summary()
            
            logger.info("🎉 Pipeline execution completed successfully!")
            logger.info("=" * 80)
            logger.info("📊 Final Statistics:")
            logger.info(f"   ⏱️ Processing time: {self.stats['processing_time_seconds']:.2f} seconds")
            logger.info(f"   📧 Emails fetched: {self.stats['emails_fetched']}")
            logger.info(f"   ⏭️ Emails skipped (already processed): {self.stats['emails_skipped']}")
            logger.info(f"   ✅ Emails processed this session: {self.stats['emails_processed']}")
            logger.info(f"   ❌ Emails failed this session: {self.stats['emails_failed']}")
            logger.info(f"   🏷️ Entities extracted: {self.stats['entities_extracted']}")
            logger.info(f"   🔗 Relationships extracted: {self.stats['relationships_extracted']}")
            logger.info(f"   💾 Entities merged to DB: {self.stats['entities_merged']}")
            logger.info(f"   💾 Relationships merged to DB: {self.stats['relationships_merged']}")
            logger.info("📈 Overall Progress:")
            logger.info(f"   📧 Total emails ever processed: {progress_summary['total_emails_processed']}")
            logger.info(f"   🎯 Overall success rate: {progress_summary['success_rate']:.1f}%")
            logger.info(f"   🔄 Total processing sessions: {progress_summary['processing_sessions']}")
            
            return {
                "status": "completed",
                "session_id": session_id,
                "statistics": self.stats,
                "progress_summary": progress_summary
            }
            
        except Exception as e:
            error_msg = f"❌ Pipeline execution failed: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            
            # Save progress even on failure
            self.progress_tracker.save()
            
            return {
                "status": "failed",
                "error": error_msg,
                "statistics": self.stats,
                "progress_summary": self.progress_tracker.get_progress_summary()
            }
    
    async def _add_permissions_to_extraction_result(self, result: Dict[str, Any], emails: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Add permissions from email data to entities and relationships"""
        try:
            email_id = result.get('item_id', '')
            
            # Find the corresponding email
            email_data = None
            for email in emails:
                if email.get('id') == email_id:
                    email_data = email
                    break
            
            if not email_data:
                logger.warning(f"⚠️ Could not find email data for ID: {email_id}")
                return result
            
            # Extract permissions from email metadata
            permissions = email_data.get('metadata', {}).get('permissions', [])
            if not permissions:
                logger.debug(f"📧 No permissions found in email {email_id}")
                return result
            
            logger.debug(f"📧 Found {len(permissions)} permissions in email {email_id}: {permissions}")
            
            # Add permissions to entities
            entities = result.get('entities', [])
            for entity in entities:
                if 'attributes' not in entity:
                    entity['attributes'] = {}
                entity['attributes']['permissions'] = permissions.copy()
            
            # Add permissions to relationships
            relationships = result.get('relationships', [])
            for relationship in relationships:
                relationship['permissions'] = permissions.copy()
            
            logger.debug(f"✅ Added permissions to {len(entities)} entities and {len(relationships)} relationships")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error adding permissions to extraction result: {e}")
            return result
    
    async def _verify_permissions_tracking(self, extraction_results: List[Dict[str, Any]]) -> None:
        """Verify that permissions are properly tracked in entities and relationships"""
        try:
            total_entities = 0
            entities_with_permissions = 0
            total_relationships = 0
            relationships_with_permissions = 0
            total_permissions_found = 0
            
            for result in extraction_results:
                # Check entities
                for entity in result.get('entities', []):
                    total_entities += 1
                    permissions = entity.get('attributes', {}).get('permissions', [])
                    if permissions:
                        entities_with_permissions += 1
                        total_permissions_found += len(permissions)
                
                # Check relationships
                for relationship in result.get('relationships', []):
                    total_relationships += 1
                    permissions = relationship.get('permissions', [])
                    if permissions:
                        relationships_with_permissions += 1
                        total_permissions_found += len(permissions)
            
            logger.info(f"🔐 Permissions Tracking Verification:")
            logger.info(f"   🏷️ Entities with permissions: {entities_with_permissions}/{total_entities}")
            logger.info(f"   🔗 Relationships with permissions: {relationships_with_permissions}/{total_relationships}")
            logger.info(f"   📧 Total permission entries: {total_permissions_found}")
            
            if total_entities > 0:
                entities_pct = (entities_with_permissions / total_entities) * 100
                logger.info(f"   📊 Entity permissions coverage: {entities_pct:.1f}%")
            
            if total_relationships > 0:
                relationships_pct = (relationships_with_permissions / total_relationships) * 100
                logger.info(f"   📊 Relationship permissions coverage: {relationships_pct:.1f}%")
                
        except Exception as e:
            logger.error(f"❌ Error verifying permissions tracking: {e}")
    
    async def _verify_email_source_tracking(self, extraction_results: List[Dict[str, Any]]) -> None:
        """Verify that email source IDs are properly tracked"""
        try:
            total_entities = 0
            entities_with_sources = 0
            total_relationships = 0
            relationships_with_sources = 0
            
            for result in extraction_results:
                email_id = result.get('item_id', '')
                
                # Check entities
                for entity in result.get('entities', []):
                    total_entities += 1
                    sources = entity.get('attributes', {}).get('sources', [])
                    if email_id in sources:
                        entities_with_sources += 1
                
                # Check relationships
                for relationship in result.get('relationships', []):
                    total_relationships += 1
                    sources = relationship.get('sources', [])
                    if email_id in sources:
                        relationships_with_sources += 1
            
            logger.info(f"📧 Email Source Tracking Verification:")
            logger.info(f"   🏷️ Entities with email source: {entities_with_sources}/{total_entities}")
            logger.info(f"   🔗 Relationships with email source: {relationships_with_sources}/{total_relationships}")
            
            if total_entities > 0 and entities_with_sources != total_entities:
                logger.warning(f"⚠️ Some entities missing email source tracking!")
            
            if total_relationships > 0 and relationships_with_sources != total_relationships:
                logger.warning(f"⚠️ Some relationships missing email source tracking!")
                
        except Exception as e:
            logger.error(f"❌ Error verifying email source tracking: {e}")
    
    async def _save_fetched_emails(self, emails: List[Dict[str, Any]]) -> None:
        """Save fetched emails to file"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = Path(self.config.output_dir) / f"fetched_emails_{timestamp}.json"
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump({
                    "metadata": {
                        "fetched_at": datetime.now().isoformat(),
                        "count": len(emails),
                        "vespa_endpoint": self.config.vespa_endpoint
                    },
                    "emails": emails
                }, f, indent=2, ensure_ascii=False)
            
            logger.info(f"💾 Saved fetched emails to: {output_file}")
            
        except Exception as e:
            logger.error(f"❌ Error saving fetched emails: {e}")
    
    async def _save_extraction_results(self, extraction_results: List[Dict[str, Any]]) -> None:
        """Save extraction results to file"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = Path(self.config.output_dir) / f"extraction_results_{timestamp}.json"
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump({
                    "metadata": {
                        "extracted_at": datetime.now().isoformat(),
                        "count": len(extraction_results),
                        "llm_model": self.config.llm_model,
                        "total_entities": sum(r.get('entity_count', 0) for r in extraction_results),
                        "total_relationships": sum(r.get('relationship_count', 0) for r in extraction_results)
                    },
                    "results": extraction_results
                }, f, indent=2, ensure_ascii=False)
            
            logger.info(f"💾 Saved extraction results to: {output_file}")
            
        except Exception as e:
            logger.error(f"❌ Error saving extraction results: {e}")


# Utility functions for easy usage
async def run_vespa_pipeline(config: Optional[VespaEmailPipelineConfig] = None) -> Dict[str, Any]:
    """Run the complete Vespa email pipeline"""
    pipeline = VespaEmailPipeline(config)
    return await pipeline.run_complete_pipeline()


async def get_progress_summary(progress_file: str = "data/email_processing_progress.json") -> Dict[str, Any]:
    """Get progress summary without running pipeline"""
    tracker = EmailProgressTracker(progress_file)
    return tracker.get_progress_summary()


async def reset_failed_emails(progress_file: str = "data/email_processing_progress.json") -> int:
    """Reset failed emails to allow retry"""
    tracker = EmailProgressTracker(progress_file)
    count = tracker.reset_failed_emails()
    tracker.save()
    return count


async def main():
    """Main function for testing the pipeline"""
    try:
        logger.info("🧪 Testing Vespa Email Pipeline with Progress Tracking")
        
        # Show current progress
        progress = await get_progress_summary()
        logger.info(f"📊 Current Progress: {progress}")
        
        # Run pipeline
        config = VespaEmailPipelineConfig()
        # Limit emails for testing
        config.max_emails = 10
        config.batch_size = 5
        
        result = await run_vespa_pipeline(config)
        
        logger.info(f"🏁 Pipeline Result: {result['status']}")
        if result.get('statistics'):
            stats = result['statistics']
            logger.info(f"📊 Processed {stats['emails_processed']} emails")
            logger.info(f"🏷️ Extracted {stats['entities_extracted']} entities")
            logger.info(f"🔗 Extracted {stats['relationships_extracted']} relationships")
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
