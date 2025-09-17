#!/usr/bin/env python3
"""
Merge Pipeline for Knowledge Graph
Processes batches of extracted entities and relations, performing deduplication
and merging according to established rules.
"""

import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path

from workspace_kg.utils.kuzu_db_handler import KuzuDBHandler
from workspace_kg.utils.merge_handler import MergeHandler

logger = logging.getLogger(__name__)

class MergePipeline:
    def __init__(self, kuzu_api_url: str = "http://localhost:7000", schema_file: str = 'schema.yaml'):
        self.db_handler = KuzuDBHandler(kuzu_api_url, schema_file)
        self.merge_handler = MergeHandler(self.db_handler)
        self.stats = {
            "total_batches": 0,
            "total_entities_processed": 0,
            "total_relations_processed": 0,
            "errors": 0,
            "start_time": None,
            "end_time": None
        }

    async def initialize(self):
        """Initialize the database connection and validate schema."""
        try:
            # Test connection by executing a simple query
            await self.db_handler.execute_cypher("RETURN 'connection_test' as status")
            logger.info("✅ Database connection established")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to initialize database connection: {e}")
            return False

    async def process_extracted_file(self, file_path: str) -> Dict[str, Any]:
        """
        Process a single extracted entities/relations JSON file.
        
        Args:
            file_path: Path to the extracted JSON file
            
        Returns:
            Processing statistics
        """
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return {"status": "error", "message": f"File not found: {file_path}"}

        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            logger.info(f"📂 Processing file: {file_path}")
            
            # Handle different file formats
            if 'results' in data:
                # Standard extracted data format
                batches = data['results']
                logger.info(f"Found {len(batches)} items to process")
            elif 'entities' in data and 'relations' in data:
                # Direct batch format
                batches = [data]
                logger.info("Processing single batch format")
            else:
                logger.error(f"Unknown file format in {file_path}")
                return {"status": "error", "message": "Unknown file format"}

            return await self.process_batches(batches)
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in {file_path}: {e}")
            return {"status": "error", "message": f"Invalid JSON: {e}"}
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
            return {"status": "error", "message": f"Processing error: {e}"}

    async def process_batches(self, batches: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Process multiple batches of entities and relations.
        
        Args:
            batches: List of batch data dictionaries
            
        Returns:
            Processing statistics
        """
        self.stats["start_time"] = datetime.now()
        self.stats["total_batches"] = len(batches)
        
        logger.info(f"🔄 Starting merge pipeline for {len(batches)} batches")
        
        batch_results = []
        
        for i, batch in enumerate(batches):
            logger.info(f"Processing batch {i+1}/{len(batches)}")
            
            try:
                # Use systematic merge processing if available
                if hasattr(self.merge_handler, 'process_batch_systematic'):
                    logger.info(f"📞 Calling systematic merge for batch {i+1}")
                    result = await self.merge_handler.process_batch_systematic(batch)
                else:
                    logger.info(f"📞 Calling standard merge for batch {i+1}")
                    result = await self.merge_handler.process_batch(batch)
                batch_results.append(result)
                
                if result.get("status") == "success":
                    self.stats["total_entities_processed"] += result.get("entities_processed", 0)
                    self.stats["total_relations_processed"] += result.get("relations_processed", 0)
                else:
                    self.stats["errors"] += 1
                    logger.warning(f"Batch {i+1} failed: {result}")
                    
            except Exception as e:
                logger.error(f"Error processing batch {i+1}: {e}")
                self.stats["errors"] += 1
                batch_results.append({"status": "error", "message": str(e)})

        self.stats["end_time"] = datetime.now()
        processing_time = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
        
        logger.info(f"✅ Merge pipeline completed in {processing_time:.2f} seconds")
        logger.info(f"📊 Processed {self.stats['total_entities_processed']} entities and {self.stats['total_relations_processed']} relations")
        
        if self.stats["errors"] > 0:
            logger.warning(f"⚠️  {self.stats['errors']} batches had errors")

        return {
            "status": "completed",
            "statistics": self.stats,
            "batch_results": batch_results,
            "processing_time_seconds": processing_time
        }

    async def process_directory(self, directory_path: str, pattern: str = "*.json") -> Dict[str, Any]:
        """
        Process all JSON files in a directory matching the pattern.
        
        Args:
            directory_path: Path to directory containing JSON files
            pattern: File pattern to match (default: "*.json")
            
        Returns:
            Combined processing statistics
        """
        directory = Path(directory_path)
        if not directory.exists():
            logger.error(f"Directory not found: {directory_path}")
            return {"status": "error", "message": f"Directory not found: {directory_path}"}

        json_files = list(directory.glob(pattern))
        if not json_files:
            logger.warning(f"No files matching {pattern} found in {directory_path}")
            return {"status": "warning", "message": f"No files matching {pattern} found"}

        logger.info(f"📁 Found {len(json_files)} files to process in {directory_path}")
        
        combined_stats = {
            "files_processed": 0,
            "total_entities": 0,
            "total_relations": 0,
            "total_errors": 0,
            "file_results": []
        }
        
        for file_path in json_files:
            logger.info(f"Processing file: {file_path.name}")
            result = await self.process_extracted_file(str(file_path))
            
            combined_stats["files_processed"] += 1
            combined_stats["file_results"].append({
                "file": file_path.name,
                "result": result
            })
            
            if result.get("status") == "completed":
                stats = result.get("statistics", {})
                combined_stats["total_entities"] += stats.get("total_entities_processed", 0)
                combined_stats["total_relations"] += stats.get("total_relations_processed", 0)
            else:
                combined_stats["total_errors"] += 1

        logger.info(f"✅ Directory processing completed")
        logger.info(f"📊 Total: {combined_stats['total_entities']} entities, {combined_stats['total_relations']} relations")
        
        return {
            "status": "completed",
            "combined_statistics": combined_stats
        }

    async def get_database_statistics(self) -> Dict[str, Any]:
        """Get current database statistics after processing."""
        try:
            stats = {}
            
            # Count entities by type
            for entity_type in self.db_handler.entity_schemas.keys():
                query = f"MATCH (n:{entity_type}) RETURN count(n) as count"
                result = await self.db_handler.execute_cypher(query)
                count = result.get('data', [{}])[0].get('count', 0) if result.get('data') else 0
                stats[f"{entity_type}_count"] = count
            
            # Count total relations
            query = "MATCH ()-[r:Relation]->() RETURN count(r) as count"
            result = await self.db_handler.execute_cypher(query)
            stats["total_relations"] = result.get('data', [{}])[0].get('count', 0) if result.get('data') else 0
            
            # Calculate total entities
            stats["total_entities"] = sum(v for k, v in stats.items() if k.endswith('_count'))
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting database statistics: {e}")
            return {"error": str(e)}

    async def cleanup(self):
        """Cleanup resources."""
        await self.db_handler.close()

# Utility functions for command-line usage

async def process_file(file_path: str, kuzu_url: str = "http://localhost:7000") -> Dict[str, Any]:
    """Process a single file."""
    pipeline = MergePipeline(kuzu_url)
    
    try:
        if not await pipeline.initialize():
            return {"status": "error", "message": "Failed to initialize pipeline"}
        
        result = await pipeline.process_extracted_file(file_path)
        
        # Get final database statistics
        db_stats = await pipeline.get_database_statistics()
        result["database_statistics"] = db_stats
        
        return result
        
    finally:
        await pipeline.cleanup()

async def process_directory(directory_path: str, pattern: str = "*.json", kuzu_url: str = "http://localhost:7000") -> Dict[str, Any]:
    """Process all files in a directory."""
    pipeline = MergePipeline(kuzu_url)
    
    try:
        if not await pipeline.initialize():
            return {"status": "error", "message": "Failed to initialize pipeline"}
        
        result = await pipeline.process_directory(directory_path, pattern)
        
        # Get final database statistics
        db_stats = await pipeline.get_database_statistics()
        result["database_statistics"] = db_stats
        
        return result
        
    finally:
        await pipeline.cleanup()

async def main():
    """Main function for command-line usage."""
    import sys
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    if len(sys.argv) < 2:
        print("🔧 Merge Pipeline for Knowledge Graph")
        print("=" * 50)
        print("Usage:")
        print("  python merge_pipeline.py <file_path>        - Process single file")
        print("  python merge_pipeline.py <directory_path>   - Process directory")
        print("")
        print("Examples:")
        print("  python merge_pipeline.py data/temp_extracted_entities_relations.json")
        print("  python merge_pipeline.py data/")
        return

    path = sys.argv[1]
    
    if os.path.isfile(path):
        print(f"📂 Processing file: {path}")
        result = await process_file(path)
    elif os.path.isdir(path):
        print(f"📁 Processing directory: {path}")
        result = await process_directory(path)
    else:
        print(f"❌ Path not found: {path}")
        return

    print("\n📊 Final Results:")
    print(json.dumps(result, indent=2, default=str))

if __name__ == "__main__":
    asyncio.run(main())