import asyncio
import json
import os
from typing import List, Dict, Any, Optional
from datetime import datetime

from workspace_kg.components.entity_extractor import EntityExtractor
from workspace_kg.config import PARALELL_LLM_CALLS, BATCH_SIZE, INCLUDE_METADATA, INCLUDE_STATS
from workspace_kg.utils.prompt import DEFAULT_ENTITY_TYPES
from workspace_kg.utils.prompt_factory import DataType
from workspace_kg.utils.output_formatter import OutputFormatter, OutputFormat

class ExtractPipeline:
    def __init__(self, 
                 entity_types: List[str] = None,
                 model: str = "gemini-2.5-flash",
                 data_type: DataType = DataType.EMAIL,
                 auto_detect_data_type: bool = True,
                 batch_size: int = None,
                 parallel_calls: int = None):
        self.entity_types = entity_types if entity_types is not None else DEFAULT_ENTITY_TYPES
        self.entity_extractor = EntityExtractor(
            model=model,
            data_type=data_type,
            auto_detect_data_type=auto_detect_data_type
        )
        self.batch_size = batch_size or BATCH_SIZE
        self.parallel_calls = parallel_calls or PARALELL_LLM_CALLS
        self.output_formatter = OutputFormatter(
            include_metadata=INCLUDE_METADATA,
            include_stats=INCLUDE_STATS
        )
        self.pipeline_stats = {
            "start_time": None,
            "end_time": None,
            "total_items": 0,
            "successful_extractions": 0,
            "failed_extractions": 0,
            "total_entities": 0,
            "total_relationships": 0
        }

    async def _process_batch(self, batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Processes a single batch of data for entity extraction.
        """
        return await self.entity_extractor.extract_entities_batch(batch, self.entity_types)

    async def run(self, data: List[Dict[str, Any]], 
                  save_results: bool = False,
                  output_file: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Runs the extraction pipeline on the provided data.
        
        Args:
            data: A list of dictionaries, where each dictionary represents an item
                  to be processed (e.g., an email with 'id' and 'content' keys).
            save_results: Whether to save results to a file
            output_file: Optional custom output file path
        
        Returns:
            A list of dictionaries, each containing extracted entities and relationships
            for the corresponding input item.
        """
        self.pipeline_stats["start_time"] = datetime.now().isoformat()
        self.pipeline_stats["total_items"] = len(data)
        
        print(f"🚀 Starting extraction pipeline for {len(data)} items")
        print(f"📦 Batch size: {self.batch_size}, Parallel calls: {self.parallel_calls}")
        
        all_results = []
        
        # Divide data into batches
        batches = [data[i:i + self.batch_size] for i in range(0, len(data), self.batch_size)]
        
        print(f"📋 Processing {len(batches)} batchesworkspace_kg..")
        
        # Process batches with a limited number of parallel LLM calls
        semaphore = asyncio.Semaphore(self.parallel_calls)
        
        async def sem_process_batch(batch_data, batch_index):
            async with semaphore:
                print(f"⚡ Processing batch {batch_index + 1}/{len(batches)} ({len(batch_data)} items)")
                return await self._process_batch(batch_data)

        tasks = [sem_process_batch(batch, i) for i, batch in enumerate(batches)]
        
        try:
            for result_batch in await asyncio.gather(*tasks, return_exceptions=True):
                if isinstance(result_batch, Exception):
                    print(f"❌ Batch processing failed: {str(result_batch)}")
                    self.pipeline_stats["failed_extractions"] += self.batch_size
                else:
                    all_results.extend(result_batch)
                    # Update stats
                    for result in result_batch:
                        if "error" not in result or not result["error"]:
                            self.pipeline_stats["successful_extractions"] += 1
                            self.pipeline_stats["total_entities"] += result.get("entity_count", 0)
                            self.pipeline_stats["total_relationships"] += result.get("relationship_count", 0)
                        else:
                            self.pipeline_stats["failed_extractions"] += 1
            
            self.pipeline_stats["end_time"] = datetime.now().isoformat()
            
            # Save results if requested
            if save_results:
                output_path = output_file or self._generate_output_filename()
                await self.save_results(all_results, output_path)
                print(f"💾 Results saved to {output_path}")
            
            self._print_pipeline_summary()
            
        except Exception as e:
            print(f"❌ Pipeline failed: {str(e)}")
            raise
            
        return all_results
    
    def _generate_output_filename(self) -> str:
        """Generate timestamped output filename"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"data/extracted_entities_pipeline_{timestamp}.json"
    
    async def save_results(self, results: List[Dict[str, Any]], output_file: str, output_format: OutputFormat = OutputFormat.JSON):
        """Save results to file with proper formatting and validation"""
        # Ensure directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Format results with metadata and statistics
        formatted_data = self.output_formatter.format_results(
            results=results,
            pipeline_stats=self.pipeline_stats,
            entity_types=self.entity_types,
            batch_size=self.batch_size,
            parallel_calls=self.parallel_calls,
            output_format=output_format
        )
        
        # Validate results
        validation_report = self.output_formatter.validate_results(results)
        if not validation_report["is_valid"]:
            print(f"⚠️  Validation errors found: {len(validation_report['errors'])} errors")
            for error in validation_report["errors"]:
                print(f"   - {error}")
        
        if validation_report["warnings"]:
            print(f"⚠️  Validation warnings: {len(validation_report['warnings'])} warnings")
        
        # Add validation report to output
        formatted_data["validation"] = validation_report
        
        # Export to specified format
        self.output_formatter.export_to_format(formatted_data, output_format, output_file)
    
    def _print_pipeline_summary(self):
        """Print pipeline execution summary"""
        stats = self.pipeline_stats
        
        print(f"\n📊 Pipeline Execution Summary:")
        print(f"  ⏱️  Total time: {self._calculate_duration()}")
        print(f"  📄  Total items processed: {stats['total_items']}")
        print(f"  ✅  Successful extractions: {stats['successful_extractions']}")
        print(f"  ❌  Failed extractions: {stats['failed_extractions']}")
        print(f"  🏷️  Total entities extracted: {stats['total_entities']}")
        print(f"  🔗  Total relationships extracted: {stats['total_relationships']}")
        
        if stats['total_items'] > 0:
            success_rate = (stats['successful_extractions'] / stats['total_items']) * 100
            print(f"  📈  Success rate: {success_rate:.1f}%")
    
    def _calculate_duration(self) -> str:
        """Calculate pipeline execution duration"""
        if self.pipeline_stats["start_time"] and self.pipeline_stats["end_time"]:
            start = datetime.fromisoformat(self.pipeline_stats["start_time"])
            end = datetime.fromisoformat(self.pipeline_stats["end_time"])
            duration = end - start
            return str(duration).split('.')[0]  # Remove microseconds
        return "Unknown"

def load_data_from_json(file_path: str) -> List[Dict[str, Any]]:
    """
    Load data from JSON file, handling different formats.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        if isinstance(data, dict) and 'documents' in data:
            return data['documents']
        elif isinstance(data, list):
            return data
        elif isinstance(data, dict):
            for key in ['emails', 'data', 'messages', 'documents']:
                if key in data and isinstance(data[key], list):
                    return data[key]
            return [data]
        else:
            print(f"Unexpected data format in {file_path}")
            return []
            
    except Exception as e:
        print(f"Error loading data from {file_path}: {str(e)}")
        return []

async def main():
    """
    Main function to demonstrate the extraction pipeline.
    """
    # Example usage:
    file_path = "data/vespa_by_type_20250902_164758/email_documents.json" # Replace with your data path
    
    # Load data
    data_to_process = load_data_from_json(file_path)
    data_to_process = data_to_process[:20] # Limit for testing
    
    if not data_to_process:
        print(f"No data found in {file_path} to process.")
        return

    print(f"Found {len(data_to_process)} items to process.")

    # Initialize and run pipeline
    pipeline = ExtractPipeline()
    results = await pipeline.run(data_to_process)

    # Save results
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"data/extracted_entities_pipeline_{timestamp}.json"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\nExtraction complete! Results saved to {output_file}")

    # Print summary
    total_entities = sum(len(result['entities']) for result in results)
    total_relationships = sum(len(result['relationships']) for result in results)
    successful_extractions = len([r for r in results if len(r['entities']) > 0])
    
    print(f"\n📊 Extraction Summary:")
    print(f"  - Total items processed: {len(results)}")
    print(f"  - Successful extractions: {successful_extractions}")
    print(f"  - Total entities extracted: {total_entities}")
    print(f"  - Total relationships extracted: {total_relationships}")

if __name__ == "__main__":
    asyncio.run(main())
