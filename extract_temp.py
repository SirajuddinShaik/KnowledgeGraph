#!/usr/bin/env python3
"""
Extract entities from temp.json using the modular workspace-kg pipeline
"""

import sys
import os
import asyncio
import json
from datetime import datetime
from typing import Dict, List, Any

# Add the src directory to Python path
src_path = os.path.join(os.path.dirname(__file__), 'src')
sys.path.insert(0, src_path)

# Add workspace-kg to path
workspace_kg_path = os.path.join(src_path, 'workspace-kg')
sys.path.insert(0, workspace_kg_path)

# Direct imports
from workspace_kg.pipeline.extract_pipeline import ExtractPipeline, load_data_from_json
from workspace_kg.utils.prompt_factory import DataType
from workspace_kg.utils.output_formatter import OutputFormat

async def main():
    """Extract entities from temp.json file"""
    
    # Load data from temp.json
    file_path = "temp.json"
    
    print(f"🔍 Loading data from {file_path}...")
    data_to_process = load_data_from_json(file_path)
    
    if not data_to_process:
        print(f"❌ No data found in {file_path}")
        return
    
    print(f"✅ Found {len(data_to_process)} email documents to process")
    
    # Limit to first 10 items for testing (remove this limit for full processing)
    data_to_process = data_to_process[:10]
    print(f"📝 Processing first {len(data_to_process)} items for testing...")
    
    # Initialize the modular extraction pipeline
    pipeline = ExtractPipeline(
        data_type=DataType.EMAIL,           # Specify email data type
        auto_detect_data_type=True,         # Enable auto-detection
        batch_size=3,                       # Process 3 items per batch
        parallel_calls=2                    # 2 parallel LLM calls
    )
    
    # Run extraction with automatic saving
    print(f"\n🚀 Starting entity extraction...")
    results = await pipeline.run(
        data_to_process,
        save_results=True,                  # Auto-save results
        output_file=None                    # Auto-generate filename
    )
    
    print(f"\n✅ Extraction completed successfully!")
    
    # Export to additional formats
    if len(results) > 0:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        print(f"\n📤 Exporting to additional formats:")
        
        # Export to JSONL
        jsonl_file = f"data/temp_extracted_{timestamp}.jsonl"
        await pipeline.save_results(results, jsonl_file, OutputFormat.JSONL)
        print(f"  📄 JSONL: {jsonl_file}")
        
        # Export to CSV
        csv_file = f"data/temp_extracted_{timestamp}.csv"
        await pipeline.save_results(results, csv_file, OutputFormat.CSV)
        print(f"  📊 CSV: {csv_file}")
        
        # Show sample results
        print(f"\n🔍 Sample extracted data:")
        for i, result in enumerate(results[:2]):  # Show first 2 results
            print(f"\n📧 Item {i+1} ({result.get('item_id', 'unknown')}):")
            print(f"  - Data type: {result.get('data_type', 'unknown')}")
            print(f"  - Entities: {len(result.get('entities', []))}")
            print(f"  - Relationships: {len(result.get('relationships', []))}")
            
            # Show sample entities
            entities = result.get('entities', [])[:3]  # First 3 entities
            if entities:
                print(f"  - Sample entities:")
                for entity in entities:
                    name = entity.get('entity_name', 'Unknown')
                    entity_type = entity.get('entity_type', 'Unknown')
                    print(f"    • {name} ({entity_type})")
    
    print(f"\n🎉 Entity extraction from temp.json completed!")
    print(f"💡 Files are saved in the 'data/' directory with structured JSON format")

if __name__ == "__main__":
    asyncio.run(main())