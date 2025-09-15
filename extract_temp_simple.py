#!/usr/bin/env python3
"""
Simple script to extract entities from temp.json
"""

import os
import json
import asyncio
import openai
import re
from datetime import datetime
from typing import Dict, List, Any
try:
    from dotenv import load_dotenv
    # Load environment variables from .env file
    load_dotenv()
    print("✅ Loaded environment variables from .env file")
except ImportError:
    print("⚠️ python-dotenv not found, using system environment variables")
    # Manual env loading as fallback
    if os.path.exists('.env'):
        with open('.env', 'r') as f:
            for line in f:
                if '=' in line and not line.strip().startswith('#'):
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value
        print("✅ Manually loaded environment variables from .env file")

# Configuration from workspace-kg
PARALELL_LLM_CALLS = 5  # Number of parallel LLM calls
BATCH_SIZE = 5          # Number of documents to process in each batch

# Simple entity extractor without complex imports
class SimpleEntityExtractor:
    def __init__(self, model: str = None):
        # Use model from environment if not specified
        self.model = model or os.getenv("LLM_MODEL_NAME", "gemini-2.5-flash")
        
        # Use AsyncOpenAI for proper async support
        self.client = openai.AsyncOpenAI(
            api_key=os.getenv("OPENAI_API_KEY"),
            base_url=os.getenv("LLM_BASE_URL"),
        )
        
        print(f"🤖 Using model: {self.model}")
        print(f"🔗 API endpoint: {os.getenv('LLM_BASE_URL')}")
        
        # Email-specific system prompt
        self.system_prompt = """---Goal---
Your goal is to extract workspace-level entities and relationships from email data in tuple format. Focus on extracting business-relevant information that helps understand the organizational structure, projects, and collaboration patterns.

Use English as output language.

---CRITICAL FORMATTING REQUIREMENTS---
1. **MUST return ONLY tuple format** - no JSON, no markdown, no code blocks
2. **Use specific delimiters** - <|> between fields, ## between records
3. **ALL string values MUST be properly escaped** - escape quotes and special characters
4. **NO line breaks inside string values** - use \\n for line breaks if needed
5. **End with completion delimiter** - <|COMPLETE|>

---Entity Types to Extract---
Focus on: Person, Organization, Repository, CodeChangeRequest, Issue, Team, Project

---MANDATORY OUTPUT FORMAT---
For each entity, output ONE line in this exact format:
("entity"<|>"<entity_name>"<|>"<entity_type>"<|>"<attribute_name>": "<attribute_value>"<|>"<attribute_name>": "<attribute_value>")##

For relationships, output ONE line in this exact format:
("relationship"<|>"<source_entity>"<|>"<target_entity>"<|>"<relationship_type>"<|>"<description>"<|><strength>)##
"""

    async def extract_entities(self, email_content: str, item_id: str) -> Dict[str, Any]:
        """Extract entities from email content"""
        
        prompt = f"""
---Real Data---
Entity_types: Person, Organization, Repository, CodeChangeRequest, Issue, Team, Project
Email Text: {email_content}

IMPORTANT: Return ONLY the tuple format below, end with <|COMPLETE|>:"""

        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": prompt}
        ]
        
        try:
            # Use async client with proper await
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=0.2
            )
            
            llm_output = response.choices[0].message.content
            entities, relationships = self.parse_llm_output(llm_output, item_id)
            
            return {
                "item_id": item_id,
                "entities": entities,
                "relationships": relationships,
                "entity_count": len(entities),
                "relationship_count": len(relationships),
                "processed_at": datetime.now().isoformat(),
                "raw_llm_output": llm_output
            }
            
        except Exception as e:
            print(f"❌ Error extracting entities for {item_id}: {str(e)}")
            return {
                "item_id": item_id,
                "entities": [],
                "relationships": [],
                "entity_count": 0,
                "relationship_count": 0,
                "error": str(e),
                "processed_at": datetime.now().isoformat()
            }

    def parse_llm_output(self, llm_output: str, item_id: str) -> tuple:
        """Parse LLM output to extract entities and relationships with email source tracking"""
        entities = []
        relationships = []
        
        try:
            records = llm_output.split('##')
            
            for record in records:
                record = record.strip()
                if not record or record == '<|COMPLETE|>':
                    continue
                    
                if record.startswith('("entity"'):
                    entity = self.parse_entity_record(record, item_id)
                    if entity:
                        entities.append(entity)
                elif record.startswith('("relationship"'):
                    relationship = self.parse_relationship_record(record, item_id)
                    if relationship:
                        relationships.append(relationship)
                        
        except Exception as e:
            print(f"⚠️ Error parsing LLM output: {str(e)}")
            
        return entities, relationships
    
    def parse_entity_record(self, record: str, item_id: str) -> Dict[str, Any]:
        """Parse entity record from tuple format with email source tracking"""
        try:
            record = record.strip()
            if record.startswith('("entity"') and record.endswith(')'):
                record = record[1:-1]
            
            parts = record.split('<|>')
            if len(parts) < 3:
                return None
                
            entity_name = parts[1].strip().strip('"')
            entity_type = parts[2].strip().strip('"')
            
            attributes = {"name": entity_name}
            
            for i in range(3, len(parts)):
                attr_part = parts[i].strip()
                if ':' in attr_part:
                    attr_match = re.match(r'"([^"]+)":\s*"([^"]*)"', attr_part)
                    if attr_match:
                        attr_name = attr_match.group(1)
                        attr_value = attr_match.group(2)
                        attributes[attr_name] = attr_value
            
            # Always add email source ID to sources array
            if 'sources' not in attributes:
                attributes['sources'] = []
            elif not isinstance(attributes['sources'], list):
                attributes['sources'] = [attributes['sources']]
            
            # Ensure the email source ID is included
            if item_id not in attributes['sources']:
                attributes['sources'].append(item_id)
            
            return {
                "entity_name": entity_name,
                "entity_type": entity_type,
                "attributes": attributes
            }
            
        except Exception as e:
            print(f"⚠️ Error parsing entity record: {str(e)}")
            return None
    
    def parse_relationship_record(self, record: str, item_id: str) -> Dict[str, Any]:
        """Parse relationship record from tuple format with email source tracking"""
        try:
            record = record.strip()
            if record.startswith('("relationship"') and record.endswith(')'):
                record = record[1:-1]
            
            parts = record.split('<|>')
            if len(parts) < 6:
                return None
                
            source_entity = parts[1].strip().strip('"')
            target_entity = parts[2].strip().strip('"')
            relationship_type = parts[3].strip().strip('"')
            description = parts[4].strip().strip('"')
            
            strength_part = parts[5].strip()
            strength_match = re.search(r'(\d+(?:\.\d+)?)', strength_part)
            strength = float(strength_match.group(1)) if strength_match else 5.0
            
            return {
                "source_entity": source_entity,
                "target_entity": target_entity,
                "relationship_type": relationship_type,
                "description": description,
                "strength": strength,
                "sources": [item_id]  # Always add email source ID to relationships
            }
            
        except Exception as e:
            print(f"⚠️ Error parsing relationship record: {str(e)}")
            return None

def load_temp_json(file_path: str) -> List[Dict[str, Any]]:
    """Load data from temp.json file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        if isinstance(data, dict) and 'documents' in data:
            return data['documents']
        elif isinstance(data, list):
            return data
        else:
            print(f"❌ Unexpected data format in {file_path}")
            return []
            
    except Exception as e:
        print(f"❌ Error loading data from {file_path}: {str(e)}")
        return []

async def extract_with_semaphore(extractor, semaphore, item, index, total):
    """Extract entities for a single item with semaphore control"""
    async with semaphore:
        item_id = item.get('id', f'item_{index}')
        content = item.get('content', '')
        
        print(f"⚡ Processing {index+1}/{total}: {item_id}")
        
        if content.strip():
            return await extractor.extract_entities(content, item_id)
        else:
            print(f"⚠️ Skipping {item_id} - no content")
            return {
                "item_id": item_id,
                "entities": [],
                "relationships": [],
                "entity_count": 0,
                "relationship_count": 0,
                "error": "No content",
                "processed_at": datetime.now().isoformat()
            }

async def main():
    """Main extraction function with parallel processing"""
    
    print("🚀 Starting entity extraction from temp.json")
    
    # Load data
    data_to_process = load_temp_json("data/temp.json")
    
    if not data_to_process:
        print("❌ No data found in temp.json")
        return
    
    print(f"✅ Found {len(data_to_process)} documents")
    
    # Process all documents (remove limit for full processing)
    data_to_process = data_to_process[:10]  # Limit to 10 for testing - remove this line for full processing
    print(f"📝 Processing {len(data_to_process)} documents with {PARALELL_LLM_CALLS} parallel calls...")
    
    # Add timing for performance measurement
    start_time = datetime.now()
    
    # Initialize extractor
    extractor = SimpleEntityExtractor()
    
    # Create semaphore for controlling parallel calls
    semaphore = asyncio.Semaphore(PARALELL_LLM_CALLS)
    
    # Create tasks for parallel processing
    tasks = []
    for i, item in enumerate(data_to_process):
        task = extract_with_semaphore(extractor, semaphore, item, i, len(data_to_process))
        tasks.append(task)
    
    # Execute all tasks in parallel with semaphore control
    print(f"🔄 Running {len(tasks)} extraction tasks in parallel...")
    all_results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Filter out exceptions and process results
    processed_results = []
    for i, result in enumerate(all_results):
        if isinstance(result, Exception):
            print(f"❌ Task {i+1} failed: {str(result)}")
            processed_results.append({
                "item_id": f"item_{i}",
                "entities": [],
                "relationships": [],
                "entity_count": 0,
                "relationship_count": 0,
                "error": str(result),
                "processed_at": datetime.now().isoformat()
            })
        else:
            processed_results.append(result)
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"data/temp_extracted_parallel_{timestamp}.json"
    
    # Create data directory if it doesn't exist
    os.makedirs("data", exist_ok=True)
    
    # Prepare formatted output
    output_data = {
        "metadata": {
            "extraction_timestamp": timestamp,
            "total_items_processed": len(processed_results),
            "source_file": "temp.json",
            "parallel_calls": PARALELL_LLM_CALLS,
            "processing_mode": "parallel_async"
        },
        "results": processed_results,
        "summary": {
            "total_entities": sum(r.get('entity_count', 0) for r in processed_results),
            "total_relationships": sum(r.get('relationship_count', 0) for r in processed_results),
            "successful_extractions": len([r for r in processed_results if not r.get('error') or r.get('error') == 'No content']),
            "failed_extractions": len([r for r in processed_results if r.get('error') and r.get('error') != 'No content'])
        }
    }
    
    # Save to JSON
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    # Calculate processing time
    end_time = datetime.now()
    processing_time = end_time - start_time
    
    print(f"\n✅ Parallel extraction completed!")
    print(f"💾 Results saved to: {output_file}")
    print(f"⏱️ Total processing time: {processing_time}")
    
    # Print summary
    total_entities = sum(r.get('entity_count', 0) for r in processed_results)
    total_relationships = sum(r.get('relationship_count', 0) for r in processed_results)
    successful = len([r for r in processed_results if not r.get('error') or r.get('error') == 'No content'])
    failed = len([r for r in processed_results if r.get('error') and r.get('error') != 'No content'])
    
    print(f"\n📊 Parallel Processing Summary:")
    print(f"  📄 Items processed: {len(processed_results)}")
    print(f"  ⚡ Parallel calls: {PARALELL_LLM_CALLS}")
    print(f"  ✅ Successful extractions: {successful}")
    print(f"  ❌ Failed extractions: {failed}")
    print(f"  🏷️ Total entities: {total_entities}")
    print(f"  🔗 Total relationships: {total_relationships}")
    
    # Show sample results
    successful_results = [r for r in processed_results if r.get('entity_count', 0) > 0]
    if successful_results:
        print(f"\n🔍 Sample entities from first successful result:")
        first_result = successful_results[0]
        for entity in first_result.get('entities', [])[:3]:
            name = entity.get('entity_name', 'Unknown')
            entity_type = entity.get('entity_type', 'Unknown')
            print(f"  • {name} ({entity_type})")
        
        print(f"\n📈 Performance insights:")
        avg_entities = total_entities / successful if successful > 0 else 0
        avg_relationships = total_relationships / successful if successful > 0 else 0
        print(f"  📊 Average entities per document: {avg_entities:.1f}")
        print(f"  🔗 Average relationships per document: {avg_relationships:.1f}")

if __name__ == "__main__":
    asyncio.run(main())
