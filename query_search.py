#!/usr/bin/env python3
"""
Interactive Real-time Knowledge Graph Search System
Allows users to perform semantic searches on the knowledge graph using natural language queries.
"""

import asyncio
import json
import logging
import os
import sys
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from src.workspace_kg.utils.kuzu_db_handler import KuzuDBHandler
from src.workspace_kg.components.ollama_embedder import InferenceProvider

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class InteractiveSearchSystem:
    def __init__(self, kuzu_url: str = "http://localhost:7000"):
        self.db_handler = KuzuDBHandler(kuzu_url)
        self.embedder = None
        self.setup_embedder()
        
    def setup_embedder(self):
        """Setup Ollama embedder with environment variables"""
        try:
            # Set default values if not in environment
            if not os.getenv("OLLAMA_BASE_URL"):
                os.environ["OLLAMA_BASE_URL"] = "http://localhost:7008"
            if not os.getenv("OLLAMA_EMBEDDING_MODEL"):
                os.environ["OLLAMA_EMBEDDING_MODEL"] = "huggingface.co/Qwen/Qwen3-Embedding-0.6B-GGUF:latest"
                
            self.embedder = InferenceProvider()
            print(f"🤖 Ollama Embedder Ready")
            print(f"   📡 URL: {os.getenv('OLLAMA_BASE_URL')}")
            print(f"   🧠 Model: {os.getenv('OLLAMA_EMBEDDING_MODEL')}")
            
        except Exception as e:
            print(f"❌ Failed to initialize Ollama embedder: {e}")
            print("💡 Make sure Ollama is running and the model is available")
            self.embedder = None

    async def search_entities(self, query: str, k: int = 10, show_details: bool = True) -> List[Dict[str, Any]]:
        """Search for entities using semantic similarity"""
        if not self.embedder:
            print("❌ Embedder not available")
            return []
        
        try:
            # Generate embedding for search query
            search_entity = {
                "name": "SearchQuery",
                "rawDescriptions": [query]
            }
            
            query_vector = self.embedder.embed_entity("Person", search_entity)
            
            if not query_vector:
                print("❌ Failed to generate embedding for query")
                return []
            
            # Execute vector similarity search
            vector_query = """
            CALL QUERY_VECTOR_INDEX(
                'Nodes',
                'node_index', 
                $query_vector,
                $k,
                efs := 200
            )
            RETURN node.name, node.type, node.rawDescriptions, node.aliases, 
                   node.sources, node.permissions, distance
            """
            
            params = {
                "query_vector": query_vector,
                "k": k
            }
            
            result = await self.db_handler.execute_cypher(vector_query, params)
            
            if result and (result.get('data') or result.get('rows')):
                data = result.get('data') or result.get('rows')
                
                # Process and format results
                results = []
                for row in data:
                    entity = {
                        'name': row.get('node.name', 'N/A'),
                        'type': row.get('node.type', 'N/A'),
                        'distance': row.get('distance', 'N/A'),
                        'descriptions': row.get('node.rawDescriptions', []),
                        'aliases': row.get('node.aliases', []),
                        'sources': row.get('node.sources', []),
                        'permissions': row.get('node.permissions', [])
                    }
                    results.append(entity)
                
                return results
            else:
                return []
                
        except Exception as e:
            print(f"❌ Error during search: {e}")
            return []

    def display_results(self, results: List[Dict[str, Any]], query: str, show_details: bool = True):
        """Display search results in a formatted way"""
        if not results:
            print(f"🔍 No results found for: '{query}'")
            return
        
        print(f"\n🎯 Found {len(results)} results for: '{query}'")
        print("=" * 80)
        
        for i, entity in enumerate(results, 1):
            name = entity['name']
            entity_type = entity['type']
            distance = entity['distance']
            descriptions = entity['descriptions'] or []
            aliases = entity['aliases'] or []
            sources = entity['sources'] or []
            permissions = entity['permissions'] or []
            
            # Calculate similarity percentage (lower distance = higher similarity)
            similarity = max(0, (1 - distance) * 100) if isinstance(distance, (int, float)) else 0
            
            print(f"\n📋 {i}. {name} ({entity_type})")
            print(f"   🎯 Similarity: {similarity:.1f}% (distance: {distance:.4f})")
            
            if show_details:
                if descriptions:
                    print(f"   📝 Descriptions ({len(descriptions)}):")
                    for j, desc in enumerate(descriptions[:3], 1):  # Show max 3 descriptions
                        print(f"      {j}. {desc}")
                    if len(descriptions) > 3:
                        print(f"      ... and {len(descriptions) - 3} more")
                else:
                    print(f"   📝 No descriptions available")
                
                if aliases:
                    print(f"   🏷️ Aliases: {', '.join(aliases[:3])}{'...' if len(aliases) > 3 else ''}")
                
            
            print("-" * 80)

    async def get_entity_details(self, entity_name: str, entity_type: str):
        """Get detailed information about a specific entity"""
        try:
            query = """
            MATCH (n:Nodes) 
            WHERE n.name = $name AND n.type = $type
            RETURN n
            """
            
            params = {"name": entity_name, "type": entity_type}
            result = await self.db_handler.execute_cypher(query, params)
            
            if result and (result.get('data') or result.get('rows')):
                data = result.get('data') or result.get('rows')
                entity_data = data[0]['n']
                
                print(f"\n🔍 Detailed Information for: {entity_name} ({entity_type})")
                print("=" * 80)
                
                for key, value in entity_data.items():
                    if value and key != 'embedding':  # Skip empty values and large embedding
                        if isinstance(value, list):
                            if len(value) > 0:
                                print(f"   {key}: {value}")
                        else:
                            print(f"   {key}: {value}")
                            
            else:
                print(f"❌ Entity not found: {entity_name} ({entity_type})")
                
        except Exception as e:
            print(f"❌ Error getting entity details: {e}")

    async def get_entity_relationships(self, entity_name: str, entity_type: str):
        """Get relationships for a specific entity"""
        try:
            query = """
            MATCH (n:Nodes)-[r:Relation]-(connected:Nodes)
            WHERE n.name = $name AND n.type = $type
            RETURN connected.name, connected.type, r.type, r.relationTag, r.description, 
                   r.strength, startNode(r) = n as is_outgoing
            LIMIT 20
            """
            
            params = {"name": entity_name, "type": entity_type}
            result = await self.db_handler.execute_cypher(query, params)
            
            if result and (result.get('data') or result.get('rows')):
                data = result.get('data') or result.get('rows')
                
                print(f"\n🔗 Relationships for: {entity_name} ({entity_type})")
                print("=" * 80)
                
                for i, row in enumerate(data, 1):
                    connected_name = row.get('connected.name', 'N/A')
                    connected_type = row.get('connected.type', 'N/A')
                    rel_type = row.get('r.type', 'N/A')
                    rel_tag = row.get('r.relationTag', ['N/A'])
                    rel_desc = row.get('r.description', ['N/A'])
                    strength = row.get('r.strength', 'N/A')
                    is_outgoing = row.get('is_outgoing', False)
                    
                    direction = "→" if is_outgoing else "←"
                    print(f"   {i}. {entity_name} {direction} {connected_name} ({connected_type})")
                    print(f"      Type: {rel_type} | Tag: {rel_tag} | Strength: {strength}")
                    if rel_desc and rel_desc != ['N/A']:
                        print(f"      Description: {rel_desc}")
                    print()
                    
            else:
                print(f"❌ No relationships found for: {entity_name} ({entity_type})")
                
        except Exception as e:
            print(f"❌ Error getting relationships: {e}")

    def show_help(self):
        """Show available commands"""
        help_text = """
🔍 Interactive Knowledge Graph Search Commands:

Basic Search:
  <your query>                 - Search for entities (e.g., "pull request automation")
  
Advanced Commands:
  details <name> <type>        - Get detailed info about an entity
  relations <name> <type>      - Show relationships for an entity
  top <number>                 - Set number of results to show (default: 10)
  simple                       - Toggle simple/detailed view
  stats                        - Show database statistics
  help                         - Show this help message
  exit/quit                    - Exit the search system

Examples:
  pull request automation
  details euler.bot Person
  relations euler.bot Person
  top 5
  simple

💡 Tips:
  - Use natural language queries for best results
  - Entity names are case-sensitive for details/relations commands
  - Use quotes for multi-word entity names if needed
        """
        print(help_text)

    async def get_database_stats(self):
        """Get basic database statistics"""
        try:
            stats_query = """
            MATCH (n:Nodes)
            RETURN n.type as entity_type, count(n) as count
            ORDER BY count DESC
            """
            
            result = await self.db_handler.execute_cypher(stats_query)
            
            if result and (result.get('data') or result.get('rows')):
                data = result.get('data') or result.get('rows')
                
                print("\n📊 Database Statistics")
                print("=" * 40)
                total = 0
                for row in data:
                    entity_type = row.get('entity_type', 'Unknown')
                    count = row.get('count', 0)
                    total += count
                    print(f"   {entity_type}: {count}")
                
                print(f"\n   Total Entities: {total}")
                
                # Get relationship count
                rel_query = "MATCH ()-[r:Relation]->() RETURN count(r) as rel_count"
                rel_result = await self.db_handler.execute_cypher(rel_query)
                if rel_result and (rel_result.get('data') or rel_result.get('rows')):
                    rel_data = rel_result.get('data') or rel_result.get('rows')
                    rel_count = rel_data[0].get('rel_count', 0)
                    print(f"   Total Relationships: {rel_count}")
                    
        except Exception as e:
            print(f"❌ Error getting database stats: {e}")

    async def run_interactive_search(self):
        """Main interactive search loop"""
        print("\n🚀 Welcome to Interactive Knowledge Graph Search!")
        print("=" * 60)
        print("💡 Type your search queries in natural language")
        print("💡 Use 'help' for available commands")
        print("💡 Type 'exit' or 'quit' to stop")
        print("=" * 60)
        
        if not self.embedder:
            print("⚠️ Warning: Ollama embedder not available. Please check your setup.")
            return
        
        # Default settings
        max_results = 10
        show_details = True
        
        while True:
            try:
                # Get user input
                query = input("\n🔍 Enter your search query: ").strip()
                
                if not query:
                    continue
                
                # Handle special commands
                if query.lower() in ['exit', 'quit']:
                    print("👋 Goodbye!")
                    break
                elif query.lower() == 'help':
                    self.show_help()
                elif query.lower() == 'stats':
                    await self.get_database_stats()
                elif query.lower() == 'simple':
                    show_details = not show_details
                    print(f"📋 Display mode: {'Detailed' if show_details else 'Simple'}")
                elif query.lower().startswith('top '):
                    try:
                        new_max = int(query.split()[1])
                        max_results = max(1, min(50, new_max))  # Limit between 1-50
                        print(f"📊 Results limit set to: {max_results}")
                    except (IndexError, ValueError):
                        print("❌ Invalid format. Use: top <number>")
                elif query.lower().startswith('details '):
                    parts = query.split()[1:]
                    if len(parts) >= 2:
                        entity_name = ' '.join(parts[:-1])
                        entity_type = parts[-1]
                        await self.get_entity_details(entity_name, entity_type)
                    else:
                        print("❌ Invalid format. Use: details <entity_name> <entity_type>")
                elif query.lower().startswith('relations '):
                    parts = query.split()[1:]
                    if len(parts) >= 2:
                        entity_name = ' '.join(parts[:-1])
                        entity_type = parts[-1]
                        await self.get_entity_relationships(entity_name, entity_type)
                    else:
                        print("❌ Invalid format. Use: relations <entity_name> <entity_type>")
                else:
                    # Regular search query
                    print(f"🔮 Searching for: '{query}'...")
                    results = await self.search_entities(query, k=max_results, show_details=show_details)
                    self.display_results(results, query, show_details)
                
            except KeyboardInterrupt:
                print("\n\n👋 Search interrupted. Goodbye!")
                break
            except Exception as e:
                print(f"❌ An error occurred: {e}")
                continue

    async def close(self):
        """Close database connection"""
        await self.db_handler.close()

async def main():
    """Main function"""
    search_system = InteractiveSearchSystem()
    
    try:
        await search_system.run_interactive_search()
    finally:
        await search_system.close()

if __name__ == "__main__":
    asyncio.run(main())