import os
import requests
import json
from typing import Dict, Any, List

class InferenceProvider:
    def __init__(self):
        self.model_name = os.getenv("OLLAMA_EMBEDDING_MODEL")
        self.base_url = os.getenv("OLLAMA_BASE_URL")
        self.api_endpoint = f"{self.base_url}/api/embeddings"
        
    def embed_text(self, text: str) -> List[float]:
        """
        Generates embeddings for a given text using Ollama API.
        Returns a list of floats for database compatibility.
        """
        if not text or not isinstance(text, str):
            return []
            
        try:
            payload = {
                "model": self.model_name,
                "prompt": text
            }
            
            response = requests.post(
                self.api_endpoint,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30,
                verify=False  # Disable SSL verification for ngrok tunnels
            )
            
            response.raise_for_status()
            result = response.json()
            
            if "embedding" in result:
                return result["embedding"]
            else:
                print(f"Warning: No embedding found in response: {result}")
                return []
                
        except requests.exceptions.RequestException as e:
            print(f"Error making request to Ollama API: {e}")
            return []
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON response: {e}")
            return []
        except Exception as e:
            print(f"Unexpected error in embed_text: {e}")
            return []

    def embed_entity(self, entity_type: str, entity_data: Dict[str, Any]) -> List[float]:
        """
        Generates embeddings for an entity/node based on its type and attributes.
        """
        # Create a text representation of the entity
        text_parts = [entity_type]
        
        # Add name if available
        if 'name' in entity_data:
            text_parts.append(f"Name: {entity_data['name']}")
        
        # Add description from rawDescriptions if available
        if 'rawDescriptions' in entity_data and isinstance(entity_data['rawDescriptions'], list):
            descriptions = [desc for desc in entity_data['rawDescriptions'] if desc]
            if descriptions:
                text_parts.append(f"Description: {' '.join(descriptions[:3])}")  # Limit to first 3 descriptions
        
        # Add other key attributes
        key_attrs = ['title', 'email', 'organization', 'role']
        for attr in key_attrs:
            if attr in entity_data and entity_data[attr]:
                text_parts.append(f"{attr.title()}: {entity_data[attr]}")
        
        entity_text = ". ".join(text_parts)
        return self.embed_text(entity_text)

    def embed_relation(self, relation_data: Dict[str, Any]) -> List[float]:
        """
        Generates embeddings for a relation based on its properties.
        """
        # Create a text representation of the relation
        text_parts = []
        
        # Add relation type/tag
        if 'relationTag' in relation_data:
            text_parts.append(f"Relation: {relation_data['relationTag']}")
        elif 'type' in relation_data:
            text_parts.append(f"Relation: {relation_data['type']}")
        
        # Add description if available
        if 'description' in relation_data and relation_data['description']:
            text_parts.append(f"Description: {relation_data['description']}")
        
        # Add strength if available
        if 'strength' in relation_data:
            text_parts.append(f"Strength: {relation_data['strength']}")
        
        relation_text = ". ".join(text_parts) if text_parts else "Generic relation"
        return self.embed_text(relation_text)

    def test_connection(self) -> bool:
        """
        Test if Ollama API is accessible and the model is available.
        """
        try:
            # Test with a simple prompt
            test_embedding = self.embed_text("test")
            return len(test_embedding) > 0
        except Exception as e:
            print(f"Connection test failed: {e}")
            return False
