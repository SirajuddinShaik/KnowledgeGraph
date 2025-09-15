import os
from transformers import AutoTokenizer, AutoModel
import torch
from typing import Dict, Any, List

class InferenceProvider:
    def __init__(self):
        model_name = os.getenv("EMBEDDING_MODEL")
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name)

    def embed_text(self, text: str) -> List[float]:
        """
        Generates embeddings for a given text.
        Returns a list of floats for database compatibility.
        """
        if not text or not isinstance(text, str):
            return []
            
        encoded_input = self.tokenizer(text, padding=True, truncation=True, return_tensors='pt', max_length=512)
        with torch.no_grad():
            model_output = self.model(**encoded_input)
        # Mean pooling to get a single vector for the sentence
        sentence_embeddings = self._mean_pooling(model_output, encoded_input['attention_mask'])
        # Convert to list for database storage
        return sentence_embeddings.squeeze().tolist()

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

    def _mean_pooling(self, model_output, attention_mask):
        """Mean pooling to get a single vector for the sentence."""
        token_embeddings = model_output[0]  # First element contains all token embeddings
        input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
        return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(input_mask_expanded.sum(1), min=1e-9)
