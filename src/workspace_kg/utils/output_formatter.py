import json
from typing import Dict, List, Any, Optional
from datetime import datetime
from enum import Enum


class OutputFormat(Enum):
    JSON = "json"
    JSONL = "jsonl"
    CSV = "csv"


class OutputFormatter:
    """Handles formatting and validation of extraction results"""
    
    def __init__(self, include_metadata: bool = True, include_stats: bool = True):
        self.include_metadata = include_metadata
        self.include_stats = include_stats
    
    def format_results(self, 
                      results: List[Dict[str, Any]], 
                      pipeline_stats: Dict[str, Any],
                      entity_types: List[str],
                      batch_size: int,
                      parallel_calls: int,
                      output_format: OutputFormat = OutputFormat.JSON) -> Dict[str, Any]:
        """Format extraction results with metadata and statistics"""
        
        formatted_output = {}
        
        if self.include_metadata:
            formatted_output["metadata"] = {
                "pipeline_version": "2.0.0",
                "generated_at": datetime.now().isoformat(),
                "pipeline_config": {
                    "entity_types": entity_types,
                    "batch_size": batch_size,
                    "parallel_calls": parallel_calls
                }
            }
        
        if self.include_stats:
            formatted_output["statistics"] = self._calculate_detailed_stats(results, pipeline_stats)
        
        # Format individual results
        formatted_output["results"] = [self._format_single_result(result) for result in results]
        
        return formatted_output
    
    def _format_single_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Format a single extraction result"""
        formatted_result = {
            "item_id": result.get("item_id"),
            "data_type": result.get("data_type", "unknown"),
            "processed_at": result.get("processed_at"),
            "extraction_status": "success" if not result.get("error") else "failed",
            "entities": self._format_entities(result.get("entities", [])),
            "relationships": self._format_relationships(result.get("relationships", []))
        }
        
        if result.get("error"):
            formatted_result["error"] = {
                "message": result["error"],
                "raw_output": result.get("raw_llm_output", "")[:500]  # Truncate for readability
            }
        
        return formatted_result
    
    def _format_entities(self, entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format entity list with validation"""
        formatted_entities = []
        
        for entity in entities:
            formatted_entity = {
                "name": entity.get("entity_name"),
                "type": entity.get("entity_type"),
                "attributes": entity.get("attributes", {}),
                "confidence_score": self._calculate_entity_confidence(entity)
            }
            formatted_entities.append(formatted_entity)
        
        return formatted_entities
    
    def _format_relationships(self, relationships: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format relationship list with validation"""
        formatted_relationships = []
        
        for rel in relationships:
            formatted_rel = {
                "source": rel.get("source_entity"),
                "target": rel.get("target_entity"),
                "type": rel.get("relationship_type"),
                "description": rel.get("description"),
                "strength": rel.get("strength", 5.0)
            }
            formatted_relationships.append(formatted_rel)
        
        return formatted_relationships
    
    def _calculate_entity_confidence(self, entity: Dict[str, Any]) -> float:
        """Calculate confidence score for an entity based on attributes"""
        confidence = 0.5  # Base confidence
        
        attributes = entity.get("attributes", {})
        
        # Boost confidence for entities with identifying attributes
        if attributes.get("email"):
            confidence += 0.3
        if attributes.get("id"):
            confidence += 0.2
        if attributes.get("url"):
            confidence += 0.2
        if len(attributes) > 2:
            confidence += 0.1
        
        return min(confidence, 1.0)
    
    def _calculate_detailed_stats(self, results: List[Dict[str, Any]], pipeline_stats: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate detailed statistics from results"""
        total_items = len(results)
        successful_items = len([r for r in results if not r.get("error")])
        failed_items = total_items - successful_items
        
        # Entity type breakdown
        entity_type_counts = {}
        relationship_type_counts = {}
        data_type_counts = {}
        
        total_entities = 0
        total_relationships = 0
        
        for result in results:
            data_type = result.get("data_type", "unknown")
            data_type_counts[data_type] = data_type_counts.get(data_type, 0) + 1
            
            for entity in result.get("entities", []):
                entity_type = entity.get("entity_type", "Unknown")
                entity_type_counts[entity_type] = entity_type_counts.get(entity_type, 0) + 1
                total_entities += 1
            
            for rel in result.get("relationships", []):
                rel_type = rel.get("relationship_type", "Unknown")
                relationship_type_counts[rel_type] = relationship_type_counts.get(rel_type, 0) + 1
                total_relationships += 1
        
        return {
            "processing": {
                "total_items": total_items,
                "successful_extractions": successful_items,
                "failed_extractions": failed_items,
                "success_rate": (successful_items / total_items * 100) if total_items > 0 else 0
            },
            "extraction_counts": {
                "total_entities": total_entities,
                "total_relationships": total_relationships,
                "avg_entities_per_item": total_entities / total_items if total_items > 0 else 0,
                "avg_relationships_per_item": total_relationships / total_items if total_items > 0 else 0
            },
            "breakdowns": {
                "entity_types": entity_type_counts,
                "relationship_types": relationship_type_counts,
                "data_types": data_type_counts
            },
            "performance": pipeline_stats
        }
    
    def validate_results(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate extraction results and return validation report"""
        validation_report = {
            "is_valid": True,
            "warnings": [],
            "errors": [],
            "recommendations": []
        }
        
        for i, result in enumerate(results):
            # Check for required fields
            if "item_id" not in result:
                validation_report["errors"].append(f"Result {i}: Missing item_id")
                validation_report["is_valid"] = False
            
            # Check entity format
            for j, entity in enumerate(result.get("entities", [])):
                if not entity.get("entity_name"):
                    validation_report["warnings"].append(f"Result {i}, Entity {j}: Missing entity_name")
                if not entity.get("entity_type"):
                    validation_report["warnings"].append(f"Result {i}, Entity {j}: Missing entity_type")
            
            # Check relationship format
            for j, rel in enumerate(result.get("relationships", [])):
                if not rel.get("source_entity") or not rel.get("target_entity"):
                    validation_report["warnings"].append(f"Result {i}, Relationship {j}: Missing source or target entity")
                if not rel.get("relationship_type"):
                    validation_report["warnings"].append(f"Result {i}, Relationship {j}: Missing relationship_type")
        
        # Add recommendations
        if len(validation_report["warnings"]) > len(results) * 0.1:
            validation_report["recommendations"].append("High number of warnings detected. Consider reviewing prompt configuration.")
        
        return validation_report
    
    def export_to_format(self, 
                        formatted_data: Dict[str, Any], 
                        output_format: OutputFormat, 
                        file_path: str):
        """Export formatted data to specified format"""
        
        if output_format == OutputFormat.JSON:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(formatted_data, f, indent=2, ensure_ascii=False)
        
        elif output_format == OutputFormat.JSONL:
            with open(file_path, 'w', encoding='utf-8') as f:
                for result in formatted_data.get("results", []):
                    f.write(json.dumps(result, ensure_ascii=False) + '\n')
        
        elif output_format == OutputFormat.CSV:
            self._export_to_csv(formatted_data, file_path)
    
    def _export_to_csv(self, formatted_data: Dict[str, Any], file_path: str):
        """Export data to CSV format (entities and relationships as separate files)"""
        import csv
        
        # Export entities
        entities_file = file_path.replace('.csv', '_entities.csv')
        with open(entities_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['item_id', 'entity_name', 'entity_type', 'attributes'])
            
            for result in formatted_data.get("results", []):
                item_id = result.get("item_id")
                for entity in result.get("entities", []):
                    writer.writerow([
                        item_id,
                        entity.get("name"),
                        entity.get("type"),
                        json.dumps(entity.get("attributes", {}))
                    ])
        
        # Export relationships
        relationships_file = file_path.replace('.csv', '_relationships.csv')
        with open(relationships_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['item_id', 'source', 'target', 'relationship_type', 'description', 'strength'])
            
            for result in formatted_data.get("results", []):
                item_id = result.get("item_id")
                for rel in result.get("relationships", []):
                    writer.writerow([
                        item_id,
                        rel.get("source"),
                        rel.get("target"),
                        rel.get("type"),
                        rel.get("description"),
                        rel.get("strength")
                    ])