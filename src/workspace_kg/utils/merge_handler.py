import asyncio
from typing import Dict, Any, List, Optional
import logging
import hashlib
import json
import os

from workspace_kg.utils.kuzu_db_handler import KuzuDBHandler
from workspace_kg.utils.merge_config import merge_config, MergeStrategy
from workspace_kg.components.embedder import InferenceProvider

logger = logging.getLogger(__name__)

class MergeHandler:
    def __init__(self, kuzu_db_handler: KuzuDBHandler):
        self.db_handler = kuzu_db_handler
        self.inference_provider = InferenceProvider()

    def _generate_entity_id(self, entity_type: str, attributes: Dict[str, Any]) -> str:
        """
        Generates a consistent entity_id based on entity type and key attributes.
        This is a simplified version; a more robust solution would consider the merge rules.
        """
        # For now, use a simple hash of a combination of attributes
        # This needs to be refined based on the specific merge rules for each entity type.
        
        unique_str = f"{entity_type}"
        
        if entity_type == "Person":
            if "email" in attributes:
                unique_str += f"::email::{attributes['email'].lower()}"
            elif "name" in attributes and "email_domain" in attributes:
                unique_str += f"::name_domain::{attributes['name'].lower()}::{attributes['email_domain'].lower()}"
            elif "name" in attributes and "organization" in attributes:
                unique_str += f"::name_org::{attributes['name'].lower()}::{attributes['organization'].lower()}"
            elif "name" in attributes:
                unique_str += f"::name::{attributes['name'].lower()}"
            else:
                unique_str += f"::fallback::{json.dumps(attributes, sort_keys=True)}"
        elif entity_type == "Organization":
            if "domain" in attributes:
                unique_str += f"::domain::{attributes['domain'].lower()}"
            elif "name" in attributes:
                unique_str += f"::name::{attributes['name'].lower()}"
            else:
                unique_str += f"::fallback::{json.dumps(attributes, sort_keys=True)}"
        elif entity_type == "Repository":
            if "url" in attributes:
                unique_str += f"::url::{attributes['url'].lower()}"
            elif "name" in attributes and "organization" in attributes: # Assuming organization is an attribute
                unique_str += f"::name_org::{attributes['name'].lower()}::{attributes['organization'].lower()}"
            elif "name" in attributes:
                unique_str += f"::name::{attributes['name'].lower()}"
            else:
                unique_str += f"::fallback::{json.dumps(attributes, sort_keys=True)}"
        elif entity_type == "Issue":
            if "name" in attributes: # Assuming name is the issue ID
                unique_str += f"::id::{attributes['name'].lower()}"
            elif "title" in attributes and "repo" in attributes:
                unique_str += f"::title_repo::{attributes['title'].lower()}::{attributes['repo'].lower()}"
            else:
                unique_str += f"::fallback::{json.dumps(attributes, sort_keys=True)}"
        elif entity_type == "CodeChangeRequest":
            if "name" in attributes: # Assuming name is the PR ID/title
                unique_str += f"::id::{attributes['name'].lower()}"
            elif "title" in attributes and "repo" in attributes and "branch" in attributes and "author" in attributes:
                unique_str += f"::title_repo_branch_author::{attributes['title'].lower()}::{attributes['repo'].lower()}::{attributes['branch'].lower()}::{attributes['author'].lower()}"
            else:
                unique_str += f"::fallback::{json.dumps(attributes, sort_keys=True)}"
        elif entity_type == "Project":
            if "name" in attributes and "organization" in attributes:
                unique_str += f"::name_org::{attributes['name'].lower()}::{attributes['organization'].lower()}"
            elif "name" in attributes:
                unique_str += f"::name::{attributes['name'].lower()}"
            else:
                unique_str += f"::fallback::{json.dumps(attributes, sort_keys=True)}"
        elif entity_type == "Branch":
            if "name" in attributes and "repo" in attributes:
                unique_str += f"::name_repo::{attributes['name'].lower()}::{attributes['repo'].lower()}"
            else:
                unique_str += f"::fallback::{json.dumps(attributes, sort_keys=True)}"
        elif entity_type == "Event":
            if "event_id" in attributes:
                unique_str += f"::id::{attributes['event_id'].lower()}"
            elif "title" in attributes and "startTime" in attributes and "linkedProject" in attributes:
                unique_str += f"::title_time_project::{attributes['title'].lower()}::{attributes['startTime'].lower()}::{attributes['linkedProject'].lower()}"
            else:
                unique_str += f"::fallback::{json.dumps(attributes, sort_keys=True)}"
        elif entity_type == "Topic":
            if "topic_id" in attributes:
                unique_str += f"::id::{attributes['topic_id'].lower()}"
            elif "keywords" in attributes:
                unique_str += f"::keywords::{json.dumps(sorted(attributes['keywords']), sort_keys=True)}"
            else:
                unique_str += f"::fallback::{json.dumps(attributes, sort_keys=True)}"
        else:
            # Fallback for other entity types
            if "name" in attributes:
                unique_str += f"::name::{attributes['name'].lower()}"
            else:
                unique_str += f"::fallback::{json.dumps(attributes, sort_keys=True)}"

        return hashlib.sha256(unique_str.encode('utf-8')).hexdigest()

    def _generate_relation_id(self, 
                               from_entity_id: str, 
                               to_entity_id: str, 
                               relation_type: str, 
                               relation_tag: str) -> str:
        """
        Generates a consistent relation_id.
        """
        unique_str = f"{from_entity_id}::{relation_type}::{relation_tag}::{to_entity_id}"
        return hashlib.sha256(unique_str.encode('utf-8')).hexdigest()

    async def _find_existing_entity(self, entity_type: str, entity_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Finds an existing entity in the DB based on merge rules.
        This is a placeholder and needs to implement the full merge rulebook.
        """
        # For now, try to find by generated ID
        generated_id = self._generate_entity_id(entity_type, entity_data.get('attributes', {}))
        existing_entity = await self.db_handler.get_entity(entity_type, generated_id)
        if existing_entity:
            return existing_entity
        
        # Implement specific merge rules here
        attributes = entity_data.get('attributes', {})

        if entity_type == "Person":
            # 1. Email
            if "email" in attributes:
                query = f"MATCH (p:Person {{email: $email}}) RETURN p"
                params = {"email": attributes['email']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['p']
            
            # 1b. Name + Email Domain (common pattern in extracted data)
            if "name" in attributes and "email_domain" in attributes:
                query = f"MATCH (p:Person) WHERE p.name = $name AND (p.email ENDS WITH $email_domain OR p.email_domain = $email_domain) RETURN p"
                params = {"name": attributes['name'], "email_domain": attributes['email_domain']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['p']
            # 2. Source system IDs (e.g., HR, GitHub UID, Slack ID) - assuming 'sourceSystemId' attribute
            if "sourceSystemId" in attributes:
                query = f"MATCH (p:Person {{sourceSystemId: $sourceSystemId}}) RETURN p"
                params = {"sourceSystemId": attributes['sourceSystemId']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['p']
            # 3a. Check username/alias field (common in extracted data)
            if "username" in attributes:
                query = f"MATCH (p:Person) WHERE p.username = $username OR $username IN p.aliases RETURN p"
                params = {"username": attributes['username']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['p']
            
            if "alias" in attributes:
                query = f"MATCH (p:Person) WHERE p.alias = $alias OR $alias IN p.aliases RETURN p"
                params = {"alias": attributes['alias']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['p']
            
            # 3. Aliases list (nicknames, alternate spellings) - assuming 'aliases' is a list
            if "aliases" in attributes and isinstance(attributes['aliases'], list):
                for alias in attributes['aliases']:
                    query = f"MATCH (p:Person) WHERE $alias IN p.aliases RETURN p"
                    params = {"alias": alias}
                    result = await self.db_handler.execute_cypher(query, params)
                    if result and result.get('data'):
                        return result['data'][0]['p']
            # 4. Name + Organization/Team (same full name *within the same org*) - requires 'name' and 'organization' attributes
            if "name" in attributes and "organization" in attributes:
                query = f"MATCH (p:Person {{name: $name, organization: $organization}}) RETURN p"
                params = {"name": attributes['name'], "organization": attributes['organization']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['p']
            # Fallback: Name only (less reliable, but better than nothing if no other identifiers)
            if "name" in attributes:
                query = f"MATCH (p:Person {{name: $name}}) RETURN p"
                params = {"name": attributes['name']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data') and len(result['data']) == 1: # Only merge if unique by name
                    return result['data'][0]['p']

        elif entity_type == "Organization":
            # 1. Domain
            if "domain" in attributes:
                query = f"MATCH (o:Organization {{domain: $domain}}) RETURN o"
                params = {"domain": attributes['domain']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['o']
            # 2. Organization name
            if "name" in attributes:
                query = f"MATCH (o:Organization {{name: $name}}) RETURN o"
                params = {"name": attributes['name']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data') and len(result['data']) == 1: # Only merge if unique by name
                    return result['data'][0]['o']
            # 3. Source system ID
            if "sourceSystemId" in attributes:
                query = f"MATCH (o:Organization {{sourceSystemId: $sourceSystemId}}) RETURN o"
                params = {"sourceSystemId": attributes['sourceSystemId']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['o']

        elif entity_type == "Team":
            # 1. Team name (case-insensitive exact match) + Org ID (team under same organization)
            if "name" in attributes and "organizationId" in attributes: # Assuming organizationId attribute
                query = f"MATCH (t:Team) WHERE toLower(t.name) = toLower($name) AND t.organizationId = $organizationId RETURN t"
                params = {"name": attributes['name'], "organizationId": attributes['organizationId']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['t']
            # 2. Source system ID
            if "sourceSystemId" in attributes:
                query = f"MATCH (t:Team {{sourceSystemId: $sourceSystemId}}) RETURN t"
                params = {"sourceSystemId": attributes['sourceSystemId']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['t']
            # Fallback: Team name only (if no org ID)
            if "name" in attributes:
                query = f"MATCH (t:Team) WHERE toLower(t.name) = toLower($name) RETURN t"
                params = {"name": attributes['name']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data') and len(result['data']) == 1: # Only merge if unique by name
                    return result['data'][0]['t']

        elif entity_type == "Project":
            # 1. Project name + Org (must match both) - assuming 'organization' attribute
            if "name" in attributes and "organization" in attributes:
                query = f"MATCH (p:Project {{name: $name, organization: $organization}}) RETURN p"
                params = {"name": attributes['name'], "organization": attributes['organization']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['p']
            # 2. Client ID (if linked) - assuming 'clientId' attribute
            if "clientId" in attributes:
                query = f"MATCH (p:Project {{clientId: $clientId}}) RETURN p"
                params = {"clientId": attributes['clientId']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['p']
            # 3. Source system ID (Jira project ID, Asana project ID) - assuming 'sourceSystemId' attribute
            if "sourceSystemId" in attributes:
                query = f"MATCH (p:Project {{sourceSystemId: $sourceSystemId}}) RETURN p"
                params = {"sourceSystemId": attributes['sourceSystemId']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['p']
            # Fallback: Project name only (less reliable)
            if "name" in attributes:
                query = f"MATCH (p:Project {{name: $name}}) RETURN p"
                params = {"name": attributes['name']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data') and len(result['data']) == 1: # Only merge if unique by name
                    return result['data'][0]['p']

        elif entity_type == "Repository":
            # 1. Repo URL (canonical identifier)
            if "url" in attributes:
                query = f"MATCH (r:Repository {{url: $url}}) RETURN r"
                params = {"url": attributes['url']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['r']
            # 2. Repo name + Org/Project (disambiguate if same name in different orgs) - assuming 'name' and 'organization' or 'project'
            if "name" in attributes and ("organization" in attributes or "project" in attributes):
                if "organization" in attributes:
                    query = f"MATCH (r:Repository {{name: $name, organization: $organization}}) RETURN r"
                    params = {"name": attributes['name'], "organization": attributes['organization']}
                else: # Assuming 'project'
                    query = f"MATCH (r:Repository {{name: $name, project: $project}}) RETURN r"
                    params = {"name": attributes['name'], "project": attributes['project']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['r']
            # 3. Source system ID (GitHub/GitLab/Bitbucket repo ID) - assuming 'sourceSystemId'
            if "sourceSystemId" in attributes:
                query = f"MATCH (r:Repository {{sourceSystemId: $sourceSystemId}}) RETURN r"
                params = {"sourceSystemId": attributes['sourceSystemId']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['r']
            # Fallback: Repo name only (less reliable)
            if "name" in attributes:
                query = f"MATCH (r:Repository {{name: $name}}) RETURN r"
                params = {"name": attributes['name']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data') and len(result['data']) == 1: # Only merge if unique by name
                    return result['data'][0]['r']

        elif entity_type == "Branch":
            # 1. Branch name + Repo (combination must be unique) - assuming 'name' and 'repo' attributes
            if "name" in attributes and "repo" in attributes:
                query = f"MATCH (b:Branch {{name: $name, repo: $repo}}) RETURN b"
                params = {"name": attributes['name'], "repo": attributes['repo']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['b']
            # 2. CreatedBy + Timestamp (helps resolve re-created branches) - assuming 'createdBy' and 'createdAt'
            if "createdBy" in attributes and "createdAt" in attributes:
                query = f"MATCH (b:Branch {{createdBy: $createdBy, createdAt: $createdAt}}) RETURN b"
                params = {"createdBy": attributes['createdBy'], "createdAt": attributes['createdAt']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['b']

        elif entity_type == "CodeChangeRequest":
            # 1. ID from source system (GitHub PR #, GitLab MR #) - assuming 'sourceSystemId'
            if "sourceSystemId" in attributes:
                query = f"MATCH (ccr:CodeChangeRequest {{sourceSystemId: $sourceSystemId}}) RETURN ccr"
                params = {"sourceSystemId": attributes['sourceSystemId']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['ccr']
            # 2. Title + Repo + Branch + Author (as fallback) - assuming 'title', 'repo', 'branch', 'author'
            if all(k in attributes for k in ["title", "repo", "branch", "author"]):
                query = f"MATCH (ccr:CodeChangeRequest {{title: $title, repo: $repo, branch: $branch, author: $author}}) RETURN ccr"
                params = {k: attributes[k] for k in ["title", "repo", "branch", "author"]}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['ccr']
            # 3. CreatedAt timestamp - assuming 'createdAt'
            if "createdAt" in attributes:
                query = f"MATCH (ccr:CodeChangeRequest {{createdAt: $createdAt}}) RETURN ccr"
                params = {"createdAt": attributes['createdAt']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['ccr']

        elif entity_type == "Issue":
            # 1. Issue ID from source system (Jira ID, GitHub Issue #) - assuming 'sourceSystemId'
            if "sourceSystemId" in attributes:
                query = f"MATCH (i:Issue {{sourceSystemId: $sourceSystemId}}) RETURN i"
                params = {"sourceSystemId": attributes['sourceSystemId']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['i']
            # 2. Title + Repo/Project - assuming 'title' and 'repo' or 'project'
            if "title" in attributes and ("repo" in attributes or "project" in attributes):
                if "repo" in attributes:
                    query = f"MATCH (i:Issue {{title: $title, repo: $repo}}) RETURN i"
                    params = {"title": attributes['title'], "repo": attributes['repo']}
                else: # Assuming 'project'
                    query = f"MATCH (i:Issue {{title: $title, project: $project}}) RETURN i"
                    params = {"title": attributes['title'], "project": attributes['project']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['i']
            # 3. Reporter + CreatedAt - assuming 'reporter' and 'createdAt'
            if "reporter" in attributes and "createdAt" in attributes:
                query = f"MATCH (i:Issue {{reporter: $reporter, createdAt: $createdAt}}) RETURN i"
                params = {"reporter": attributes['reporter'], "createdAt": attributes['createdAt']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['i']

        elif entity_type == "Event":
            # 1. Event ID (from calendar/system) - assuming 'sourceSystemId' or 'eventId'
            if "sourceSystemId" in attributes:
                query = f"MATCH (e:Event {{sourceSystemId: $sourceSystemId}}) RETURN e"
                params = {"sourceSystemId": attributes['sourceSystemId']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['e']
            elif "eventId" in attributes:
                query = f"MATCH (e:Event {{eventId: $eventId}}) RETURN e"
                params = {"eventId": attributes['eventId']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['e']
            # 2. Title + StartTime + LinkedProject - assuming 'title', 'startTime', 'linkedProject'
            if all(k in attributes for k in ["title", "startTime", "linkedProject"]):
                query = f"MATCH (e:Event {{title: $title, startTime: $startTime, linkedProject: $linkedProject}}) RETURN e"
                params = {k: attributes[k] for k in ["title", "startTime", "linkedProject"]}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['e']
            # 3. Participants list (if available) - assuming 'participants' is a list
            if "participants" in attributes and isinstance(attributes['participants'], list):
                for participant in attributes['participants']:
                    query = f"MATCH (e:Event) WHERE $participant IN e.participants RETURN e"
                    params = {"participant": participant}
                    result = await self.db_handler.execute_cypher(query, params)
                    if result and result.get('data'):
                        return result['data'][0]['e']

        elif entity_type == "Topic":
            # 1. Topic ID (if generated by clustering) - assuming 'sourceSystemId' or 'topicId'
            if "sourceSystemId" in attributes:
                query = f"MATCH (t:Topic {{sourceSystemId: $sourceSystemId}}) RETURN t"
                params = {"sourceSystemId": attributes['sourceSystemId']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['t']
            elif "topicId" in attributes:
                query = f"MATCH (t:Topic {{topicId: $topicId}}) RETURN t"
                params = {"topicId": attributes['topicId']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['t']
            # 2. Keywords set (exact match of cluster signature) - assuming 'keywords' is a list
            if "keywords" in attributes and isinstance(attributes['keywords'], list):
                # This requires a more complex query to match exact set of keywords
                # For simplicity, we'll check if all keywords are present
                keyword_match_clauses = [f"$keyword{i} IN t.keywords" for i in range(len(attributes['keywords']))]
                query = f"MATCH (t:Topic) WHERE {' AND '.join(keyword_match_clauses)} AND size(t.keywords) = $keyword_count RETURN t"
                params = {f"keyword{i}": kw for i, kw in enumerate(attributes['keywords'])}
                params["keyword_count"] = len(attributes['keywords'])
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['t']
            # 3. RelatedThreads overlap (if same thread set) - assuming 'relatedThreads' is a list
            if "relatedThreads" in attributes and isinstance(attributes['relatedThreads'], list):
                for thread in attributes['relatedThreads']:
                    query = f"MATCH (t:Topic) WHERE $thread IN t.relatedThreads RETURN t"
                    params = {"thread": thread}
                    result = await self.db_handler.execute_cypher(query, params)
                    if result and result.get('data'):
                        return result['data'][0]['t']

        return None

    def _process_attributes(self, entity_type: str, attributes: Dict[str, Any], source_item_id: str, entity_name: str, is_from_agent: bool = False) -> Dict[str, Any]:
        """
        Process and transform attributes according to merge configuration.
        Maps LLM fields to database schema fields with proper transformations.
        """
        processed = {}
        
        # Initialize required array fields
        processed['rawDescriptions'] = []
        
        # Only initialize sources if entity type supports it
        entity_array_fields = merge_config.get_entity_array_fields(entity_type)
        if 'sources' in entity_array_fields:
            processed['sources'] = []
        
        # Process each attribute according to configuration
        for llm_field, value in attributes.items():
            if not merge_config.should_merge_field(entity_type, llm_field, is_from_agent):
                logger.debug(f"Skipping agent-only field '{llm_field}' for {entity_type}")
                continue
            
            # Get target field and transformation
            target_field = merge_config.get_target_field(entity_type, llm_field)
            transformed_value = merge_config.transform_value(entity_type, llm_field, value, target_field)
            
            # Handle field mapping
            if llm_field == "description":
                # Description goes into rawDescriptions array
                if transformed_value and isinstance(transformed_value, list):
                    processed['rawDescriptions'].extend(transformed_value)
                elif transformed_value:
                    processed['rawDescriptions'].append(transformed_value)
            elif target_field in merge_config.field_mappings.get('timestamp_fields', []):
                # Keep timestamp fields as strings - let DB handle conversion
                processed[target_field] = transformed_value
            else:
                processed[target_field] = transformed_value
        
        # Add source tracking (only if entity supports sources field)
        if 'sources' in processed and source_item_id not in processed['sources']:
            processed['sources'].append(source_item_id)

        
        # Ensure entity-specific array fields are properly formatted
        entity_array_fields = merge_config.field_mappings.get('entity_array_fields', {}).get(entity_type, [])
        for field in entity_array_fields:
            if field in processed and not isinstance(processed[field], list):
                processed[field] = [processed[field]]
            elif field not in processed:
                processed[field] = []
        
        return processed

    def _merge_attributes(self, entity_type: str, existing_entity: Dict[str, Any], new_attributes: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge new attributes with existing entity according to configuration.
        """
        updates = {}
        
        for field, new_value in new_attributes.items():
            if field in merge_config.field_mappings.get('always_preserve', []):
                # Never update these fields
                continue
            
            strategy = merge_config.get_merge_strategy(entity_type, field)
            existing_value = existing_entity.get(field)
            
            if strategy == MergeStrategy.PRESERVE_EXISTING:
                # Only update if existing is None/empty
                if not existing_value:
                    updates[field] = new_value
            
            elif strategy == MergeStrategy.APPEND_UNIQUE:
                # Merge arrays with unique values
                if isinstance(new_value, list):
                    existing_list = existing_value if isinstance(existing_value, list) else []
                    merged_list = list(existing_list)
                    for item in new_value:
                        if item not in merged_list:
                            merged_list.append(item)
                    updates[field] = merged_list
                else:
                    # Single value to append to array
                    existing_list = existing_value if isinstance(existing_value, list) else []
                    if new_value not in existing_list:
                        updates[field] = existing_list + [new_value]
            
            elif strategy == MergeStrategy.REPLACE_ALWAYS:
                # Always use new value
                updates[field] = new_value
            
            elif strategy == MergeStrategy.REPLACE_IF_BETTER:
                # Use new value if it's "better" (longer description, more complete, etc.)
                if not existing_value or (isinstance(new_value, str) and len(new_value) > len(str(existing_value))):
                    updates[field] = new_value
            
            elif strategy == MergeStrategy.AGENT_ONLY:
                # Should not happen for LLM data, but skip if it does
                logger.debug(f"Skipping agent-only field '{field}' in merge")
                continue
        
        return updates

    async def process_batch(self, batch_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Processes a batch of extracted entities and relationships,
        merging duplicates and storing them in KuzuDB.
        
        Args:
            batch_data: Dict with 'entities' and 'relations' keys OR single item from extracted data
        """
        # Handle both formats: direct batch_data or item from extracted data
        if 'entities' in batch_data and 'relations' in batch_data:
            # Direct format from test files
            entities_list = batch_data['entities']
            relations_list = batch_data['relations']
            source_item_id = batch_data.get('source_item_id', 'unknown')
        elif 'item_id' in batch_data:
            # Format from extracted data file
            entities_list = batch_data.get('entities', [])
            relations_list = batch_data.get('relationships', [])
            source_item_id = batch_data['item_id']
        else:
            logger.error(f"Unknown batch data format: {batch_data.keys()}")
            return {"status": "error", "message": "Unknown batch data format"}

        processed_entities = {} # entity_name -> entity_data
        processed_relations = []

        # Step 1: Process Entities
        for entity_raw in entities_list:
            # Handle both old and new formats
            entity_type = entity_raw.get('entity_type') or entity_raw.get('type')
            entity_name = entity_raw.get('entity_name') or entity_raw.get('name')
            
            if not entity_type or not entity_name:
                logger.warning(f"Skipping entity due to missing type or name: {entity_raw}")
                continue
                
            attributes = entity_raw.get('attributes', {}).copy()
            
            # Transform and map attributes according to configuration
            processed_attributes = self._process_attributes(entity_type, attributes, source_item_id, entity_name)

            # Try to find an existing entity based on merge rules
            existing_entity = await self._find_existing_entity(entity_type, entity_raw)

            if existing_entity:
                # Merge: Update existing entity using configuration-based merging
                entity_id = existing_entity['entity_id']
                updates = self._merge_attributes(entity_type, existing_entity, processed_attributes)
                
                # Generate embedding if significant content has changed
                if updates and any(field in updates for field in ['name', 'rawDescriptions', 'title', 'description']):
                    try:
                        # Create combined entity data for embedding
                        combined_data = {**existing_entity, **updates}
                        embedding = self.inference_provider.embed_entity(entity_type, combined_data)
                        if embedding:
                            updates['embedding'] = embedding
                            logger.debug(f"Generated embedding for updated entity {entity_type}:{entity_name}")
                    except Exception as e:
                        logger.warning(f"Failed to generate embedding for entity {entity_type}:{entity_name}: {e}")
                
                if updates:
                    await self.db_handler.update_entity(entity_type, entity_id, updates)
                
                processed_entities[entity_name] = {'entity_id': entity_id, 'entity_type': entity_type}
                logger.info(f"Merged entity {entity_type}:{entity_name} -> {entity_id}")
            else:
                # Create new entity
                entity_id = self._generate_entity_id(entity_type, processed_attributes)
                processed_attributes['entity_id'] = entity_id
                
                # Generate embedding for new entity
                try:
                    embedding = self.inference_provider.embed_entity(entity_type, processed_attributes)
                    if embedding:
                        processed_attributes['embedding'] = embedding
                        logger.debug(f"Generated embedding for new entity {entity_type}:{entity_name}")
                except Exception as e:
                    logger.warning(f"Failed to generate embedding for entity {entity_type}:{entity_name}: {e}")
                
                new_entity = await self.db_handler.create_entity(entity_type, processed_attributes)
                if new_entity:
                    processed_entities[entity_name] = {'entity_id': entity_id, 'entity_type': entity_type}
                    logger.info(f"Created new entity {entity_type}:{entity_name} -> {entity_id}")
                else:
                    logger.warning(f"Could not create entity: {entity_raw}")
                    # Fallback: if creation fails, use a temporary ID for relations in this batch
                    processed_entities[entity_name] = {'entity_id': f"temp_{hashlib.sha256(entity_name.encode()).hexdigest()}", 'entity_type': entity_type}

        # Step 2: Process Relationships
        for rel_raw in relations_list:
            # Handle both old and new formats
            source_entity_name = rel_raw.get('source_entity') or rel_raw.get('source')
            target_entity_name = rel_raw.get('target_entity') or rel_raw.get('target')
            relationship_type = rel_raw.get('relationship_type') or rel_raw.get('type')
            
            if not source_entity_name or not target_entity_name or not relationship_type:
                logger.warning(f"Skipping relation due to missing source/target/type: {rel_raw}")
                continue
                
            description = rel_raw.get('description', '')
            strength = rel_raw.get('strength', 1.0)

            if source_entity_name not in processed_entities or target_entity_name not in processed_entities:
                logger.warning(f"Skipping relation due to missing entities: {rel_raw}")
                continue

            from_entity_id = processed_entities[source_entity_name]['entity_id']
            from_entity_type = processed_entities[source_entity_name]['entity_type']
            to_entity_id = processed_entities[target_entity_name]['entity_id']
            to_entity_type = processed_entities[target_entity_name]['entity_type']

            relation_tag = relationship_type # Using relationship_type as relationTag for now
            relation_id = self._generate_relation_id(from_entity_id, to_entity_id, relationship_type, relation_tag)

            relation_properties = {
                "relation_id": relation_id,
                "relationTag": relation_tag,
                "description": description,
                "strength": strength,
                "sources": [source_item_id],
                "type": relationship_type # Store original relationship type
            }

            # Check for existing relation
            existing_relation = await self.db_handler.get_relation(relation_id)
            
            if existing_relation:
                # Update existing relation (e.g., add source, update strength if needed)
                updates = {
                    "sources": [source_item_id],
                    "strength": max(existing_relation.get('strength', 0), strength) # Take max strength
                }
                
                # Generate embedding if significant content has changed
                if updates and any(field in updates for field in ['description', 'relationTag', 'type']):
                    try:
                        # Create combined relation data for embedding
                        combined_data = {**existing_relation, **updates}
                        embedding = self.inference_provider.embed_relation(combined_data)
                        if embedding:
                            updates['embedding'] = embedding
                            logger.debug(f"Generated embedding for updated relation {relation_id}")
                    except Exception as e:
                        logger.warning(f"Failed to generate embedding for relation {relation_id}: {e}")
                
                await self.db_handler.update_relation(relation_id, updates)
                logger.info(f"Updated relation {relation_id}")
            else:
                # Generate embedding for new relation
                try:
                    embedding = self.inference_provider.embed_relation(relation_properties)
                    if embedding:
                        relation_properties['embedding'] = embedding
                        logger.debug(f"Generated embedding for new relation {relation_id}")
                except Exception as e:
                    logger.warning(f"Failed to generate embedding for relation {relation_id}: {e}")
                
                # Create new relation
                new_relation = await self.db_handler.create_relation(
                    from_entity_type, from_entity_id, to_entity_type, to_entity_id, relation_properties
                )
                if new_relation:
                    processed_relations.append(new_relation)
                    logger.info(f"Created new relation {relation_id}: {from_entity_id} -> {to_entity_id}")
                else:
                    logger.warning(f"Could not create relation: {rel_raw}")

        return {
            "status": "success",
            "entities_processed": len(processed_entities),
            "relations_processed": len(processed_relations)
        }
