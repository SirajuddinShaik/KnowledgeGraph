DEFAULT_ENTITY_TYPES = [
    "Person",
    "Team",
    "Organization",
    "Project",
    "Repository",
    "Branch",
    "CodeChangeRequest",  # PR/MR
    "Issue",
    "Event",
    "Topic"
]

EMAIL_SYSTEM_PROMPT = """---Goal---
Your goal is to extract workspace-level entities and relationships from email data in tuple format. Focus on extracting business-relevant information that helps understand the organizational structure, projects, and collaboration patterns. All entity attributes are optional - extract only what is present in the data.

Use English as output language.

---CRITICAL FORMATTING REQUIREMENTS---
1. **MUST return ONLY tuple format** - no JSON, no markdown, no code blocks, with some reasoning explanatory text before generation
2. **Use specific delimiters** - <|> between fields, ## between records
3. **ALL string values MUST be properly escaped** - escape quotes and special characters
4. **NO line breaks inside string values** - use \\n for line breaks if needed
5. **End with completion delimiter** - <|COMPLETE|>
6. **Use specific names that qualify the entity type** - be descriptive and specific
7. **Names must be unique** - avoid generic names like "Project A" or "Team 1". Only extract the specific names found in the data.

---CRITICAL DATA QUALITY REQUIREMENTS---
**CONSISTENCY RULES - FOLLOW THESE STRICTLY:**

**Entity Naming Standards:**
- **Person**: Use full names when available, include email in attributes for merging (e.g., "John Smith" with email "john.smith@company.com")
- **Organization**: Use official company names, always include domain attribute (e.g., "Juspay" with domain "juspay.in")
- **Repository**: Use full repository path format "owner/repo-name" (e.g., "JBIZ/rescript-euler-dashboard")
- **CodeChangeRequest**: Use format "Pull Request #[number]" or "Merge Request #[number]" (e.g., "Pull Request #12854")
- **Issue**: Use format "Issue {{id}}" (e.g., "Issue OF-946", "Issue JP-3111")
- **Branch**: Use exact branch name as found in data

**Required Attributes for Merging:**
- **Person**: MUST include email when available, role when mentioned
- **Organization**: MUST include domain attribute for merging
- **Repository**: MUST include owner and url when available
- **CodeChangeRequest**: MUST include id, repo, author, status
- **Issue**: MUST include id when available
- **Branch**: MUST include repo attribute

**HIERARCHICAL RELATIONSHIP STANDARDS:**
- **CREATE HIERARCHICAL GRAPH STRUCTURE** - Build relationships that form a proper hierarchy, not a flat network
- **CONNECT TO IMMEDIATE PARENT/CHILD ONLY** - Do not create direct relationships between distant entities
- **FOLLOW NATURAL HIERARCHY** - Person → Team → Organization, Issue → Repository → Organization, Branch → Repository

**MANDATORY HIERARCHICAL RULES:**
1. **Person entities** should connect to their IMMEDIATE team/role, not directly to organization
2. **Issues/PRs** should connect to their IMMEDIATE repository, not to people unless they are direct authors/reviewers
3. **Branches** should connect to their IMMEDIATE repository only
4. **Teams** should connect to their IMMEDIATE parent organization
5. **Projects** should connect to their IMMEDIATE owning team/organization

**CORE HIERARCHICAL RELATIONSHIPS:**
- "AUTHORED" (person to pull request/issue) - only for direct authorship
- "REVIEWED" (person to pull request) - only for direct code review
- "BELONGS_TO" (issue/branch/pull request to immediate repository) - structural ownership
- "ASSIGNED_TO" (issue to immediate assignee) - direct assignment only
- "MERGES_FROM/MERGES_TO" (pull request to immediate source/target branches)
- "MEMBER_OF" (person to immediate team) - direct team membership
- "MANAGES" (person to immediate subordinate/project) - direct management only

**AVOID FLAT CONNECTIONS:**
- DO NOT connect every person to every organization mentioned
- DO NOT connect issues to people unless they are direct authors/assignees
- DO NOT create relationships that skip hierarchy levels
- DO NOT connect entities just because they appear in the same email

**HIERARCHICAL STRENGTH SCORING:**
- Direct parent-child relationships: 9-10
- Immediate working relationships: 7-8
- Inferred but direct relationships: 6-7
- Avoid creating relationships with strength < 6

**Significance Filter:**
- DO NOT create entities for: generic topics ("Code Review"), common branches ("master" unless specifically relevant) then use master - reponame, trivial mentions
- DO create entities for: specific people, projects, repositories, issues, organizations, significant events
- Focus on entities that have clear business value and can be meaningfully connected

---Entity Types and Attributes to Extract---
Extract the following information for each entity type (all attributes are optional and flexible):

**IMPORTANT ATTRIBUTE FLEXIBILITY:**
- **All attributes are optional** - extract only what is present in the data
- **Attributes can be merged** - when updating existing entities, combine new information with previous data
- **Update descriptions** - enhance entity descriptions with new context while preserving existing information
- **Preserve existing attributes** - when merging, keep all previously extracted attributes and add new ones

**Person**: [name, email, role, aliases, sourceSystemId, description, worksAt]
**Team**: [name, description]
**Project**: [name, description, status, startDate, endDate, client, tags]
**Organization**: [name, description, domain, industry, location]
**Repository**: [name, url, description]
**Branch**: [name, repo, createdBy, createdAt]
**CodeChangeRequest**: [id, title, description, status, author, createdAt, mergedAt, repo, branch]
**Issue**: [id, title, description, status, reporter, labels, createdAt, closedAt]
**Event**: [id, title, description, type, startTime, linkedProject]
**Topic**: [id, name, keywords, relatedThreads]

---Email-Specific Instructions---
2. **Person Entities**: **STRICT PERSON ENTITY REQUIREMENTS** - Only create Person entities when you have UNIQUE IDENTIFYING INFORMATION that enables merging:
   - **REQUIRED**: Email address (preferred) OR specific role/title OR aliases OR other distinguishing attributes
   - **ALWAYS include email when available** for merging
   - **DO NOT create Person entities for names only** - if you only have a name without additional context, do not create the entity
   - **Include comprehensive descriptions** with organizational context, role, and work activities within the Person entity description
3. **Workspace Focus**: Prioritize entities that represent work activities (projects, repositories, issues, teams)
4. **Focus on Business Relationships**: Create meaningful business relationships between entities (AUTHORED, REVIEWED, etc.)
5. **Business Context**: Extract information about code reviews, project work, organizational structure, and collaboration
6. **Extract only entities found that are useful** - don't create entities for every mention, focus on meaningful business entities
7. **Use specific names** - entity names should be descriptive and qualify the entity type
8. **ONLY CREATE SIGNIFICANT ENTITIES** - Only create an entity if it represents something important or substantial. Avoid creating entities for minor mentions, casual references, or trivial items. Focus on entities that are central to the business context or have meaningful relationships.

---MANDATORY OUTPUT FORMAT---
For each entity, output ONE line in this exact format:
("entity"<|>"<entity_name>"<|>"<entity_type>"<|>"<attribute_name>": "<attribute_value>"<|>"<attribute_name>": "<attribute_value>")##

For relationships, output ONE line in this exact format:
("relationship"<|>"<source_entity>"<|>"<target_entity>"<|>"<relationship_type>"<|>"<description>"<|><strength>)##

---FORMATTING EXAMPLES---
✅ CORRECT Entity: ("entity"<|>"Alex Johnson"<|>"Person"<|>"name": "Alex Johnson"<|>"email": "alex.johnson@company.com"<|>"role": "Software Engineer")##

✅ CORRECT Relationship: ("relationship"<|>"Alex Johnson"<|>"Authentication Project"<|>"WORKS_ON"<|>"Alex is actively developing the authentication system"<|>8)##

---STRING ESCAPING EXAMPLES---
✅ CORRECT: "description": "John said \\"Hello world\\" to the team"
❌ WRONG: "description": "John said "Hello world" to the team"
"""

ENTITY_EXTRACTION_PROMPT = """
---Real Data---
Entity_types: {entity_types}
Email Text: {context}

IMPORTANT: Return ONLY the tuple format below, end with <|COMPLETE|>:"""


ENTITY_EXTRACTION_EXAMPLES = [
    """Example 1:

Entity_types: [Person, Organization, Team, Project]
Text:
```
Sarah is a software engineer at Acme Inc, working on the new Phoenix project. She is part of the Core team. Her email is sarah.jones@acme.com and she reports to the Engineering Manager.
```

Output:
("entity"<|>"Sarah Jones"<|>"Person"<|>"name": "Sarah Jones"<|>"email": "sarah.jones@acme.com"<|>"role": "Software Engineer"<|>"description": "Software engineer at Acme Inc working on the Phoenix project as part of the Core team, reports to Engineering Manager")##
("entity"<|>"Acme Inc"<|>"Organization"<|>"name": "Acme Inc"<|>"domain": "acme.com")##
("entity"<|>"Phoenix Project"<|>"Project"<|>"name": "Phoenix Project"<|>"status": "active")##
("entity"<|>"Core Team"<|>"Team"<|>"name": "Core Team")##
("relationship"<|>"Sarah Jones"<|>"Acme Inc"<|>"WORKS_AT"<|>"Sarah is an employee of Acme Inc"<|>9)##
("relationship"<|>"Sarah Jones"<|>"Phoenix Project"<|>"WORKS_ON"<|>"Sarah is working on the Phoenix project"<|>8)##
("relationship"<|>"Sarah Jones"<|>"Core Team"<|>"MEMBER_OF"<|>"Sarah is a member of the Core team"<|>7)##
<|COMPLETE|>
#############################""",
    """Example 2:

Entity_types: [Person, Organization, Repository, Issue, Team]
Text:
```
John Smith (john.smith@techcorp.com) opened issue #1234 "Fix login bug" in the user-auth repository. The issue was assigned to the Backend team for resolution.
```

Output:
("entity"<|>"John Smith"<|>"Person"<|>"name": "John Smith"<|>"email": "john.smith@techcorp.com"<|>"description": "TechCorp employee who opened issue #1234 for fixing login bug in user-auth repository")##
("entity"<|>"TechCorp"<|>"Organization"<|>"name": "TechCorp"<|>"domain": "techcorp.com")##
("entity"<|>"user-auth Repository"<|>"Repository"<|>"name": "user-auth")##
("entity"<|>"Issue #1234 Fix Login Bug"<|>"Issue"<|>"id": "1234"<|>"title": "Fix login bug"<|>"status": "open"<|>"reporter": "John Smith")##
("entity"<|>"Backend Team"<|>"Team"<|>"name": "Backend Team")##
("relationship"<|>"John Smith"<|>"Issue #1234 Fix Login Bug"<|>"CREATED"<|>"John Smith opened the issue"<|>9)##
("relationship"<|>"Issue #1234 Fix Login Bug"<|>"user-auth Repository"<|>"BELONGS_TO"<|>"Issue is in the user-auth repository"<|>8)##
("relationship"<|>"Issue #1234 Fix Login Bug"<|>"Backend Team"<|>"ASSIGNED_TO"<|>"Issue was assigned to Backend team"<|>8)##
<|COMPLETE|>
#############################""",
]
