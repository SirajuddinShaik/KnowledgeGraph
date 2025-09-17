#!/usr/bin/env python3
"""
Simple script to print all person names, aliases, and emails from KuzuDB
"""

import asyncio
import httpx
import json

async def print_all_persons():
    """Print all person names, aliases, and emails from the database"""
    
    client = httpx.AsyncClient(base_url="http://localhost:7000", timeout=30.0)
    
    try:
        # Test connection
        response = await client.get("/")
        print("🔗 Connected to KuzuDB")
        
        # Query all persons with name, aliases, and emails
        query = "MATCH (p:Person) RETURN p.name as name, p.aliases as aliases, p.emails as emails ORDER BY p.name"
        response = await client.post("/cypher", json={"query": query})
        result = response.json()
        
        persons = result.get('rows', [])
        
        print(f"\n👥 Found {len(persons)} persons:")
        print("=" * 80)
        
        for i, person in enumerate(persons, 1):
            name = person.get('name', 'N/A')
            aliases = person.get('aliases', None)
            emails = person.get('emails', None) # Changed from 'email' to 'emails'
            
            print(f"\n{i:2d}. Name: {name}")
            
            if aliases:
                if isinstance(aliases, list):
                    print(f"    Aliases: {', '.join(aliases)}")
                else:
                    print(f"    Aliases: {aliases}")
            else:
                print(f"    Aliases: None")
            
            if emails: # Changed from 'email' to 'emails'
                if isinstance(emails, list): # Handle emails as a list
                    print(f"    Emails: {', '.join(emails)}")
                else:
                    print(f"    Emails: {emails}")
            else:
                print(f"    Emails: None")
        
        print("\n" + "=" * 80)
        print(f"Total: {len(persons)} persons")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    
    finally:
        await client.aclose()

if __name__ == "__main__":
    asyncio.run(print_all_persons())
