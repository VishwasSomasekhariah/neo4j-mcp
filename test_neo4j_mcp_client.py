#!/usr/bin/env python3
"""
Comprehensive test client for Neo4j Memory MCP tools.
Tests all functionality including complex property handling.
"""

import asyncio
import json
import uuid
import os
from datetime import datetime
from fastmcp import Client
from fastmcp.client.transports import SSETransport

# Configuration
SERVER_URL = "http://localhost:8100/sse"
ENTITY_ID_PREFIX = "test_entity_"

async def print_result(result):
    """Pretty print the result from an MCP tool call"""
    content_item = result[0]
    result_data = json.loads(content_item.text)
    print(json.dumps(result_data, indent=2))
    return result_data

async def test_neo4j_create_entity_with_complex_props(client):
    """Test creating an entity with complex properties in Neo4j"""
    print("\n=== Testing neo4j_create_entity with complex properties ===")
    
    # Generate a unique ID for this test run
    entity_id = f"{ENTITY_ID_PREFIX}{uuid.uuid4().hex[:8]}"
    
    # Create complex properties
    complex_props = {
        "id": entity_id,
        "name": "ComplexClass",
        "language": "python",
        "file_path": "src/services/complex_service.py",
        "created_at": datetime.now().isoformat(),
        # Complex dictionary
        "metadata": {
            "author": "Test User",
            "version": "1.0.0",
            "tags": ["service", "core", "api"]
        },
        # Complex list of dictionaries
        "methods": [
            {"name": "process_data", "line": 42, "visibility": "public"},
            {"name": "validate_input", "line": 78, "visibility": "private"}
        ],
        # Mixed list
        "dependencies": ["os", "json", {"name": "requests", "version": "2.28.1"}]
    }
    
    response = await client.call_tool(
        "neo4j_create_entity",
        {
            "entity_type": "CodeClass",
            "properties": complex_props
        }
    )
    
    result = await print_result(response)
    return entity_id if result.get("success") else None

async def test_neo4j_get_entity_with_complex_props(client, entity_id):
    """Test retrieving an entity with deserialized complex properties"""
    print("\n=== Testing neo4j_get_entity_with_complex_props ===")
    
    if not entity_id:
        print("Skipping test: No valid entity ID available")
        return False
    
    response = await client.call_tool(
        "neo4j_get_entity_with_complex_props",
        {
            "entity_id": entity_id
        }
    )
    
    result = await print_result(response)
    return result.get("success", False)

async def test_neo4j_add_observation(client, entity_id):
    """Test adding a complex observation to an entity"""
    print("\n=== Testing neo4j_add_observation with complex data ===")
    
    if not entity_id:
        print("Skipping test: No valid entity ID available")
        return False
    
    # Create a complex observation
    observation = {
        "type": "code_metrics",
        "timestamp": datetime.now().isoformat(),
        "data": {
            "complexity": 15,
            "lines_of_code": 120,
            "maintainability_index": 65,
            "dependencies": ["os", "sys", "json"]
        }
    }
    
    response = await client.call_tool(
        "neo4j_add_observation",
        {
            "entity_id": entity_id,
            "observation": observation
        }
    )
    
    result = await print_result(response)
    return result.get("success", False)

async def test_neo4j_create_relationship(client, source_id):
    """Test creating a relationship with complex properties"""
    print("\n=== Testing neo4j_create_relationship with complex properties ===")
    
    if not source_id:
        print("Skipping test: No valid source entity ID available")
        return False
    
    # Create a target entity first
    target_id = f"{ENTITY_ID_PREFIX}{uuid.uuid4().hex[:8]}"
    
    target_response = await client.call_tool(
        "neo4j_create_entity",
        {
            "entity_type": "CodeMethod",
            "properties": {
                "id": target_id,
                "name": "processData",
                "language": "python",
                "file_path": "src/services/complex_service.py",
                "line_number": 42,
                "parameters": [
                    {"name": "data", "type": "dict"},
                    {"name": "options", "type": "Options", "optional": True}
                ]
            }
        }
    )
    
    target_result = await print_result(target_response)
    
    if not target_result.get("success"):
        print("Failed to create target entity for relationship test")
        return False
    
    # Now create the relationship with complex properties
    response = await client.call_tool(
        "neo4j_create_relationship",
        {
            "source_id": source_id,
            "target_id": target_id,
            "rel_type": "CONTAINS",
            "properties": {
                "created_at": datetime.now().isoformat(),
                "visibility": "public",
                "metrics": {
                    "coupling": "high",
                    "cohesion": "medium"
                }
            }
        }
    )
    
    result = await print_result(response)
    return result.get("success", False)

async def test_neo4j_batch_create_entities(client):
    """Test batch creation of entities with complex properties"""
    print("\n=== Testing neo4j_batch_create_entities ===")
    
    # Create multiple entities with complex properties
    entities = [
        {
            "type": "CodeFile",
            "properties": {
                "id": f"{ENTITY_ID_PREFIX}file_{uuid.uuid4().hex[:6]}",
                "name": "complex_service.py",
                "path": "src/services",
                "imports": [
                    {"module": "os", "alias": None},
                    {"module": "json", "alias": None},
                    {"module": "requests", "alias": "req"}
                ]
            }
        },
        {
            "type": "CodeFunction",
            "properties": {
                "id": f"{ENTITY_ID_PREFIX}func_{uuid.uuid4().hex[:6]}",
                "name": "process_data",
                "line_number": 120,
                "parameters": [
                    {"name": "data", "type": "dict"},
                    {"name": "timeout", "type": "int", "default": 30}
                ],
                "return_type": "Dict[str, Any]"
            }
        }
    ]
    
    response = await client.call_tool(
        "neo4j_batch_create_entities",
        {
            "entities": entities
        }
    )
    
    result = await print_result(response)
    return result.get("success", False)

async def test_neo4j_search_entities(client, search_term="ComplexClass"):
    """Test searching for entities"""
    print(f"\n=== Testing neo4j_search_entities for '{search_term}' ===")
    
    # Wait a moment for the index to update
    await asyncio.sleep(1)
    
    response = await client.call_tool(
        "neo4j_search_entities",
        {
            "search_query": search_term,
            "limit": 5
        }
    )
    
    result = await print_result(response)
    return result.get("success", False)

async def test_neo4j_get_subgraph(client, entity_id):
    """Test retrieving a subgraph around an entity"""
    print("\n=== Testing neo4j_get_subgraph ===")
    
    if not entity_id:
        print("Skipping test: No valid entity ID available")
        return False
    
    response = await client.call_tool(
        "neo4j_get_subgraph",
        {
            "entity_id": entity_id,
            "depth": 2
        }
    )
    
    result = await print_result(response)
    return result.get("success", False)

async def run_all_tests():
    """Run all Neo4j memory tool tests"""
    print(f"Connecting to MCP server at {SERVER_URL}")
    
    async with Client(transport=SSETransport(SERVER_URL)) as client:
        # Test entity creation with complex properties first
        tools = await client.list_tools()
        print("Available tools:", [tool.name for tool in tools])
        
        entity_id = await test_neo4j_create_entity_with_complex_props(client)
        
        if not entity_id:
            print("ERROR: Entity creation failed, cannot continue with other tests")
            return
        
        # Test retrieving entity with complex properties
        await test_neo4j_get_entity_with_complex_props(client, entity_id)
        
        # Test remaining tools
        await test_neo4j_add_observation(client, entity_id)
        await test_neo4j_create_relationship(client, entity_id)
        await test_neo4j_batch_create_entities(client)
        await test_neo4j_search_entities(client)
        await test_neo4j_get_subgraph(client, entity_id)
        
        print("\n=== All tests completed ===")

if __name__ == "__main__":
    asyncio.run(run_all_tests())
