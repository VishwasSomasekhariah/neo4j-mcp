# src/tools/neo4j_memory/new_tools.py

import os
import json
import asyncio
import uuid
from typing import Dict, Any, List, Optional
from datetime import datetime
from neo4j import AsyncGraphDatabase, AsyncDriver
from fastmcp import Context


import logging
logger = logging.getLogger(__name__)


class Neo4jGraphService:
    def __init__(self):
        self.driver: Optional[AsyncDriver] = None
        self.connected = False
        self.retry_count = 0
        self.max_retries = 3

    async def connect(self):
        """Establish connection with retry logic"""
        if self.connected:
            return

        try:
            self.driver = AsyncGraphDatabase.driver(
                os.getenv("NEO4J_URI", "bolt://host.docker.internal:7687"),
                auth=(
                    os.getenv("NEO4J_USER", "neo4j"),
                    os.getenv("NEO4J_PASSWORD", "tree-sitter")
                ),
                max_connection_pool_size=50,
                connection_timeout=30
            )

            # Verify connection
            await self.driver.verify_connectivity()
            self.connected = True
            self.retry_count = 0

            # Initialize schema
            await self.initialize_schema()
        except Exception as e:
            self.retry_count += 1
            if self.retry_count > self.max_retries:
                raise Exception(f"Failed to connect to Neo4j after {self.max_retries} attempts: {e}")
            # Exponential backoff
            await asyncio.sleep(2 ** self.retry_count)
            await self.connect()

    async def close(self):
        if self.driver:
            await self.driver.close()
            self.connected = False

    async def execute_query(self, query: str, params: Dict = None):
        if not self.connected:
            await self.connect()

        logger.debug("Running Cypher query:\n%s\nwith params: %r", query, params)
        start = datetime.utcnow()
        async with self.driver.session() as session:
            async def work(tx):
                result = await tx.run(query, params or {})
                records = []
                async for record in result:
                    records.append(record.data())
                return records

            try:
                records = await session.execute_write(work)
                elapsed = (datetime.utcnow() - start).total_seconds()
                logger.debug("Query returned %d records (%.3fs)", len(records), elapsed)
                return records
            except Exception as e:
                logger.error("Cypher error after %.3fs: %s", (datetime.utcnow()-start).total_seconds(), e, exc_info=True)
                raise


    async def initialize_schema(self):
        """Create necessary indexes and constraints for the project schema"""
        try:
            # Create indexes for each node type in the schema
            node_types = ["Project", "File", "Function", "Type", "Variable", "Namespace", "Macro", "Block", "Literal"]
            
            for node_type in node_types:
                # Create index on name property for each node type
                await self.execute_query(
                    f"CREATE INDEX {node_type.lower()}_name_idx IF NOT EXISTS FOR (n:{node_type}) ON (n.name)"
                )
                
                # Create index on file_path for relevant node types
                if node_type in ["Project", "File", "Function", "Type", "Variable", "Namespace", "Macro", "Block", "Literal"]:
                    await self.execute_query(
                        f"CREATE INDEX {node_type.lower()}_filepath_idx IF NOT EXISTS FOR (n:{node_type}) ON (n.file_path)"
                    )
            
            # Add checksum index for File nodes
            await self.execute_query(
                "CREATE INDEX file_checksum_idx IF NOT EXISTS FOR (n:File) ON (n.checksum)"
            )

            # Create id index per label for fast deterministic lookup.
            # Neo4j requires a label in index definitions, so we create one per node type.
            id_index_labels = ["Project", "File", "Function", "Type", "Variable",
                               "Namespace", "Macro", "Block", "Literal", "Statement"]
            for label in id_index_labels:
                await self.execute_query(
                    f"CREATE INDEX {label.lower()}_id_idx IF NOT EXISTS FOR (n:{label}) ON (n.id)"
                )
            
            # Create fulltext search indexes
            fulltext_indexes = [
                (
                    "codeSearch",
                    "CREATE FULLTEXT INDEX codeSearch IF NOT EXISTS FOR (n:Function) ON EACH [n.name, n.body, n.value]"
                ),
                (
                    "FuzzySearchIdx",
                    "CREATE FULLTEXT INDEX FuzzySearchIdx IF NOT EXISTS FOR (n:Function|Type|File|Statement|Literal) ON EACH [n.name, n.text, n.body, n.value] OPTIONS {indexConfig: {`fulltext.analyzer`: 'simple', `fulltext.eventually_consistent`: false}}"
                ),
            ]
            for index_name, index_query in fulltext_indexes:
                try:
                    await self.execute_query(index_query)
                except Exception as e:
                    print(f"Full-text index creation warning ({index_name}): {str(e)}")
                
            print("Neo4j schema initialized successfully")
        except Exception as e:
            print(f"Schema initialization error: {str(e)}")


    # def _flatten_complex_properties(self, properties: Dict[str, Any]) -> Dict[str, Any]:
    #     """Convert complex properties to Neo4j-compatible format"""
    #     flattened = {}

    #     for key, value in properties.items():
    #         # Direct primitive types and arrays of primitives
    #         if isinstance(value, (str, int, float, bool, type(None))) or (
    #             isinstance(value, list) and all(isinstance(x, (str, int, float, bool, type(None))) for x in value)
    #         ):
    #             flattened[key] = value
    #         # Handle dictionaries
    #         elif isinstance(value, dict):
    #             flattened[f"{key}_json"] = json.dumps(value)
    #         # Handle lists of dictionaries
    #         elif isinstance(value, list) and all(isinstance(x, dict) for x in value):
    #             flattened[f"{key}_json"] = json.dumps(value)
    #             # Extract text fields if available
    #             if all("text" in x for x in value):
    #                 flattened[f"{key}_texts"] = [x["text"] for x in value]
    #         # Other complex types
    #         else:
    #             flattened[f"{key}_str"] = str(value)

    #     return flattened

    # def _unflatten_complex_properties(self, properties: Dict[str, Any]) -> Dict[str, Any]:
    #     """Convert Neo4j-stored properties back to complex types"""
    #     unflattened = {}

    #     for key, value in properties.items():
    #         if key.endswith("_json") and isinstance(value, str):
    #             try:
    #                 base_key = key[:-5]  # Remove _json suffix
    #                 unflattened[base_key] = json.loads(value)
    #             except:
    #                 unflattened[key] = value
    #         else:
    #             unflattened[key] = value

    #     return unflattened


def _validate_checksum(node_type: str, properties: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate that File and Project nodes have a checksum property
    
    Args:
        node_type: Type of node being created
        properties: Node properties
        
    Returns:
        Dictionary with validation result
    """
    # Only enforce checksum for File and Project nodes
    if node_type in ["File"]:
        if "checksum" not in properties or not properties["checksum"]:
            return {
                "valid": False,
                "error": f"Checksum is required for {node_type} nodes"
            }
    
    return {"valid": True}


neo4j_graph = Neo4jGraphService()

def _flatten_complex_properties(props):
    """
    Flatten complex properties (tuples, lists, dicts) to JSON strings
    for Neo4j compatibility
    """
    import json
    
    flat_props = {}
    for k, v in props.items():
        if isinstance(v, (tuple, list)):
            # Convert tuples and lists to JSON strings
            flat_props[k] = json.dumps(v)
        elif isinstance(v, dict):
            # Convert dicts to JSON strings
            flat_props[k] = json.dumps(v)
        elif v is None:
            # Skip None values
            continue
        else:
            flat_props[k] = v
    return flat_props

def _unflatten_complex_properties(props):
    """
    Restore complex properties from JSON strings
    """
    import json
    
    # If props is a tuple or not a dictionary, return it as is
    if not isinstance(props, dict):
        return props
    
    unflat_props = {}
    for k, v in props.items():
        if isinstance(v, str) and (v.startswith('[') or v.startswith('{')):
            try:
                unflat_props[k] = json.loads(v)
            except json.JSONDecodeError:
                unflat_props[k] = v
        else:
            unflat_props[k] = v
    return unflat_props

def _extract_key_properties(props):
    """
    Extract only key properties for node matching to avoid oversized queries
    """
    key_props = {}
    
    # Priority order for properties to use for node matching.
    # 'id' is first — it's the schema-defined deterministic key ({type}:{file_path}:{name}).
    # When present it's sufficient alone; other properties are fallbacks for older nodes.
    priority_props = ['id', 'name', 'project_path', 'file_path', 'start_point', 'end_point', 'start_byte', 'end_byte', 'checksum']
    
    # First try to find priority properties
    for key in priority_props:
        if key in props and props[key] is not None:
            key_props[key] = props[key]
    
    # If we don't have any key properties, use a subset of available properties
    if not key_props:
        # Avoid large properties like 'body', 'symbols_location', etc.
        excluded_props = ['body', 'symbols_location', 'base_list', 'fields', 'documentation']
        for key, value in props.items():
            if key not in excluded_props and value is not None:
                key_props[key] = value
                # Limit to 3 properties to avoid oversized queries
                if len(key_props) >= 3:
                    break
    
    return key_props


def register_tools(mcp):
    @mcp.tool()
    async def neo4j_execute_query(
        query: str,
        limit: int = 100,
        params: Dict[str, Any] = None,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """
        Execute a raw Cypher query against the Neo4j database

        Args:
            query: Cypher query to execute
            limit: Maximum number of results to return
            params: Optional query parameters for parameterized queries (e.g., {"project_name": "HelloWorldApp"})
            ctx: Context

        Returns:
            Dictionary with query results
        """
        try:
            # Execute the query with parameters
            result = await neo4j_graph.execute_query(query, params or {})
            
            # Limit results if needed
            if limit and len(result) > limit:
                result = result[:limit]
            
            # Process and unflatten results
            processed_results = []
            for item in result:
                if isinstance(item, dict):
                    processed_item = _unflatten_complex_properties(item)
                    processed_results.append(processed_item)
                else:
                    processed_results.append(item)
            
            return {
                "success": True,
                "results": processed_results,
                "count": len(processed_results),
                "query": query
            }
        except Exception as e:
            if ctx:
                await ctx.error(f"Query execution failed: {str(e)}")
            logger.error(f"Query execution failed: {str(e)}", exc_info=True)
            return {"success": False, "error": str(e), "query": query}

    @mcp.tool()
    async def neo4j_get_version(
        ctx: Context = None
    ) -> Dict[str, Any]:
        """
        Get Neo4j database version and Cypher syntax guidance.

        IMPORTANT: Call this tool BEFORE writing complex Cypher queries to ensure correct syntax.

        Returns:
            Dictionary with:
            - version: Neo4j kernel version (e.g., "5.23.0")
            - major_version: Major version number (e.g., 5)
            - cypher_syntax_notes: Important syntax differences for this version
        """
        try:
            result = await neo4j_graph.execute_query(
                "CALL dbms.components() YIELD name, versions WHERE name = 'Neo4j Kernel' RETURN versions[0] AS version"
            )

            version = result[0]["version"] if result else "unknown"
            major_version = int(version.split(".")[0]) if version != "unknown" else 0

            # Provide syntax guidance based on version
            if major_version >= 5:
                syntax_notes = {
                    "version_info": f"Neo4j {version} - Use Neo4j 5.x Cypher syntax",
                    "pattern_counting": {
                        "rule": "Cannot use size() or COUNT() with pattern expressions in RETURN",
                        "solution": "Use MATCH with aggregation, or COUNT {{ }} subquery syntax",
                        "wrong_patterns": ["size((n)-[:REL]->())", "COUNT((n)-[:REL]->())"],
                        "correct_approach": "MATCH (n)-[:REL]->(x) RETURN n, count(x) AS cnt",
                        "alternative": "Use COUNT { (n)-[:REL]->() } subquery syntax"
                    },
                    "pattern_expressions": "Pattern expressions can only be used in EXISTS() or WHERE clauses",
                    "subqueries": "Use CALL { WITH n ... RETURN count(*) } for complex per-row aggregations",
                    "tip": "Use schema tools to discover actual graph structure before writing queries"
                }
            else:
                syntax_notes = {
                    "version_info": f"Neo4j {version} - Neo4j 4.x Cypher syntax",
                    "pattern_counting": "size((n)-[:REL]->()) is supported in Neo4j 4.x"
                }

            return {
                "success": True,
                "version": version,
                "major_version": major_version,
                "cypher_syntax_notes": syntax_notes
            }
        except Exception as e:
            if ctx:
                await ctx.error(f"Failed to get Neo4j version: {str(e)}")
            logger.error(f"Failed to get Neo4j version: {str(e)}", exc_info=True)
            return {"success": False, "error": str(e)}

    @mcp.tool()
    async def neo4j_batch_execute_queries(
        queries: List[Dict[str, Any]],
        limit: int = 100,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """
        Execute multiple Cypher queries in a single batch operation
        
        Args:
            queries: List of query objects, each containing 'query' and optional 'params' and 'limit'
                    Format: [{"query": "MATCH...", "params": {}, "limit": 50}, ...]
            limit: Default maximum number of results per query if not specified in individual queries
            ctx: Context
            
        Returns:
            Dictionary with batch results including individual query results and errors
        """
        try:
            batch_results = []
            total_success_count = 0
            total_error_count = 0
            
            logger.info(f"Executing batch of {len(queries)} queries")
            
            for i, query_obj in enumerate(queries):
                query_result = {
                    "query_index": i,
                    "success": False,
                    "results": [],
                    "count": 0,
                    "error": None,
                    "query": None
                }
                
                try:
                    # Extract query details
                    if isinstance(query_obj, dict):
                        cypher_query = query_obj.get("query", "")
                        query_params = query_obj.get("params", {})
                        query_limit = query_obj.get("limit", limit)
                    elif isinstance(query_obj, str):
                        # Support simple string queries for backward compatibility
                        cypher_query = query_obj
                        query_params = {}
                        query_limit = limit
                    else:
                        raise ValueError(f"Invalid query format at index {i}: expected dict or str")
                    
                    if not cypher_query:
                        raise ValueError(f"Empty query at index {i}")
                    
                    query_result["query"] = cypher_query
                    
                    # Execute the individual query
                    logger.debug(f"Executing query {i+1}/{len(queries)}: {cypher_query[:100]}...")
                    result = await neo4j_graph.execute_query(cypher_query, query_params)
                    
                    # Apply limit if needed
                    if query_limit and len(result) > query_limit:
                        result = result[:query_limit]
                    
                    # Process and unflatten results
                    processed_results = []
                    for item in result:
                        if isinstance(item, dict):
                            processed_item = _unflatten_complex_properties(item)
                            processed_results.append(processed_item)
                        else:
                            processed_results.append(item)
                    
                    query_result.update({
                        "success": True,
                        "results": processed_results,
                        "count": len(processed_results)
                    })
                    
                    total_success_count += 1
                    logger.debug(f"Query {i+1} succeeded with {len(processed_results)} results")
                    
                except Exception as e:
                    query_result.update({
                        "success": False,
                        "error": str(e),
                        "error_type": type(e).__name__
                    })
                    total_error_count += 1
                    logger.warning(f"Query {i+1} failed: {str(e)}")
                
                batch_results.append(query_result)
            
            # Calculate batch summary
            batch_summary = {
                "batch_success": total_error_count == 0,
                "total_queries": len(queries),
                "successful_queries": total_success_count,
                "failed_queries": total_error_count,
                "total_results": sum(r["count"] for r in batch_results if r["success"])
            }
            
            logger.info(f"Batch execution completed: {total_success_count} succeeded, {total_error_count} failed")
            
            return {
                "success": True,
                "batch_results": batch_results,
                "summary": batch_summary
            }
            
        except Exception as e:
            if ctx:
                await ctx.error(f"Batch query execution failed: {str(e)}")
            logger.error(f"Batch query execution failed: {str(e)}", exc_info=True)
            return {
                "success": False, 
                "error": str(e),
                "batch_results": [],
                "summary": {
                    "batch_success": False,
                    "total_queries": len(queries) if isinstance(queries, list) else 0,
                    "successful_queries": 0,
                    "failed_queries": len(queries) if isinstance(queries, list) else 0,
                    "total_results": 0
                }
            }

    @mcp.tool()
    async def neo4j_create_node(
        node_type: str,
        properties: Dict[str, Any],
        ctx: Context = None
    ) -> Dict[str, Any]:
        """
        Create a new node in the knowledge graph based on the project schema
        
        Args:
            node_type: Type of node (File, Function, Type, Variable, Namespace, Macro, Block, Literal)
            properties: Node properties according to the schema
            ctx: Context
            
        Returns:
            Dictionary with operation result
        """
        try:
            # Validate node type against schema
            valid_node_types = ["Project", "File", "Function", "Type", "Variable", "Namespace", "Macro", "Block", "Literal"]
            if node_type not in valid_node_types:
                return {
                    "success": False,
                    "error": f"Invalid node type: {node_type}. Valid types are: {', '.join(valid_node_types)}"
                }
            
            # Validate checksum for File nodes
            checksum_validation = _validate_checksum(node_type, properties)
            if not checksum_validation["valid"]:
                return {
                    "success": False,
                    "error": checksum_validation["error"]
                }
            
            # Flatten complex properties
            flattened_props = neo4j_graph._flatten_complex_properties(properties)
            
            # Create the node
            query = (
                f"CREATE (n:{node_type} $props) "
                "RETURN n { .* } as node"
            )
            
            result = await neo4j_graph.execute_query(
                query,
                {"props": flattened_props}
            )
            
            # Unflatten for response
            unflattened = neo4j_graph._unflatten_complex_properties(result[0]["node"])
            
            return {
                "success": True,
                "node": unflattened,
                "message": f"{node_type} node created successfully"
            }
        except Exception as e:
            await ctx.error(f"Node creation failed: {str(e)}")
            return {"success": False, "error": str(e)}
    
    @mcp.tool()
    async def neo4j_create_relationship(
        from_node_type: str,
        from_node_props: Dict[str, Any],
        to_node_type: str,
        to_node_props: Dict[str, Any],
        rel_type: str,
        properties: Optional[Dict[str, Any]] = None,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """
        Create a relationship between two nodes
        
        Args:
            from_node_type: Type of source node
            from_node_props: Properties to identify source node
            to_node_type: Type of target node
            to_node_props: Properties to identify target node
            rel_type: Relationship type
            properties: Optional relationship properties
            ctx: Context
            
        Returns:
            Dictionary with operation result
        """
        try:
            # Sanitize relationship type (remove any special characters)
            rel_type = rel_type.split("?")[0]  # Handle cases like "IMPLEMENTS?INHERITS_FROM"
            
            # Extract key properties for node matching
            from_key_props = _extract_key_properties(from_node_props)
            to_key_props = _extract_key_properties(to_node_props)
            
            # Flatten properties
            from_props_flat = _flatten_complex_properties(from_key_props)
            to_props_flat = _flatten_complex_properties(to_key_props)
            rel_props_flat = _flatten_complex_properties(properties or {})
            
            # Build property match conditions
            from_props_match = " AND ".join([f"a.{k} = ${k}_from" for k in from_props_flat.keys()])
            to_props_match = " AND ".join([f"b.{k} = ${k}_to" for k in to_props_flat.keys()])
            
            # Prepare parameters with prefixes to avoid conflicts
            params = {
                "rel_props": rel_props_flat
            }
            
            # Add prefixed properties to avoid name collisions
            for k, v in from_props_flat.items():
                params[f"{k}_from"] = v
            
            for k, v in to_props_flat.items():
                params[f"{k}_to"] = v
            
            # Prefer id-based matching when both nodes carry the deterministic 'id' property
            if from_props_flat.get("id") and to_props_flat.get("id"):
                query = f"""
                MATCH (a:{from_node_type} {{id: $from_id}}), (b:{to_node_type} {{id: $to_id}})
                MERGE (a)-[r:{rel_type}]->(b)
                SET r += $rel_props
                RETURN type(r) as type, r as relationship,
                    a as source, labels(a) as source_labels,
                    b as target, labels(b) as target_labels
                """
                params = {
                    "from_id": from_props_flat["id"],
                    "to_id": to_props_flat["id"],
                    "rel_props": rel_props_flat
                }
            else:
                query = f"""
                MATCH (a:{from_node_type}), (b:{to_node_type})
                WHERE {from_props_match or "true"} AND {to_props_match or "true"}
                MERGE (a)-[r:{rel_type}]->(b)
                SET r += $rel_props
                RETURN type(r) as type, r as relationship,
                    a as source, labels(a) as source_labels,
                    b as target, labels(b) as target_labels
                """
            
            result = await neo4j_graph.execute_query(query, params)
            
            if not result:
                return {
                    "success": False,
                    "error": f"No matching nodes found for relationship {from_node_type}-[{rel_type}]->{to_node_type}"
                }
            
            item = result[0]
            rel_data = _unflatten_complex_properties(item.get("relationship", {}))
            source_data = _unflatten_complex_properties(item.get("source", {}))
            target_data = _unflatten_complex_properties(item.get("target", {}))
            
            return {
                "success": True,
                "type": item.get("type", rel_type),
                "relationship": rel_data,
                "source": {
                    "type": item.get("source_labels", [from_node_type])[0],
                    "properties": source_data
                },
                "target": {
                    "type": item.get("target_labels", [to_node_type])[0],
                    "properties": target_data
                }
            }
            
        except Exception as e:
            if ctx:
                await ctx.error(f"Relationship creation failed: {str(e)}")
            logger.error(f"Relationship creation failed: {str(e)}", exc_info=True)
            return {"success": False, "error": str(e)}

    @mcp.tool()
    async def neo4j_batch_create_relationships(
        relationships: List[Dict[str, Any]],
        ctx: Context = None
    ) -> Dict[str, Any]:
        """
        Create multiple relationships in a single transaction
        
        Args:
            relationships: List of relationship definitions
            ctx: Context
            
        Returns:
            Dictionary with operation result
        """
        try:
            # Group relationships by their type and node types
            rel_groups = {}
            for rel in relationships:
                logger.info(rel)
                # await asyncio.sleep(100)
                from_type = rel.get("from_node_type")
                to_type = rel.get("to_node_type")
                rel_type = rel.get("rel_type", "").split("?")[0]  # Sanitize relationship type
                
                key = f"{from_type}|{rel_type}|{to_type}"
                if key not in rel_groups:
                    rel_groups[key] = []
                
                # Extract only key properties for matching
                from_key_props = _extract_key_properties(rel.get("from_node_props", {}))
                to_key_props = _extract_key_properties(rel.get("to_node_props", {}))
                
                rel_groups[key].append({
                    "from_props": _flatten_complex_properties(from_key_props),
                    "to_props": _flatten_complex_properties(to_key_props),
                    "rel_props": _flatten_complex_properties(rel.get("properties", {}))
                })
            
            # Process each group with a separate query
            processed_rels = []
            
            for key, batch in rel_groups.items():
                from_type, rel_type, to_type = key.split("|", 2)

                # Use id-based matching when all items in the batch carry both ids —
                # this is O(log n) via the node_id_idx index and avoids multi-prop scans.
                use_id_match = all(
                    rel.get("from_props", {}).get("id") and rel.get("to_props", {}).get("id")
                    for rel in batch
                )

                if use_id_match:
                    query = f"""
                    UNWIND $batch AS rel
                    MATCH (a:{from_type} {{id: rel.from_props.id}})
                    MATCH (b:{to_type} {{id: rel.to_props.id}})
                    MERGE (a)-[r:{rel_type}]->(b)
                    SET r += rel.rel_props
                    RETURN type(r) as type, r as relationship,
                        a as source, labels(a) as source_labels,
                        b as target, labels(b) as target_labels
                    """
                else:
                    # Fallback: multi-property matching for nodes without an id
                    query = f"""
                    UNWIND $batch AS rel
                    MATCH (a:{from_type}), (b:{to_type})
                    WHERE all(k IN keys(rel.from_props) WHERE a[k] = rel.from_props[k])
                    AND all(k IN keys(rel.to_props) WHERE b[k] = rel.to_props[k])
                    MERGE (a)-[r:{rel_type}]->(b)
                    SET r += rel.rel_props
                    RETURN type(r) as type, r as relationship,
                        a as source, labels(a) as source_labels,
                        b as target, labels(b) as target_labels
                    """

                result = await neo4j_graph.execute_query(query, {"batch": batch})
                
                for item in result:
                    # Check if item is a dictionary before accessing its contents
                    if not isinstance(item, dict):
                        logger.warning(f"Unexpected result format: {type(item)}")
                        continue
                        
                    # Get relationship data, ensuring we have a dictionary
                    relationship = item.get("relationship")
                    if not isinstance(relationship, dict):
                        relationship = {}
                    
                    # Get source data, ensuring we have a dictionary
                    source = item.get("source")
                    if not isinstance(source, dict):
                        source = {}
                    
                    # Get target data, ensuring we have a dictionary
                    target = item.get("target")
                    if not isinstance(target, dict):
                        target = {}
                    
                    rel_data = _unflatten_complex_properties(relationship)
                    source_data = _unflatten_complex_properties(source)
                    target_data = _unflatten_complex_properties(target)
                    
                    processed_rels.append({
                        "type": item.get("type", rel_type),
                        "relationship": rel_data,
                        "source": {
                            "type": item.get("source_labels", [from_type])[0] if isinstance(item.get("source_labels"), list) else from_type,
                            "properties": source_data
                        },
                        "target": {
                            "type": item.get("target_labels", [to_type])[0] if isinstance(item.get("target_labels"), list) else to_type,
                            "properties": target_data
                        }
                    })
            
            return {
                "success": True,
                "relationships": processed_rels,
                "count": len(processed_rels),
                "message": f"Created {len(processed_rels)} relationships successfully"
            }
            
        except Exception as e:
            if ctx:
                await ctx.error(f"Batch relationship creation failed: {str(e)}")
            logger.error(f"Batch relationship creation failed: {str(e)}", exc_info=True)
            return {"success": False, "error": str(e)}

    @mcp.tool()
    async def neo4j_batch_create_nodes(
        nodes: List[Dict[str, Any]],
        ctx: Context = None
    ) -> Dict[str, Any]:
        """
        Create multiple nodes in a single transaction
        
        Args:
            nodes: List of node definitions with 'type' and 'properties' keys
            ctx: Context
            
        Returns:
            Dictionary with operation result
        """
        try:
            logger.debug("batch_create_nodes called with %d nodes", len(nodes))
            
            # Group nodes by type
            node_groups = {}
            for node in nodes:
                node_type = node.get("type")
                properties = node.get("properties", {})
                
                # Flatten complex properties
                flattened_props = _flatten_complex_properties(properties)
                
                if node_type not in node_groups:
                    node_groups[node_type] = []
                
                node_groups[node_type].append(flattened_props)
            
            # Process each node type separately
            processed_nodes = []

            for node_type, props_list in node_groups.items():
                # Split: nodes with a deterministic 'id' use MERGE (idempotent upsert);
                # nodes without 'id' fall back to CREATE (legacy behaviour).
                with_id    = [p for p in props_list if p.get("id")]
                without_id = [p for p in props_list if not p.get("id")]

                batches = []
                if with_id:
                    batches.append((with_id, True))
                if without_id:
                    batches.append((without_id, False))

                for subset, use_merge in batches:
                    if use_merge:
                        # MERGE on 'id' so re-runs are idempotent and stubs get full properties.
                        query = f"""
                        UNWIND $props AS props
                        MERGE (n:{node_type} {{id: props.id}})
                        SET n += props
                        RETURN n AS node, labels(n) AS labels
                        """
                    else:
                        query = f"""
                        UNWIND $props AS props
                        CREATE (n:{node_type})
                        SET n = props
                        RETURN n AS node, labels(n) AS labels
                        """

                    result = await neo4j_graph.execute_query(query, {"props": subset})

                    for item in result:
                        node_data = _unflatten_complex_properties(item["node"])
                        label = item["labels"][0] if item.get("labels") else node_type

                        processed_nodes.append({
                            "type": label,
                            "properties": node_data
                        })
            
            return {
                "success": True,
                "nodes": processed_nodes,
                "count": len(processed_nodes),
                "message": f"Created {len(processed_nodes)} nodes successfully"
            }
            
        except Exception as e:
            if ctx:
                await ctx.error(f"Batch node creation failed: {str(e)}")
            logger.error(f"Batch node creation failed: {str(e)}", exc_info=True)
            return {"success": False, "error": str(e)}

    @mcp.tool()
    async def neo4j_get_node(
        node_type: str,
        properties: Dict[str, Any],
        ctx: Context = None
    ) -> Dict[str, Any]:
        """
        Get a node by type and properties
        
        Args:
            node_type: Type of node to retrieve
            properties: Properties to identify the node
            ctx: Context
            
        Returns:
            Dictionary with operation result
        """
        try:
            # Flatten properties for matching
            flattened_props = neo4j_graph._flatten_complex_properties(properties)
            
            # Build property match conditions
            props_match = " AND ".join([f"n.{k} = ${k}" for k in flattened_props.keys()])
            
            query = (
                f"MATCH (n:{node_type}) "
                f"WHERE {props_match} "
                "RETURN n { .* } as node"
            )
            
            result = await neo4j_graph.execute_query(query, flattened_props)
            
            if not result:
                return {
                    "success": False,
                    "error": f"No {node_type} node found with the specified properties"
                }
            
            # Unflatten for response
            unflattened = neo4j_graph._unflatten_complex_properties(result[0]["node"])
            
            return {
                "success": True,
                "node": unflattened
            }
        except Exception as e:
            await ctx.error(f"Node retrieval failed: {str(e)}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    async def neo4j_search_code(
        search_query: str,
        node_types: List[str] = None,
        limit: int = 10,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """
        Search code elements using full-text search
        
        Args:
            search_query: Text to search for
            node_types: Optional list of node types to restrict search
            limit: Maximum number of results to return
            ctx: Context
            
        Returns:
            Dictionary with search results
        """
        try:
            # Build type filter if specified
            type_filter = ""
            if node_types and len(node_types) > 0:
                type_labels = [f"n:{node_type}" for node_type in node_types]
                type_filter = f"WHERE {' OR '.join(type_labels)}"
            
            # Try full-text search first
            try:
                query = (
                    "CALL db.index.fulltext.queryNodes('codeSearch', $query) "
                    "YIELD node, score "
                    f"{type_filter} "
                    "RETURN node { .* } as node, labels(node) as type, score "
                    "ORDER BY score DESC "
                    "LIMIT $limit"
                )
                
                result = await neo4j_graph.execute_query(
                    query,
                    {"query": search_query, "limit": limit}
                )
            except Exception as e:
                # Fall back to basic pattern matching
                await ctx.warning(f"Full-text search failed, falling back to basic search: {str(e)}")
                
                # Build the WHERE clause for basic search
                where_clauses = []
                if node_types and len(node_types) > 0:
                    type_labels = [f"n:{node_type}" for node_type in node_types]
                    where_clauses.append(f"({' OR '.join(type_labels)})")
                
                # Add property search conditions
                property_search = (
                    "(n.name CONTAINS $query OR "
                    "n.body CONTAINS $query OR "
                    "n.value CONTAINS $query OR "
                    "n.file_path CONTAINS $query)"
                )
                where_clauses.append(property_search)
                
                # Combine all WHERE conditions
                where_clause = " AND ".join(where_clauses)
                
                query = (
                    "MATCH (n) "
                    f"WHERE {where_clause} "
                    "RETURN n { .* } as node, labels(n) as type, 1.0 as score "
                    "LIMIT $limit"
                )
                
                result = await neo4j_graph.execute_query(
                    query,
                    {"query": search_query, "limit": limit}
                )
            
            # Process results
            processed_results = []
            for item in result:
                node_data = neo4j_graph._unflatten_complex_properties(item["node"])
                node_type = item["type"][0]  # Get first label as the node type
                
                processed_results.append({
                    "type": node_type,
                    "properties": node_data,
                    "score": item["score"]
                })
            
            return {
                "success": True,
                "results": processed_results,
                "count": len(processed_results)
            }
        except Exception as e:
            await ctx.error(f"Code search failed: {str(e)}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    async def neo4j_fuzzy_search(
        search_term: str,
        limit: int = 15,
        min_score: float = 0.5,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """
        Fuzzy search for code entities by name. Use this to find entities when you're
        unsure of exact spelling or case. Handles case insensitivity and typos.

        Args:
            search_term: Name to search for (e.g., "workerz", "Helper.FormatMessage", "ProcessData()")
            limit: Maximum number of results to return (default: 15)
            min_score: Minimum relevance score threshold (default: 0.5)
            ctx: Context

        Returns:
            Dictionary with:
            - search_term: Original search term
            - processed_query: Processed query string sent to Lucene
            - found: Boolean indicating if any results were found
            - result_count: Number of results
            - results: List of matching entities with labels, properties, and scores
        """
        import re

        # Preprocess search term for Lucene fuzzy search
        # Remove parentheses and brackets
        term = re.sub(r'[(){}\[\]]', '', search_term)

        # Split on dots, spaces, underscores, and other delimiters
        parts = re.split(r'[.\s_\-:]+', term)

        # Filter empty parts and strip whitespace
        parts = [p.strip() for p in parts if p.strip()]

        if not parts:
            return {
                "success": True,
                "search_term": search_term,
                "processed_query": "",
                "found": False,
                "result_count": 0,
                "results": [],
                "error": "Could not parse search term"
            }

        # Add fuzzy operator (~) to each part and combine with AND
        # The ~ enables Lucene fuzzy matching for case-insensitivity and typos
        fuzzy_parts = [f"{part}~" for part in parts]
        processed_query = " AND ".join(fuzzy_parts)

        try:
            # Fuzzy search query using FuzzySearchIdx
            cypher_query = f"""
            CALL db.index.fulltext.queryNodes('FuzzySearchIdx', '{processed_query}')
            YIELD node, score
            WITH node, score,
                 [p IN ['name','text','value','body']
                  WHERE node[p] IS NOT NULL
                  | p + ': ' + substring(toString(node[p]), 0, 150)
                 ] AS presentProps
            WHERE score >= {min_score}
            RETURN
              id(node) AS id,
              labels(node) AS labels,
              node.name AS name,
              node.file_path AS file_path,
              presentProps,
              score
            ORDER BY score DESC
            LIMIT {limit}
            """

            result = await neo4j_graph.execute_query(cypher_query)

            processed_results = []
            for item in result:
                processed_results.append({
                    "id": item.get("id"),
                    "labels": item.get("labels", []),
                    "name": item.get("name"),
                    "file_path": item.get("file_path"),
                    "properties_preview": item.get("presentProps", []),
                    "score": item.get("score", 0)
                })

            return {
                "success": True,
                "search_term": search_term,
                "processed_query": processed_query,
                "found": len(processed_results) > 0,
                "result_count": len(processed_results),
                "results": processed_results
            }

        except Exception as e:
            logger.error(f"Fuzzy search failed: {str(e)}", exc_info=True)
            if ctx:
                await ctx.error(f"Fuzzy search failed: {str(e)}")
            return {
                "success": False,
                "search_term": search_term,
                "processed_query": processed_query,
                "found": False,
                "result_count": 0,
                "results": [],
                "error": str(e)
            }

    @mcp.tool()
    async def resolve_symbol(
        symbol_name: str,
        kind: Optional[str] = None,
        scope: Optional[str] = None,
        exact_match: bool = False,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """
        Resolve a symbol name to Neo4j node IDs using the SymbolIndex.

        This tool provides case-insensitive symbol resolution for LLM-driven queries.
        Use this to convert symbol names (class names, function names, etc.) into
        exact Neo4j node IDs before executing graph queries.

        The SymbolIndex stores lowercase keys (key_lc) that map to actual graph node IDs,
        enabling case-insensitive lookups while preserving the case-sensitive graph.

        Args:
            symbol_name: The symbol name to resolve (case-insensitive).
                         Examples: "WorkerA", "formatmessage", "Process", "INotifier"
            kind: Optional filter by symbol kind. Valid values:
                  - "Type" (classes, interfaces, structs)
                  - "Function" (methods, functions)
                  - "Variable" (fields, variables)
                  - "Namespace" (namespaces)
                  - "Macro" (macros, preprocessor definitions)
            scope: Optional filter by enclosing scope (e.g., class name for methods).
                   Examples: "Manager" for Manager.Notify, "Helper" for Helper.FormatMessage
            exact_match: If True, requires exact match on key_lc.
                        If False (default), uses CONTAINS for partial matching.
            ctx: Context

        Returns:
            Dictionary with:
            - success: Whether the resolution succeeded
            - symbol_name: Original search term
            - found: Whether any matches were found
            - result_count: Number of matching symbols
            - results: List of resolved symbols, each containing:
                - key_lc: Lowercase symbol name
                - kind: Symbol kind (Type, Function, etc.)
                - scope: Enclosing scope
                - arity: Number of parameters (for functions)
                - signature_lc: Lowercase parameter signature (for functions)
                - targets: List of Neo4j node IDs
                - resolved_nodes: Actual node data (name, file_path) for each target

        Example usage:
            # Find all worker-related types
            resolve_symbol("worker", kind="Type")

            # Find FormatMessage function in Helper class
            resolve_symbol("formatmessage", kind="Function", scope="Helper")

            # Exact match for Process method
            resolve_symbol("process", kind="Function", exact_match=True)
        """
        try:
            # Normalize the symbol name to lowercase for matching
            symbol_lc = symbol_name.lower().strip()

            if not symbol_lc:
                return {
                    "success": False,
                    "symbol_name": symbol_name,
                    "found": False,
                    "result_count": 0,
                    "results": [],
                    "error": "Empty symbol name provided"
                }

            # Build the WHERE clause
            where_clauses = []
            params = {"symbol_lc": symbol_lc}

            if exact_match:
                where_clauses.append("s.key_lc = $symbol_lc")
            else:
                where_clauses.append("s.key_lc CONTAINS $symbol_lc")

            if kind:
                where_clauses.append("s.kind = $kind")
                params["kind"] = kind

            if scope:
                where_clauses.append("toLower(s.scope) CONTAINS toLower($scope)")
                params["scope"] = scope

            where_clause = " AND ".join(where_clauses)

            # Query SymbolIndex
            query = f"""
            MATCH (s:SymbolIndex)
            WHERE {where_clause}
            RETURN s.key_lc AS key_lc,
                   s.kind AS kind,
                   s.scope AS scope,
                   s.arity AS arity,
                   s.signature_lc AS signature_lc,
                   s.targets AS targets
            ORDER BY s.kind, s.scope, s.key_lc
            """

            result = await neo4j_graph.execute_query(query, params)

            if not result:
                return {
                    "success": True,
                    "symbol_name": symbol_name,
                    "found": False,
                    "result_count": 0,
                    "results": [],
                    "message": f"No symbols found matching '{symbol_name}'"
                }

            # Process results and optionally resolve to actual nodes
            processed_results = []
            all_target_ids = set()

            for item in result:
                targets = item.get("targets", [])
                if targets:
                    all_target_ids.update(targets)

                processed_results.append({
                    "key_lc": item.get("key_lc"),
                    "kind": item.get("kind"),
                    "scope": item.get("scope", ""),
                    "arity": item.get("arity"),
                    "signature_lc": item.get("signature_lc"),
                    "targets": targets
                })

            # Resolve target IDs to actual node data
            if all_target_ids:
                resolve_query = """
                MATCH (n)
                WHERE id(n) IN $target_ids
                RETURN id(n) AS node_id,
                       labels(n) AS labels,
                       n.name AS name,
                       n.file_path AS file_path
                """
                node_results = await neo4j_graph.execute_query(
                    resolve_query,
                    {"target_ids": list(all_target_ids)}
                )

                # Build a lookup map
                node_map = {}
                for node in node_results:
                    node_map[node["node_id"]] = {
                        "id": node["node_id"],
                        "labels": node.get("labels", []),
                        "name": node.get("name"),
                        "file_path": node.get("file_path")
                    }

                # Add resolved node data to results
                for res in processed_results:
                    res["resolved_nodes"] = [
                        node_map.get(tid, {"id": tid, "error": "not found"})
                        for tid in res.get("targets", [])
                    ]

            return {
                "success": True,
                "symbol_name": symbol_name,
                "found": True,
                "result_count": len(processed_results),
                "results": processed_results,
                "all_target_ids": list(all_target_ids),
                "message": f"Found {len(processed_results)} symbol(s) matching '{symbol_name}'"
            }

        except Exception as e:
            logger.error(f"Symbol resolution failed: {str(e)}", exc_info=True)
            if ctx:
                await ctx.error(f"Symbol resolution failed: {str(e)}")
            return {
                "success": False,
                "symbol_name": symbol_name,
                "found": False,
                "result_count": 0,
                "results": [],
                "error": str(e)
            }

    @mcp.tool()
    async def neo4j_get_subgraph(
        node_type: str,
        properties: Dict[str, Any],
        depth: int = 2,
        relationship_types: List[str] = None,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """
        Retrieve subgraph around a node
        
        Args:
            node_type: Type of the central node
            properties: Properties to identify the central node
            depth: Traversal depth (default: 2)
            relationship_types: Optional list of relationship types to traverse
            ctx: Context
            
        Returns:
            Dictionary with subgraph data
        """
        try:
            # Flatten properties for matching
            flattened_props = neo4j_graph._flatten_complex_properties(properties)
            
            # Build property match conditions
            props_match = " AND ".join([f"n.{k} = ${k}" for k in flattened_props.keys()])
            
            # Build relationship filter if specified
            rel_filter = ""
            if relationship_types and len(relationship_types) > 0:
                rel_types = "|".join(relationship_types)
                rel_filter = f":{rel_types}"
            
            query = (
                f"MATCH (n:{node_type}) "
                f"WHERE {props_match} "
                f"CALL apoc.path.subgraphAll(n, {{relationshipFilter: '{rel_filter}', maxLevel: $depth}}) "
                "YIELD nodes, relationships "
                "RETURN nodes, relationships"
            )
            
            # Check if APOC is available
            try:
                result = await neo4j_graph.execute_query(
                    query,
                    {**flattened_props, "depth": depth}
                )
            except Exception as e:
                # Fall back to standard Cypher if APOC is not available
                await ctx.warning(f"APOC procedure not available, falling back to standard Cypher: {str(e)}")
                
                query = (
                    f"MATCH (n:{node_type}) "
                    f"WHERE {props_match} "
                    f"MATCH path = (n)-[{rel_filter}*..{depth}]-(connected) "
                    "RETURN path"
                )
                
                result = await neo4j_graph.execute_query(
                    query,
                    flattened_props
                )
            
            # Process and unflatten the results
            nodes = []
            relationships = []
            
            # Extract nodes and relationships from the result
            if "nodes" in result[0] and "relationships" in result[0]:
                # APOC result format
                for node in result[0]["nodes"]:
                    node_data = neo4j_graph._unflatten_complex_properties(node)
                    nodes.append(node_data)
                
                for rel in result[0]["relationships"]:
                    rel_data = neo4j_graph._unflatten_complex_properties(rel)
                    relationships.append(rel_data)
            else:
                # Standard Cypher result format
                # This would require additional processing to extract nodes and relationships from paths
                # For simplicity, we'll return the raw paths
                return {
                    "success": True,
                    "paths": result,
                    "message": "Subgraph retrieved using standard Cypher"
                }
            
            return {
                "success": True,
                "nodes": nodes,
                "relationships": relationships,
                "count": {
                    "nodes": len(nodes),
                    "relationships": len(relationships)
                },
                "message": "Subgraph retrieved successfully"
            }
        except Exception as e:
            await ctx.error(f"Subgraph retrieval failed: {str(e)}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    async def neo4j_get_nodes_by_checksum(
        checksum: str,
        node_type: str = "File",
        ctx: Context = None
    ) -> Dict[str, Any]:
        """
        Find nodes with a specific checksum value
        
        Args:
            checksum: The checksum value to search for
            node_type: Type of node to search (default: File)
            ctx: Context
            
        Returns:
            Dictionary with matching nodes
        """
        try:
            query = (
                f"MATCH (n:{node_type}) "
                "WHERE n.checksum = $checksum "
                "RETURN n { .* } as node"
            )
            
            result = await neo4j_graph.execute_query(
                query, 
                {"checksum": checksum}
            )
            
            if not result:
                return {
                    "success": True,
                    "nodes": [],
                    "count": 0,
                    "message": f"No {node_type} nodes found with checksum: {checksum}"
                }
            
            # Process and unflatten results
            nodes = []
            for item in result:
                node_data = neo4j_graph._unflatten_complex_properties(item["node"])
                nodes.append(node_data)
            
            return {
                "success": True,
                "nodes": nodes,
                "count": len(nodes),
                "message": f"Found {len(nodes)} {node_type} nodes with matching checksum"
            }
        except Exception as e:
            await ctx.error(f"Checksum query failed: {str(e)}")
            return {"success": False, "error": str(e)}

    # @mcp.tool()
    # async def neo4j_create_relationship(
    #     from_node_type: str,
    #     from_node_props: Dict[str, Any],
    #     to_node_type: str,
    #     to_node_props: Dict[str, Any],
    #     rel_type: str,
    #     properties: Dict[str, Any] = None,
    #     ctx: Context = None
    # ) -> Dict[str, Any]:
    #     """
    #     Create relationship between nodes based on the project schema
        
    #     Args:
    #         from_node_type: Type of source node
    #         from_node_props: Properties to identify source node
    #         to_node_type: Type of target node
    #         to_node_props: Properties to identify target node
    #         rel_type: Relationship type from schema (CALLS, DEFINED_IN, etc.)
    #         properties: Optional relationship properties
    #         ctx: Context
            
    #     Returns:
    #         Dictionary with operation result
    #     """
    #     try:
    #         # Validate relationship type against schema
    #         valid_rel_types = ["CALLS", "DEFINED_IN", "INCLUDED_IN", "HAS_PARAMETER", 
    #                           "CONTAINS", "INHERITS_FROM", "IMPLEMENTS", "DECLARED_IN"]
    #         if rel_type not in valid_rel_types:
    #             return {
    #                 "success": False,
    #                 "error": f"Invalid relationship type: {rel_type}. Valid types are: {', '.join(valid_rel_types)}"
    #             }
            
    #         # Flatten properties for source and target node matching
    #         from_props_flat = neo4j_graph._flatten_complex_properties(from_node_props)
    #         to_props_flat = neo4j_graph._flatten_complex_properties(to_node_props)
            
    #         # Prepare relationship properties
    #         rel_props = {}
    #         if properties:
    #             rel_props = neo4j_graph._flatten_complex_properties(properties)
            
    #         # Build property match conditions for source and target
    #         from_props_match = " AND ".join([f"a.{k} = ${k}" for k in from_props_flat.keys()])
    #         to_props_match = " AND ".join([f"b.{k} = ${k}" for k in to_props_flat.keys()])
            
    #         # Combine all parameters
    #         params = {**{k: v for k, v in from_props_flat.items()}, 
    #                  **{k: v for k, v in to_props_flat.items()}, 
    #                  "rel_props": rel_props}
            
    #         # Create the relationship
    #         query = (
    #             f"MATCH (a:{from_node_type}), (b:{to_node_type}) "
    #             f"WHERE {from_props_match} AND {to_props_match} "
    #             f"CREATE (a)-[r:{rel_type} $rel_props]->(b) "
    #             "RETURN r { .* } as relationship, "
    #             "a { .* } as source, "
    #             "b { .* } as target"
    #         )
            
    #         result = await neo4j_graph.execute_query(query, params)
            
    #         if not result:
    #             return {
    #                 "success": False,
    #                 "error": "No matching nodes found to create relationship"
    #             }
            
    #         # Unflatten for response
    #         relationship = neo4j_graph._unflatten_complex_properties(result[0]["relationship"])
    #         source = neo4j_graph._unflatten_complex_properties(result[0]["source"])
    #         target = neo4j_graph._unflatten_complex_properties(result[0]["target"])
            
    #         return {
    #             "success": True,
    #             "relationship": relationship,
    #             "source": source,
    #             "target": target,
    #             "message": f"Relationship {rel_type} created successfully"
    #         }
    #     except Exception as e:
    #         await ctx.error(f"Relationship creation failed: {str(e)}")
    #         return {"success": False, "error": str(e)}

    
    # @mcp.tool()
    # async def neo4j_batch_create_relationships(
    #     relationships: List[Dict[str, Any]],
    #     ctx: Context = None
    # ) -> Dict[str, Any]:
    #     """
    #     Create multiple relationships in a single transaction
        
    #     Args:
    #         relationships: List of relationship definitions with source, target, and type information
    #         ctx: Context
            
    #     Returns:
    #         Dictionary with operation result
    #     """
    #     try:
    #         # Group relationships by their type and node types
    #         rel_groups = {}
    #         for rel in relationships:
    #             from_type = rel.get("from_node_type")
    #             to_type = rel.get("to_node_type")
    #             rel_type = rel.get("rel_type")
                
    #             key = f"{from_type}_{rel_type}_{to_type}"
    #             if key not in rel_groups:
    #                 rel_groups[key] = []
                
    #             rel_groups[key].append({
    #                 "from_props": neo4j_graph._flatten_complex_properties(rel.get("from_node_props", {})),
    #                 "to_props": neo4j_graph._flatten_complex_properties(rel.get("to_node_props", {})),
    #                 "rel_props": neo4j_graph._flatten_complex_properties(rel.get("properties", {}))
    #             })
            
    #         # Process each group with a separate query
    #         processed_rels = []
            
    #         for key, batch in rel_groups.items():
    #             from_type, rel_type, to_type = key.split("_", 2)
                
    #             query = f"""
    #             UNWIND $batch AS rel
    #             MATCH (a:{from_type}), (b:{to_type})
    #             WHERE all(k IN keys(rel.from_props) WHERE a[k] = rel.from_props[k])
    #             AND all(k IN keys(rel.to_props) WHERE b[k] = rel.to_props[k])
    #             CREATE (a)-[r:{rel_type}]->(b)
    #             SET r = rel.rel_props
    #             RETURN type(r) as type, r as relationship,
    #                 a as source, labels(a) as source_labels,
    #                 b as target, labels(b) as target_labels
    #             """
                
    #             result = await neo4j_graph.execute_query(query, {"batch": batch})
                
    #             for item in result:
    #                 rel_data = neo4j_graph._unflatten_complex_properties(item.get("relationship", {}))
    #                 source_data = neo4j_graph._unflatten_complex_properties(item.get("source", {}))
    #                 target_data = neo4j_graph._unflatten_complex_properties(item.get("target", {}))
                    
    #                 processed_rels.append({
    #                     "type": item.get("type", rel_type),
    #                     "relationship": rel_data,
    #                     "source": {
    #                         "type": item.get("source_labels", [from_type])[0],
    #                         "properties": source_data
    #                     },
    #                     "target": {
    #                         "type": item.get("target_labels", [to_type])[0],
    #                         "properties": target_data
    #                     }
    #                 })
            
    #         return {
    #             "success": True,
    #             "relationships": processed_rels,
    #             "count": len(processed_rels),
    #             "message": f"Created {len(processed_rels)} relationships successfully"
    #         }
            
    #     except Exception as e:
    #         await ctx.error(f"Batch relationship creation failed: {str(e)}")
    #         return {"success": False, "error": str(e)}

    
    



    
    # @mcp.tool()
    # async def neo4j_batch_create_nodes(
    #     nodes: List[Dict[str, Any]],
    #     ctx: Context = None
    # ) -> Dict[str, Any]:
    #     """
    #     Create multiple nodes in a single transaction
        
    #     Args:
    #         nodes: List of node definitions with 'type' and 'properties' keys
    #         ctx: Context
            
    #     Returns:
    #         Dictionary with operation result
    #     """
    #     try:
    #         logger.debug("batch_create_nodes called with %d nodes", len(nodes))
    #         # Prepare batch parameters
    #         batch_params = []
            
    #         for node in nodes:
    #             node_type = node.get("type")
    #             properties = node.get("properties", {})
                
    #             # Validate node type
    #             # valid_node_types = ["Project", "File", "Function", "Type", "Variable", "Namespace", "Macro", "Block", "Literal"]
    #             # if node_type not in valid_node_types:
    #             #     return {
    #             #         "success": False,
    #             #         "error": f"Invalid node type: {node_type}. Valid types are: {', '.join(valid_node_types)}"
    #             #     }
                
    #             # Validate checksum for File nodes
    #             checksum_validation = _validate_checksum(node_type, properties)
    #             if not checksum_validation["valid"]:
    #                 return {
    #                     "success": False,
    #                     "error": f"Node at index {len(batch_params)}: {checksum_validation['error']}"
    #                 }
                
    #             # Flatten complex properties
    #             flattened_props = neo4j_graph._flatten_complex_properties(properties)
                
    #             batch_params.append({
    #                 "type": node_type,
    #                 "props": flattened_props
    #             })
            
    #         # Execute batch creation
    #         query = """
    #         UNWIND $batch AS node
    #         CREATE (n:$node.type)
    #         SET n = node.props
    #         RETURN n { .* } as node, labels(n) as labels
    #         """
            
    #         result = await neo4j_graph.execute_query(query, {"batch": batch_params})
            
    #         # Process results
    #         processed_nodes = []
    #         for item in result:
    #             node_data = neo4j_graph._unflatten_complex_properties(item["node"])
    #             node_type = item["labels"][0]  # Get the node type from labels
    #             processed_nodes.append({
    #                 "type": node_type,
    #                 "properties": node_data
    #             })
            
    #         return {
    #             "success": True,
    #             "nodes": processed_nodes,
    #             "count": len(processed_nodes),
    #             "message": f"Created {len(processed_nodes)} nodes successfully"
    #         }
    #     except Exception as e:
    #         await ctx.error(f"Batch node creation failed: {str(e)}")
    #         return {"success": False, "error": str(e)}

    # @mcp.tool()
    # async def neo4j_batch_create_relationships(
    #     relationships: List[Dict[str, Any]],
    #     ctx: Context = None
    # ) -> Dict[str, Any]:
    #     """
    #     Create multiple relationships in a single transaction
        
    #     Args:
    #         relationships: List of relationship definitions with source, target, and type information
    #         ctx: Context
            
    #     Returns:
    #         Dictionary with operation result
    #     """
    #     try:
    #         # Validate and prepare batch parameters
    #         batch_params = []
            
    #         for rel in relationships:
    #             from_node_type = rel.get("from_node_type")
    #             from_node_props = rel.get("from_node_props", {})
    #             to_node_type = rel.get("to_node_type")
    #             to_node_props = rel.get("to_node_props", {})
    #             rel_type = rel.get("rel_type")
    #             properties = rel.get("properties", {})
                
    #             # Validate relationship type
    #             # valid_rel_types = ["CALLS", "DEFINED_IN", "INCLUDED_IN", "HAS_PARAMETER", 
    #             #                   "CONTAINS", "INHERITS_FROM", "IMPLEMENTS", "DECLARED_IN"]
    #             # if rel_type not in valid_rel_types:
    #             #     return {
    #             #         "success": False,
    #             #         "error": f"Invalid relationship type: {rel_type}. Valid types are: {', '.join(valid_rel_types)}"
    #             #     }
                
    #             # Flatten properties
    #             from_props_flat = neo4j_graph._flatten_complex_properties(from_node_props)
    #             to_props_flat = neo4j_graph._flatten_complex_properties(to_node_props)
    #             rel_props_flat = neo4j_graph._flatten_complex_properties(properties)
                
    #             batch_params.append({
    #                 "from_type": from_node_type,
    #                 "from_props": from_props_flat,
    #                 "to_type": to_node_type,
    #                 "to_props": to_props_flat,
    #                 "rel_type": rel_type,
    #                 "rel_props": rel_props_flat
    #             })
            
    #         # Execute batch creation
    #         query = """
    #         UNWIND $batch AS rel
    #         MATCH (a:$rel.from_type), (b:$rel.to_type)
    #         WHERE all(k IN keys(rel.from_props) WHERE a[k] = rel.from_props[k])
    #         AND all(k IN keys(rel.to_props) WHERE b[k] = rel.to_props[k])
    #         CREATE (a)-[r:$rel.rel_type]->(b)
    #         SET r = rel.rel_props
    #         RETURN type(r) as type, r { .* } as relationship,
    #         a { .* } as source, labels(a) as source_labels,
    #         b { .* } as target, labels(b) as target_labels
    #         """
            
    #         result = await neo4j_graph.execute_query(query, {"batch": batch_params})
            
    #         # Process results
    #         processed_rels = []
    #         for item in result:
    #             rel_data = neo4j_graph._unflatten_complex_properties(item["relationship"])
    #             source_data = neo4j_graph._unflatten_complex_properties(item["source"])
    #             target_data = neo4j_graph._unflatten_complex_properties(item["target"])
                
    #             processed_rels.append({
    #                 "type": item["type"],
    #                 "relationship": rel_data,
    #                 "source": {
    #                     "type": item["source_labels"][0],
    #                     "properties": source_data
    #                 },
    #                 "target": {
    #                     "type": item["target_labels"][0],
    #                     "properties": target_data
    #                 }
    #             })
            
    #         return {
    #             "success": True,
    #             "relationships": processed_rels,
    #             "count": len(processed_rels),
    #             "message": f"Created {len(processed_rels)} relationships successfully"
    #         }
    #     except Exception as e:
    #         await ctx.error(f"Batch relationship creation failed: {str(e)}")
    #         return {"success": False, "error": str(e)}

    