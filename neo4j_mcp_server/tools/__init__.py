"""Tools for the Neo4j MCP Server."""

from . import neo4j_memory

def register_all_tools(mcp):
    """Register all available tools with the MCP server."""
    from .neo4j_memory.tools import register_tools as register_neo4j_memory
    
    # Register Neo4j tools
    register_neo4j_memory(mcp)
