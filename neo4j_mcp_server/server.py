#!/usr/bin/env python3
"""
Neo4j MCP Server - Dedicated server for Neo4j Code Property Graph operations
"""

import os
import uvicorn
from contextlib import asynccontextmanager
from starlette.applications import Starlette
from starlette.routing import Mount

import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Try importing from installed package first
try:
    from mcp.server.fastmcp import FastMCP
    from neo4j_mcp_server.tools.neo4j_memory.tools import register_tools as register_neo4j_tools
# Fall back to direct imports if not installed
except ImportError:
    from mcp.server.fastmcp import FastMCP
    from neo4j_mcp_server.tools.neo4j_memory.tools import register_tools as register_neo4j_tools

# Create the MCP server instance
mcp = FastMCP("Neo4j MCP Server")

# Register Neo4j tools only
register_neo4j_tools(mcp)

@asynccontextmanager
async def lifespan_context(app):
    """ASGI lifespan context manager for database connections"""
    # Use the same try/except pattern for this import
    try:
        from neo4j_mcp_server.tools.neo4j_memory.tools import neo4j_graph #, create_fulltext_index
    except ImportError:
        from neo4j_mcp_server.tools.neo4j_memory.tools import neo4j_graph #, create_fulltext_index
    
    # Startup: connect to Neo4j
    print("Establishing Neo4j connection...")
    await neo4j_graph.connect()
    # Create required indexes
    # await create_fulltext_index()
    yield
    # Shutdown: close Neo4j connection
    print("Closing Neo4j connection...")
    await neo4j_graph.close()

# Create a Starlette application with both SSE and HTTP endpoints
app = Starlette(
    routes=[
        Mount("/", app=mcp.sse_app()),  # Mount at root path to handle all requests
    ],
    lifespan=lifespan_context
)

def main():
    """Run the MCP server."""
    print("Starting Code Analysis MCP server...")
    print("Available transports:")
    print("- SSE: http://0.0.0.0:8100")
    uvicorn.run(app, host="0.0.0.0", port=8100)

if __name__ == "__main__":
    main()
