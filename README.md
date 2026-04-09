# Neo4j MCP Server

A [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that exposes Neo4j graph database operations as tools, purpose-built for Code Property Graph (CPG) analysis workflows.

## Features

- **Raw Cypher execution** — run any query with optional parameters and result limiting
- **Version-aware query guidance** — get Neo4j version and syntax notes before writing complex queries
- **Batch operations** — create nodes and relationships in bulk for high-throughput ingestion
- **Node & relationship management** — create, retrieve, and search graph entities
- **Full-text & fuzzy search** — search code entities by name, type, or semantic content
- **Symbol resolution** — resolve symbol names to node IDs via the SymbolIndex (case-insensitive)
- **Subgraph extraction** — retrieve connected subgraphs around a focal node
- **Checksum-based deduplication** — find nodes by content checksum to prevent duplicate ingestion
- **SSE transport** — served over HTTP using Server-Sent Events for real-time streaming

## Requirements

- Python >= 3.12.8, < 3.13
- A running Neo4j 5.x instance (local or remote)

## Installation

### With uv (recommended — exact lockfile reproducibility)

```bash
git clone https://github.com/VishwasSomasekhariah/neo4j-mcp.git
cd neo4j-mcp
uv sync
```

### With pip

```bash
git clone https://github.com/VishwasSomasekhariah/neo4j-mcp.git
cd neo4j-mcp
pip install -e .
```

## Configuration

### Environment variables

The server reads Neo4j connection details from environment variables:

| Variable | Default | Description |
|---|---|---|
| `NEO4J_URI` | `bolt://host.docker.internal:7687` | Bolt URI of the Neo4j instance |
| `NEO4J_USER` | `neo4j` | Neo4j username |
| `NEO4J_PASSWORD` | *(required)* | Neo4j password |

Copy `configs/examples/.env.example` to `.env` and fill in your values:

```bash
cp configs/examples/.env.example .env
# then export before running:
export $(grep -v '^#' .env | xargs)
```

### MCP client config file

Clients that connect to this server (e.g. `project-analyzer`) use a JSON config file to locate it:

```json
{
    "mcpServers": {
        "neo4j_memory": {
            "type": "http",
            "url": "http://localhost:8100/sse"
        }
    }
}
```

See [`configs/examples/neo4j_config.example.json`](configs/examples/neo4j_config.example.json) for a ready-to-use template.

Pass this file to the CLI with `--config-file`:

```bash
project-analyzer --config-file neo4j_config.json analyze --project-path /path/to/your/project
```

## Running the server

```bash
neo4j-mcp-server
```

The server starts on port **8100** and exposes one SSE endpoint:

```
http://0.0.0.0:8100/sse
```

## Connecting programmatically

The server is consumed via `mcp_use`:

```python
from mcp_use import MCPClient

client = MCPClient.from_config_file("neo4j_config.json")
session = await client.create_session("neo4j_memory")

result = await session.call_tool(
    "neo4j_execute_query",
    {"query": "MATCH (n:Function) RETURN n.name LIMIT 5"}
)

await client.close_session("neo4j_memory")
```

Or directly with `fastmcp`:

```python
import json
from fastmcp import Client
from fastmcp.client.transports import SSETransport

async with Client(transport=SSETransport("http://localhost:8100/sse")) as client:
    result = await client.call_tool("neo4j_get_version", {})
    data = json.loads(result[0].text)
    print("Neo4j version:", data["version"])
```

See [`tests/test_neo4j_mcp_client.py`](tests/test_neo4j_mcp_client.py) for a comprehensive integration test covering all tools.

## Available tools

| Tool | Description |
|---|---|
| `neo4j_execute_query` | Execute a raw Cypher query |
| `neo4j_get_version` | Get Neo4j version and syntax guidance |
| `neo4j_batch_execute_queries` | Execute multiple Cypher queries in one call |
| `neo4j_create_node` | Create a single labeled node with properties |
| `neo4j_batch_create_nodes` | Create multiple nodes in bulk |
| `neo4j_create_relationship` | Create a relationship between two nodes |
| `neo4j_batch_create_relationships` | Create multiple relationships in bulk |
| `neo4j_get_node` | Retrieve a node by ID or unique property |
| `neo4j_search_code` | Full-text search over code entity names/types |
| `neo4j_fuzzy_search` | Fuzzy/semantic search over code entities |
| `resolve_symbol` | Resolve a symbol name to node IDs via SymbolIndex |
| `neo4j_get_subgraph` | Extract a connected subgraph around a focal node |
| `neo4j_get_nodes_by_checksum` | Find nodes matching a content checksum |

## Project structure

```
neo4j-mcp/
├── neo4j_mcp_server/
│   ├── server.py                          # ASGI app + server entry point (port 8100)
│   └── tools/
│       └── neo4j_memory/
│           └── tools.py                   # All MCP tool implementations
├── tests/
│   └── test_neo4j_mcp_client.py           # Integration test / example client
├── configs/
│   └── examples/
│       ├── neo4j_config.example.json      # Example MCP client config
│       └── .env.example                   # Example environment variable config
├── pyproject.toml
├── requirements.txt
└── uv.lock                                # Pinned dependency lockfile
```

## Contributing

Contributions are welcome. Please open an issue first to discuss significant changes.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Commit your changes following [Conventional Commits](https://www.conventionalcommits.org/)
4. Open a pull request against `main`

## License

MIT — see [LICENSE](LICENSE).
