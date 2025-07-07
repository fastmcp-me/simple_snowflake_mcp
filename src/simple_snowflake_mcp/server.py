import asyncio
import snowflake.connector
import os
from dotenv import load_dotenv

from mcp.server.models import InitializationOptions
import mcp.types as types
from mcp.server import NotificationOptions, Server
from pydantic import AnyUrl
import mcp.server.stdio

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

# Store notes as a simple key-value dict to demonstrate state management
notes: dict[str, str] = {}

server = Server("simple_snowflake_mcp")

# Configuration Snowflake (à adapter avec vos identifiants)
SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
}

# Ajout dynamique des paramètres optionnels si présents
def _add_optional_snowflake_params(config):
    for opt in [
        ("warehouse", "SNOWFLAKE_WAREHOUSE"),
        ("database", "SNOWFLAKE_DATABASE"),
        ("schema", "SNOWFLAKE_SCHEMA")
    ]:
        val = os.getenv(opt[1])
        if val:
            config[opt[0]] = val
_add_optional_snowflake_params(SNOWFLAKE_CONFIG)

# Ajout d'une variable globale pour le mode read-only par défaut (TRUE par défaut)
MCP_READ_ONLY = os.getenv("MCP_READ_ONLY", "TRUE").lower() == "true"

@server.list_resources()
async def handle_list_resources() -> list[types.Resource]:
    """
    List available note resources.
    Each note is exposed as a resource with a custom note:// URI scheme.
    """
    return [
        types.Resource(
            uri=AnyUrl(f"note://internal/{name}"),
            name=f"Note: {name}",
            description=f"A simple note named {name}",
            mimeType="text/plain",
        )
        for name in notes
    ]

@server.read_resource()
async def handle_read_resource(uri: AnyUrl) -> str:
    """
    Read a specific note's content by its URI.
    The note name is extracted from the URI host component.
    """
    if uri.scheme != "note":
        raise ValueError(f"Unsupported URI scheme: {uri.scheme}")

    name = uri.path
    if name is not None:
        name = name.lstrip("/")
        return notes[name]
    raise ValueError(f"Note not found: {name}")

@server.list_prompts()
async def handle_list_prompts() -> list[types.Prompt]:
    """
    List available prompts.
    Each prompt can have optional arguments to customize its behavior.
    """
    return [
        types.Prompt(
            name="summarize-notes",
            description="Creates a summary of all notes",
            arguments=[
                types.PromptArgument(
                    name="style",
                    description="Style of the summary (brief/detailed)",
                    required=False,
                )
            ],
        )
    ]

@server.get_prompt()
async def handle_get_prompt(
    name: str, arguments: dict[str, str] | None
) -> types.GetPromptResult:
    """
    Generate a prompt by combining arguments with server state.
    The prompt includes all current notes and can be customized via arguments.
    """
    if name != "summarize-notes":
        raise ValueError(f"Unknown prompt: {name}")

    style = (arguments or {}).get("style", "brief")
    detail_prompt = " Give extensive details." if style == "detailed" else ""

    return types.GetPromptResult(
        description="Summarize the current notes",
        messages=[
            types.PromptMessage(
                role="user",
                content=types.TextContent(
                    type="text",
                    text=f"Here are the current notes to summarize:{detail_prompt}\n\n"
                    + "\n".join(
                        f"- {name}: {content}"
                        for name, content in notes.items()
                    ),
                ),
            )
        ],
    )

@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    """
    List available tools.
    Each tool specifies its arguments using JSON Schema validation.
    """
    return [
        types.Tool(
            name="execute-snowflake-sql",
            description="Execute a SQL query on Snowflake and return the result.",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SQL query to execute"}
                },
                "required": ["sql"],
            },
        ),
        types.Tool(
            name="list-snowflake-warehouses",
            description="List available Data Warehouses (DWH) on Snowflake.",
            inputSchema={"type": "object", "properties": {}},
        ),
        types.Tool(
            name="list-databases",
            description="List all accessible Snowflake databases.",
            inputSchema={"type": "object", "properties": {}},
        ),
        types.Tool(
            name="list-views",
            description="List all views in a database and schema.",
            inputSchema={
                "type": "object",
                "properties": {
                    "database": {"type": "string"},
                    "schema": {"type": "string"}
                },
                "required": ["database", "schema"]
            },
        ),
        types.Tool(
            name="describe-view",
            description="Get details of a view (columns, SQL).",
            inputSchema={
                "type": "object",
                "properties": {
                    "database": {"type": "string"},
                    "schema": {"type": "string"},
                    "view": {"type": "string"}
                },
                "required": ["database", "schema", "view"]
            },
        ),
        types.Tool(
            name="query-view",
            description="Query a view with an optional row limit (markdown result).",
            inputSchema={
                "type": "object",
                "properties": {
                    "database": {"type": "string"},
                    "schema": {"type": "string"},
                    "view": {"type": "string"},
                    "limit": {"type": "integer"}
                },
                "required": ["database", "schema", "view"]
            },
        ),
        types.Tool(
            name="execute-query",
            description="Execute a SQL query in read-only mode (SELECT, SHOW, DESCRIBE, EXPLAIN, WITH) or not (if 'read_only' is false), result in markdown format.",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SQL query to execute"},
                    "read_only": {"type": "boolean", "default": True, "description": "Allow only read-only queries"}
                },
                "required": ["sql"]
            },
        ),
    ]

@server.call_tool()
async def handle_call_tool(
    name: str, arguments: dict | None
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    """
    Handle tool execution requests.
    Tools can modify server state and notify clients of changes.
    """
    # Suppression du traitement de l'outil add-note
    if name == "execute-snowflake-sql":
        if not arguments or "sql" not in arguments:
            raise ValueError("Missing argument 'sql'")
        sql = arguments["sql"]
        try:
            ctx = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            cur = ctx.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            result = [dict(zip(columns, row)) for row in rows]
            cur.close()
            ctx.close()
            return [types.TextContent(type="text", text=str(result))]
        except Exception as e:
            return [types.TextContent(type="text", text=f"Snowflake error: {e}")]

    if name == "list-snowflake-warehouses":
        try:
            ctx = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            cur = ctx.cursor()
            cur.execute("SHOW WAREHOUSES;")
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            result = [dict(zip(columns, row)) for row in rows]
            cur.close()
            ctx.close()
            return [types.TextContent(type="text", text=str(result))]
        except Exception as e:
            return [types.TextContent(type="text", text=f"Snowflake error: {e}")]

    if name == "list-databases":
        try:
            ctx = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            cur = ctx.cursor()
            cur.execute("SHOW DATABASES;")
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            result = [dict(zip(columns, row)) for row in rows]
            cur.close()
            ctx.close()
            return [types.TextContent(type="text", text=str(result))]
        except Exception as e:
            return [types.TextContent(type="text", text=f"Snowflake error: {e}")]

    if name == "list-views":
        try:
            database = arguments["database"]
            schema = arguments["schema"]
            ctx = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            cur = ctx.cursor()
            cur.execute(f"SHOW VIEWS IN {database}.{schema};")
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            result = [dict(zip(columns, row)) for row in rows]
            cur.close()
            ctx.close()
            return [types.TextContent(type="text", text=str(result))]
        except Exception as e:
            return [types.TextContent(type="text", text=f"Snowflake error: {e}")]

    if name == "describe-view":
        try:
            database = arguments["database"]
            schema = arguments["schema"]
            view = arguments["view"]
            ctx = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            cur = ctx.cursor()
            cur.execute(f"DESC VIEW {database}.{schema}.{view};")
            desc_rows = cur.fetchall()
            desc_columns = [desc[0] for desc in cur.description]
            cur.execute(f"SHOW VIEWS LIKE '{view}' IN {database}.{schema};")
            show_rows = cur.fetchall()
            show_columns = [desc[0] for desc in cur.description]
            cur.close()
            ctx.close()
            return [types.TextContent(type="text", text=f"DESC:\n{desc_columns}\n{desc_rows}\nSHOW:\n{show_columns}\n{show_rows}")]
        except Exception as e:
            return [types.TextContent(type="text", text=f"Snowflake error: {e}")]

    if name == "query-view":
        try:
            database = arguments["database"]
            schema = arguments["schema"]
            view = arguments["view"]
            limit = arguments.get("limit", 100)
            ctx = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            cur = ctx.cursor()
            cur.execute(f"SELECT * FROM {database}.{schema}.{view} LIMIT {limit};")
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            # Format markdown
            md = "| " + " | ".join(columns) + " |\n|" + "---|"*len(columns) + "\n"
            for row in rows:
                md += "| " + " | ".join(str(cell) for cell in row) + " |\n"
            cur.close()
            ctx.close()
            return [types.TextContent(type="text", text=md)]
        except Exception as e:
            return [types.TextContent(type="text", text=f"Snowflake error: {e}")]

    if name == "execute-query":
        try:
            sql = arguments["sql"]
            # Priorité à l'argument d'appel, sinon valeur globale
            read_only = arguments.get("read_only", MCP_READ_ONLY)
            allowed = sql.strip().split()[0].upper() in ["SELECT", "SHOW", "DESCRIBE", "EXPLAIN", "WITH"]
            if read_only and not allowed:
                return [types.TextContent(type="text", text="Only read-only queries are allowed.")]
            ctx = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            cur = ctx.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            # Format markdown
            md = "| " + " | ".join(columns) + " |\n|" + "---|"*len(columns) + "\n"
            for row in rows:
                md += "| " + " | ".join(str(cell) for cell in row) + " |\n"
            cur.close()
            ctx.close()
            return [types.TextContent(type="text", text=md)]
        except Exception as e:
            return [types.TextContent(type="text", text=f"Snowflake error: {e}")]

    raise ValueError(f"Unknown tool: {name}")

async def test_snowflake_connection():
    try:
        ctx = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cur = ctx.cursor()
        cur.execute("SELECT CURRENT_TIMESTAMP;")
        result = cur.fetchone()
        cur.close()
        ctx.close()
        print(f"Snowflake connection OK, CURRENT_TIMESTAMP: {result[0]}")
    except Exception as e:
        print(f"Snowflake connection error: {e}")

async def main():
    # Run the server using stdin/stdout streams
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="simple_snowflake_mcp",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())