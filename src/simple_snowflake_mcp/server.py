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
    # Optionnel : "warehouse", "database", "schema"
}

# Ajout d'une variable globale pour le mode read-only par défaut
MCP_READ_ONLY = os.getenv("MCP_READ_ONLY", "true").lower() == "true"

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
            name="add-note",
            description="Add a new note",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "content": {"type": "string"},
                },
                "required": ["name", "content"],
            },
        ),
        types.Tool(
            name="execute-snowflake-sql",
            description="Exécute une requête SQL sur Snowflake et retourne le résultat.",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "Requête SQL à exécuter"}
                },
                "required": ["sql"],
            },
        ),
        types.Tool(
            name="list-snowflake-warehouses",
            description="Liste les Data Warehouses (DWH) disponibles sur Snowflake.",
            inputSchema={"type": "object", "properties": {}},
        ),
        types.Tool(
            name="list-databases",
            description="Liste toutes les bases de données Snowflake accessibles.",
            inputSchema={"type": "object", "properties": {}},
        ),
        types.Tool(
            name="list-views",
            description="Liste toutes les vues d'une base et d'un schéma.",
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
            description="Donne les détails d'une vue (colonnes, SQL).",
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
            description="Interroge une vue avec une limite de lignes optionnelle.",
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
            description="Exécute une requête SQL en lecture seule (SELECT, SHOW, DESCRIBE, EXPLAIN, WITH) et retourne le résultat au format markdown.",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "Requête SQL à exécuter"},
                    "read_only": {"type": "boolean", "default": True, "description": "N'autoriser que les requêtes en lecture seule"}
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
    if name == "add-note":
        if not arguments:
            raise ValueError("Missing arguments")
        note_name = arguments.get("name")
        content = arguments.get("content")
        if not note_name or not content:
            raise ValueError("Missing name or content")
        # Update server state
        notes[note_name] = content
        # Notify clients that resources have changed
        await server.request_context.session.send_resource_list_changed()
        return [
            types.TextContent(
                type="text",
                text=f"Added note '{note_name}' with content: {content}",
            )
        ]

    if name == "execute-snowflake-sql":
        if not arguments or "sql" not in arguments:
            raise ValueError("Argument 'sql' manquant")
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
            return [types.TextContent(type="text", text=f"Erreur Snowflake: {e}")]

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
            return [types.TextContent(type="text", text=f"Erreur Snowflake: {e}")]

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
            return [types.TextContent(type="text", text=f"Erreur Snowflake: {e}")]

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
            return [types.TextContent(type="text", text=f"Erreur Snowflake: {e}")]

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
            return [types.TextContent(type="text", text=f"Erreur Snowflake: {e}")]

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
            return [types.TextContent(type="text", text=f"Erreur Snowflake: {e}")]

    if name == "execute-query":
        try:
            sql = arguments["sql"]
            # Priorité à l'argument d'appel, sinon valeur globale
            read_only = arguments.get("read_only", MCP_READ_ONLY)
            allowed = sql.strip().split()[0].upper() in ["SELECT", "SHOW", "DESCRIBE", "EXPLAIN", "WITH"]
            if read_only and not allowed:
                return [types.TextContent(type="text", text="Seules les requêtes en lecture seule sont autorisées.")]
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
            return [types.TextContent(type="text", text=f"Erreur Snowflake: {e}")]

    raise ValueError(f"Unknown tool: {name}")

async def test_snowflake_connection():
    try:
        ctx = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cur = ctx.cursor()
        cur.execute("SELECT CURRENT_TIMESTAMP;")
        result = cur.fetchone()
        cur.close()
        ctx.close()
        print(f"Connexion Snowflake OK, CURRENT_TIMESTAMP: {result[0]}")
    except Exception as e:
        print(f"Erreur de connexion Snowflake: {e}")

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