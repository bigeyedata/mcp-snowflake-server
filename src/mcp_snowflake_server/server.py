import importlib.metadata
import json
import logging
import os
from functools import wraps
from typing import Any, Callable

import mcp.server.stdio
import mcp.types as types
import yaml
from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions
from pydantic import AnyUrl, BaseModel

from .auth import SnowflakeAuthClient
from .db_client import SnowflakeDB
from .write_detector import SQLWriteDetector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("mcp_snowflake_server")


def data_to_yaml(data: Any) -> str:
    return yaml.dump(data, indent=2, sort_keys=False)

# Custom serializer that checks for 'date' type
def data_json_serializer(obj):
    from datetime import date, datetime
    if isinstance(obj, date) or isinstance(obj, datetime):
        return obj.isoformat()
    else:
        return obj


def handle_tool_errors(func: Callable) -> Callable:
    """Decorator to standardize tool error handling"""

    @wraps(func)
    async def wrapper(*args, **kwargs) -> list[types.TextContent]:
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}")
            return [types.TextContent(type="text", text=f"Error: {str(e)}")]

    return wrapper


class Tool(BaseModel):
    name: str
    description: str
    input_schema: dict[str, Any]
    handler: Callable[
        [str, dict[str, Any] | None],
        list[types.TextContent | types.ImageContent | types.EmbeddedResource],
    ]
    tags: list[str] = []


# Tool handlers
async def handle_list_databases(arguments, db, *_, exclusion_config=None):
    query = "SELECT DATABASE_NAME FROM INFORMATION_SCHEMA.DATABASES"
    data, data_id = await db.execute_query(query)

    # Filter out excluded databases
    if exclusion_config and "databases" in exclusion_config and exclusion_config["databases"]:
        filtered_data = []
        for item in data:
            db_name = item.get("DATABASE_NAME", "")
            exclude = False
            for pattern in exclusion_config["databases"]:
                if pattern.lower() in db_name.lower():
                    exclude = True
                    break
            if not exclude:
                filtered_data.append(item)
        data = filtered_data

    output = {
        "type": "data",
        "data_id": data_id,
        "data": data,
    }
    yaml_output = data_to_yaml(output)
    json_output = json.dumps(output)
    return [
        types.TextContent(type="text", text=yaml_output),
        types.EmbeddedResource(
            type="resource",
            resource=types.TextResourceContents(uri=f"data://{data_id}", text=json_output, mimeType="application/json"),
        ),
    ]


async def handle_list_schemas(arguments, db, *_, exclusion_config=None):
    if not arguments or "database" not in arguments:
        raise ValueError("Missing required 'database' parameter")

    database = arguments["database"]
    query = f"SELECT SCHEMA_NAME FROM {database.upper()}.INFORMATION_SCHEMA.SCHEMATA"
    data, data_id = await db.execute_query(query)

    # Filter out excluded schemas
    if exclusion_config and "schemas" in exclusion_config and exclusion_config["schemas"]:
        filtered_data = []
        for item in data:
            schema_name = item.get("SCHEMA_NAME", "")
            exclude = False
            for pattern in exclusion_config["schemas"]:
                if pattern.lower() in schema_name.lower():
                    exclude = True
                    break
            if not exclude:
                filtered_data.append(item)
        data = filtered_data

    output = {
        "type": "data",
        "data_id": data_id,
        "database": database,
        "data": data,
    }
    yaml_output = data_to_yaml(output)
    json_output = json.dumps(output)
    return [
        types.TextContent(type="text", text=yaml_output),
        types.EmbeddedResource(
            type="resource",
            resource=types.TextResourceContents(uri=f"data://{data_id}", text=json_output, mimeType="application/json"),
        ),
    ]


async def handle_list_tables(arguments, db, *_, exclusion_config=None):
    if not arguments or "database" not in arguments or "schema" not in arguments:
        raise ValueError("Missing required 'database' and 'schema' parameters")

    database = arguments["database"]
    schema = arguments["schema"]

    query = f"""
        SELECT table_catalog, table_schema, table_name, comment 
        FROM {database}.information_schema.tables 
        WHERE table_schema = '{schema.upper()}'
    """
    data, data_id = await db.execute_query(query)

    # Filter out excluded tables
    if exclusion_config and "tables" in exclusion_config and exclusion_config["tables"]:
        filtered_data = []
        for item in data:
            table_name = item.get("TABLE_NAME", "")
            exclude = False
            for pattern in exclusion_config["tables"]:
                if pattern.lower() in table_name.lower():
                    exclude = True
                    break
            if not exclude:
                filtered_data.append(item)
        data = filtered_data

    output = {
        "type": "data",
        "data_id": data_id,
        "database": database,
        "schema": schema,
        "data": data,
    }
    yaml_output = data_to_yaml(output)
    json_output = json.dumps(output)
    return [
        types.TextContent(type="text", text=yaml_output),
        types.EmbeddedResource(
            type="resource",
            resource=types.TextResourceContents(uri=f"data://{data_id}", text=json_output, mimeType="application/json"),
        ),
    ]


async def handle_describe_table(arguments, db, *_):
    if not arguments or "table_name" not in arguments:
        raise ValueError("Missing table_name argument")

    table_spec = arguments["table_name"]
    split_identifier = table_spec.split(".")

    # Parse the fully qualified table name
    if len(split_identifier) < 3:
        raise ValueError("Table name must be fully qualified as 'database.schema.table'")

    database_name = split_identifier[0].upper()
    schema_name = split_identifier[1].upper()
    table_name = split_identifier[2].upper()

    query = f"""
        SELECT column_name, column_default, is_nullable, data_type, comment 
        FROM {database_name}.information_schema.columns 
        WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
    """
    data, data_id = await db.execute_query(query)

    output = {
        "type": "data",
        "data_id": data_id,
        "database": database_name,
        "schema": schema_name,
        "table": table_name,
        "data": data,
    }
    yaml_output = data_to_yaml(output)
    json_output = json.dumps(output)
    return [
        types.TextContent(type="text", text=yaml_output),
        types.EmbeddedResource(
            type="resource",
            resource=types.TextResourceContents(uri=f"data://{data_id}", text=json_output, mimeType="application/json"),
        ),
    ]


async def handle_read_query(arguments, db, write_detector, *_):
    if not arguments or "query" not in arguments:
        raise ValueError("Missing query argument")

    if write_detector.analyze_query(arguments["query"])["contains_write"]:
        raise ValueError("Calls to read_query should not contain write operations")

    data, data_id = await db.execute_query(arguments["query"])

    output = {
        "type": "data",
        "data_id": data_id,
        "data": data,
    }
    yaml_output = data_to_yaml(output)
    json_output = json.dumps(output, default=data_json_serializer)
    return [
        types.TextContent(type="text", text=yaml_output),
        types.EmbeddedResource(
            type="resource",
            resource=types.TextResourceContents(uri=f"data://{data_id}", text=json_output, mimeType="application/json"),
        ),
    ]


async def handle_append_insight(arguments, db, _, __, server):
    if not arguments or "insight" not in arguments:
        raise ValueError("Missing insight argument")

    db.add_insight(arguments["insight"])
    await server.request_context.session.send_resource_updated(AnyUrl("memo://insights"))
    return [types.TextContent(type="text", text="Insight added to memo")]


async def handle_write_query(arguments, db, _, allow_write, __):
    if not allow_write:
        raise ValueError("Write operations are not allowed for this data connection")
    if arguments["query"].strip().upper().startswith("SELECT"):
        raise ValueError("SELECT queries are not allowed for write_query")

    results, data_id = await db.execute_query(arguments["query"])
    return [types.TextContent(type="text", text=str(results))]


async def handle_create_table(arguments, db, _, allow_write, __):
    if not allow_write:
        raise ValueError("Write operations are not allowed for this data connection")
    if not arguments["query"].strip().upper().startswith("CREATE TABLE"):
        raise ValueError("Only CREATE TABLE statements are allowed")

    results, data_id = await db.execute_query(arguments["query"])
    return [types.TextContent(type="text", text=f"Table created successfully. data_id = {data_id}")]


async def prefetch_tables(db: SnowflakeDB, credentials: dict) -> dict:
    """Prefetch table and column information"""
    try:
        logger.info("Prefetching table descriptions")
        table_results, data_id = await db.execute_query(
            f"""SELECT table_name, comment 
                FROM {credentials['database']}.information_schema.tables 
                WHERE table_schema = '{credentials['schema'].upper()}'"""
        )

        column_results, data_id = await db.execute_query(
            f"""SELECT table_name, column_name, data_type, comment 
                FROM {credentials['database']}.information_schema.columns 
                WHERE table_schema = '{credentials['schema'].upper()}'"""
        )

        tables_brief = {}
        for row in table_results:
            tables_brief[row["TABLE_NAME"]] = {**row, "COLUMNS": {}}

        for row in column_results:
            row_without_table_name = row.copy()
            del row_without_table_name["TABLE_NAME"]
            tables_brief[row["TABLE_NAME"]]["COLUMNS"][row["COLUMN_NAME"]] = row_without_table_name

        return tables_brief

    except Exception as e:
        logger.error(f"Error prefetching table descriptions: {e}")
        return f"Error prefetching table descriptions: {e}"


# Authentication handlers
async def handle_authenticate_snowflake(arguments, db, _, __, server, auth_client=None, **kwargs):
    """Authenticate with Snowflake using connection parameters"""
    if not arguments:
        raise ValueError("Missing connection parameters")
    
    required_params = ['account', 'user', 'password']
    missing = [p for p in required_params if p not in arguments]
    if missing:
        raise ValueError(f"Missing required parameters: {', '.join(missing)}")
    
    # Build connection parameters
    connection_params = {
        'account': arguments['account'],
        'user': arguments['user'], 
        'password': arguments['password']
    }
    
    # Add optional parameters
    optional_params = ['warehouse', 'database', 'schema', 'role']
    for param in optional_params:
        if param in arguments:
            connection_params[param] = arguments[param]
    
    # Test authentication
    auth_result = auth_client.test_authentication(connection_params)
    
    if not auth_result.get('valid', False):
        return [
            types.TextContent(
                type="text",
                text=f"Authentication failed: {auth_result.get('error', 'Unknown error')}"
            )
        ]
    
    # Set credentials in auth client
    auth_client.set_credentials(connection_params)
    
    # Save if requested
    if arguments.get('save_credentials', True):
        auth_client.storage.save_credentials(
            connection_params['account'],
            connection_params['user'],
            connection_params
        )
    
    # Create and initialize the database connection
    # This is passed by reference from main()
    new_db = SnowflakeDB(connection_params)
    new_db.start_init_connection()
    
    # Update the db reference in the main scope
    # We'll need to pass this through kwargs
    if 'db_setter' in kwargs:
        kwargs['db_setter'](new_db)
    
    return [
        types.TextContent(
            type="text",
            text=f"""Successfully authenticated to Snowflake:
- Account: {auth_result['account']}
- User: {auth_result['user']}
- Role: {auth_result['role']}
- Warehouse: {auth_result['warehouse']}
- Credentials saved: {arguments.get('save_credentials', True)}"""
        )
    ]


async def handle_use_saved_credentials(arguments, db, _, __, server, auth_client=None, **kwargs):
    """Use previously saved credentials"""
    if not arguments or 'account' not in arguments or 'user' not in arguments:
        raise ValueError("Missing required 'account' and 'user' parameters")
    
    connection_params = auth_client.storage.get_credentials(
        arguments['account'],
        arguments['user']
    )
    
    if not connection_params:
        return [
            types.TextContent(
                type="text",
                text=f"No saved credentials found for account '{arguments['account']}' and user '{arguments['user']}'"
            )
        ]
    
    # Test that credentials still work
    auth_result = auth_client.test_authentication(connection_params)
    
    if auth_result['valid']:
        auth_client.set_credentials(connection_params)
        
        # Create and initialize the database connection
        new_db = SnowflakeDB(connection_params)
        new_db.start_init_connection()
        
        # Update the db reference in the main scope
        if 'db_setter' in kwargs:
            kwargs['db_setter'](new_db)
        
        return [
            types.TextContent(
                type="text",
                text=f"Connected to Snowflake account '{arguments['account']}' as user '{arguments['user']}'"
            )
        ]
    else:
        return [
            types.TextContent(
                type="text",
                text="Saved credentials are no longer valid. Please authenticate again."
            )
        ]


async def handle_list_saved_credentials(arguments, db, _, __, server, auth_client=None, **kwargs):
    """List all saved credentials"""
    saved = auth_client.storage.list_saved_credentials()
    
    if not saved:
        return [
            types.TextContent(
                type="text",
                text="No saved credentials found."
            )
        ]
    
    output = "Saved Snowflake credentials:\n\n"
    for account, users in saved.items():
        output += f"Account: {account}\n"
        for user in users:
            output += f"  - User: {user}\n"
    
    return [types.TextContent(type="text", text=output)]


async def handle_delete_saved_credentials(arguments, db, _, __, server, auth_client=None, **kwargs):
    """Delete saved credentials"""
    account = arguments.get('account') if arguments else None
    user = arguments.get('user') if arguments else None
    
    auth_client.storage.delete_credentials(account, user)
    
    if not account and not user:
        message = "All saved credentials have been deleted."
    elif account and user:
        message = f"Deleted credentials for account '{account}' and user '{user}'."
    elif account:
        message = f"Deleted all credentials for account '{account}'."
    else:
        message = "Invalid parameters for credential deletion."
    
    return [types.TextContent(type="text", text=message)]


async def main(
    allow_write: bool = False,
    connection_args: dict = None,
    log_dir: str = None,
    prefetch: bool = False,
    log_level: str = "INFO",
    exclude_tools: list[str] = [],
    config_file: str = "runtime_config.json",
    exclude_patterns: dict = None,
    connection_config_file: str = "config.json",
):
    # Setup logging
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
        logger.handlers.append(logging.FileHandler(os.path.join(log_dir, "mcp_snowflake_server.log")))
    if log_level:
        logger.setLevel(log_level)

    logger.info("Starting Snowflake MCP Server")
    logger.info("Allow write operations: %s", allow_write)
    logger.info("Prefetch table descriptions: %s", prefetch)
    logger.info("Excluded tools: %s", exclude_tools)

    # Load configuration from file if provided
    config = {}
    #
    if config_file:
        try:
            with open(config_file, "r") as f:
                config = json.load(f)
                logger.info(f"Loaded configuration from {config_file}")
        except Exception as e:
            logger.error(f"Error loading configuration file: {e}")

    # Merge exclude_patterns from parameters with config file
    exclusion_config = config.get("exclude_patterns", {})
    if exclude_patterns:
        # Merge patterns from parameters with those from config file
        for key, patterns in exclude_patterns.items():
            if key in exclusion_config:
                exclusion_config[key].extend(patterns)
            else:
                exclusion_config[key] = patterns

    # Set default patterns if none are specified
    if not exclusion_config:
        exclusion_config = {"databases": [], "schemas": [], "tables": []}

    # Ensure all keys exist in the exclusion config
    for key in ["databases", "schemas", "tables"]:
        if key not in exclusion_config:
            exclusion_config[key] = []

    logger.info(f"Exclusion patterns: {exclusion_config}")

    # Initialize authentication client
    auth_client = SnowflakeAuthClient()
    db = None
    
    # Create a reference holder for db that can be updated
    db_ref = {'db': None}
    
    def set_db(new_db):
        nonlocal db
        db = new_db
        db_ref['db'] = new_db
    
    # First, try to load connection config from file
    connection_config = None
    if connection_config_file and os.path.exists(connection_config_file):
        try:
            with open(connection_config_file, 'r') as f:
                connection_config = json.load(f)
                logger.info(f"Loaded connection configuration from {connection_config_file}")
                # Merge with any command-line args
                if connection_args:
                    connection_config.update(connection_args)
                connection_args = connection_config
        except Exception as e:
            logger.error(f"Error loading connection config file: {e}")
    
    # Check if we have pre-configured credentials
    if connection_args and all(k in connection_args for k in ['account', 'user', 'password']):
        # Use config-based authentication
        logger.info("Using pre-configured authentication")
        auth_client.set_credentials(connection_args)
        db = SnowflakeDB(connection_args)
        db.start_init_connection()
    else:
        # Dynamic authentication mode
        logger.info("Starting in dynamic authentication mode")
        logger.info("No valid credentials found. Use 'authenticate_snowflake' tool to connect.")
        # Create a placeholder DB that will be initialized after authentication
        db = None
    
    server = Server("snowflake-manager")
    write_detector = SQLWriteDetector()

    tables_info = (await prefetch_tables(db, connection_args)) if (prefetch and db) else {}
    tables_brief = data_to_yaml(tables_info) if (prefetch and db) else ""

    all_tools = [
        Tool(
            name="list_databases",
            description="List all available databases in Snowflake",
            input_schema={
                "type": "object",
                "properties": {},
            },
            handler=handle_list_databases,
        ),
        Tool(
            name="list_schemas",
            description="List all schemas in a database",
            input_schema={
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "Database name to list schemas from",
                    },
                },
                "required": ["database"],
            },
            handler=handle_list_schemas,
        ),
        Tool(
            name="list_tables",
            description="List all tables in a specific database and schema",
            input_schema={
                "type": "object",
                "properties": {
                    "database": {"type": "string", "description": "Database name"},
                    "schema": {"type": "string", "description": "Schema name"},
                },
                "required": ["database", "schema"],
            },
            handler=handle_list_tables,
        ),
        Tool(
            name="describe_table",
            description="Get the schema information for a specific table",
            input_schema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Fully qualified table name in the format 'database.schema.table'",
                    },
                },
                "required": ["table_name"],
            },
            handler=handle_describe_table,
        ),
        Tool(
            name="read_query",
            description="Execute a SELECT query.",
            input_schema={
                "type": "object",
                "properties": {"query": {"type": "string", "description": "SELECT SQL query to execute"}},
                "required": ["query"],
            },
            handler=handle_read_query,
        ),
        Tool(
            name="append_insight",
            description="Add a data insight to the memo",
            input_schema={
                "type": "object",
                "properties": {
                    "insight": {
                        "type": "string",
                        "description": "Data insight discovered from analysis",
                    }
                },
                "required": ["insight"],
            },
            handler=handle_append_insight,
            tags=["resource_based"],
        ),
        Tool(
            name="write_query",
            description="Execute an INSERT, UPDATE, or DELETE query on the Snowflake database",
            input_schema={
                "type": "object",
                "properties": {"query": {"type": "string", "description": "SQL query to execute"}},
                "required": ["query"],
            },
            handler=handle_write_query,
            tags=["write"],
        ),
        Tool(
            name="create_table",
            description="Create a new table in the Snowflake database",
            input_schema={
                "type": "object",
                "properties": {"query": {"type": "string", "description": "CREATE TABLE SQL statement"}},
                "required": ["query"],
            },
            handler=handle_create_table,
            tags=["write"],
        ),
    ]
    
    # Add authentication tools
    auth_tools = [
        Tool(
            name="authenticate_snowflake",
            description="Authenticate with Snowflake using connection parameters",
            input_schema={
                "type": "object",
                "properties": {
                    "account": {
                        "type": "string",
                        "description": "Snowflake account identifier (e.g., 'myorg-myaccount' or 'myaccount.region')"
                    },
                    "user": {
                        "type": "string",
                        "description": "Snowflake username"
                    },
                    "password": {
                        "type": "string",
                        "description": "Snowflake password"
                    },
                    "warehouse": {
                        "type": "string",
                        "description": "Warehouse to use (optional)"
                    },
                    "database": {
                        "type": "string",
                        "description": "Default database (optional)"
                    },
                    "schema": {
                        "type": "string",
                        "description": "Default schema (optional)"
                    },
                    "role": {
                        "type": "string",
                        "description": "Role to use (optional)"
                    },
                    "save_credentials": {
                        "type": "boolean",
                        "description": "Whether to save credentials for future use (default: true)",
                        "default": True
                    }
                },
                "required": ["account", "user", "password"]
            },
            handler=handle_authenticate_snowflake,
            tags=["auth"]
        ),
        Tool(
            name="use_saved_credentials",
            description="Use previously saved Snowflake credentials",
            input_schema={
                "type": "object",
                "properties": {
                    "account": {
                        "type": "string",
                        "description": "Snowflake account identifier"
                    },
                    "user": {
                        "type": "string",
                        "description": "Snowflake username"
                    }
                },
                "required": ["account", "user"]
            },
            handler=handle_use_saved_credentials,
            tags=["auth"]
        ),
        Tool(
            name="list_saved_credentials",
            description="List all saved Snowflake credentials",
            input_schema={
                "type": "object",
                "properties": {}
            },
            handler=handle_list_saved_credentials,
            tags=["auth"]
        ),
        Tool(
            name="delete_saved_credentials",
            description="Delete saved Snowflake credentials",
            input_schema={
                "type": "object",
                "properties": {
                    "account": {
                        "type": "string",
                        "description": "Snowflake account identifier (optional, deletes all if not specified)"
                    },
                    "user": {
                        "type": "string",
                        "description": "Snowflake username (optional)"
                    }
                }
            },
            handler=handle_delete_saved_credentials,
            tags=["auth"]
        )
    ]
    
    # Combine all tools
    all_tools = auth_tools + all_tools

    exclude_tags = []
    if not allow_write:
        exclude_tags.append("write")
    allowed_tools = [
        tool for tool in all_tools if tool.name not in exclude_tools and not any(tag in exclude_tags for tag in tool.tags)
    ]

    logger.info("Allowed tools: %s", [tool.name for tool in allowed_tools])

    # Register handlers
    @server.list_resources()
    async def handle_list_resources() -> list[types.Resource]:
        resources = [
            types.Resource(
                uri=AnyUrl("snowflake://auth/status"),
                name="Authentication Status",
                description="Current Snowflake authentication status",
                mimeType="text/plain",
            ),
            types.Resource(
                uri=AnyUrl("memo://insights"),
                name="Data Insights Memo",
                description="A living document of discovered data insights",
                mimeType="text/plain",
            )
        ]
        table_brief_resources = [
            types.Resource(
                uri=AnyUrl(f"context://table/{table_name}"),
                name=f"{table_name} table",
                description=f"Description of the {table_name} table",
                mimeType="text/plain",
            )
            for table_name in tables_info.keys()
        ]
        resources += table_brief_resources
        return resources

    @server.read_resource()
    async def handle_read_resource(uri: AnyUrl) -> str:
        if str(uri) == "snowflake://auth/status":
            if auth_client.is_authenticated:
                return f"""Authenticated to Snowflake:
- Account: {auth_client.current_connection_params.get('account', 'N/A')}
- User: {auth_client.current_connection_params.get('user', 'N/A')}
- Warehouse: {auth_client.current_connection_params.get('warehouse', 'Default')}
- Database: {auth_client.current_connection_params.get('database', 'Not set')}
- Schema: {auth_client.current_connection_params.get('schema', 'Not set')}
- Status: ✓ Connected"""
            else:
                saved = auth_client.storage.list_saved_credentials()
                if saved:
                    return f"""Not authenticated. Saved credentials available for:
{json.dumps(saved, indent=2)}

Use 'authenticate_snowflake' tool to connect."""
                else:
                    return """Not authenticated to Snowflake.

Use 'authenticate_snowflake' tool with your credentials to connect."""
        elif str(uri) == "memo://insights":
            return db.get_memo() if db else "Database not initialized. Please authenticate first."
        elif str(uri).startswith("context://table"):
            table_name = str(uri).split("/")[-1]
            if table_name in tables_info:
                return data_to_yaml(tables_info[table_name])
            else:
                raise ValueError(f"Unknown table: {table_name}")
        else:
            raise ValueError(f"Unknown resource: {uri}")

    @server.list_prompts()
    async def handle_list_prompts() -> list[types.Prompt]:
        return []

    @server.get_prompt()
    async def handle_get_prompt(name: str, arguments: dict[str, str] | None) -> types.GetPromptResult:
        raise ValueError(f"Unknown prompt: {name}")

    @server.call_tool()
    @handle_tool_errors
    async def handle_call_tool(
        name: str, arguments: dict[str, Any] | None
    ) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
        if name in exclude_tools:
            return [types.TextContent(type="text", text=f"Tool {name} is excluded from this data connection")]

        handler = next((tool.handler for tool in allowed_tools if tool.name == name), None)
        if not handler:
            raise ValueError(f"Unknown tool: {name}")

        # Check if db is None for non-auth tools
        if db is None and name not in ["authenticate_snowflake", "use_saved_credentials", "list_saved_credentials", "delete_saved_credentials"]:
            return [types.TextContent(type="text", text="Not authenticated. Please use 'authenticate_snowflake' tool first.")]
        
        # Pass appropriate parameters based on tool type
        if name in ["authenticate_snowflake", "use_saved_credentials", "list_saved_credentials", "delete_saved_credentials"]:
            # Authentication tools
            return await handler(
                arguments,
                db,
                write_detector,
                allow_write,
                server,
                auth_client=auth_client,
                db_setter=set_db
            )
        elif name in ["list_databases", "list_schemas", "list_tables"]:
            # Listing functions with exclusion config
            return await handler(
                arguments,
                db,
                write_detector,
                allow_write,
                server,
                exclusion_config=exclusion_config,
            )
        else:
            # Other tools
            return await handler(arguments, db, write_detector, allow_write, server)

    @server.list_tools()
    async def handle_list_tools() -> list[types.Tool]:
        logger.info("Listing tools")
        logger.error(f"Allowed tools: {allowed_tools}")
        tools = [
            types.Tool(
                name=tool.name,
                description=tool.description,
                inputSchema=tool.input_schema,
            )
            for tool in allowed_tools
        ]
        return tools

    # Start server
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        logger.info("Server running with stdio transport")
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="snowflake",
                server_version=importlib.metadata.version("mcp_snowflake_server"),
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )
