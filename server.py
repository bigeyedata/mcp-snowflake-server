"""
MCP Server for Snowflake with Dynamic Authentication

This server connects to Snowflake and exposes tools for database operations.
It supports dynamic authentication through chat without requiring configuration files.
"""

from mcp.server.fastmcp import FastMCP, Context
import os
import sys
import json
from typing import Optional, Dict, Any, List
from pathlib import Path

# Add src directory to path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Import our modules
from mcp_snowflake_server.auth import SnowflakeAuthClient
from mcp_snowflake_server.db_client import SnowflakeDB
from mcp_snowflake_server.write_detector import SQLWriteDetector

# Create config module
config = {}

# Create an MCP server
mcp = FastMCP("Snowflake")

# Debug function
def debug_print(message: str):
    """Print debug messages to stderr"""
    if config.get("debug") or os.environ.get("SNOWFLAKE_DEBUG", "false").lower() in ["true", "1", "yes"]:
        print(f"[SNOWFLAKE MCP DEBUG] {message}", file=sys.stderr)

# Load configuration
def load_config():
    """Load configuration from file and environment"""
    global config
    
    # Default configuration
    DEFAULT_CONFIG = {
        "account": None,
        "user": None,
        "password": None,
        "warehouse": None,
        "database": None,
        "schema": None,
        "role": None,
        "debug": False,
        "allow_write": False
    }
    
    # Try to load from config.json
    config_file = os.path.join(os.path.dirname(__file__), "config.json")
    file_config = {}
    
    try:
        if os.path.exists(config_file):
            with open(config_file, "r") as f:
                file_config = json.load(f)
                debug_print(f"Loaded configuration from {config_file}")
    except Exception as e:
        debug_print(f"Error loading config file: {str(e)}")
    
    # Build config with environment variable overrides
    config = {}
    for key, default in DEFAULT_CONFIG.items():
        # Environment variable takes precedence
        env_key = f"SNOWFLAKE_{key.upper()}"
        if env_key in os.environ:
            config[key] = os.environ[env_key]
        # Then file config
        elif key in file_config:
            config[key] = file_config[key]
        # Then default
        else:
            config[key] = default
    
    # Special handling for boolean values
    if isinstance(config["debug"], str):
        config["debug"] = config["debug"].lower() in ["true", "1", "yes"]
    if isinstance(config["allow_write"], str):
        config["allow_write"] = config["allow_write"].lower() in ["true", "1", "yes"]
    
    return config

# Load configuration on startup
config = load_config()

# Initialize clients
auth_client = SnowflakeAuthClient()
db = None
write_detector = SQLWriteDetector()

# Check if we have pre-configured credentials
if config.get("account") and config.get("user") and config.get("password"):
    debug_print("Using pre-configured authentication")
    connection_params = {
        k: v for k, v in config.items() 
        if k in ["account", "user", "password", "warehouse", "database", "schema", "role"] and v
    }
    auth_client.set_credentials(connection_params)
    db = SnowflakeDB(connection_params)
    db.start_init_connection()
else:
    debug_print("Starting in dynamic authentication mode")

# Authentication status resource
@mcp.resource("snowflake://auth/status")
async def auth_status() -> str:
    """Current authentication status"""
    if auth_client.is_authenticated:
        params = auth_client.current_connection_params
        return f"""Authenticated to Snowflake:
- Account: {params.get('account', 'N/A')}
- User: {params.get('user', 'N/A')}
- Warehouse: {params.get('warehouse', 'Default')}
- Database: {params.get('database', 'Not set')}
- Schema: {params.get('schema', 'Not set')}
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

# Authentication tools
@mcp.tool()
async def authenticate_snowflake(
    account: str,
    user: str,
    password: str,
    warehouse: Optional[str] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    role: Optional[str] = None,
    save_credentials: bool = True
) -> Dict[str, Any]:
    """
    Authenticate with Snowflake using connection parameters.
    
    Args:
        account: Snowflake account identifier (e.g., 'myorg-myaccount' or 'myaccount.region')
        user: Snowflake username
        password: Snowflake password
        warehouse: Warehouse to use (optional)
        database: Default database (optional)
        schema: Default schema (optional)
        role: Role to use (optional)
        save_credentials: Whether to save credentials for future use
    """
    global db
    
    # Build connection parameters
    connection_params = {
        'account': account,
        'user': user,
        'password': password
    }
    
    # Add optional parameters
    if warehouse:
        connection_params['warehouse'] = warehouse
    if database:
        connection_params['database'] = database
    if schema:
        connection_params['schema'] = schema
    if role:
        connection_params['role'] = role
    
    # Test authentication
    auth_result = auth_client.test_authentication(connection_params)
    
    if not auth_result.get('valid', False):
        return {
            'success': False,
            'error': auth_result.get('error', 'Unknown error')
        }
    
    # Set credentials
    auth_client.set_credentials(connection_params)
    
    # Save if requested
    if save_credentials:
        auth_client.storage.save_credentials(account, user, connection_params)
    
    # Create and initialize database connection
    db = SnowflakeDB(connection_params)
    db.start_init_connection()
    
    return {
        'success': True,
        'authenticated': True,
        'account': auth_result['account'],
        'user': auth_result['user'],
        'role': auth_result['role'],
        'warehouse': auth_result['warehouse'],
        'credentials_saved': save_credentials
    }

@mcp.tool()
async def use_saved_credentials(
    account: str,
    user: str
) -> Dict[str, Any]:
    """
    Use previously saved Snowflake credentials.
    """
    global db
    
    connection_params = auth_client.storage.get_credentials(account, user)
    
    if not connection_params:
        return {
            'success': False,
            'error': f'No saved credentials found for account "{account}" and user "{user}"'
        }
    
    # Test that credentials still work
    auth_result = auth_client.test_authentication(connection_params)
    
    if auth_result['valid']:
        auth_client.set_credentials(connection_params)
        
        # Create and initialize database connection
        db = SnowflakeDB(connection_params)
        db.start_init_connection()
        
        return {
            'success': True,
            'message': f'Connected to Snowflake account "{account}" as user "{user}"'
        }
    else:
        return {
            'success': False,
            'error': 'Saved credentials are no longer valid. Please authenticate again.'
        }

@mcp.tool()
async def list_saved_credentials() -> Dict[str, Any]:
    """List all saved Snowflake credentials."""
    saved = auth_client.storage.list_saved_credentials()
    
    if not saved:
        return {
            'success': True,
            'message': 'No saved credentials found.',
            'credentials': {}
        }
    
    return {
        'success': True,
        'credentials': saved
    }

@mcp.tool()
async def delete_saved_credentials(
    account: Optional[str] = None,
    user: Optional[str] = None
) -> Dict[str, Any]:
    """
    Delete saved Snowflake credentials.
    
    Args:
        account: Account to delete (optional, deletes all if not specified)
        user: User to delete (optional)
    """
    auth_client.storage.delete_credentials(account, user)
    
    if not account and not user:
        message = 'All saved credentials have been deleted.'
    elif account and user:
        message = f'Deleted credentials for account "{account}" and user "{user}".'
    elif account:
        message = f'Deleted all credentials for account "{account}".'
    else:
        message = 'Invalid parameters for credential deletion.'
    
    return {
        'success': True,
        'message': message
    }

# Database tools
@mcp.tool()
async def list_databases() -> Dict[str, Any]:
    """List all available databases in Snowflake."""
    if not db:
        return {
            'success': False,
            'error': 'Not authenticated. Please use authenticate_snowflake first.'
        }
    
    try:
        query = "SELECT DATABASE_NAME FROM INFORMATION_SCHEMA.DATABASES ORDER BY DATABASE_NAME"
        data, data_id = await db.execute_query(query)
        
        return {
            'success': True,
            'databases': [row['DATABASE_NAME'] for row in data],
            'count': len(data)
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }

@mcp.tool()
async def list_schemas(database: str) -> Dict[str, Any]:
    """
    List all schemas in a database.
    
    Args:
        database: Database name
    """
    if not db:
        return {
            'success': False,
            'error': 'Not authenticated. Please use authenticate_snowflake first.'
        }
    
    try:
        query = f"SELECT SCHEMA_NAME FROM {database.upper()}.INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME"
        data, data_id = await db.execute_query(query)
        
        return {
            'success': True,
            'database': database,
            'schemas': [row['SCHEMA_NAME'] for row in data],
            'count': len(data)
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }

@mcp.tool()
async def list_tables(database: str, schema: str) -> Dict[str, Any]:
    """
    List all tables in a specific database and schema.
    
    Args:
        database: Database name
        schema: Schema name
    """
    if not db:
        return {
            'success': False,
            'error': 'Not authenticated. Please use authenticate_snowflake first.'
        }
    
    try:
        query = f"""
            SELECT TABLE_NAME, TABLE_TYPE, ROW_COUNT, BYTES, COMMENT 
            FROM {database.upper()}.INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{schema.upper()}'
            ORDER BY TABLE_NAME
        """
        data, data_id = await db.execute_query(query)
        
        tables = []
        for row in data:
            tables.append({
                'name': row['TABLE_NAME'],
                'type': row['TABLE_TYPE'],
                'row_count': row.get('ROW_COUNT'),
                'bytes': row.get('BYTES'),
                'comment': row.get('COMMENT', '')
            })
        
        return {
            'success': True,
            'database': database,
            'schema': schema,
            'tables': tables,
            'count': len(tables)
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }

@mcp.tool()
async def describe_table(table_name: str) -> Dict[str, Any]:
    """
    Get the schema information for a specific table.
    
    Args:
        table_name: Fully qualified table name (database.schema.table)
    """
    if not db:
        return {
            'success': False,
            'error': 'Not authenticated. Please use authenticate_snowflake first.'
        }
    
    try:
        parts = table_name.split('.')
        if len(parts) != 3:
            return {
                'success': False,
                'error': 'Table name must be fully qualified as database.schema.table'
            }
        
        database, schema, table = parts
        
        query = f"""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COMMENT
            FROM {database.upper()}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema.upper()}'
            AND TABLE_NAME = '{table.upper()}'
            ORDER BY ORDINAL_POSITION
        """
        data, data_id = await db.execute_query(query)
        
        columns = []
        for row in data:
            columns.append({
                'name': row['COLUMN_NAME'],
                'type': row['DATA_TYPE'],
                'nullable': row['IS_NULLABLE'] == 'YES',
                'default': row.get('COLUMN_DEFAULT'),
                'comment': row.get('COMMENT', '')
            })
        
        return {
            'success': True,
            'table': table_name,
            'columns': columns,
            'column_count': len(columns)
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }

@mcp.tool()
async def read_query(query: str) -> Dict[str, Any]:
    """
    Execute a SELECT query on Snowflake.
    
    Args:
        query: SELECT SQL query to execute
    """
    if not db:
        return {
            'success': False,
            'error': 'Not authenticated. Please use authenticate_snowflake first.'
        }
    
    # Check if it's a read query
    if not write_detector.is_read_query(query):
        return {
            'success': False,
            'error': 'Only SELECT queries are allowed. Use write_query for INSERT/UPDATE/DELETE operations.'
        }
    
    try:
        data, data_id = await db.execute_query(query)
        
        return {
            'success': True,
            'data': data,
            'row_count': len(data),
            'data_id': data_id
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }

@mcp.tool()
async def write_query(query: str) -> Dict[str, Any]:
    """
    Execute an INSERT, UPDATE, or DELETE query on Snowflake.
    
    Args:
        query: SQL query to execute
    """
    if not config.get("allow_write", False):
        return {
            'success': False,
            'error': 'Write operations are not allowed. Start the server with allow_write=true to enable.'
        }
    
    if not db:
        return {
            'success': False,
            'error': 'Not authenticated. Please use authenticate_snowflake first.'
        }
    
    # Check if it's a write query
    if not write_detector.is_write_query(query):
        return {
            'success': False,
            'error': 'Only INSERT, UPDATE, or DELETE queries are allowed here. Use read_query for SELECT operations.'
        }
    
    try:
        data, data_id = await db.execute_query(query)
        
        return {
            'success': True,
            'message': 'Query executed successfully',
            'data_id': data_id
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }

# Data insights resource
@mcp.resource("memo://insights")
async def get_insights() -> str:
    """Get collected data insights"""
    if not db:
        return "Not authenticated. Please use authenticate_snowflake first."
    return db.get_memo()

@mcp.tool()
async def append_insight(insight: str) -> Dict[str, Any]:
    """
    Add a data insight to the memo.
    
    Args:
        insight: Data insight discovered from analysis
    """
    if not db:
        return {
            'success': False,
            'error': 'Not authenticated. Please use authenticate_snowflake first.'
        }
    
    db.add_insight(insight)
    
    return {
        'success': True,
        'message': 'Insight added to memo'
    }

# Run the server if executed directly
if __name__ == "__main__":
    # Check for command line arguments
    import argparse
    
    parser = argparse.ArgumentParser(description='Snowflake MCP Server')
    parser.add_argument('--allow-write', action='store_true', help='Allow write operations')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    
    args = parser.parse_args()
    
    # Override config with command line arguments
    if args.allow_write:
        config['allow_write'] = True
    if args.debug:
        config['debug'] = True
    
    debug_print(f"Starting Snowflake MCP Server (allow_write={config['allow_write']})")
    mcp.run()