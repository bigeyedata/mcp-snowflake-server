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
    # Note: We'll initialize the connection on first use since we can't await here
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
    await db.start_init_connection()
    
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
        await db.start_init_connection()
        
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
    
    # Check for placeholder values
    if '<' in database or '>' in database or 'placeholder' in database.lower():
        return {
            'success': False,
            'error': f'Database name contains placeholder value: "{database}". Please use an actual database name from list_databases.',
            'hint': 'First use list_databases to get available databases, then use one of those names.'
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
    
    # Check for placeholder values
    if '<' in database or '>' in database or 'placeholder' in database.lower():
        return {
            'success': False,
            'error': f'Database name contains placeholder value: "{database}". Please use an actual database name from list_databases.',
            'hint': 'First use list_databases to get available databases, then use one of those names.'
        }
    
    if '<' in schema or '>' in schema or 'placeholder' in schema.lower():
        return {
            'success': False,
            'error': f'Schema name contains placeholder value: "{schema}". Please use an actual schema name from list_schemas.',
            'hint': f'First use list_schemas with database "{database}" to get available schemas, then use one of those names.'
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
    
    # Check for placeholder values
    if '<' in table_name or '>' in table_name or 'placeholder' in table_name.lower():
        return {
            'success': False,
            'error': f'Table name contains placeholder value: "{table_name}". Please use an actual table name from list_tables or search_tables.',
            'hint': 'First use list_tables or search_tables to find actual table names, then use the fully qualified name (database.schema.table).'
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
    
    # Check for placeholder values
    placeholders = ["<database>", "<schema>", "<table", "placeholder", "PLACEHOLDER", 
                   "<orders_table>", "<customers_table>", "<sales_table>"]
    if any(placeholder in query for placeholder in placeholders):
        return {
            'success': False,
            'error': 'SQL query contains placeholder values. Please use actual table names discovered from search_tables or list_tables tools.',
            'query': query,
            'hint': 'First use search_tables to find tables matching your needs, then use the actual table names returned.'
        }
    
    # Check if it's a read query
    if write_detector.analyze_query(query)['contains_write']:
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
    
    # Check for placeholder values
    placeholders = ["<database>", "<schema>", "<table", "placeholder", "PLACEHOLDER", 
                   "<orders_table>", "<customers_table>", "<sales_table>"]
    if any(placeholder in query for placeholder in placeholders):
        return {
            'success': False,
            'error': 'SQL query contains placeholder values. Please use actual table names discovered from search_tables or list_tables tools.',
            'query': query,
            'hint': 'First use search_tables to find tables matching your needs, then use the actual table names returned.'
        }
    
    # Check if it's a write query
    if not write_detector.analyze_query(query)['contains_write']:
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

@mcp.tool()
async def profile_table(table_name: str) -> Dict[str, Any]:
    """
    Get statistical profile of a table including row count, column statistics, and sample values.
    
    Args:
        table_name: Fully qualified table name (database.schema.table)
    """
    if not db:
        return {
            'success': False,
            'error': 'Not authenticated. Please use authenticate_snowflake first.'
        }
    
    # Check for placeholder values
    if '<' in table_name or '>' in table_name or 'placeholder' in table_name.lower():
        return {
            'success': False,
            'error': f'Table name contains placeholder value: "{table_name}". Please use an actual table name from list_tables or search_tables.',
            'hint': 'First use list_tables or search_tables to find actual table names, then use the fully qualified name (database.schema.table).'
        }
    
    try:
        # Basic table info query
        row_count_query = f"SELECT COUNT(*) as row_count FROM {table_name}"
        row_count_result, _ = await db.execute_query(row_count_query)
        row_count = row_count_result[0]["ROW_COUNT"] if row_count_result else 0
        
            
        # Get column information with statistics
        # Parse the table name to get database, schema, and table parts
        parts = table_name.split('.')
        if len(parts) != 3:
            raise ValueError(f"Table name must be fully qualified as database.schema.table, got: {table_name}")
        
        db_name, schema_name, table = parts
        
        profile_query = f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            COMMENT
        FROM {db_name.upper()}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table.upper()}'
        AND TABLE_SCHEMA = '{schema_name.upper()}'
        AND TABLE_CATALOG = '{db_name.upper()}'
        """
        
        columns_info, _ = await db.execute_query(profile_query)
        
        # Get statistics for each column
        column_stats = []
        for col in columns_info:
            col_name = col["COLUMN_NAME"]
            stats = {
                "column_name": col_name,
                "data_type": col["DATA_TYPE"],
                "nullable": col["IS_NULLABLE"] == "YES",
                "default": col["COLUMN_DEFAULT"],
                "comment": col["COMMENT"]
            }
            
            # Get additional statistics based on data type
            if "NUMBER" in col["DATA_TYPE"] or "INT" in col["DATA_TYPE"] or "FLOAT" in col["DATA_TYPE"]:
                # Numeric column statistics
                stats_query = f"""
                SELECT 
                    MIN({col_name}) as min_value,
                    MAX({col_name}) as max_value,
                    AVG({col_name}) as avg_value,
                    MEDIAN({col_name}) as median_value,
                    COUNT(DISTINCT {col_name}) as distinct_count,
                    COUNT(CASE WHEN {col_name} IS NULL THEN 1 END) as null_count
                FROM {table_name}
                """
                stats_result, _ = await db.execute_query(stats_query)
                if stats_result:
                    stats.update({
                        "min": stats_result[0]["MIN_VALUE"],
                        "max": stats_result[0]["MAX_VALUE"],
                        "avg": stats_result[0]["AVG_VALUE"],
                        "median": stats_result[0]["MEDIAN_VALUE"],
                        "distinct_count": stats_result[0]["DISTINCT_COUNT"],
                        "null_count": stats_result[0]["NULL_COUNT"],
                        "null_percentage": (stats_result[0]["NULL_COUNT"] / row_count * 100) if row_count > 0 else 0
                    })
            else:
                # String/other column statistics
                stats_query = f"""
                SELECT 
                    COUNT(DISTINCT {col_name}) as distinct_count,
                    COUNT(CASE WHEN {col_name} IS NULL THEN 1 END) as null_count,
                    MIN(LENGTH({col_name})) as min_length,
                    MAX(LENGTH({col_name})) as max_length,
                    AVG(LENGTH({col_name})) as avg_length
                FROM {table_name}
                """
                stats_result, _ = await db.execute_query(stats_query)
                if stats_result:
                    stats.update({
                        "distinct_count": stats_result[0]["DISTINCT_COUNT"],
                        "null_count": stats_result[0]["NULL_COUNT"],
                        "null_percentage": (stats_result[0]["NULL_COUNT"] / row_count * 100) if row_count > 0 else 0,
                        "min_length": stats_result[0]["MIN_LENGTH"],
                        "max_length": stats_result[0]["MAX_LENGTH"],
                        "avg_length": stats_result[0]["AVG_LENGTH"]
                    })
            
            column_stats.append(stats)
        
        
        return {
            'success': True,
            'table_name': table_name,
            'row_count': row_count,
            'column_count': len(columns_info),
            'columns': column_stats
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }

@mcp.tool()
async def get_sample_data(
    table_name: str,
    sample_size: int = 10,
    sample_method: str = "top",
    columns: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Get sample data from a table with various sampling options.
    
    Args:
        table_name: Fully qualified table name (database.schema.table)
        sample_size: Number of rows to sample (default: 10)
        sample_method: Sampling method: 'top', 'random', or 'bottom' (default: 'top')
        columns: Optional list of columns to include
    """
    if not db:
        return {
            'success': False,
            'error': 'Not authenticated. Please use authenticate_snowflake first.'
        }
    
    # Check for placeholder values
    if '<' in table_name or '>' in table_name or 'placeholder' in table_name.lower():
        return {
            'success': False,
            'error': f'Table name contains placeholder value: "{table_name}". Please use an actual table name from list_tables or search_tables.',
            'hint': 'First use list_tables or search_tables to find actual table names, then use the fully qualified name (database.schema.table).'
        }
    
    try:
            
        # Build column list
        column_list = ", ".join(columns) if columns else "*"
        
        # Build query based on sample method
        if sample_method == "random":
            query = f"""
            SELECT {column_list}
            FROM {table_name}
            SAMPLE ({sample_size} ROWS)
            """
        elif sample_method == "bottom":
            query = f"""
            SELECT {column_list}
            FROM (
                SELECT *
                FROM {table_name}
                ORDER BY 1 DESC
                LIMIT {sample_size}
            )
            ORDER BY 1
            """
        else:  # default to "top"
            query = f"""
            SELECT {column_list}
            FROM {table_name}
            LIMIT {sample_size}
            """
        
        # Execute query
        sample_data, data_id = await db.execute_query(query)
        
        # Also get total row count for context
        count_query = f"SELECT COUNT(*) as total_rows FROM {table_name}"
        count_result, _ = await db.execute_query(count_query)
        total_rows = count_result[0]["TOTAL_ROWS"] if count_result else 0
        
        
        return {
            'success': True,
            'table_name': table_name,
            'total_rows': total_rows,
            'sample_size': len(sample_data),
            'sample_method': sample_method,
            'columns': columns if columns else "all",
            'data': sample_data,
            'data_id': data_id
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }

@mcp.tool()
async def search_tables(
    search_pattern: str,
    search_type: str = "table_name",
    database: Optional[str] = None,
    schema: Optional[str] = None
) -> Dict[str, Any]:
    """
    Search for tables by name pattern, column name, or comment.
    
    Args:
        search_pattern: Pattern to search for (supports % wildcards)
        search_type: Type of search: 'table_name', 'column_name', or 'comment'
        database: Optional database name to limit search
        schema: Optional schema name to limit search
    """
    if not db:
        return {
            'success': False,
            'error': 'Not authenticated. Please use authenticate_snowflake first.'
        }
    
    try:
        # Build the base query based on search type
        if search_type == "column_name":
            # Search for tables containing a specific column
            query = f"""
            SELECT DISTINCT 
                c.TABLE_CATALOG as DATABASE_NAME,
                c.TABLE_SCHEMA as SCHEMA_NAME,
                c.TABLE_NAME,
                t.COMMENT as TABLE_COMMENT,
                t.ROW_COUNT,
                t.BYTES,
                ARRAY_AGG(c.COLUMN_NAME) as MATCHING_COLUMNS
            FROM INFORMATION_SCHEMA.COLUMNS c
            JOIN INFORMATION_SCHEMA.TABLES t 
                ON c.TABLE_CATALOG = t.TABLE_CATALOG 
                AND c.TABLE_SCHEMA = t.TABLE_SCHEMA 
                AND c.TABLE_NAME = t.TABLE_NAME
            WHERE UPPER(c.COLUMN_NAME) LIKE UPPER('%{search_pattern}%')
            """
        elif search_type == "comment":
            # Search in table comments
            query = f"""
            SELECT 
                TABLE_CATALOG as DATABASE_NAME,
                TABLE_SCHEMA as SCHEMA_NAME,
                TABLE_NAME,
                COMMENT as TABLE_COMMENT,
                ROW_COUNT,
                BYTES
            FROM INFORMATION_SCHEMA.TABLES
            WHERE UPPER(COMMENT) LIKE UPPER('%{search_pattern}%')
            """
        else:  # default to table_name search
            query = f"""
            SELECT 
                TABLE_CATALOG as DATABASE_NAME,
                TABLE_SCHEMA as SCHEMA_NAME,
                TABLE_NAME,
                COMMENT as TABLE_COMMENT,
                ROW_COUNT,
                BYTES
            FROM INFORMATION_SCHEMA.TABLES
            WHERE UPPER(TABLE_NAME) LIKE UPPER('%{search_pattern}%')
            """
        
        # Add database filter if provided
        if database:
            query += f"\nAND TABLE_CATALOG = '{database}'"
        
        # Add schema filter if provided
        if schema:
            query += f"\nAND TABLE_SCHEMA = '{schema}'"
        
        # Add grouping for column search
        if search_type == "column_name":
            query += "\nGROUP BY c.TABLE_CATALOG, c.TABLE_SCHEMA, c.TABLE_NAME, t.COMMENT, t.ROW_COUNT, t.BYTES"
        
        # Add ordering
        query += "\nORDER BY DATABASE_NAME, SCHEMA_NAME, TABLE_NAME"
        
        # Execute the search
        results, _ = await db.execute_query(query)
        
        # Format results
        formatted_results = []
        for row in results:
            result = {
                "database": row["DATABASE_NAME"],
                "schema": row["SCHEMA_NAME"],
                "table": row["TABLE_NAME"],
                "full_name": f"{row['DATABASE_NAME']}.{row['SCHEMA_NAME']}.{row['TABLE_NAME']}",
                "comment": row.get("TABLE_COMMENT", ""),
                "row_count": row.get("ROW_COUNT", 0),
                "size_bytes": row.get("BYTES", 0)
            }
            
            # Add matching columns for column search
            if search_type == "column_name" and "MATCHING_COLUMNS" in row:
                result["matching_columns"] = row["MATCHING_COLUMNS"]
            
            formatted_results.append(result)
        
        return {
            'success': True,
            'search_pattern': search_pattern,
            'search_type': search_type,
            'database_filter': database,
            'schema_filter': schema,
            'results_count': len(formatted_results),
            'results': formatted_results
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }

@mcp.tool()
async def get_table_relationships(table_name: str) -> Dict[str, Any]:
    """
    Get foreign key relationships and primary keys for a table.
    
    Args:
        table_name: Fully qualified table name (database.schema.table)
    """
    if not db:
        return {
            'success': False,
            'error': 'Not authenticated. Please use authenticate_snowflake first.'
        }
    
    # Check for placeholder values
    if '<' in table_name or '>' in table_name or 'placeholder' in table_name.lower():
        return {
            'success': False,
            'error': f'Table name contains placeholder value: "{table_name}". Please use an actual table name from list_tables or search_tables.',
            'hint': 'First use list_tables or search_tables to find actual table names, then use the fully qualified name (database.schema.table).'
        }
    
    try:
        # Parse table name
        parts = table_name.split(".")
        if len(parts) == 3:
            db_name, schema_name, table = parts
        else:
            return {
                'success': False,
                'error': 'Table name must be in format "database.schema.table"'
            }
        
        # Query for foreign key constraints
        fk_query = f"""
        SELECT 
            fk.CONSTRAINT_NAME,
            fk.TABLE_CATALOG as FK_DATABASE,
            fk.TABLE_SCHEMA as FK_SCHEMA,
            fk.TABLE_NAME as FK_TABLE,
            fk.COLUMN_NAME as FK_COLUMN,
            pk.TABLE_CATALOG as PK_DATABASE,
            pk.TABLE_SCHEMA as PK_SCHEMA,
            pk.TABLE_NAME as PK_TABLE,
            pk.COLUMN_NAME as PK_COLUMN
        FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
        JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            ON rc.CONSTRAINT_CATALOG = tc.CONSTRAINT_CATALOG
            AND rc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
            AND rc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE fk
            ON rc.CONSTRAINT_CATALOG = fk.CONSTRAINT_CATALOG
            AND rc.CONSTRAINT_SCHEMA = fk.CONSTRAINT_SCHEMA
            AND rc.CONSTRAINT_NAME = fk.CONSTRAINT_NAME
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk
            ON rc.UNIQUE_CONSTRAINT_CATALOG = pk.CONSTRAINT_CATALOG
            AND rc.UNIQUE_CONSTRAINT_SCHEMA = pk.CONSTRAINT_SCHEMA
            AND rc.UNIQUE_CONSTRAINT_NAME = pk.CONSTRAINT_NAME
        WHERE (fk.TABLE_CATALOG = '{db_name}' AND fk.TABLE_SCHEMA = '{schema_name}' AND fk.TABLE_NAME = '{table}')
           OR (pk.TABLE_CATALOG = '{db_name}' AND pk.TABLE_SCHEMA = '{schema_name}' AND pk.TABLE_NAME = '{table}')
        ORDER BY fk.CONSTRAINT_NAME, fk.ORDINAL_POSITION
        """
        
        fk_results, _ = await db.execute_query(fk_query)
        
        # Also look for primary key constraints
        pk_query = f"""
        SELECT 
            CONSTRAINT_NAME,
            COLUMN_NAME,
            ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_CATALOG = '{db_name}'
            AND TABLE_SCHEMA = '{schema_name}'
            AND TABLE_NAME = '{table}'
            AND CONSTRAINT_NAME IN (
                SELECT CONSTRAINT_NAME 
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
                WHERE TABLE_CATALOG = '{db_name}'
                    AND TABLE_SCHEMA = '{schema_name}'
                    AND TABLE_NAME = '{table}'
                    AND CONSTRAINT_TYPE = 'PRIMARY KEY'
            )
        ORDER BY ORDINAL_POSITION
        """
        
        pk_results, _ = await db.execute_query(pk_query)
        
        # Process foreign key relationships
        outgoing_fks = []  # This table references other tables
        incoming_fks = []  # Other tables reference this table
        
        for row in fk_results:
            if (row["FK_DATABASE"] == db_name and 
                row["FK_SCHEMA"] == schema_name and 
                row["FK_TABLE"] == table):
                # Outgoing FK (this table references another)
                outgoing_fks.append({
                    "constraint_name": row["CONSTRAINT_NAME"],
                    "from_column": row["FK_COLUMN"],
                    "to_table": f"{row['PK_DATABASE']}.{row['PK_SCHEMA']}.{row['PK_TABLE']}",
                    "to_column": row["PK_COLUMN"]
                })
            else:
                # Incoming FK (another table references this)
                incoming_fks.append({
                    "constraint_name": row["CONSTRAINT_NAME"],
                    "from_table": f"{row['FK_DATABASE']}.{row['FK_SCHEMA']}.{row['FK_TABLE']}",
                    "from_column": row["FK_COLUMN"],
                    "to_column": row["PK_COLUMN"]
                })
        
        # Process primary keys
        primary_keys = [row["COLUMN_NAME"] for row in pk_results]
        
        return {
            'success': True,
            'table_name': table_name,
            'primary_keys': primary_keys,
            'foreign_keys': {
                'outgoing': outgoing_fks,
                'incoming': incoming_fks
            },
            'relationship_summary': {
                'references_tables': len(set(fk["to_table"] for fk in outgoing_fks)),
                'referenced_by_tables': len(set(fk["from_table"] for fk in incoming_fks)),
                'total_relationships': len(outgoing_fks) + len(incoming_fks)
            }
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }


@mcp.tool()
async def cortex_analyst(
    question: str,
    context_tables: Optional[List[str]] = None,
    model: str = "mistral-large2",
    execute_sql: bool = True,
    temperature: float = 0.0,
    max_tokens: int = 4096
) -> Dict[str, Any]:
    """
    Use Snowflake Cortex LLM to analyze data using natural language queries.
    
    Args:
        question: The natural language question to ask about your data
        context_tables: List of table names to provide as context (e.g., ['DB.SCHEMA.ORDERS'])
        model: The LLM model to use (default: 'mistral-large2')
        execute_sql: Whether to execute the generated SQL query and return results
        temperature: Temperature for response generation (0.0-1.0, default: 0.0)
        max_tokens: Maximum tokens for the response (default: 4096)
    """
    if not db:
        return {
            'success': False,
            'error': 'Not authenticated. Please use authenticate_snowflake first.'
        }
    
    try:
        # Build context from specified tables
        context = ""
        if context_tables:
            context = "Database context:\n"
            for table_name in context_tables[:5]:  # Limit to 5 tables
                try:
                    # Get table schema
                    desc_query = f"DESCRIBE TABLE {table_name}"
                    columns, _ = await db.execute_query(desc_query)
                    
                    context += f"\nTable: {table_name}\nColumns:\n"
                    for col in columns[:20]:  # Limit columns
                        context += f"  - {col['name']}: {col['type']}"
                        if col.get('comment'):
                            context += f" ({col['comment']})"
                        context += "\n"
                    
                    # Get sample data
                    sample_query = f"SELECT * FROM {table_name} LIMIT 3"
                    sample_data, _ = await db.execute_query(sample_query)
                    if sample_data:
                        context += f"Sample data: {json.dumps(sample_data[:3], indent=2)}\n"
                        
                except Exception as e:
                    debug_print(f"Failed to get context for table {table_name}: {str(e)}")
        
        # Construct the prompt
        prompt = f"""You are a SQL expert analyzing a Snowflake database. 
{context}

User question: {question}

Please provide:
1. A clear answer to the question
2. The SQL query that would answer this question
3. Any relevant insights or recommendations

Format your response as JSON with keys: "answer", "sql_query", "insights"
"""
        
        # Use CORTEX.COMPLETE function with structured output
        cortex_query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            '{model}',
            '{prompt.replace("'", "''")}',
            {{
                'temperature': {temperature},
                'max_tokens': {max_tokens},
                'response_format': {{
                    'type': 'json_object',
                    'schema': {{
                        'type': 'object',
                        'properties': {{
                            'answer': {{'type': 'string'}},
                            'sql_query': {{'type': 'string'}},
                            'insights': {{'type': 'array', 'items': {{'type': 'string'}}}}
                        }},
                        'required': ['answer', 'sql_query']
                    }}
                }}
            }}
        ) as response;
        """
        
        result, _ = await db.execute_query(cortex_query)
        
        if result and len(result) > 0:
            response = json.loads(result[0]["RESPONSE"])
            
            # Format the output
            output = {
                'success': True,
                'question': question,
                'model': model,
                'answer': response.get("answer", "No answer generated"),
                'sql_query': response.get("sql_query"),
                'insights': response.get("insights", []),
                'query_results': None
            }
            
            # Execute the generated SQL if requested
            if execute_sql and output["sql_query"]:
                try:
                    # Clean the SQL query
                    sql_query = output["sql_query"].strip()
                    if sql_query and not sql_query.endswith(';'):
                        sql_query += ';'
                    
                    query_results, _ = await db.execute_query(sql_query)
                    output["query_results"] = query_results
                    
                except Exception as e:
                    output["query_error"] = str(e)
            
            
            return output
        else:
            raise ValueError("No response received from Cortex")
            
    except Exception as e:
        debug_print(f"Cortex query failed: {str(e)}")
        
        # Fallback to simpler approach without structured output
        try:
            simple_query = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                '{model}',
                'User question: {question.replace("'", "''")}\\n\\nProvide a SQL query to answer this question and explain your approach.'
            ) as response;
            """
            
            result, _ = await db.execute_query(simple_query)
            if result and len(result) > 0:
                return {
                    'success': True,
                    'question': question,
                    'model': model,
                    'response': result[0]["RESPONSE"],
                    'note': 'Using simplified Cortex response without structured output'
                }
        except Exception as fallback_error:
            debug_print(f"Fallback query also failed: {str(fallback_error)}")
        
        return {
            'success': False,
            'error': 'Cortex query failed',
            'details': str(e),
            'suggestion': 'Ensure you have the SNOWFLAKE.CORTEX_USER role and Cortex is enabled for your account'
        }

@mcp.tool()
async def get_data_summary(
    database: Optional[str] = None,
    include_schemas: bool = True,
    include_largest_tables: bool = True,
    include_recent_tables: bool = True
) -> Dict[str, Any]:
    """
    Get a summary of the data warehouse including database statistics, largest tables, and recent changes.
    
    Args:
        database: Optional database name to filter results
        include_schemas: Include schema statistics (default: true)
        include_largest_tables: Include list of largest tables (default: true)
        include_recent_tables: Include recently created/modified tables (default: true)
    """
    if not db:
        return {
            'success': False,
            'error': 'Not authenticated. Please use authenticate_snowflake first.'
        }
    
    try:
        results = {}
        
        # Get database statistics
        db_query = "SELECT COUNT(DISTINCT TABLE_CATALOG) as database_count FROM INFORMATION_SCHEMA.TABLES"
        if database:
            db_query += f" WHERE TABLE_CATALOG = '{database}'"
        
        db_result, _ = await db.execute_query(db_query)
        results["database_count"] = db_result[0]["DATABASE_COUNT"] if db_result else 0
        
        # Get schema statistics
        if include_schemas:
            schema_query = """
            SELECT 
                TABLE_CATALOG as DATABASE_NAME,
                COUNT(DISTINCT TABLE_SCHEMA) as SCHEMA_COUNT,
                COUNT(DISTINCT TABLE_NAME) as TABLE_COUNT,
                SUM(ROW_COUNT) as TOTAL_ROWS,
                SUM(BYTES) as TOTAL_BYTES
            FROM INFORMATION_SCHEMA.TABLES
            """
            if database:
                schema_query += f" WHERE TABLE_CATALOG = '{database}'"
            schema_query += " GROUP BY TABLE_CATALOG ORDER BY TABLE_CATALOG"
            
            schema_results, _ = await db.execute_query(schema_query)
            results["databases"] = [
                {
                    "database": row["DATABASE_NAME"],
                    "schema_count": row["SCHEMA_COUNT"],
                    "table_count": row["TABLE_COUNT"],
                    "total_rows": row["TOTAL_ROWS"] or 0,
                    "total_bytes": row["TOTAL_BYTES"] or 0,
                    "total_gb": round((row["TOTAL_BYTES"] or 0) / (1024**3), 2)
                }
                for row in schema_results
            ]
        
        # Get largest tables
        if include_largest_tables:
            largest_query = """
            SELECT 
                TABLE_CATALOG as DATABASE_NAME,
                TABLE_SCHEMA as SCHEMA_NAME,
                TABLE_NAME,
                ROW_COUNT,
                BYTES,
                ROUND(BYTES / (1024*1024*1024), 2) as SIZE_GB
            FROM INFORMATION_SCHEMA.TABLES
            WHERE ROW_COUNT > 0
            """
            if database:
                largest_query += f" AND TABLE_CATALOG = '{database}'"
            largest_query += " ORDER BY BYTES DESC NULLS LAST LIMIT 10"
            
            largest_results, _ = await db.execute_query(largest_query)
            results["largest_tables"] = [
                {
                    "full_name": f"{row['DATABASE_NAME']}.{row['SCHEMA_NAME']}.{row['TABLE_NAME']}",
                    "row_count": row["ROW_COUNT"],
                    "size_bytes": row["BYTES"],
                    "size_gb": row["SIZE_GB"]
                }
                for row in largest_results
            ]
        
        # Get recently created/modified tables
        if include_recent_tables:
            recent_query = """
            SELECT 
                TABLE_CATALOG as DATABASE_NAME,
                TABLE_SCHEMA as SCHEMA_NAME,
                TABLE_NAME,
                CREATED,
                LAST_ALTERED,
                ROW_COUNT
            FROM INFORMATION_SCHEMA.TABLES
            WHERE CREATED IS NOT NULL
            """
            if database:
                recent_query += f" AND TABLE_CATALOG = '{database}'"
            recent_query += " ORDER BY GREATEST(CREATED, LAST_ALTERED) DESC LIMIT 10"
            
            recent_results, _ = await db.execute_query(recent_query)
            results["recent_tables"] = [
                {
                    "full_name": f"{row['DATABASE_NAME']}.{row['SCHEMA_NAME']}.{row['TABLE_NAME']}",
                    "created": str(row["CREATED"]) if row["CREATED"] else None,
                    "last_altered": str(row["LAST_ALTERED"]) if row["LAST_ALTERED"] else None,
                    "row_count": row["ROW_COUNT"] or 0
                }
                for row in recent_results
            ]
        
        # Calculate summary statistics
        if "databases" in results:
            total_tables = sum(db_info["table_count"] for db_info in results["databases"])
            total_rows = sum(db_info["total_rows"] for db_info in results["databases"])
            total_gb = sum(db_info["total_gb"] for db_info in results["databases"])
            
            results["summary"] = {
                "total_databases": len(results["databases"]),
                "total_tables": total_tables,
                "total_rows": total_rows,
                "total_size_gb": round(total_gb, 2)
            }
        
        return {
            'success': True,
            **results
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }


# Run the server if executed directly
if __name__ == "__main__":
    # Check for command line arguments
    import argparse
    import asyncio
    import signal
    
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