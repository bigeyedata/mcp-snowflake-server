#!/usr/bin/env python3
"""
WebSocket server for Snowflake MCP.
This allows the MCP server to be accessed over WebSocket instead of stdio.
"""
import os
import sys
import asyncio
import json
import logging
from typing import Optional, Dict, Any
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

# Add parent directories to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(title="Snowflake MCP WebSocket Server")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class MCPWebSocketHandler:
    """Handler for MCP over WebSocket."""
    
    def __init__(self):
        self.db = None
        self.server = None
        self.auth_client = None
        self.write_detector = None
        self.allowed_tools = []
        self._initialized = False
        
    async def initialize_server(self):
        """Initialize the MCP server components."""
        if self._initialized:
            return
            
        try:
            # Import required modules
            from mcp_snowflake_server.server import (
                SnowflakeDB, 
                SnowflakeAuthClient,
                SQLWriteDetector,
                handle_list_databases,
                handle_list_schemas,
                handle_list_tables,
                handle_describe_table,
                handle_read_query,
                handle_authenticate_snowflake,
                handle_use_saved_credentials,
                handle_list_saved_credentials,
                handle_delete_saved_credentials
            )
            from mcp.server import Server
            
            # Initialize components
            self.auth_client = SnowflakeAuthClient()
            self.write_detector = SQLWriteDetector()
            self.server = Server("snowflake-manager")
            
            # Try to use environment credentials
            connection_args = {
                'account': os.getenv('SNOWFLAKE_ACCOUNT'),
                'user': os.getenv('SNOWFLAKE_USER'),
                'password': os.getenv('SNOWFLAKE_PASSWORD'),
                'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
                'database': os.getenv('SNOWFLAKE_DATABASE'),
                'schema': os.getenv('SNOWFLAKE_SCHEMA'),
                'role': os.getenv('SNOWFLAKE_ROLE')
            }
            
            # Filter out None values
            connection_args = {k: v for k, v in connection_args.items() if v is not None}
            
            if all(k in connection_args for k in ['account', 'user', 'password']):
                logger.info("Using environment credentials for Snowflake")
                self.auth_client.set_credentials(connection_args)
                self.db = SnowflakeDB(connection_args)
                await self.db.start_init_connection()
            else:
                logger.info("No environment credentials found. Authentication required.")
            
            # Define available tools
            self.tool_handlers = {
                "authenticate_snowflake": handle_authenticate_snowflake,
                "use_saved_credentials": handle_use_saved_credentials,
                "list_saved_credentials": handle_list_saved_credentials,
                "delete_saved_credentials": handle_delete_saved_credentials,
                "list_databases": handle_list_databases,
                "list_schemas": handle_list_schemas,
                "list_tables": handle_list_tables,
                "describe_table": handle_describe_table,
                "read_query": handle_read_query
            }
            
            self._initialized = True
            logger.info("MCP server components initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize MCP server: {e}", exc_info=True)
            raise
            
    def set_db(self, new_db):
        """Update the database connection."""
        self.db = new_db
            
    async def handle_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Handle a JSON-RPC request."""
        method = request.get("method")
        params = request.get("params", {})
        request_id = request.get("id")
        
        try:
            # Initialize server if needed
            if not self._initialized:
                await self.initialize_server()
            
            # Handle different methods
            if method == "initialize":
                # MCP initialization
                result = {
                    "protocolVersion": "0.1.0",
                    "capabilities": {
                        "tools": {},
                        "prompts": {},
                        "resources": {}
                    },
                    "serverInfo": {
                        "name": "snowflake-mcp",
                        "version": "0.1.0"
                    }
                }
                
            elif method == "tools/list":
                # List available tools
                tools = []
                for name in self.tool_handlers.keys():
                    tool_info = {
                        "name": name,
                        "description": self._get_tool_description(name),
                        "inputSchema": self._get_tool_schema(name)
                    }
                    tools.append(tool_info)
                result = {"tools": tools}
                
            elif method == "tools/call":
                # Call a tool
                tool_name = params.get("name")
                arguments = params.get("arguments", {})
                
                handler = self.tool_handlers.get(tool_name)
                if not handler:
                    raise ValueError(f"Unknown tool: {tool_name}")
                
                # Check if db is None for non-auth tools
                if self.db is None and tool_name not in ["authenticate_snowflake", "use_saved_credentials", "list_saved_credentials", "delete_saved_credentials"]:
                    result = {
                        "content": [{
                            "type": "text",
                            "text": "Not authenticated. Please use 'authenticate_snowflake' tool first."
                        }]
                    }
                else:
                    # Call the tool handler
                    if tool_name in ["authenticate_snowflake", "use_saved_credentials", "list_saved_credentials", "delete_saved_credentials"]:
                        # Authentication tools
                        content = await handler(
                            arguments,
                            self.db,
                            self.write_detector,
                            False,  # allow_write
                            self.server,
                            auth_client=self.auth_client,
                            db_setter=self.set_db
                        )
                    else:
                        # Other tools
                        content = await handler(
                            arguments,
                            self.db,
                            self.write_detector,
                            False,  # allow_write
                            self.server
                        )
                    
                    # Convert content to result format
                    result = {
                        "content": []
                    }
                    for item in content:
                        if hasattr(item, 'type') and item.type == 'text':
                            result["content"].append({
                                "type": "text",
                                "text": item.text
                            })
                        elif hasattr(item, 'type') and item.type == 'resource':
                            # Handle embedded resources
                            result["content"].append({
                                "type": "text",
                                "text": item.resource.text
                            })
                
            else:
                raise ValueError(f"Unknown method: {method}")
                
            # Return success response
            return {
                "jsonrpc": "2.0",
                "result": result,
                "id": request_id
            }
            
        except Exception as e:
            logger.error(f"Error handling request: {e}", exc_info=True)
            return {
                "jsonrpc": "2.0",
                "error": {
                    "code": -32603,
                    "message": str(e)
                },
                "id": request_id
            }
            
    def _get_tool_description(self, tool_name: str) -> str:
        """Get description for a tool."""
        descriptions = {
            "authenticate_snowflake": "Authenticate with Snowflake using connection parameters",
            "use_saved_credentials": "Use previously saved Snowflake credentials",
            "list_saved_credentials": "List all saved Snowflake credentials",
            "delete_saved_credentials": "Delete saved Snowflake credentials",
            "list_databases": "List all available databases in Snowflake",
            "list_schemas": "List all schemas in a database",
            "list_tables": "List all tables in a specific database and schema",
            "describe_table": "Get the schema information for a specific table",
            "read_query": "Execute a SELECT query"
        }
        return descriptions.get(tool_name, f"Tool: {tool_name}")
        
    def _get_tool_schema(self, tool_name: str) -> Dict[str, Any]:
        """Get input schema for a tool."""
        schemas = {
            "authenticate_snowflake": {
                "type": "object",
                "properties": {
                    "account": {"type": "string", "description": "Snowflake account identifier"},
                    "user": {"type": "string", "description": "Snowflake username"},
                    "password": {"type": "string", "description": "Snowflake password"},
                    "warehouse": {"type": "string", "description": "Warehouse to use (optional)"},
                    "database": {"type": "string", "description": "Default database (optional)"},
                    "schema": {"type": "string", "description": "Default schema (optional)"},
                    "role": {"type": "string", "description": "Role to use (optional)"}
                },
                "required": ["account", "user", "password"]
            },
            "use_saved_credentials": {
                "type": "object",
                "properties": {
                    "account": {"type": "string", "description": "Snowflake account identifier"},
                    "user": {"type": "string", "description": "Snowflake username"}
                },
                "required": ["account", "user"]
            },
            "list_saved_credentials": {
                "type": "object",
                "properties": {}
            },
            "delete_saved_credentials": {
                "type": "object",
                "properties": {
                    "account": {"type": "string", "description": "Snowflake account identifier"},
                    "user": {"type": "string", "description": "Snowflake username"}
                }
            },
            "list_databases": {
                "type": "object",
                "properties": {}
            },
            "list_schemas": {
                "type": "object",
                "properties": {
                    "database": {"type": "string", "description": "Database name"}
                },
                "required": ["database"]
            },
            "list_tables": {
                "type": "object",
                "properties": {
                    "database": {"type": "string", "description": "Database name"},
                    "schema": {"type": "string", "description": "Schema name"}
                },
                "required": ["database", "schema"]
            },
            "describe_table": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "Fully qualified table name"}
                },
                "required": ["table_name"]
            },
            "read_query": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "SELECT SQL query to execute"}
                },
                "required": ["query"]
            }
        }
        return schemas.get(tool_name, {"type": "object", "properties": {}})

# Create a global handler instance
handler = MCPWebSocketHandler()

@app.websocket("/mcp")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for MCP communication."""
    await websocket.accept()
    logger.info(f"WebSocket client connected from {websocket.client}")
    
    try:
        while True:
            # Receive message
            try:
                message = await websocket.receive_json()
                logger.debug(f"Received: {message}")
                
                # Handle the request
                response = await handler.handle_request(message)
                
                # Send response
                await websocket.send_json(response)
                logger.debug(f"Sent: {response}")
                
            except WebSocketDisconnect:
                logger.info("WebSocket client disconnected")
                break
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON: {e}")
                await websocket.send_json({
                    "jsonrpc": "2.0",
                    "error": {
                        "code": -32700,
                        "message": "Parse error"
                    },
                    "id": None
                })
                
    except Exception as e:
        logger.error(f"WebSocket error: {e}", exc_info=True)
    finally:
        logger.info("WebSocket connection closed")

@app.get("/")
async def root():
    """Health check endpoint."""
    return {"status": "ok", "service": "Snowflake MCP WebSocket Server"}

async def main():
    """Run the WebSocket server."""
    host = os.getenv("WS_HOST", "0.0.0.0")
    port = int(os.getenv("WS_PORT", "8765"))
    
    logger.info(f"Starting Snowflake MCP WebSocket server on {host}:{port}")
    
    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        log_level="info"
    )
    server = uvicorn.Server(config)
    await server.serve()

if __name__ == "__main__":
    asyncio.run(main())