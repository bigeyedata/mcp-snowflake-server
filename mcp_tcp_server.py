#!/usr/bin/env python3
"""
Simple TCP server for Snowflake MCP using FastMCP.
"""
import os
import sys
import asyncio
import logging

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Now we can import our server
from server import mcp, config, auth_client, db

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def handle_client(reader, writer):
    """Handle a TCP client connection."""
    addr = writer.get_extra_info('peername')
    logger.info(f"Client connected from {addr}")
    
    try:
        # The FastMCP server expects to communicate via stdio
        # We need to create a bridge between TCP and the MCP protocol
        
        # For now, just run the MCP server in stdio mode
        # This is a placeholder - we need to integrate properly
        pass
        
    except Exception as e:
        logger.error(f"Error handling client: {e}")
    finally:
        writer.close()
        await writer.wait_closed()
        logger.info(f"Client {addr} disconnected")


async def main():
    """Run TCP server."""
    port = int(os.getenv("TCP_PORT", "8765"))
    
    # Import and initialize the MCP app
    logger.info(f"Starting Snowflake MCP TCP server on port {port}")
    
    # For now, let's just run the FastMCP server directly
    # It already supports different transports
    from mcp.server.fastmcp import FastMCP
    
    # The mcp object is already created in server.py
    # We need to run it with TCP transport instead of stdio
    
    # FastMCP doesn't directly support TCP, but we can use websockets
    # Let's try a different approach...
    
    # Actually, let's just expose the MCP server over HTTP
    import uvicorn
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware
    
    app = FastAPI()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Mount the MCP endpoints
    @app.post("/mcp")
    async def mcp_endpoint(request: dict):
        """Handle MCP requests over HTTP."""
        # This would need to be implemented to handle MCP protocol
        return {"error": "Not implemented"}
    
    # Run the HTTP server
    await uvicorn.Server(
        uvicorn.Config(app, host="0.0.0.0", port=port)
    ).serve()


if __name__ == "__main__":
    asyncio.run(main())