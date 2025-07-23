#!/usr/bin/env python3
"""
WebSocket proxy for Snowflake MCP server.
This proxies WebSocket connections to the stdio-based MCP server.
"""
import os
import sys
import asyncio
import json
import logging
import subprocess
from typing import Optional, Dict, Any
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(title="Snowflake MCP WebSocket Proxy")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class MCPServerProxy:
    """Proxy for stdio-based MCP server."""
    
    def __init__(self):
        self.process = None
        self.reader = None
        self.writer = None
        
    async def start(self):
        """Start the MCP server process."""
        # Set up environment
        env = os.environ.copy()
        
        # Start the MCP server
        # Change to src directory and run the module
        src_dir = os.path.join(os.path.dirname(__file__), 'src')
        # Set PYTHONPATH to include src directory
        env['PYTHONPATH'] = src_dir + ':' + env.get('PYTHONPATH', '')
        cmd = [sys.executable, "-m", "mcp_snowflake_server"]
        logger.info(f"Starting MCP server: {' '.join(cmd)} in {src_dir}")
        
        self.process = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            cwd=src_dir
        )
        
        self.reader = self.process.stdout
        self.writer = self.process.stdin
        
        # Start error logger
        asyncio.create_task(self._log_stderr())
        
    async def _log_stderr(self):
        """Log stderr output from the MCP server."""
        while True:
            line = await self.process.stderr.readline()
            if not line:
                break
            logger.info(f"MCP Server: {line.decode().strip()}")
                
    async def send_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Send a request to the MCP server and get response."""
        # Send request
        request_str = json.dumps(request) + '\n'
        self.writer.write(request_str.encode())
        await self.writer.drain()
        
        # Read response
        response_line = await self.reader.readline()
        if not response_line:
            raise Exception("MCP server closed")
            
        return json.loads(response_line.decode())
        
    async def close(self):
        """Close the MCP server."""
        if self.process:
            self.process.terminate()
            await self.process.wait()

@app.websocket("/mcp")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for MCP communication."""
    await websocket.accept()
    logger.info(f"WebSocket client connected from {websocket.client}")
    
    # Create a new MCP server instance for this connection
    server = MCPServerProxy()
    
    try:
        # Start the MCP server
        await server.start()
        
        while True:
            # Receive message from WebSocket
            try:
                message = await websocket.receive_json()
                logger.info(f"Received: {message}")
                
                # Forward to MCP server
                response = await server.send_request(message)
                
                # Send response back
                await websocket.send_json(response)
                logger.info(f"Sent: {response}")
                
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
        await server.close()
        logger.info("WebSocket connection closed")

@app.get("/")
async def root():
    """Health check endpoint."""
    return {"status": "ok", "service": "Snowflake MCP WebSocket Proxy"}

async def main():
    """Run the WebSocket server."""
    host = os.getenv("WS_HOST", "0.0.0.0")
    port = int(os.getenv("WS_PORT", "8765"))
    
    logger.info(f"Starting Snowflake MCP WebSocket proxy on {host}:{port}")
    
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