#!/usr/bin/env python3
"""
TCP bridge for Snowflake MCP server.
This allows the stdio-based MCP server to be accessed over TCP.
"""
import asyncio
import json
import logging
import os
import sys
import subprocess
from typing import Optional, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class MCPServerBridge:
    """Bridge between TCP socket and stdio MCP server."""
    
    def __init__(self, host: str = "0.0.0.0", port: int = 5001):
        self.host = host
        self.port = port
        self.server = None
        self.clients = set()
        
    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle a client connection."""
        client_addr = writer.get_extra_info('peername')
        logger.info(f"New client connected: {client_addr}")
        self.clients.add(writer)
        
        # Start MCP server process for this client
        process = None
        try:
            # Start the MCP server
            cmd = [sys.executable, "server.py"]
            logger.info(f"Starting MCP server: {' '.join(cmd)}")
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=os.environ.copy()
            )
            
            # Create tasks for bidirectional communication
            tasks = [
                asyncio.create_task(self._client_to_server(reader, process.stdin)),
                asyncio.create_task(self._server_to_client(process.stdout, writer)),
                asyncio.create_task(self._log_stderr(process.stderr))
            ]
            
            # Wait for any task to complete
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            
            # Cancel remaining tasks
            for task in pending:
                task.cancel()
                
        except Exception as e:
            logger.error(f"Error handling client: {e}")
        finally:
            # Clean up
            self.clients.discard(writer)
            writer.close()
            await writer.wait_closed()
            
            if process:
                process.terminate()
                await process.wait()
                
            logger.info(f"Client disconnected: {client_addr}")
            
    async def _client_to_server(self, reader: asyncio.StreamReader, server_stdin):
        """Forward messages from client to server."""
        try:
            while True:
                # Read line from client
                data = await reader.readline()
                if not data:
                    break
                    
                # Forward to server
                server_stdin.write(data)
                await server_stdin.drain()
                
                # Log the message
                try:
                    msg = json.loads(data.decode().strip())
                    logger.info(f"Client -> Server: {json.dumps(msg)}")
                except:
                    pass
                    
        except Exception as e:
            logger.error(f"Error in client_to_server: {e}")
            
    async def _server_to_client(self, server_stdout, writer: asyncio.StreamWriter):
        """Forward messages from server to client."""
        try:
            while True:
                # Read line from server
                data = await server_stdout.readline()
                if not data:
                    break
                    
                # Forward to client
                writer.write(data)
                await writer.drain()
                
                # Log the message
                try:
                    msg = json.loads(data.decode().strip())
                    logger.info(f"Server -> Client: {json.dumps(msg)}")
                except:
                    pass
                    
        except Exception as e:
            logger.error(f"Error in server_to_client: {e}")
            
    async def _log_stderr(self, stderr):
        """Log stderr output from the server."""
        try:
            while True:
                line = await stderr.readline()
                if not line:
                    break
                logger.info(f"MCP Server: {line.decode().strip()}")
        except Exception as e:
            logger.error(f"Error reading stderr: {e}")
            
    async def start(self):
        """Start the TCP server."""
        self.server = await asyncio.start_server(
            self.handle_client, self.host, self.port
        )
        
        addr = self.server.sockets[0].getsockname()
        logger.info(f"TCP bridge listening on {addr[0]}:{addr[1]}")
        
        async with self.server:
            await self.server.serve_forever()


async def main():
    """Run the TCP bridge."""
    host = os.getenv("TCP_HOST", "0.0.0.0")
    port = int(os.getenv("TCP_PORT", "5001"))
    
    bridge = MCPServerBridge(host, port)
    await bridge.start()


if __name__ == "__main__":
    asyncio.run(main())