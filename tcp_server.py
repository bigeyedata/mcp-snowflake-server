#!/usr/bin/env python3
"""
TCP Server wrapper for the Snowflake MCP server.
This allows the MCP server to be accessed over TCP instead of stdio.
"""
import asyncio
import json
import logging
import os
import sys
from typing import Dict, Any, Set

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from mcp.server import Server
from mcp.server.models import InitializationOptions
from mcp_snowflake_server.server import main as mcp_main
import mcp_snowflake_server.server as mcp_server

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class MCPTCPServer:
    """TCP server wrapper for MCP."""
    
    def __init__(self, host: str = "0.0.0.0", port: int = 8765):
        self.host = host
        self.port = port
        self.clients: Set[asyncio.StreamWriter] = set()
        
    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle a TCP client connection."""
        client_addr = writer.get_extra_info('peername')
        logger.info(f"New client connected from {client_addr}")
        self.clients.add(writer)
        
        try:
            # Create input/output streams for MCP
            client_read_stream = MCPStreamAdapter(reader)
            client_write_stream = MCPStreamAdapter(writer)
            
            # Run the MCP server with these streams
            await mcp_server.main(
                allow_write=os.getenv("ALLOW_WRITE", "false").lower() == "true",
                connection_args=self._get_connection_args(),
                log_dir=os.getenv("LOG_DIR"),
                prefetch=os.getenv("PREFETCH", "false").lower() == "true",
                log_level=os.getenv("LOG_LEVEL", "INFO"),
                exclude_tools=[],
                config_file=os.getenv("CONFIG_FILE", "runtime_config.json"),
                exclude_patterns=None,
                connection_config_file=os.getenv("CONNECTION_CONFIG_FILE", "config.json"),
                # Pass the streams instead of using stdio
                read_stream=client_read_stream,
                write_stream=client_write_stream
            )
            
        except Exception as e:
            logger.error(f"Error handling client {client_addr}: {e}")
        finally:
            self.clients.discard(writer)
            writer.close()
            await writer.wait_closed()
            logger.info(f"Client {client_addr} disconnected")
            
    def _get_connection_args(self) -> Dict[str, Any]:
        """Get Snowflake connection arguments from environment."""
        return {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
            "role": os.getenv("SNOWFLAKE_ROLE"),
        }
        
    async def start(self):
        """Start the TCP server."""
        server = await asyncio.start_server(
            self.handle_client, self.host, self.port
        )
        
        addr = server.sockets[0].getsockname()
        logger.info(f"MCP TCP Server listening on {addr[0]}:{addr[1]}")
        
        async with server:
            await server.serve_forever()


class MCPStreamAdapter:
    """Adapter to make asyncio streams compatible with MCP's expected interface."""
    
    def __init__(self, stream):
        self.stream = stream
        self._buffer = b""
        
    async def readline(self) -> bytes:
        """Read a line from the stream."""
        if isinstance(self.stream, asyncio.StreamReader):
            # Reading from client
            while b'\n' not in self._buffer:
                data = await self.stream.read(1024)
                if not data:
                    break
                self._buffer += data
                
            if b'\n' in self._buffer:
                line, self._buffer = self._buffer.split(b'\n', 1)
                return line + b'\n'
            else:
                line = self._buffer
                self._buffer = b""
                return line
        else:
            # Should not be called for writer
            raise NotImplementedError()
            
    def write(self, data: bytes):
        """Write data to the stream."""
        if isinstance(self.stream, asyncio.StreamWriter):
            self.stream.write(data)
        else:
            raise NotImplementedError()
            
    async def drain(self):
        """Drain the write buffer."""
        if isinstance(self.stream, asyncio.StreamWriter):
            await self.stream.drain()


async def main():
    """Main entry point."""
    host = os.getenv("TCP_HOST", "0.0.0.0")
    port = int(os.getenv("TCP_PORT", "8765"))
    
    server = MCPTCPServer(host, port)
    await server.start()


if __name__ == "__main__":
    asyncio.run(main())