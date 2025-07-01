# Snowflake MCP Server - Configuration File Authentication

The Snowflake MCP server supports authentication via a configuration file, allowing you to securely provide credentials without exposing them in command-line arguments.

## Setup Instructions

1. **Create the configuration directory** (if it doesn't exist):
   ```bash
   mkdir -p ~/.snowflake-mcp
   ```

2. **Create the configuration file** at `~/.snowflake-mcp/config.json`:
   ```json
   {
     "account": "your-account-identifier",
     "user": "your-username",
     "password": "your-password",
     "warehouse": "your-warehouse",
     "database": "your-database",
     "schema": "your-schema",
     "role": "your-role"
   }
   ```

   Required fields:
   - `account`: Your Snowflake account identifier (e.g., "myorg-myaccount" or "myaccount.region")
   - `user`: Your Snowflake username
   - `password`: Your Snowflake password

   Optional fields:
   - `warehouse`: The warehouse to use for queries
   - `database`: Default database
   - `schema`: Default schema
   - `role`: Role to use for the session

3. **Secure the configuration file**:
   ```bash
   chmod 600 ~/.snowflake-mcp/config.json
   ```

4. **Update your Claude Desktop configuration** at `~/Library/Application Support/Claude/claude_desktop_config.json`:
   ```json
   {
     "mcpServers": {
       "snowflake": {
         "command": "docker",
         "args": [
           "run",
           "--rm",
           "-i",
           "-v",
           "/Users/YOUR_USERNAME/.snowflake-mcp/config.json:/app/config.json:ro",
           "-v",
           "/Users/YOUR_USERNAME/.snowflake-mcp:/home/mcp/.snowflake-mcp",
           "mcp-snowflake-server:latest"
         ]
       }
     }
   }
   ```

   Replace `YOUR_USERNAME` with your actual username.

5. **Restart Claude Desktop** to apply the changes.

## Authentication Methods

The server supports multiple authentication methods in this order of precedence:

1. **Configuration file** (`/app/config.json` in the container)
2. **Command-line arguments** (passed via Docker)
3. **Environment variables** (SNOWFLAKE_* prefix)
4. **Dynamic authentication** via chat tools (if no credentials found)

## Dynamic Authentication Fallback

If no configuration file is found or credentials are invalid, the server will start in dynamic authentication mode. You can then use these tools in chat:

- `authenticate_snowflake`: Provide credentials and optionally save them
- `use_saved_credentials`: Use previously saved credentials
- `list_saved_credentials`: See available saved credentials
- `delete_saved_credentials`: Remove saved credentials

## Troubleshooting

1. **Connection errors**: Verify your account identifier format and network connectivity
2. **Permission errors**: Ensure the config file has proper permissions (600)
3. **Authentication failures**: Check credentials and that your user has necessary Snowflake permissions
4. **Docker mount issues**: Verify the file paths in your Claude Desktop configuration

## Security Notes

- The configuration file contains sensitive credentials - keep it secure
- Use file permissions to restrict access (chmod 600)
- Consider using Snowflake key-pair authentication for enhanced security (future feature)
- The Docker mount uses `:ro` (read-only) to prevent the container from modifying your config