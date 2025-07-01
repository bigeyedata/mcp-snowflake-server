# Snowflake MCP Server - Dynamic Authentication

The Snowflake MCP server now supports dynamic authentication, allowing users to provide credentials through the chat interface without editing configuration files.

## Authentication Methods

### 1. Dynamic Authentication (Recommended)

You can authenticate directly through the chat interface using the `authenticate_snowflake` tool:

```
Use the authenticate_snowflake tool with:
- account: your-account-identifier
- user: your-username
- password: your-password
- warehouse: your-warehouse (optional)
- database: default-database (optional)
- schema: default-schema (optional)
- role: your-role (optional)
- save_credentials: true (optional, defaults to true)
```

The server will securely store your credentials encrypted on disk for future use.

### 2. Using Saved Credentials

Once you've authenticated and saved credentials, you can quickly reconnect using:

```
Use the use_saved_credentials tool with:
- account: your-account-identifier
- user: your-username
```

### 3. Pre-configured Authentication (Legacy)

You can still provide credentials via command-line arguments when starting the server:

```bash
mcp_snowflake_server --account myaccount --user myuser --password mypassword --warehouse mywarehouse
```

Or via environment variables:

```bash
export SNOWFLAKE_ACCOUNT=myaccount
export SNOWFLAKE_USER=myuser
export SNOWFLAKE_PASSWORD=mypassword
export SNOWFLAKE_WAREHOUSE=mywarehouse
```

## Authentication Tools

### authenticate_snowflake
Authenticate with Snowflake using connection parameters. Credentials can be saved securely for future use.

### use_saved_credentials
Use previously saved credentials to connect to Snowflake.

### list_saved_credentials
List all saved Snowflake account/user combinations.

### delete_saved_credentials
Delete saved credentials for a specific account/user or all saved credentials.

## Authentication Status

You can check your current authentication status by reading the `snowflake://auth/status` resource, which will show:
- Current connection details (if authenticated)
- Available saved credentials (if not authenticated)
- Instructions for authentication

## Security

- Credentials are encrypted using Fernet symmetric encryption
- Encryption keys are stored separately with restricted permissions (0600)
- Passwords are never logged or displayed in clear text
- Stored credentials are located in `~/.snowflake-mcp/credentials.enc`

## Example Workflow

1. Start the MCP server without any credentials
2. Use `authenticate_snowflake` to provide your credentials
3. The server will test the connection and save the credentials
4. Use Snowflake tools like `list_databases`, `read_query`, etc.
5. Next time, use `use_saved_credentials` to quickly reconnect

## Troubleshooting

- If authentication fails, check your account identifier format (e.g., 'myorg-myaccount' or 'myaccount.region')
- Ensure your user has the necessary permissions in Snowflake
- Check network connectivity to your Snowflake account
- Delete and re-save credentials if they become invalid