"""
Configuration module for Snowflake MCP Server

Loads configuration from environment variables with fallbacks to config.json file.
"""

import os
import json
import sys
from typing import Dict, Any

# Path to the configuration file
CONFIG_FILE = os.path.join(os.path.dirname(__file__), "config.json")

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

def load_config_from_file() -> Dict[str, Any]:
    """Load configuration from JSON file."""
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "r") as f:
                return json.load(f)
        else:
            print(f"[SNOWFLAKE MCP CONFIG] Warning: Config file {CONFIG_FILE} not found", file=sys.stderr)
            return {}
    except Exception as e:
        print(f"[SNOWFLAKE MCP CONFIG] Error loading config file: {str(e)}", file=sys.stderr)
        return {}

# Load configuration from file
file_config = load_config_from_file()

# Create the configuration with environment variable overrides
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

# Log the configuration (without password for security)
if config["debug"]:
    safe_config = config.copy()
    if safe_config["password"]:
        safe_config["password"] = "***********"
    print(f"[SNOWFLAKE MCP CONFIG] Loaded configuration: {json.dumps(safe_config, indent=2)}", file=sys.stderr)