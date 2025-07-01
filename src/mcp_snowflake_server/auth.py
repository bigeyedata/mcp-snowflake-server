"""
Authentication module for Snowflake MCP Server

Provides secure credential storage and authentication management.
"""

import os
import json
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from cryptography.fernet import Fernet
import snowflake.connector


class SecureStorage:
    """Secure credential storage with encryption"""
    
    def __init__(self):
        self.storage_path = Path.home() / '.snowflake-mcp' / 'credentials.enc'
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        self.key = self._get_or_create_key()
        self.cipher = Fernet(self.key)
    
    def _get_or_create_key(self) -> bytes:
        """Get or create encryption key"""
        key_path = Path.home() / '.snowflake-mcp' / '.key'
        if key_path.exists():
            return key_path.read_bytes()
        else:
            key = Fernet.generate_key()
            key_path.write_bytes(key)
            # Set restrictive permissions (Unix-like systems)
            try:
                os.chmod(key_path, 0o600)
            except:
                pass  # Windows doesn't support chmod
            return key
    
    def save_credentials(self, account: str, username: str, connection_params: Dict[str, Any]):
        """Save encrypted credentials"""
        creds = {}
        if self.storage_path.exists():
            try:
                encrypted = self.storage_path.read_bytes()
                decrypted = self.cipher.decrypt(encrypted)
                creds = json.loads(decrypted)
            except:
                pass  # Start fresh if decryption fails
        
        # Store by account and username
        if account not in creds:
            creds[account] = {}
        
        # Don't store the password in plain text in memory
        creds[account][username] = {
            'connection_params': connection_params,
            'saved_at': datetime.now().isoformat()
        }
        
        encrypted = self.cipher.encrypt(json.dumps(creds).encode())
        self.storage_path.write_bytes(encrypted)
        try:
            os.chmod(self.storage_path, 0o600)
        except:
            pass  # Windows doesn't support chmod
    
    def get_credentials(self, account: str, username: str) -> Optional[Dict[str, Any]]:
        """Retrieve connection parameters for account/username"""
        if not self.storage_path.exists():
            return None
        
        try:
            encrypted = self.storage_path.read_bytes()
            decrypted = self.cipher.decrypt(encrypted)
            creds = json.loads(decrypted)
            
            if account in creds and username in creds[account]:
                return creds[account][username]['connection_params']
        except:
            pass
        
        return None
    
    def list_saved_credentials(self) -> Dict[str, List[str]]:
        """List all saved account/username combinations"""
        if not self.storage_path.exists():
            return {}
        
        try:
            encrypted = self.storage_path.read_bytes()
            decrypted = self.cipher.decrypt(encrypted)
            creds = json.loads(decrypted)
            
            result = {}
            for account, users in creds.items():
                result[account] = list(users.keys())
            return result
        except:
            return {}
    
    def delete_credentials(self, account: Optional[str] = None, username: Optional[str] = None):
        """Delete saved credentials"""
        if not self.storage_path.exists():
            return
        
        if account is None and username is None:
            # Delete all credentials
            try:
                self.storage_path.unlink()
            except:
                pass
            return
        
        try:
            encrypted = self.storage_path.read_bytes()
            decrypted = self.cipher.decrypt(encrypted)
            creds = json.loads(decrypted)
            
            if account and account in creds:
                if username:
                    # Delete specific username
                    creds[account].pop(username, None)
                    if not creds[account]:
                        creds.pop(account, None)
                else:
                    # Delete all users for account
                    creds.pop(account, None)
            
            if creds:
                encrypted = self.cipher.encrypt(json.dumps(creds).encode())
                self.storage_path.write_bytes(encrypted)
                try:
                    os.chmod(self.storage_path, 0o600)
                except:
                    pass
            else:
                self.storage_path.unlink()
        except:
            pass


class SnowflakeAuthClient:
    """Enhanced Snowflake client with authentication management"""
    
    def __init__(self):
        self.storage = SecureStorage()
        self.current_connection_params = None
        self._connection = None
    
    @property
    def is_authenticated(self) -> bool:
        """Check if client is authenticated"""
        return bool(self.current_connection_params)
    
    def test_authentication(self, connection_params: Dict[str, Any]) -> Dict[str, Any]:
        """Test if connection parameters are valid"""
        try:
            # Create a test connection
            conn = snowflake.connector.connect(**connection_params)
            
            # Get account and user info
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
            result = cursor.fetchone()
            
            user, account, role, warehouse = result
            
            cursor.close()
            conn.close()
            
            return {
                'valid': True,
                'user': user,
                'account': account,
                'role': role,
                'warehouse': warehouse
            }
        except snowflake.connector.errors.ProgrammingError as e:
            return {
                'valid': False,
                'error': f'Authentication failed: {str(e)}'
            }
        except Exception as e:
            return {
                'valid': False,
                'error': f'Connection error: {str(e)}'
            }
    
    def discover_databases(self, connection_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover available databases"""
        try:
            conn = snowflake.connector.connect(**connection_params)
            cursor = conn.cursor()
            
            cursor.execute("SHOW DATABASES")
            databases = []
            for row in cursor:
                databases.append({
                    'name': row[1],  # database name
                    'owner': row[4],  # owner
                    'comment': row[6] if len(row) > 6 else ''  # comment
                })
            
            cursor.close()
            conn.close()
            
            return databases
        except Exception as e:
            print(f"[SNOWFLAKE AUTH DEBUG] Failed to discover databases: {str(e)}")
            return []
    
    def set_credentials(self, connection_params: Dict[str, Any]):
        """Set current connection parameters"""
        self.current_connection_params = connection_params
        # Clear any existing connection
        if self._connection:
            try:
                self._connection.close()
            except:
                pass
            self._connection = None