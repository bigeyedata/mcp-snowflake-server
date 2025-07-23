#!/usr/bin/env python3
"""Test script for Snowflake MCP authentication"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src/mcp_snowflake_server'))

from auth import SecureStorage, SnowflakeAuthClient

def test_secure_storage():
    """Test the secure storage functionality"""
    print("Testing SecureStorage...")
    
    storage = SecureStorage()
    
    # Test saving credentials
    test_params = {
        'account': 'test_account',
        'user': 'test_user',
        'password': 'test_password',
        'warehouse': 'test_warehouse'
    }
    
    print("1. Saving credentials...")
    storage.save_credentials('test_account', 'test_user', test_params)
    print("   ✓ Credentials saved")
    
    # Test retrieving credentials
    print("2. Retrieving credentials...")
    retrieved = storage.get_credentials('test_account', 'test_user')
    assert retrieved == test_params, "Retrieved credentials don't match"
    print("   ✓ Credentials retrieved successfully")
    
    # Test listing credentials
    print("3. Listing saved credentials...")
    saved = storage.list_saved_credentials()
    assert 'test_account' in saved, "Account not found in saved credentials"
    assert 'test_user' in saved['test_account'], "User not found in saved credentials"
    print(f"   ✓ Found credentials: {saved}")
    
    # Test deleting specific credentials
    print("4. Deleting specific credentials...")
    storage.delete_credentials('test_account', 'test_user')
    retrieved = storage.get_credentials('test_account', 'test_user')
    assert retrieved is None, "Credentials were not deleted"
    print("   ✓ Credentials deleted successfully")
    
    print("\nAll SecureStorage tests passed! ✓")

def test_auth_client():
    """Test the SnowflakeAuthClient"""
    print("\nTesting SnowflakeAuthClient...")
    
    client = SnowflakeAuthClient()
    
    print("1. Checking initial authentication status...")
    assert not client.is_authenticated, "Client should not be authenticated initially"
    print("   ✓ Client is not authenticated initially")
    
    print("2. Setting credentials...")
    test_params = {
        'account': 'test_account',
        'user': 'test_user',
        'password': 'test_password'
    }
    client.set_credentials(test_params)
    assert client.is_authenticated, "Client should be authenticated after setting credentials"
    print("   ✓ Client is authenticated after setting credentials")
    
    print("\nAll SnowflakeAuthClient tests passed! ✓")

if __name__ == "__main__":
    print("Running Snowflake MCP Authentication Tests\n")
    print("=" * 50)
    
    try:
        test_secure_storage()
        test_auth_client()
        print("\n" + "=" * 50)
        print("All tests passed! ✓✓✓")
    except Exception as e:
        print(f"\nTest failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)