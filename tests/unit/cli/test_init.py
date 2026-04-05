"""Tests for the init command."""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from pymongo.errors import ConnectionFailure

from mongo_replication.cli.commands.init import validate_connection, get_collections_from_source


class TestConnectionValidation:
    """Test connection validation functions."""
    
    def test_connection_success(self):
        """Test successful connection."""
        with patch('mongo_replication.cli.commands.init.MongoClient') as mock_client:
            mock_instance = MagicMock()
            mock_client.return_value = mock_instance
            mock_instance.admin.command.return_value = {'ok': 1}
            
            result = validate_connection("mongodb://localhost:27017", "test_db")
            
            assert result is True
            mock_client.assert_called_once()
            mock_instance.admin.command.assert_called_once_with('ping')
            mock_instance.close.assert_called_once()
    
    def test_connection_failure(self):
        """Test connection failure."""
        with patch('mongo_replication.cli.commands.init.MongoClient') as mock_client:
            mock_client.side_effect = ConnectionFailure("Connection failed")
            
            with patch('mongo_replication.cli.commands.init.print_error'):
                result = validate_connection("mongodb://invalid:27017", "test_db")
            
            assert result is False
    
    def test_get_collections_success(self):
        """Test successful collection listing."""
        with patch('mongo_replication.cli.commands.init.MongoClient') as mock_client:
            mock_instance = MagicMock()
            mock_client.return_value = mock_instance
            mock_db = MagicMock()
            mock_instance.__getitem__.return_value = mock_db
            mock_db.list_collection_names.return_value = ['users', 'orders', 'products']
            
            result = get_collections_from_source("mongodb://localhost:27017", "test_db")
            
            assert result == ['users', 'orders', 'products']
            mock_instance.close.assert_called_once()
    
    def test_get_collections_failure(self):
        """Test collection listing failure."""
        with patch('mongo_replication.cli.commands.init.MongoClient') as mock_client:
            mock_client.side_effect = ConnectionFailure("Connection failed")
            
            with patch('mongo_replication.cli.commands.init.print_error'):
                result = get_collections_from_source("mongodb://invalid:27017", "test_db")
            
            assert result is None
