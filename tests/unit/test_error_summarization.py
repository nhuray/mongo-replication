"""Unit tests for bulk write error summarization."""

from unittest.mock import MagicMock
from pymongo.errors import BulkWriteError

from mongo_replication.engine.replicator import _summarize_bulk_write_error


class TestBulkWriteErrorSummarization:
    """Tests for _summarize_bulk_write_error function."""
    
    def test_summarize_duplicate_key_errors(self):
        """Test summarization of duplicate key errors."""
        # Create mock BulkWriteError
        error = MagicMock(spec=BulkWriteError)
        error.details = {
            'nInserted': 0,
            'nUpserted': 0,
            'nMatched': 0,
            'nModified': 0,
            'nRemoved': 0,
            'writeErrors': [
                {
                    'index': 0,
                    'code': 11000,
                    'errmsg': "E11000 duplicate key error collection: test.users index: _id_ dup key: { _id: ObjectId('abc123') }",
                },
                {
                    'index': 1,
                    'code': 11000,
                    'errmsg': "E11000 duplicate key error collection: test.users index: _id_ dup key: { _id: ObjectId('def456') }",
                },
            ]
        }
        
        summary = _summarize_bulk_write_error(error, 'users')
        
        # Should not contain ObjectId or document data
        assert 'ObjectId' not in summary
        assert 'abc123' not in summary
        assert 'def456' not in summary
        
        # Should contain error count and code
        assert '2 error(s) with code 11000' in summary
        assert 'E11000 duplicate key error' in summary
        
        # Should contain stats
        assert 'Inserted: 0' in summary
    
    def test_summarize_groups_by_error_code(self):
        """Test that errors are grouped by code."""
        error = MagicMock(spec=BulkWriteError)
        error.details = {
            'nInserted': 100,
            'nUpserted': 0,
            'nMatched': 0,
            'nModified': 0,
            'nRemoved': 0,
            'writeErrors': [
                {'index': 0, 'code': 11000, 'errmsg': 'Duplicate key error 1'},
                {'index': 1, 'code': 11000, 'errmsg': 'Duplicate key error 2'},
                {'index': 2, 'code': 50, 'errmsg': 'Validation error 1'},
            ]
        }
        
        summary = _summarize_bulk_write_error(error, 'test_coll')
        
        # Should show 2 duplicate errors and 1 validation error
        assert '2 error(s) with code 11000' in summary
        assert '1 error(s) with code 50' in summary
        assert 'Inserted: 100' in summary
    
    def test_summarize_no_errors(self):
        """Test summarization when there are no write errors (shouldn't happen but handle gracefully)."""
        error = MagicMock(spec=BulkWriteError)
        error.details = {
            'nInserted': 50,
            'nUpserted': 0,
            'nMatched': 0,
            'nModified': 0,
            'nRemoved': 0,
            'writeErrors': []
        }
        
        summary = _summarize_bulk_write_error(error, 'test_coll')
        
        # Should contain collection name and stats
        assert 'test_coll' in summary
        assert 'Inserted: 50' in summary
    
    def test_summarize_truncates_long_messages(self):
        """Test that very long error messages are truncated."""
        very_long_msg = "E11000 duplicate key error " + ("x" * 500)
        error = MagicMock(spec=BulkWriteError)
        error.details = {
            'nInserted': 0,
            'nUpserted': 0,
            'nMatched': 0,
            'nModified': 0,
            'nRemoved': 0,
            'writeErrors': [
                {'index': 0, 'code': 11000, 'errmsg': very_long_msg},
            ]
        }
        
        summary = _summarize_bulk_write_error(error, 'test_coll')
        
        # Should be truncated to ~200 chars
        assert len(summary) < 500
        assert 'E11000 duplicate key error' in summary
    
    def test_summarize_with_upsert_stats(self):
        """Test summarization with upsert operations."""
        error = MagicMock(spec=BulkWriteError)
        error.details = {
            'nInserted': 100,
            'nUpserted': 50,
            'nMatched': 200,
            'nModified': 150,
            'nRemoved': 0,
            'writeErrors': [
                {'index': 0, 'code': 11000, 'errmsg': 'Duplicate key'},
            ]
        }
        
        summary = _summarize_bulk_write_error(error, 'test_coll')
        
        # Should show all stats
        assert 'Inserted: 100' in summary
        assert 'Upserted: 50' in summary
        assert 'Modified: 150' in summary
