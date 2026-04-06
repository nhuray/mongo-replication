"""Unit tests for cascade replication feature."""

import pytest
from unittest.mock import Mock
from bson import ObjectId

from mongo_replication.config.models import RelationshipConfig
from mongo_replication.engine.relationships import Relationship, RelationshipGraph
from mongo_replication.engine.cascade_filter import CascadeFilterBuilder, CascadeResult
from mongo_replication.cli.commands.run import parse_select_option


class TestRelationshipConfig:
    """Tests for RelationshipConfig dataclass."""
    
    def test_valid_relationship(self):
        """Test creating a valid relationship."""
        rel = RelationshipConfig(
            parent="customers",
            child="orders",
            parent_field="_id",
            child_field="customerId",
        )
        
        assert rel.parent == "customers"
        assert rel.child == "orders"
        assert rel.parent_field == "_id"
        assert rel.child_field == "customerId"
    
    def test_self_reference_raises_error(self):
        """Test that self-referencing relationships raise error."""
        with pytest.raises(ValueError, match="cannot have a relationship with itself"):
            RelationshipConfig(
                parent="users",
                child="users",
                parent_field="_id",
                child_field="managerId",
            )
    
    def test_missing_parent_raises_error(self):
        """Test that missing parent raises error."""
        with pytest.raises(ValueError, match="must specify both parent and child"):
            RelationshipConfig(
                parent="",
                child="orders",
                parent_field="_id",
                child_field="customerId",
            )
    
    def test_missing_child_raises_error(self):
        """Test that missing child raises error."""
        with pytest.raises(ValueError, match="must specify both parent and child"):
            RelationshipConfig(
                parent="customers",
                child="",
                parent_field="_id",
                child_field="customerId",
            )
    
    def test_missing_fields_raises_error(self):
        """Test that missing field names raise error."""
        with pytest.raises(ValueError, match="must specify both parent_field and child_field"):
            RelationshipConfig(
                parent="customers",
                child="orders",
                parent_field="",
                child_field="customerId",
            )


class TestRelationship:
    """Tests for Relationship dataclass."""
    
    def test_valid_relationship(self):
        """Test creating a valid relationship."""
        rel = Relationship(
            parent="customers",
            child="orders",
            parent_field="_id",
            child_field="customerId",
        )
        
        assert rel.parent == "customers"
        assert rel.child == "orders"
        assert rel.parent_field == "_id"
        assert rel.child_field == "customerId"


class TestRelationshipGraph:
    """Tests for RelationshipGraph class."""
    
    def test_simple_chain(self):
        """Test simple parent-child chain."""
        relationships = [
            Relationship("customers", "orders", "_id", "customerId"),
            Relationship("orders", "order_items", "_id", "orderId"),
        ]
        graph = RelationshipGraph(relationships)
        
        descendants = graph.get_descendants("customers")
        # get_descendants includes the root collection
        assert descendants == ["customers", "orders", "order_items"]
    
    def test_branching_tree(self):
        """Test branching tree structure."""
        relationships = [
            Relationship("customers", "orders", "_id", "customerId"),
            Relationship("customers", "addresses", "_id", "customerId"),
            Relationship("orders", "order_items", "_id", "orderId"),
        ]
        graph = RelationshipGraph(relationships)
        
        descendants = graph.get_descendants("customers")
        # BFS order: direct children first, then grandchildren
        assert "orders" in descendants
        assert "addresses" in descendants
        assert "order_items" in descendants
        # order_items should come after both orders and addresses
        assert descendants.index("order_items") > descendants.index("orders")
    
    def test_get_parent_relationship(self):
        """Test getting parent relationship."""
        relationships = [
            Relationship("customers", "orders", "_id", "customerId"),
            Relationship("orders", "order_items", "_id", "orderId"),
        ]
        graph = RelationshipGraph(relationships)
        
        parent_rel = graph.get_parent_relationship("orders")
        assert parent_rel is not None
        assert parent_rel.parent == "customers"
        assert parent_rel.child == "orders"
        
        # Root collection has no parent
        assert graph.get_parent_relationship("customers") is None
    
    def test_get_children_relationships(self):
        """Test getting children relationships."""
        relationships = [
            Relationship("customers", "orders", "_id", "customerId"),
            Relationship("customers", "addresses", "_id", "customerId"),
        ]
        graph = RelationshipGraph(relationships)
        
        children = graph.get_children_relationships("customers")
        assert len(children) == 2
        child_names = [rel.child for rel in children]
        assert "orders" in child_names
        assert "addresses" in child_names
        
        # Leaf collection has no children
        assert graph.get_children_relationships("orders") == []
    
    def test_duplicate_child_raises_error(self):
        """Test that duplicate child relationships raise error."""
        relationships = [
            Relationship("customers", "orders", "_id", "customerId"),
            Relationship("users", "orders", "_id", "userId"),  # orders can't have 2 parents
        ]
        
        with pytest.raises(ValueError, match="multiple parent relationships"):
            RelationshipGraph(relationships)
    
    def test_cycle_detection_simple(self):
        """Test cycle detection for simple cycle."""
        relationships = [
            Relationship("A", "B", "_id", "aId"),
            Relationship("B", "C", "_id", "bId"),
            Relationship("C", "A", "_id", "cId"),  # Creates cycle
        ]
        graph = RelationshipGraph(relationships)
        
        assert graph.has_cycles() is True
    
    def test_no_cycle_in_valid_tree(self):
        """Test that valid trees have no cycles."""
        relationships = [
            Relationship("customers", "orders", "_id", "customerId"),
            Relationship("orders", "order_items", "_id", "orderId"),
        ]
        graph = RelationshipGraph(relationships)
        
        assert graph.has_cycles() is False
    
    def test_get_tree_structure(self):
        """Test getting tree structure."""
        relationships = [
            Relationship("customers", "orders", "_id", "customerId"),
            Relationship("orders", "order_items", "_id", "orderId"),
        ]
        graph = RelationshipGraph(relationships)
        
        tree = graph.get_tree_structure("customers")
        assert tree["name"] == "customers"
        assert "children" in tree
        assert len(tree["children"]) == 1
        assert tree["children"][0]["name"] == "orders"
        assert len(tree["children"][0]["children"]) == 1
        assert tree["children"][0]["children"][0]["name"] == "order_items"
    
    def test_validate_collections_success(self):
        """Test successful collection validation."""
        relationships = [
            Relationship("customers", "orders", "_id", "customerId"),
        ]
        graph = RelationshipGraph(relationships)
        
        # Set of existing collections
        existing_collections = {"customers", "orders", "other"}
        
        # Should not raise
        graph.validate_collections(existing_collections)
    
    def test_validate_collections_missing(self):
        """Test validation fails for missing collections."""
        relationships = [
            Relationship("customers", "orders", "_id", "customerId"),
        ]
        graph = RelationshipGraph(relationships)
        
        # Set with missing collection
        existing_collections = {"customers"}  # orders missing
        
        with pytest.raises(ValueError, match="non-existent.*collection"):
            graph.validate_collections(existing_collections)


class TestCascadeFilterBuilder:
    """Tests for CascadeFilterBuilder class."""
    
    def test_convert_to_object_ids_valid(self):
        """Test converting valid ObjectId strings."""
        relationships = [
            Relationship("customers", "orders", "_id", "customerId"),
        ]
        graph = RelationshipGraph(relationships)
        mock_db = Mock()
        builder = CascadeFilterBuilder(mock_db, graph)  # source_db, graph
        
        valid_id = "507f1f77bcf86cd799439011"
        result = builder._convert_to_object_ids([valid_id], "customers", "_id")
        
        assert len(result) == 1
        assert isinstance(result[0], ObjectId)
        assert str(result[0]) == valid_id
    
    def test_convert_to_object_ids_invalid(self):
        """Test error on invalid ObjectId strings."""
        relationships = [
            Relationship("customers", "orders", "_id", "customerId"),
        ]
        graph = RelationshipGraph(relationships)
        mock_db = Mock()
        builder = CascadeFilterBuilder(mock_db, graph)  # source_db, graph
        
        with pytest.raises(ValueError, match="Invalid ObjectId"):
            builder._convert_to_object_ids(["invalid_id"], "customers", "_id")
    
    def test_query_field_values(self):
        """Test querying field values from collection."""
        relationships = [
            Relationship("customers", "orders", "_id", "customerId"),
        ]
        graph = RelationshipGraph(relationships)
        
        # Mock database and collection
        mock_collection = Mock()
        mock_db = Mock()
        mock_db.__getitem__ = Mock(return_value=mock_collection)
        
        # Mock cursor returning documents
        id1 = ObjectId()
        id2 = ObjectId()
        mock_collection.find.return_value = [
            {"_id": id1, "name": "Order 1"},
            {"_id": id2, "name": "Order 2"},
        ]
        
        builder = CascadeFilterBuilder(mock_db, graph)  # source_db, graph
        
        filter_query = {"customerId": {"$in": [ObjectId()]}}
        values = builder._query_field_values("orders", filter_query, "_id")
        
        assert len(values) == 2
        assert id1 in values
        assert id2 in values
    
    def test_build_filters_simple_chain(self):
        """Test building filters for simple parent-child chain."""
        relationships = [
            Relationship("customers", "orders", "_id", "customerId"),
        ]
        graph = RelationshipGraph(relationships)
        
        # Mock database
        cust_id = ObjectId()
        order_id1 = ObjectId()
        order_id2 = ObjectId()
        
        mock_customers = Mock()
        mock_customers.count_documents.return_value = 1
        mock_customers.find.return_value = [{"_id": cust_id}]
        
        mock_orders = Mock()
        mock_orders.count_documents.return_value = 2
        mock_orders.find.return_value = [
            {"_id": order_id1, "customerId": cust_id},
            {"_id": order_id2, "customerId": cust_id},
        ]
        
        mock_db = Mock()
        mock_db.list_collection_names.return_value = ["customers", "orders"]
        mock_db.__getitem__ = Mock(side_effect=lambda name: {
            "customers": mock_customers,
            "orders": mock_orders,
        }[name])
        
        builder = CascadeFilterBuilder(mock_db, graph)  # source_db, graph
        result = builder.build_filters("customers", [str(cust_id)])
        
        assert isinstance(result, CascadeResult)
        assert "customers" in result.filters
        assert "orders" in result.filters
        assert result.doc_counts["customers"] == 1
        assert result.doc_counts["orders"] == 2
        assert len(result.skipped_collections) == 0
    
    def test_build_filters_skip_empty_collections(self):
        """Test that collections with 0 documents are skipped."""
        relationships = [
            Relationship("customers", "orders", "_id", "customerId"),
            Relationship("orders", "order_items", "_id", "orderId"),
        ]
        graph = RelationshipGraph(relationships)
        
        cust_id = ObjectId()
        
        mock_customers = Mock()
        mock_customers.count_documents.return_value = 1
        mock_customers.find.return_value = [{"_id": cust_id}]
        
        # Orders has 0 documents
        mock_orders = Mock()
        mock_orders.count_documents.return_value = 0
        mock_orders.find.return_value = []
        
        mock_db = Mock()
        mock_db.list_collection_names.return_value = ["customers", "orders", "order_items"]
        mock_db.__getitem__ = Mock(side_effect=lambda name: {
            "customers": mock_customers,
            "orders": mock_orders,
        }.get(name, Mock()))
        
        builder = CascadeFilterBuilder(mock_db, graph)  # source_db, graph
        result = builder.build_filters("customers", [str(cust_id)])
        
        # orders should be skipped
        assert "orders" in result.skipped_collections
        # order_items should also be skipped (parent was skipped)
        assert "order_items" in result.skipped_collections


class TestParseSelectOption:
    """Tests for parse_select_option CLI helper."""
    
    def test_valid_single_id(self):
        """Test parsing with single ID."""
        collection, ids = parse_select_option("customers=507f1f77bcf86cd799439011")
        
        assert collection == "customers"
        assert len(ids) == 1
        assert ids[0] == "507f1f77bcf86cd799439011"
    
    def test_valid_multiple_ids(self):
        """Test parsing with multiple IDs."""
        collection, ids = parse_select_option(
            "orders=507f1f77bcf86cd799439011,507f191e810c19729de860ea"
        )
        
        assert collection == "orders"
        assert len(ids) == 2
        assert ids[0] == "507f1f77bcf86cd799439011"
        assert ids[1] == "507f191e810c19729de860ea"
    
    def test_with_whitespace(self):
        """Test parsing handles whitespace."""
        collection, ids = parse_select_option(
            "users = id1 , id2 , id3 "
        )
        
        assert collection == "users"
        assert len(ids) == 3
        assert ids == ["id1", "id2", "id3"]
    
    def test_missing_equals_raises_error(self):
        """Test error when equals sign is missing."""
        with pytest.raises(ValueError, match="Invalid --select format"):
            parse_select_option("customers")
    
    def test_empty_collection_raises_error(self):
        """Test error when collection name is empty."""
        with pytest.raises(ValueError, match="Collection name cannot be empty"):
            parse_select_option("=id1,id2")
    
    def test_empty_ids_raises_error(self):
        """Test error when no IDs provided."""
        with pytest.raises(ValueError, match="No IDs provided"):
            parse_select_option("customers=")
    
    def test_only_whitespace_ids_raises_error(self):
        """Test error when only whitespace IDs."""
        with pytest.raises(ValueError, match="No valid IDs provided"):
            parse_select_option("customers= , , ")


class TestCascadeIntegration:
    """Integration tests for cascade replication workflow."""
    
    def test_full_cascade_workflow(self):
        """Test complete cascade workflow from config to filters."""
        # Create relationship graph
        relationships = [
            Relationship("customers", "orders", "_id", "customerId"),
            Relationship("orders", "order_items", "_id", "orderId"),
        ]
        graph = RelationshipGraph(relationships)
        
        # Verify no cycles
        assert graph.has_cycles() is False
        
        # Verify tree structure
        tree = graph.get_tree_structure("customers")
        assert tree["name"] == "customers"
        assert len(tree["children"]) == 1
        assert tree["children"][0]["name"] == "orders"
        
        # Verify descendants (includes root)
        descendants = graph.get_descendants("customers")
        assert descendants == ["customers", "orders", "order_items"]
    
    def test_branching_cascade(self):
        """Test cascade with multiple branches."""
        relationships = [
            Relationship("customers", "orders", "_id", "customerId"),
            Relationship("customers", "addresses", "_id", "customerId"),
            Relationship("orders", "order_items", "_id", "orderId"),
        ]
        graph = RelationshipGraph(relationships)
        
        descendants = graph.get_descendants("customers")
        
        # Should include all descendants
        assert "orders" in descendants
        assert "addresses" in descendants
        assert "order_items" in descendants
        
        # Direct children should come before grandchildren
        orders_idx = descendants.index("orders")
        addresses_idx = descendants.index("addresses")
        items_idx = descendants.index("order_items")
        assert items_idx > orders_idx
        assert items_idx > addresses_idx
