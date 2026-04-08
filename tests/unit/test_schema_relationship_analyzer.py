"""Unit tests for schema relationship analyzer."""

from mongo_replication.engine.relationships import SchemaRelationshipAnalyzer


class TestSchemaRelationshipAnalyzer:
    """Tests for SchemaRelationshipAnalyzer."""

    def test_simple_relationship_snake_case(self):
        """Test detecting relationship with snake_case field names."""
        samples = {
            "customers": [
                {"_id": "1", "name": "Alice"},
                {"_id": "2", "name": "Bob"},
            ],
            "orders": [
                {"_id": "101", "customer_id": "1", "total": 100},
                {"_id": "102", "customer_id": "2", "total": 200},
            ],
        }

        analyzer = SchemaRelationshipAnalyzer(samples)
        relationships = analyzer.infer_relationships()

        assert len(relationships) == 1
        rel = relationships[0]
        assert rel.parent == "customers"
        assert rel.child == "orders"
        assert rel.parent_field == "_id"
        assert rel.child_field == "customer_id"

    def test_simple_relationship_camel_case(self):
        """Test detecting relationship with camelCase field names."""
        samples = {
            "customers": [
                {"_id": "1", "name": "Alice"},
            ],
            "orders": [
                {"_id": "101", "customerId": "1", "total": 100},
            ],
        }

        analyzer = SchemaRelationshipAnalyzer(samples)
        relationships = analyzer.infer_relationships()

        assert len(relationships) == 1
        rel = relationships[0]
        assert rel.parent == "customers"
        assert rel.child == "orders"
        assert rel.parent_field == "_id"
        assert rel.child_field == "customerId"

    def test_nested_field_relationship(self):
        """Test detecting relationship in nested fields."""
        samples = {
            "customers": [
                {"_id": "1", "name": "Alice"},
            ],
            "orders": [
                {"_id": "101", "meta": {"customer_id": "1"}, "total": 100},
            ],
        }

        analyzer = SchemaRelationshipAnalyzer(samples)
        relationships = analyzer.infer_relationships()

        assert len(relationships) == 1
        rel = relationships[0]
        assert rel.parent == "customers"
        assert rel.child == "orders"
        assert rel.child_field == "meta.customer_id"

    def test_multi_level_relationships(self):
        """Test detecting multi-level relationship chain."""
        samples = {
            "customers": [
                {"_id": "1", "name": "Alice"},
            ],
            "orders": [
                {"_id": "101", "customer_id": "1", "total": 100},
            ],
            "order_items": [
                {"_id": "1001", "order_id": "101", "product": "Widget"},
            ],
        }

        analyzer = SchemaRelationshipAnalyzer(samples)
        relationships = analyzer.infer_relationships()

        assert len(relationships) == 2

        # Find each relationship
        customer_order_rel = next((r for r in relationships if r.parent == "customers"), None)
        order_items_rel = next((r for r in relationships if r.parent == "orders"), None)

        assert customer_order_rel is not None
        assert customer_order_rel.child == "orders"
        assert customer_order_rel.child_field == "customer_id"

        assert order_items_rel is not None
        assert order_items_rel.child == "order_items"
        assert order_items_rel.child_field == "order_id"

    def test_no_relationships(self):
        """Test when no relationships can be inferred."""
        samples = {
            "logs": [
                {"_id": "1", "message": "Log entry"},
            ],
            "metrics": [
                {"_id": "1", "value": 42},
            ],
        }

        analyzer = SchemaRelationshipAnalyzer(samples)
        relationships = analyzer.infer_relationships()

        assert len(relationships) == 0

    def test_self_reference_ignored(self):
        """Test that self-references are ignored."""
        samples = {
            "categories": [
                {"_id": "1", "name": "Electronics", "parent_category_id": None},
                {"_id": "2", "name": "Laptops", "parent_category_id": "1"},
            ],
        }

        analyzer = SchemaRelationshipAnalyzer(samples)
        relationships = analyzer.infer_relationships()

        # Should not create self-referencing relationship
        assert len(relationships) == 0

    def test_duplicate_fields_deduplicated(self):
        """Test that duplicate relationships are deduplicated."""
        samples = {
            "customers": [
                {"_id": "1", "name": "Alice"},
            ],
            "orders": [
                {
                    "_id": "101",
                    "customer_id": "1",
                    "billing_customer_id": "1",  # Two fields point to customers
                    "total": 100,
                },
            ],
        }

        analyzer = SchemaRelationshipAnalyzer(samples)
        relationships = analyzer.infer_relationships()

        # Should only have one relationship despite two matching fields
        assert len(relationships) == 1
        rel = relationships[0]
        assert rel.parent == "customers"
        assert rel.child == "orders"

    def test_plural_collection_names(self):
        """Test matching fields to plural collection names."""
        samples = {
            "users": [
                {"_id": "1", "name": "Alice"},
            ],
            "posts": [
                {"_id": "101", "user_id": "1", "title": "Hello"},
            ],
        }

        analyzer = SchemaRelationshipAnalyzer(samples)
        relationships = analyzer.infer_relationships()

        assert len(relationships) == 1
        rel = relationships[0]
        assert rel.parent == "users"
        assert rel.child == "posts"
        assert rel.child_field == "user_id"

    def test_irregular_plurals(self):
        """Test matching fields with irregular plural forms."""
        samples = {
            "categories": [
                {"_id": "1", "name": "Electronics"},
            ],
            "products": [
                {"_id": "101", "category_id": "1", "name": "Laptop"},
            ],
        }

        analyzer = SchemaRelationshipAnalyzer(samples)
        relationships = analyzer.infer_relationships()

        assert len(relationships) == 1
        rel = relationships[0]
        assert rel.parent == "categories"
        assert rel.child == "products"
        assert rel.child_field == "category_id"

    def test_empty_samples(self):
        """Test with empty document samples."""
        samples = {
            "customers": [],
            "orders": [],
        }

        analyzer = SchemaRelationshipAnalyzer(samples)
        relationships = analyzer.infer_relationships()

        assert len(relationships) == 0

    def test_common_fields_ignored(self):
        """Test that common non-relationship fields are ignored."""
        samples = {
            "customers": [
                {"_id": "1", "name": "Alice", "created_at": "2024-01-01"},
            ],
            "orders": [
                {
                    "_id": "101",
                    "customer_id": "1",
                    "created_at": "2024-01-02",
                    "updated_at": "2024-01-03",
                },
            ],
        }

        analyzer = SchemaRelationshipAnalyzer(samples)
        relationships = analyzer.infer_relationships()

        # Should only find customer relationship, not created_at/updated_at
        assert len(relationships) == 1
        assert relationships[0].child_field == "customer_id"

    def test_camel_case_conversion(self):
        """Test camelCase to snake_case conversion."""
        analyzer = SchemaRelationshipAnalyzer({})

        assert analyzer._camel_to_snake("customerId") == "customer_id"
        assert analyzer._camel_to_snake("userId") == "user_id"
        assert analyzer._camel_to_snake("orderId") == "order_id"
        assert analyzer._camel_to_snake("productCategoryId") == "product_category_id"

    def test_extract_collection_references(self):
        """Test extraction of potential collection names from field names."""
        analyzer = SchemaRelationshipAnalyzer({})

        # Snake case with _id
        refs = analyzer._extract_collection_references("customer_id")
        assert "customer" in refs
        assert "customers" in refs

        # Camel case with Id
        refs = analyzer._extract_collection_references("customerId")
        assert "customer" in refs
        assert "customers" in refs

        # Plural _ids
        refs = analyzer._extract_collection_references("customer_ids")
        assert "customer" in refs
        assert "customers" in refs

        # Irregular plurals (category -> categories)
        refs = analyzer._extract_collection_references("category_id")
        assert "category" in refs
        assert "categories" in refs

    def test_complex_nested_structure(self):
        """Test with complex nested document structures."""
        samples = {
            "organizations": [
                {"_id": "1", "name": "Acme Corp"},
            ],
            "projects": [
                {
                    "_id": "101",
                    "name": "Project Alpha",
                    "details": {
                        "organization_id": "1",
                        "metadata": {"budget": 10000},
                    },
                },
            ],
        }

        analyzer = SchemaRelationshipAnalyzer(samples)
        relationships = analyzer.infer_relationships()

        assert len(relationships) == 1
        rel = relationships[0]
        assert rel.parent == "organizations"
        assert rel.child == "projects"
        assert rel.child_field == "details.organization_id"

    def test_multiple_children_same_parent(self):
        """Test one parent with multiple children."""
        samples = {
            "customers": [
                {"_id": "1", "name": "Alice"},
            ],
            "orders": [
                {"_id": "101", "customer_id": "1", "total": 100},
            ],
            "addresses": [
                {"_id": "201", "customer_id": "1", "street": "123 Main St"},
            ],
        }

        analyzer = SchemaRelationshipAnalyzer(samples)
        relationships = analyzer.infer_relationships()

        assert len(relationships) == 2

        parents = {rel.parent for rel in relationships}
        children = {rel.child for rel in relationships}

        assert "customers" in parents
        assert "orders" in children
        assert "addresses" in children

    def test_no_matching_collections(self):
        """Test when field names don't match any collection names."""
        samples = {
            "products": [
                {"_id": "1", "name": "Widget"},
            ],
            "inventory": [
                {"_id": "101", "product_ref": "1", "quantity": 50},
            ],
        }

        analyzer = SchemaRelationshipAnalyzer(samples)
        relationships = analyzer.infer_relationships()

        # "product_ref" doesn't match the pattern, so no relationship
        assert len(relationships) == 0
