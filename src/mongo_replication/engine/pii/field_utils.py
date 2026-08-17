"""Utility functions for field path operations and pattern matching.

This module provides shared utilities for working with MongoDB field paths,
including wildcard pattern matching and field path extraction.
"""

import re
from typing import Any


def matches_pattern(field_path: str, pattern: str) -> bool:
    """
    Check if a field path matches a pattern.

    Supports simple wildcard matching:
    - "metadata.*" matches "metadata.created_at", "metadata.user_id", etc.
    - "_id" matches exactly "_id"
    - "*.id" matches "user.id", "account.id", etc.
    - "user.*.email" matches "user.profile.email", "user.forms.email", etc.

    Args:
        field_path: The field path to check (e.g., "user.profile.email")
        pattern: The pattern to match against (e.g., "user.*.email")

    Returns:
        True if the field matches the pattern

    Examples:
        >>> matches_pattern("user.email", "user.email")
        True
        >>> matches_pattern("user.profile.email", "user.*.email")
        True
        >>> matches_pattern("metadata.created_at", "metadata.*")
        True
        >>> matches_pattern("account.id", "*.id")
        True
        >>> matches_pattern("user.name", "user.email")
        False
    """
    # Exact match
    if field_path == pattern:
        return True

    # Wildcard patterns
    if "*" in pattern:
        # Convert glob pattern to regex-like logic
        pattern_parts = pattern.split("*")

        # Pattern starts with wildcard: "*.id"
        if pattern.startswith("*"):
            if field_path.endswith(pattern[1:]):
                return True

        # Pattern ends with wildcard: "metadata.*"
        if pattern.endswith("*"):
            if field_path.startswith(pattern[:-1]):
                return True

        # Pattern has wildcard in middle: "user.*.email"
        # Check if all non-wildcard parts are present in order
        current_pos = 0
        for part in pattern_parts:
            if part:  # Skip empty parts from consecutive wildcards
                pos = field_path.find(part, current_pos)
                if pos == -1:
                    return False
                current_pos = pos + len(part)
        return True

    return False


def get_all_field_paths(doc: dict[str, Any], parent_path: str = "") -> set[str]:
    """
    Recursively extract all field paths from a document.

    This function traverses a MongoDB document and returns all field paths
    in dot notation, including paths within nested objects and arrays.

    Args:
        doc: Document to extract fields from
        parent_path: Parent path prefix (for recursion)

    Returns:
        Set of field paths in dot notation

    Examples:
        >>> doc = {"user": {"email": "test@example.com", "age": 30}}
        >>> sorted(get_all_field_paths(doc))
        ['user', 'user.age', 'user.email']

        >>> doc = {"contacts": [{"email": "a@test.com"}, {"email": "b@test.com"}]}
        >>> sorted(get_all_field_paths(doc))
        ['contacts', 'contacts.email']
    """
    paths = set()

    for key, value in doc.items():
        # Build current path
        current_path = f"{parent_path}.{key}" if parent_path else key
        paths.add(current_path)

        # Recurse into nested dicts
        if isinstance(value, dict):
            nested_paths = get_all_field_paths(value, current_path)
            paths.update(nested_paths)

        # Handle arrays (check first element for schema)
        elif isinstance(value, list) and value:
            first_item = value[0]
            if isinstance(first_item, dict):
                # For arrays, we don't include array notation in the path
                # "contacts.email" works for all array elements
                nested_paths = get_all_field_paths(first_item, current_path)
                paths.update(nested_paths)

    return paths


def normalize_array_path(field_path: str) -> str:
    """
    Remove array indices from field path.

    This is useful for normalizing field paths that may contain array indices
    like "contacts[0].email" to a canonical form "contacts.email".

    Args:
        field_path: Field path with possible array indices

    Returns:
        Normalized field path without array indices

    Examples:
        >>> normalize_array_path("invitations[0].invitee.email")
        'invitations.invitee.email'
        >>> normalize_array_path("contacts[5].name")
        'contacts.name'
        >>> normalize_array_path("simple.field")
        'simple.field'
    """
    # Remove [N] patterns and the dot that follows (if any)
    normalized = re.sub(r"\[\d+\]\.?", ".", field_path)
    # Clean up any double dots that might result
    normalized = re.sub(r"\.\.+", ".", normalized)
    # Remove leading/trailing dots
    return normalized.strip(".")
