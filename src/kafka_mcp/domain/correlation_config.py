"""Correlation configuration models — pydantic models for correlation pattern matching.

These models define the configuration structures for advanced correlation features
including regex patterns, JSONPath expressions, and correlation limits.
"""

from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field


class CorrelationPatternConfig(BaseModel):
    """Configuration for correlation pattern matching.

    Defines patterns and expressions used for extracting correlation IDs
    from Kafka messages.
    """

    regex_patterns: List[str] = Field(
        default_factory=list, description="List of regex patterns for ID extraction from message content."
    )

    jsonpath_expressions: List[str] = Field(
        default_factory=list, description="List of JSONPath expressions for structured data ID extraction."
    )

    xpath_expressions: List[str] = Field(
        default_factory=list, description="List of XPath expressions for XML data ID extraction."
    )


class CorrelationLimitsConfig(BaseModel):
    """Configuration for correlation traversal limits.

    Defines limits to prevent excessive resource consumption during
    correlation operations.
    """

    max_depth: Optional[int] = Field(
        default=None, description="Maximum correlation depth (number of hops). None means unlimited.", ge=1
    )

    max_breadth: Optional[int] = Field(
        default=None, description="Maximum correlation breadth per level. None means unlimited.", ge=1
    )

    timeout_seconds: Optional[int] = Field(
        default=30, description="Timeout for correlation operations in seconds.", ge=1, le=300
    )


class CorrelationTraversalConfig(BaseModel):
    """Configuration for correlation traversal behavior.

    Defines how correlation traversal should behave including direction
    and filtering options.
    """

    bidirectional: bool = Field(default=False, description="Whether to enable backward correlation traversal.")

    follow_causality: bool = Field(default=True, description="Whether to follow causal relationships between messages.")

    exclude_internal_topics: bool = Field(
        default=True, description="Whether to exclude internal Kafka topics from correlation."
    )


class CorrelationConfig(BaseModel):
    """Complete correlation configuration.

    Combines all correlation configuration aspects into a single model
    that can be used to configure correlation operations.
    """

    patterns: CorrelationPatternConfig = Field(
        default_factory=CorrelationPatternConfig, description="Pattern matching configuration for ID extraction."
    )

    limits: CorrelationLimitsConfig = Field(
        default_factory=CorrelationLimitsConfig, description="Limits to prevent excessive resource consumption."
    )

    traversal: CorrelationTraversalConfig = Field(
        default_factory=CorrelationTraversalConfig, description="Traversal behavior configuration."
    )
