#!/usr/bin/env python3
"""
Conformance Tests for Apache Iceberg
Abstract base class for table validators
"""

from abc import ABC, abstractmethod
from typing import Dict


class TableValidator(ABC):
    """Abstract base class for table validators"""

    def __init__(self, warehouse_path: str, specs_path: str):
        self.warehouse_path = warehouse_path
        self.specs_path = specs_path

    @abstractmethod
    def supports_s3_tables(self) -> bool:
        """Return True if validator supports S3 Tables warehouses"""
        pass

    @abstractmethod
    def supports_remote_warehouses(self) -> bool:
        """Return True if validator supports remote warehouses"""
        pass

    @abstractmethod
    def validate_tables(self, num_parallel: int = 1) -> Dict:
        """
        Validate all tables in the warehouse

        Args:
            num_parallel: Number of parallel validation workers

        Returns:
            Dict with validation results summary
        """
        pass

    @abstractmethod
    def validate_table(self, table_name: str, expected_spec: Dict) -> Dict:
        """
        Validate a single table

        Args:
            table_name: Name of the table to validate
            expected_spec: Expected table specification

        Returns:
            Dict with validation result for the table
        """
        pass

    def cleanup(self):
        """Cleanup resources (optional override)"""
        pass
