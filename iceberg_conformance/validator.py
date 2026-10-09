#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""
Apache Iceberg Table Fuzzer
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
    def validate_tables(
        self, num_parallel: int = 1, full_content_validation: bool = False
    ) -> Dict:
        """
        Validate all tables in the warehouse

        Args:
            num_parallel: Number of parallel validation workers
            full_content_validation: Whether to validate all row contents

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
