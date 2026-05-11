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
Abstract base class for Iceberg table generators
"""

import random

from abc import ABC, abstractmethod

class TableGenerator(ABC):
    """Abstract base class for table generation backends"""

    def __init__(self, warehouse_path: str, specs_path: str):
        self.warehouse_path = warehouse_path
        self.specs_path = specs_path

    @abstractmethod
    def generate(
        self,
        table_configs: list[dict],
        spec: dict,
        num_parallel: int = 1,
        delete_existing: bool = False,
    ) -> tuple[int, int]:
        """Generate tables from configs.

        Returns (success_count, total_count).
        """
        pass

    def _random_select_tables(self, table_specs: list, random_count: int) -> list:
        """Randomly select a subset of table specs"""
        if random_count is None or random_count >= len(table_specs):
            return table_specs
        return random.sample(table_specs, random_count)

    @abstractmethod
    def cleanup(self):
        """Cleanup resources"""
        pass
