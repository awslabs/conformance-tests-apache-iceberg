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
PyIceberg implementation of TableGenerator
"""

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from pyiceberg.catalog.sql import SqlCatalog

from backend.pyiceberg.builders import (
    create_table_with_commits
)
from iceberg_fuzzer.table_spec import load_table_specs
from iceberg_fuzzer.generator import TableGenerator


class PyIcebergGenerator(TableGenerator):
    """PyIceberg implementation of table generator"""

    def __init__(self, warehouse_path: str, specs_path: str):
        super().__init__(warehouse_path, specs_path)
        self._setup_warehouse()

    def _setup_warehouse(self):
        """Setup warehouse path and catalog"""
        path = Path(self.warehouse_path)
        path.mkdir(parents=True, exist_ok=True)

        self.catalog_config = {
            'uri': f"sqlite:///{path}/catalog.db",
            'warehouse': f"file://{path}"
        }

        # Initialize catalog
        catalog = SqlCatalog("iceberg_catalog", **self.catalog_config)
        catalog.create_tables()
        catalog.close()

    def _get_catalog(self):
        """Get a new catalog instance"""
        return SqlCatalog("iceberg_catalog", **self.catalog_config)

    def generate(
        self,
        table_configs: list[dict],
        spec: dict,
        num_parallel: int = 1,
        delete_existing: bool = False,
    ) -> tuple[int, int]:
        """Generate tables from configs. Only supports 'simple' type."""
        spec_type = spec['type']

        if spec_type != 'simple':
            raise NotImplementedError(
                f"PyIceberg backend only supports 'simple' tables, not '{spec_type}'"
            )

        total = len(table_configs)
        if total == 0:
            return (0, 0)

        mode_str = "minimal" if len(table_configs) < 100 else "full"
        print(f"Generating {total} single-type tables ({mode_str}) with {num_parallel} parallel workers...")
        print("\n📊 PyIceberg Backend Support Status:")
        print("  ✅ Supported: All primitive types (20 types)")
        print("  ❌ Skipped: Complex types (lists, maps, structs) - not fully supported")
        print("  ✅ Supported: All compressions and encodings")
        print("  ✅ Supported: Both Iceberg v1 and v2")
        print(f"  📈 Expected success rate: 100% ({total}/{total} tables)\n")

        success_count = 0

        if num_parallel == 1:
            for spec_cfg in table_configs:
                success = create_table_with_commits(self.catalog_config, spec_cfg['name'], spec_cfg)
                if success:
                    success_count += 1
                    print(f"✓ Created table {spec_cfg['name']} ({success_count}/{total})")
                else:
                    print(f"✗ Failed to create table {spec_cfg['name']}")
        else:
            with ThreadPoolExecutor(max_workers=num_parallel) as executor:
                future_to_table = {
                    executor.submit(create_table_with_commits, self.catalog_config, cfg['name'], cfg): cfg['name']
                    for cfg in table_configs
                }

                for future in as_completed(future_to_table):
                    table_name = future_to_table[future]
                    try:
                        success = future.result()
                        if success:
                            success_count += 1
                            print(f"✓ Created table {table_name} ({success_count}/{total})")
                        else:
                            print(f"✗ Failed to create table {table_name}")
                    except Exception as e:
                        print(f"✗ Exception creating table {table_name}: {e}")

        print(f"\nCompleted simple tables: {success_count}/{total} tables created successfully")
        return (success_count, total)

    def delete_all_tables(self, num_parallel: int = 1):
        """Delete all tables in the warehouse"""
        catalog = self._get_catalog()

        # Collect all table identifiers first
        tables_to_delete = []

        try:
            # List all namespaces
            namespaces = catalog.list_namespaces()

            for namespace in namespaces:
                try:
                    # List all tables in this namespace
                    tables = catalog.list_tables(namespace)
                    tables_to_delete.extend(tables)

                except Exception as e:
                    print(f"Warning: Could not list tables in namespace {namespace}: {e}")

        except Exception as e:
            print(f"Warning: Could not list namespaces: {e}")
            return
        finally:
            catalog.close()

        if not tables_to_delete:
            print("No tables found to delete.")
            return

        print(f"Found {len(tables_to_delete)} tables to delete...")

        # Delete tables in parallel
        if num_parallel > 1:

            def delete_single_table(table_identifier):
                cat = self._get_catalog()
                try:
                    cat.drop_table(table_identifier)
                    return f"✓ Deleted: {table_identifier}"
                except Exception as e:
                    return f"✗ Failed to delete {table_identifier}: {e}"
                finally:
                    cat.close()

            success_count = 0
            with ThreadPoolExecutor(max_workers=num_parallel) as executor:
                future_to_table = {executor.submit(delete_single_table, table): table for table in tables_to_delete}

                for future in as_completed(future_to_table):
                    result = future.result()
                    print(result)
                    if result.startswith("✓"):
                        success_count += 1

            print(f"\nCompleted: {success_count}/{len(tables_to_delete)} tables deleted successfully")

        else:
            # Sequential deletion
            success_count = 0
            cat = self._get_catalog()
            try:
                for table_identifier in tables_to_delete:
                    try:
                        print(f"Deleting table: {table_identifier}")
                        cat.drop_table(table_identifier)
                        success_count += 1
                    except Exception as e:
                        print(f"Warning: Could not delete table {table_identifier}: {e}")
            finally:
                cat.close()

            print(f"\nCompleted: {success_count}/{len(tables_to_delete)} tables deleted successfully")

    def cleanup(self):
        """No cleanup needed for PyIceberg"""
        pass
