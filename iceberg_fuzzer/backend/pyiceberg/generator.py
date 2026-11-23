#!/usr/bin/env python3
"""
Conformance Tests for Apache Iceberg
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

    def __init__(self, warehouse_path: str, specs_path: str, seed: int | None = None):
        super().__init__(warehouse_path, specs_path, seed=seed)
        self._setup_warehouse()

    def _setup_warehouse(self):
        """Setup warehouse path and catalog"""
        # This path is interpolated into a file:// URI that is stored verbatim
        # as each table's location and inside its manifests, so it has to be
        # absolute.
        path = Path(self.warehouse_path).resolve()
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

    def supports_s3_tables(self) -> bool:
        return False

    def supports_partitioned_tables(self) -> bool:
        return False

    def supports_minimal_mode(self) -> bool:
        return True

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

    def generate_simple_tables(
        self,
        num_parallel: int = 1,
        minimal: bool = False,
        random_count: int = None,
        delete_existing: bool = False
    ) -> bool:
        """Generate simple single-type tables.

        Returns:
            True if every table in the set was created, False if any failed.
        """
        table_specs = load_table_specs(self.specs_path, minimal=minimal)

        # Apply random selection if specified
        table_specs = self._random_select_tables(table_specs, random_count)

        mode_str = "minimal (uncompressed, plain encoding)" if minimal else "full"
        print(f"Generating {len(table_specs)} single-type tables ({mode_str}) with {num_parallel} parallel workers...")
        print("\n📊 PyIceberg Backend Support Status:")
        print("  ✅ Supported: All primitive types (20 types)")
        print("  ❌ Skipped: Complex types (lists, maps, structs) - not fully supported")
        print("  ✅ Supported: All compressions and encodings")
        print("  ✅ Supported: Both Iceberg v1 and v2")
        print(f"  📈 Expected success rate: 100% ({len(table_specs)}/{len(table_specs)} tables)\n")

        success_count = 0
        total_count = len(table_specs)

        if num_parallel == 1:
            for spec in table_specs:
                success = create_table_with_commits(self.catalog_config, spec['name'], spec)
                if success:
                    success_count += 1
                    print(f"✓ Created table {spec['name']} ({success_count}/{total_count})")
                else:
                    print(f"✗ Failed to create table {spec['name']}")
        else:
            with ThreadPoolExecutor(max_workers=num_parallel) as executor:
                future_to_table = {
                    executor.submit(create_table_with_commits, self.catalog_config, spec['name'], spec): spec['name']
                    for spec in table_specs
                }

                for future in as_completed(future_to_table):
                    table_name = future_to_table[future]
                    try:
                        success = future.result()
                        if success:
                            success_count += 1
                            print(f"✓ Created table {table_name} ({success_count}/{total_count})")
                        else:
                            print(f"✗ Failed to create table {table_name}")
                    except Exception as e:
                        print(f"✗ Exception creating table {table_name}: {e}")

        print(f"\nCompleted simple tables: {success_count}/{total_count} tables created successfully")
        return success_count == total_count

    def generate_single_table(self, table_name: str, delete_existing: bool = False) -> bool:
        """Generate a single table by name.

        Returns:
            True if the table was created, False if the name matched no spec
            or creation failed.
        """
        # Try to find the table in simple specs
        try:
            simple_specs = load_table_specs(self.specs_path)
            for spec in simple_specs:
                if spec['name'] == table_name:
                    print(f"Creating single table: {table_name}")
                    catalog = self._get_catalog()

                    # Check if table exists and skip if delete_existing is False
                    if not delete_existing:
                        try:
                            catalog.load_table(f"simple.{table_name}")
                            print(f"Skipping existing table: {table_name}")
                            return True
                        except Exception:
                            pass  # Table doesn't exist, continue with creation

                    success = create_table_with_commits(
                        self.catalog_config,
                        table_name,
                        spec,
                        num_commits=3,
                        rows_per_commit=334
                    )

                    if success:
                        print(f"✓ Successfully created table: {table_name}")
                        return True
                    else:
                        print(f"✗ Failed to create table: {table_name}")
                        return False
        except Exception as e:
            print(f"Error checking simple specs: {e}")

        print(f"❌ Table '{table_name}' not found in PyIceberg specs")
        print("\n💡 PyIceberg only supports simple tables")

        return False

    def generate_write_pattern_tables(
        self,
        num_parallel: int = 1,
        random_count: int = None,
        delete_existing: bool = False
    ):
        """Write-pattern tables not supported in PyIceberg"""
        raise NotImplementedError("Write-pattern tables only supported in PySpark backend")

    def generate_partitioned_tables(
        self,
        num_parallel: int = 1,
        random_count: int = None,
        minimal: bool = False,
        delete_existing: bool = False
    ):
        """Not supported in PyIceberg"""
        raise NotImplementedError("Partitioned tables not supported in PyIceberg backend")

    def generate_evolution_tables(self, num_parallel: int = 1, random_count: int = None, delete_existing: bool = False):
        """Not supported in PyIceberg"""
        raise NotImplementedError("Evolution tables not supported in PyIceberg backend")

    def generate_minimal_tables(
        self, num_parallel: int = 1, random_count: int = None, delete_existing: bool = False
    ) -> bool:
        """Generate minimal set of simple tables.

        Returns:
            True if every table in the set was created, False if any failed.
        """
        return self.generate_simple_tables(num_parallel=num_parallel, minimal=True, random_count=random_count)

    def cleanup(self):
        """No cleanup needed for PyIceberg"""
        pass
