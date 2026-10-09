"""DuckDB implementation of canonical type formatting."""

from iceberg_fuzzer.backend.canonical import CanonicalCast


class DuckDBCanonicalCast(CanonicalCast):
    """Produces DuckDB SQL expression strings for canonical formatting."""

    def cast(self, col_name: str, col_type: str) -> str:
        quoted = f'"{col_name}"'
        expr = self._expr_for_type(quoted, col_type.upper())
        return f"COALESCE({expr}, '')"

    def _expr_for_type(self, quoted_col: str, col_type: str) -> str:
        if col_type in ('INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'HUGEINT'):
            return f"CAST({quoted_col} AS VARCHAR)"

        if col_type == 'BOOLEAN':
            return f"CASE WHEN {quoted_col} THEN 'true' ELSE 'false' END"

        if col_type in ('FLOAT', 'DOUBLE', 'REAL'):
            return f"CAST({quoted_col} AS VARCHAR)"

        if col_type.startswith('DECIMAL'):
            return f"CAST({quoted_col} AS VARCHAR)"

        if col_type in ('VARCHAR', 'TEXT', 'STRING'):
            return quoted_col

        if col_type == 'DATE':
            return f"strftime({quoted_col}, '%Y-%m-%d')"

        if col_type == 'TIME':
            return f"strftime({quoted_col}, '%H:%M:%S.%f')"

        if col_type == 'TIMESTAMP':
            return f"strftime({quoted_col}, '%Y-%m-%d %H:%M:%S.%f')"

        if col_type in ('TIMESTAMP WITH TIME ZONE', 'TIMESTAMPTZ'):
            return f"strftime({quoted_col} AT TIME ZONE 'UTC', '%Y-%m-%d %H:%M:%S.%f')"

        if col_type in ('BLOB', 'BYTEA'):
            return f"lower(hex({quoted_col}))"

        if col_type == 'UUID':
            return f"CAST({quoted_col} AS VARCHAR)"

        if col_type.endswith('[]') or col_type.startswith('MAP(') or col_type.startswith('STRUCT('):
            return f"CAST(to_json({quoted_col}) AS VARCHAR)"

        return f"CAST({quoted_col} AS VARCHAR)"
