"""Tests that every backend and the validator agree on what a row holds.

Cross-engine validation is only meaningful while the writers and the validator
derive the same value for the same row ID. These tests compare each against the
shared formulas, so a backend growing its own copy again fails here rather than
surfacing as a table that looks corrupt.
"""

import importlib.util
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from iceberg_fuzzer.common.values import SUPPORTED_TYPES, primitive_value

ROW_IDS = (1, 2, 7, 23, 24, 99, 1000)

# Spec labels carry a length or precision with an underscore where the type
# writes it in brackets. Most of them the formulas do not depend on; fixed(L) is
# the exception, because the value has to be exactly that many bytes.
LABEL_TYPES = {
    'binary_small': 'binary', 'binary_xlong': 'binary',
    'string_small': 'string', 'string_xlong': 'string',
    'decimal_10_2': 'decimal',
    'fixed_16': 'fixed(16)',
}

# Built per element rather than from a scalar formula.
COMPLEX_PREFIXES = ('list<', 'map<', 'struct<')


def _spec_files():
    """Every YAML that can carry a `primitive_types` block."""
    repo = Path(__file__).resolve().parent.parent
    paths = sorted(repo.glob('specs/*.yaml'))
    paths += sorted(repo.glob('tests/*specs*.yaml'))
    paths.append(repo / 'table_specs.yaml')
    return [path for path in paths if path.exists()]


def _declared_primitive_types():
    """Every (spec file, label, Iceberg type) the spec files declare."""
    declared = []
    for path in _spec_files():
        spec = yaml.safe_load(path.read_text()) or {}
        for entry in spec.get('primitive_types', []):
            declared.append((path.name, entry['name'], str(entry['iceberg_type'])))
    return declared


SPEC_FILES = _spec_files()
DECLARED_TYPES = _declared_primitive_types()


def _normalised(iceberg_type):
    """A type name with the spellings that mean the same thing made equal.

    PyIceberg renders `fixed(16)` as `fixed[16]` and `decimal(10,2)` with a space
    after the comma, so a declared type and the type built from it are compared
    on what they mean rather than on punctuation.
    """
    return iceberg_type.replace('[', '(').replace(']', ')').replace(' ', '')

# The spec files that declare primitive types one column at a time, which is the
# shape `select_primitive_types` reads. The rest describe a whole-table schema
# and bring their own expected-row builder, so a type in one of those is not
# reached by anything below. tests/ci_specs.yaml declares types under a
# `data_types` key nothing reads, so it contributes none either.
FILES_WITH_PRIMITIVE_TYPES = {'simple.yaml', 'table_specs.yaml'}


def _load_duckdb_validator():
    """Load the DuckDB validator by path.

    `iceberg_fuzzer/validator.py` takes the `iceberg_fuzzer.validator` name, so the
    `validator/local_duckdb/` directory beneath it is not importable; the CLI runs
    the script as a subprocess instead.
    """
    path = (
        Path(__file__).resolve().parent.parent
        / 'iceberg_fuzzer' / 'validator' / 'local_duckdb' / 'validator_duckdb.py'
    )
    spec = importlib.util.spec_from_file_location('validator_duckdb', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


validator = _load_duckdb_validator()


class TestFormulas:
    """The values themselves, pinned so a change to one is a deliberate one."""

    @pytest.mark.parametrize('iceberg_type,row_id,expected', [
        ('boolean', 4, True),
        ('boolean', 5, False),
        ('int', 7, 7),
        ('long', 7, 7000),
        ('float', 7, 7.5),
        ('double', 7, 10.5),
        ('string', 7, 'value_7'),
        ('binary', 7, b'value_7'),
        ('date', 7, date(2023, 1, 8)),
        ('timestamp', 7, datetime(2023, 1, 1, 7)),
        ('time', 7, time(7, 7)),
        ('decimal(10,2)', 7, Decimal('7.07')),
    ])
    def test_value(self, iceberg_type, row_id, expected):
        assert primitive_value(iceberg_type, row_id) == expected

    @pytest.mark.parametrize('iceberg_type', [
        'int', 'long', 'float', 'double', 'string', 'binary', 'uuid', 'decimal(10,2)',
    ])
    def test_unbounded_types_are_injective(self, iceberg_type):
        # Distinct rows need distinct values, or a row read back under the wrong
        # ID would still validate.
        values = [primitive_value(iceberg_type, i) for i in range(1, 500)]
        assert len(set(values)) == len(values)

    @pytest.mark.parametrize('iceberg_type,period', [
        ('boolean', 2),
        ('date', 28),       # capped at the 28th, so every month holds the day
        ('timestamp', 24),  # hour of a fixed date
        ('timestamptz', 168),  # hour at 24 and offset at 7, so the pair at 168
        ('time', 120),      # hour cycles at 24 and minute at 60
    ])
    def test_bounded_types_repeat_on_a_known_period(self, iceberg_type, period):
        # These do repeat, so a row cannot be identified by its value alone.
        # Pinned here because validation compares a row's value against the ID it
        # was read with, and build-shapes -- which must identify a row from its
        # value -- adds sub-second variation for exactly this reason.
        values = [primitive_value(iceberg_type, i) for i in range(1, 4 * period + 1)]
        assert values[:period] == values[period:2 * period]
        # At most one distinct value per position in the cycle. timestamptz has
        # fewer: a given instant is reachable from more than one hour and offset
        # pairing, so some positions in its cycle name the same instant.
        assert len(set(values)) <= period

    def test_unknown_type_raises(self):
        # Rather than a placeholder: a silent fallback is what let unhandled types
        # be written as `value_0` strings and validate anyway.
        with pytest.raises(KeyError):
            primitive_value('interval', 1)


class TestValidatorAgrees:
    def test_expectations_come_from_the_shared_formulas(self):
        for label, iceberg_type in LABEL_TYPES.items():
            for row_id in ROW_IDS:
                assert validator._generate_primitive_data(label, row_id) == \
                    primitive_value(iceberg_type, row_id), (label, row_id)

    def test_plain_types_match(self):
        for iceberg_type in sorted(SUPPORTED_TYPES):
            for row_id in ROW_IDS:
                assert validator._generate_primitive_data(iceberg_type, row_id) == \
                    primitive_value(iceberg_type, row_id), (iceberg_type, row_id)


class TestPolarsAgrees:
    def test_generated_column_matches_row_by_row(self):
        from iceberg_fuzzer.backend.polars.generator import PolarsGenerator
        generator = PolarsGenerator.__new__(PolarsGenerator)
        types = sorted(SUPPORTED_TYPES) + ['decimal(10,2)']
        for iceberg_type in types:
            start_id = 5
            column = generator._generate_polars_data('value', iceberg_type, 10, start_id)
            for index, value in enumerate(column):
                assert value == primitive_value(iceberg_type, index + start_id), \
                    (iceberg_type, index)


class TestEveryDeclaredTypeHasAFormula:
    """Walks the specs rather than a list kept here.

    A type with no arm did not fail: every writer and the validator fell through
    to a `value_{row_id}` string, so the column was written as a string, compared
    against a string and reported valid -- coverage the corpus did not have.
    `fixed(16)` is declared in table_specs.yaml and that is what happened to it.

    Deriving the cases from the YAML means adding a type without a formula fails
    here, instead of producing a table of strings under that type's name.
    """

    def test_the_specs_declare_some_types(self):
        # Guards the rest of the class from passing because nothing was found.
        assert DECLARED_TYPES, 'no primitive_types found in any spec file'

    def test_every_file_expected_to_declare_types_does(self):
        # Without this, a spec file renamed or a key renamed drops silently out
        # of the coverage below and everything still passes.
        found = {spec_file for spec_file, _, _ in DECLARED_TYPES}
        assert found == FILES_WITH_PRIMITIVE_TYPES & {p.name for p in SPEC_FILES}, found

    def test_every_declared_type_has_one(self):
        missing = []
        for spec_file, label, iceberg_type in DECLARED_TYPES:
            if iceberg_type.startswith(COMPLEX_PREFIXES):
                continue
            try:
                primitive_value(iceberg_type, 7)
            except KeyError:
                missing.append(f'{spec_file}:{label} ({iceberg_type})')
        assert not missing, f'declared with no value formula: {missing}'

    def test_every_declared_type_varies_by_row(self):
        # A constant compares equal on every row and catches nothing, which is
        # what a fallthrough produces once it stops reading the row id.
        constant = []
        for spec_file, label, iceberg_type in DECLARED_TYPES:
            if iceberg_type.startswith(COMPLEX_PREFIXES):
                continue
            if primitive_value(iceberg_type, 7) == primitive_value(iceberg_type, 8):
                constant.append(f'{spec_file}:{label} ({iceberg_type})')
        assert not constant, f'same value for different rows: {constant}'

    def test_the_validator_expects_the_same_value_for_every_label(self):
        # The validator is keyed on the spec label, the writers on the Iceberg
        # type, so this is where the two naming schemes have to line up.
        for spec_file, label, iceberg_type in DECLARED_TYPES:
            if iceberg_type.startswith(COMPLEX_PREFIXES):
                continue
            for row_id in ROW_IDS:
                assert validator._generate_primitive_data(label, row_id) == \
                    primitive_value(iceberg_type, row_id), (spec_file, label, row_id)

    def test_polars_writes_the_same_value_for_every_declared_type(self):
        from iceberg_fuzzer.backend.polars.generator import PolarsGenerator
        generator = PolarsGenerator.__new__(PolarsGenerator)
        for spec_file, label, iceberg_type in DECLARED_TYPES:
            if iceberg_type.startswith(COMPLEX_PREFIXES):
                continue
            start_id = 5
            column = generator._generate_polars_data('value', iceberg_type, 4, start_id)
            for index, value in enumerate(column):
                assert value == primitive_value(iceberg_type, index + start_id), \
                    (spec_file, label, iceberg_type, index)

    def test_polars_declares_the_column_as_the_type_the_spec_asked_for(self):
        # The value agreeing is not enough: the column it is written into has to
        # be the declared type. `fixed(16)` fell through to `string` here, so a
        # 16-byte value was cast to text and the table held strings under a name
        # saying fixed(16).
        from iceberg_fuzzer.backend.polars.generator import PolarsGenerator
        generator = PolarsGenerator.__new__(PolarsGenerator)
        wrong = []
        for spec_file, label, iceberg_type in DECLARED_TYPES:
            declared = _normalised(str(generator._get_iceberg_type(iceberg_type)))
            if declared != _normalised(iceberg_type):
                wrong.append(f'{spec_file}:{label} asked {iceberg_type}, got {declared}')
        assert not wrong, wrong


class TestFixedIsExactlyItsDeclaredWidth:
    """Iceberg's fixed(L) and Arrow's fixed_size_binary(L) reject another width."""

    @pytest.mark.parametrize('length', [1, 4, 16, 32])
    def test_the_value_is_l_bytes(self, length):
        value = primitive_value(f'fixed({length})', 12345)
        assert isinstance(value, bytes)
        assert len(value) == length
