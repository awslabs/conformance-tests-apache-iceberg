#!/bin/bash
# Verify prerequisites and setup dependencies

set -e

echo "Verifying prerequisites for Conformance Tests for Apache Iceberg..."

# Check Python version. Must match requires-python in pyproject.toml, otherwise
# this script passes and the `uv sync` at the end fails on the constraint.
if ! command -v python3 &> /dev/null; then
    echo "❌ ERROR: python3 not found. Please install Python 3.12+ first."
    exit 1
fi

PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)

if [ "$PYTHON_MAJOR" -lt 3 ] || ([ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -lt 12 ]); then
    echo "❌ ERROR: Python $PYTHON_VERSION found, but Python 3.12+ is required."
    exit 1
fi
echo "✓ Python $PYTHON_VERSION found"

# Check Java version. PySpark 3.5.3 needs 21-23: Java 24 permanently disabled the
# Security Manager, and Spark's use of it fails at session startup with
# "UnsupportedOperationException: getSubject is not supported", which gives no
# hint that the Java version is the problem. Reject it up front instead.
if ! command -v java &> /dev/null; then
    echo "❌ ERROR: java not found. Please install Java 21 first."
    exit 1
fi

JAVA_VERSION=$(java -version 2>&1 | head -n1 | cut -d'"' -f2 | cut -d'.' -f1)
if [ "$JAVA_VERSION" -lt 21 ]; then
    echo "❌ ERROR: Java $JAVA_VERSION found, but PySpark 3.5.3 requires Java 21-23."
    exit 1
fi
if [ "$JAVA_VERSION" -gt 23 ]; then
    echo "❌ ERROR: Java $JAVA_VERSION found, but PySpark 3.5.3 supports Java 21-23 only."
    echo "  Java 24+ fails at Spark startup with 'getSubject is not supported'."
    echo "  Install Java 21 and point JAVA_HOME at it."
    exit 1
fi
echo "✓ Java $JAVA_VERSION found"

# Check JAVA_HOME
if [ -z "$JAVA_HOME" ]; then
    echo "❌ ERROR: JAVA_HOME environment variable not set."
    echo "Please set JAVA_HOME to your Java 21 installation directory."
    exit 1
fi

# Verify JAVA_HOME points to a supported Java
if [ ! -f "$JAVA_HOME/bin/java" ]; then
    echo "❌ ERROR: JAVA_HOME ($JAVA_HOME) does not contain a valid Java installation."
    exit 1
fi

JAVA_HOME_VERSION=$("$JAVA_HOME/bin/java" -version 2>&1 | head -n1 | cut -d'"' -f2 | cut -d'.' -f1)
if [ "$JAVA_HOME_VERSION" -lt 21 ]; then
    echo "❌ ERROR: JAVA_HOME points to Java $JAVA_HOME_VERSION, but PySpark 3.5.3 requires Java 21-23."
    exit 1
fi
if [ "$JAVA_HOME_VERSION" -gt 23 ]; then
    echo "❌ ERROR: JAVA_HOME points to Java $JAVA_HOME_VERSION, but PySpark 3.5.3 supports Java 21-23 only."
    echo "  Java 24+ fails at Spark startup with 'getSubject is not supported'."
    exit 1
fi
echo "✓ JAVA_HOME verified: $JAVA_HOME (Java $JAVA_HOME_VERSION)"

# Install dependencies with uv
if ! command -v uv &> /dev/null; then
    echo "❌ ERROR: uv not found. Please install uv first:"
    echo "  curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi
echo "✓ uv found"

uv sync

echo ""
echo "✅ Setup complete!"
echo "Activate with: source .venv/bin/activate"
echo "Test with: python -m iceberg_fuzzer.cli --help"
