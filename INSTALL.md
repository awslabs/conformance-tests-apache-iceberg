# Installation Guide

## Prerequisites

- **Python 3.9+** (tested up to 3.13)
- **Java 21+** (tested up to 23)
- **Build tools** (gcc, build-essential) for compiling native dependencies

## Quick Setup with uv (Recommended)

### 1. Install uv
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. Clone and Setup
```bash
git clone https://github.com/awslabs/apache-iceberg-table-fuzzer.git
cd apache-iceberg-table-fuzzer
./setup.sh
```

### 3. Activate and Test
```bash
source .venv/bin/activate
python -m iceberg_fuzzer.cli --help
```

## Manual Setup

### Install Java

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install -y openjdk-21-jdk build-essential
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
echo 'export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64' >> ~/.bashrc
```

**Amazon Linux/RHEL/CentOS:**
```bash
sudo yum install -y java-21-amazon-corretto java-21-amazon-corretto-devel
sudo yum groupinstall -y "Development Tools"
export JAVA_HOME=/usr/lib/jvm/java-21-amazon-corretto
echo 'export JAVA_HOME=/usr/lib/jvm/java-21-amazon-corretto' >> ~/.bashrc
```

**macOS:**
```bash
brew install openjdk@21
export JAVA_HOME=$(brew --prefix openjdk@21)/libexec/openjdk.jdk/Contents/Home
echo 'export JAVA_HOME=$(brew --prefix openjdk@21)/libexec/openjdk.jdk/Contents/Home' >> ~/.zshrc
```

### Install with uv
```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install dependencies
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv sync

# Or install in development mode
uv sync --dev
```

### Alternative: Install with pip
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e .

# Or for development
pip install -e ".[dev]"
```

## Verify Installation

```bash
# Test the CLI
python -m iceberg_fuzzer.cli --help

# Test all imports
python -c "import pyspark, pyiceberg, polars, pyarrow, yaml, boto3, sqlalchemy; print('All imports successful')"

# Check Java
java -version
echo $JAVA_HOME
```

## Usage

```bash
# Generate simple tables with PyIceberg
python -m iceberg_fuzzer.cli generate --backend pyiceberg --warehouse-path /tmp/warehouse --specs specs/simple.yaml --random 3

# Generate tables with Polars
python -m iceberg_fuzzer.cli generate --backend polars --warehouse-path /tmp/warehouse --specs specs/simple.yaml --random 3

# Generate with PySpark (requires Java)
python -m iceberg_fuzzer.cli generate --backend pyspark --warehouse-path /tmp/warehouse --specs specs/simple.yaml --random 3
```

## Dependencies

All dependencies are managed through `pyproject.toml`:

**Core dependencies:**
- `pyspark==3.5.3` - Apache Spark for Python
- `pyiceberg>=0.5.0` - Python implementation of Apache Iceberg
- `polars>=0.20.0` - Fast DataFrame library
- `pyarrow>=12.0.0` - Columnar in-memory analytics
- `pyyaml>=6.0` - YAML parser
- `boto3>=1.26.0` - AWS SDK for Python
- `sqlalchemy>=1.4.0` - SQL toolkit

**Development dependencies (optional):**
- `pytest>=7.0.0` - Testing framework
- `black>=22.0.0` - Code formatter
- `flake8>=4.0.0` - Linter

## Troubleshooting

### Java Issues
```bash
# Check Java installation
java -version
echo $JAVA_HOME

# Install Java if missing (Ubuntu)
sudo apt install -y openjdk-21-jdk
```

### Build Issues
```bash
# Install build tools (Ubuntu)
sudo apt install -y build-essential gcc

# Install build tools (CentOS/RHEL)
sudo yum groupinstall -y "Development Tools"
```

### Python Issues
```bash
# Check Python version (must be 3.9+)
python --version

# Check virtual environment
which python
```

### uv Issues
```bash
# Update uv to latest version
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clear cache if needed
uv cache clean
```
