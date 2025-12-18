# KONA Installation Guide

## System Requirements

### Operating System Packages (Amazon Linux 2/RHEL/CentOS)

```bash
# Install Python 3.9+ and development tools
sudo yum update -y
sudo yum install -y python3 python3-pip python3-devel
sudo yum install -y gcc gcc-c++ make
sudo yum install -y java-21-amazon-corretto java-21-amazon-corretto-devel
sudo yum install -y git

# Set JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-21-amazon-corretto
echo 'export JAVA_HOME=/usr/lib/jvm/java-21-amazon-corretto' >> ~/.bashrc
```

### Operating System Packages (Ubuntu/Debian)

```bash
# Install Python 3.9+ and development tools
sudo apt update
sudo apt install -y python3 python3-pip python3-dev
sudo apt install -y build-essential
sudo apt install -y openjdk-21-jdk
sudo apt install -y git

# Set JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
echo 'export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64' >> ~/.bashrc
```

## Python Requirements

- Python 3.9+
- Java 21+ (for PySpark)

## Installation Methods

### Virtual Environment Setup (Recommended)

```bash
git clone https://github.com/gsoundar/iceberg-table-generator.git
cd iceberg-table-generator
python3 -m venv kona-env
source kona-env/bin/activate
pip install -r requirements.txt
```

### Development Installation

```bash
git clone https://github.com/gsoundar/iceberg-table-generator.git
cd iceberg-table-generator
python3 -m venv kona-env
source kona-env/bin/activate
pip install -r requirements.txt
pip install pytest black flake8  # Optional dev tools
```

## Verify Installation

```bash
# Test the CLI
python3 -m kona.kona_cli --help

# Test Python import
python3 -c "import kona; print('KONA imported successfully')"
```

## Usage

```bash
# Generate tables with PySpark
python3 -m kona.kona_cli generate --backend pyspark --warehouse-path /tmp/warehouse --specs table_specs.yaml

# Generate tables with PyIceberg
python3 -m kona.kona_cli generate --backend pyiceberg --warehouse-path /tmp/warehouse --specs specs/simple.yaml

# Delete all tables
python3 -m kona.kona_cli delete --backend pyspark --warehouse-path /tmp/warehouse --confirm
```

## Troubleshooting

### Java Issues
- Ensure `JAVA_HOME` is set: `echo $JAVA_HOME`
- Verify Java version: `java -version` (should be 21+)

### Python Issues
- Verify Python version: `python3 --version` (should be 3.9+)
- Check virtual environment is activated: `which python3`

### Permission Issues
- Ensure warehouse directory is writable
- Use absolute paths for warehouse locations
