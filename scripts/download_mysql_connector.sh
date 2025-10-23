#!/bin/bash
# Download MySQL JDBC Driver for PySpark

echo "Downloading MySQL JDBC Driver..."

# Create lib directory if it doesn't exist
mkdir -p lib

# Download MySQL Connector/J
cd lib
wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.2.0/mysql-connector-j-8.2.0.jar

echo "MySQL JDBC Driver downloaded successfully to lib/ directory"

