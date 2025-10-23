#!/usr/bin/env python3
"""
Simple ETL Runner Script

This script provides a simple way to run the PySpark ETL pipeline.
It handles downloading the MySQL connector if needed and runs the ETL.
"""

import os
import sys
import subprocess
import urllib.request

def download_mysql_connector():
    """Download MySQL JDBC connector if not present"""
    lib_dir = "lib"
    jar_file = os.path.join(lib_dir, "mysql-connector-j-8.2.0.jar")
    
    if os.path.exists(jar_file):
        print(f"✓ MySQL connector already exists: {jar_file}")
        return jar_file
    
    print("Downloading MySQL JDBC connector...")
    os.makedirs(lib_dir, exist_ok=True)
    
    url = "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.2.0/mysql-connector-j-8.2.0.jar"
    
    try:
        urllib.request.urlretrieve(url, jar_file)
        print(f"✓ Downloaded MySQL connector to: {jar_file}")
        return jar_file
    except Exception as e:
        print(f"✗ Error downloading MySQL connector: {e}")
        sys.exit(1)

def run_etl():
    """Run the PySpark ETL script"""
    # Download MySQL connector
    jar_file = download_mysql_connector()
    
    # Run PySpark ETL
    print("\n" + "=" * 70)
    print("RUNNING PYSPARK ETL PIPELINE")
    print("=" * 70 + "\n")
    
    cmd = [
        "spark-submit",
        "--jars", jar_file,
        "--driver-memory", "4g",
        "--executor-memory", "4g",
        "scripts/etl_pyspark.py"
    ]
    
    try:
        result = subprocess.run(cmd, check=True)
        print("\n✓ ETL pipeline completed successfully!")
        return 0
    except subprocess.CalledProcessError as e:
        print(f"\n✗ ETL pipeline failed with error code: {e.returncode}")
        return 1
    except FileNotFoundError:
        print("\n✗ Error: spark-submit not found. Please install PySpark.")
        print("Run: pip install -r requirements.txt")
        return 1

if __name__ == "__main__":
    sys.exit(run_etl())

