#!/usr/bin/env python3
"""
Database Schema Visualization and Stats

This script connects to the MySQL database and displays:
- Table structures
- Record counts
- Sample data from each table
"""

import mysql.connector
from mysql.connector import Error

def create_connection():
    """Create MySQL connection"""
    try:
        connection = mysql.connector.connect(
            host='localhost',
            port=3306,
            database='home_db',
            user='db_user',
            password='6equj5_db_user'
        )
        if connection.is_connected():
            print("✓ Successfully connected to MySQL database\n")
            return connection
    except Error as e:
        print(f"✗ Error connecting to MySQL: {e}")
        return None

def get_table_stats(connection):
    """Get statistics for all tables"""
    cursor = connection.cursor()
    
    print("=" * 70)
    print("DATABASE STATISTICS")
    print("=" * 70)
    
    tables = ['property', 'leads', 'taxes', 'valuation', 'hoa', 'rehab']
    
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"{table:20} {count:>10,} records")
        except Error as e:
            print(f"{table:20} Error: {e}")
    
    print()

def show_table_structure(connection, table_name):
    """Show table structure"""
    cursor = connection.cursor()
    
    print(f"\n{table_name.upper()} TABLE STRUCTURE")
    print("-" * 70)
    
    cursor.execute(f"DESCRIBE {table_name}")
    rows = cursor.fetchall()
    
    print(f"{'Field':<30} {'Type':<20} {'Null':<8} {'Key':<8}")
    print("-" * 70)
    for row in rows:
        print(f"{row[0]:<30} {row[1]:<20} {row[2]:<8} {row[3]:<8}")

def show_sample_data(connection, table_name, limit=3):
    """Show sample data from table"""
    cursor = connection.cursor()
    
    print(f"\n{table_name.upper()} - SAMPLE DATA (First {limit} records)")
    print("-" * 70)
    
    cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
    rows = cursor.fetchall()
    
    # Get column names
    cursor.execute(f"DESCRIBE {table_name}")
    columns = [col[0] for col in cursor.fetchall()]
    
    for i, row in enumerate(rows, 1):
        print(f"\nRecord {i}:")
        for col, val in zip(columns, row):
            if val is not None:
                print(f"  {col}: {val}")

def show_relationships(connection):
    """Show foreign key relationships"""
    cursor = connection.cursor()
    
    print("\n" + "=" * 70)
    print("FOREIGN KEY RELATIONSHIPS")
    print("=" * 70)
    
    query = """
    SELECT 
        TABLE_NAME,
        COLUMN_NAME,
        REFERENCED_TABLE_NAME,
        REFERENCED_COLUMN_NAME
    FROM
        INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE
        REFERENCED_TABLE_SCHEMA = 'home_db'
        AND REFERENCED_TABLE_NAME IS NOT NULL
    ORDER BY TABLE_NAME
    """
    
    cursor.execute(query)

    
    rows = cursor.fetchall()
    
    print(f"{'Table':<15} {'Column':<20} {'References':<15} {'Column':<20}")
    print("-" * 70)
    for row in rows:
        print(f"{row[0]:<15} {row[1]:<20} → {row[2]:<15} {row[3]:<20}")

def main():
    """Main function"""
    connection = create_connection()
    
    if not connection:
        return
    
    try:
        # Show statistics
        get_table_stats(connection)
        
        # Show relationships
        show_relationships(connection)
        
        # Show structure and sample data for main tables
        show_table_structure(connection, 'property')
        show_sample_data(connection, 'property', limit=2)
        
        print("\n" + "=" * 70)
        print("For more details, connect to MySQL using DBeaver or CLI")
        print("=" * 70)
        
    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            connection.close()
            print("\n✓ MySQL connection closed")

if __name__ == "__main__":
    main()

