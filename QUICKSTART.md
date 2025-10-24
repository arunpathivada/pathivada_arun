# Quick Start Guide

## Fast Setup (5 commands)

```bash
# 1. Start MySQL
docker-compose -f docker-compose.initial.yml up --build -d

# 2. Install dependencies to run the project
pip install -r requirements.txt

# 3. Create database schema
docker exec -i mysql_ctn mysql -u db_user -p6equj5_db_user home_db < sql/01_create_schema.sql

# 4. Run ETL pipeline
python scripts/run_etl.py

# 5. Verify data loaded
docker exec -it mysql_ctn mysql -u db_user -p6equj5_db_user home_db -e "SELECT 'property', COUNT(*) FROM property UNION ALL SELECT 'leads', COUNT(*) FROM leads UNION ALL SELECT 'taxes', COUNT(*) FROM taxes;"
```

## What Gets Created

### Database Tables (6 tables)
- **property** - Main property information (10,088 records)
- **leads** - Sales/lead tracking (10,088 records)
- **taxes** - Property tax data (10,088 records)
- **valuation** - Multiple valuations per property (~40K records)
- **hoa** - HOA fees and flags (~20K records)
- **rehab** - Rehabilitation estimates (~30K records)

### ETL Pipeline
- **PySpark-based** distributed data processing
- Normalizes nested JSON into relational tables
- Handles 1:N relationships (arrays) properly
- Full logging and error handling

## Accessing MySQL

### Via DBeaver (GUI)
- **Host:** localhost
- **Port:** 3306
- **Database:** home_db
- **Username:** db_user
- **Password:** 6equj5_db_user

### Via Command Line
```bash
docker exec -it mysql_ctn mysql -u db_user -p6equj5_db_user home_db
```

## File Overview

- `sql/01_create_schema.sql` - Database DDL (creates 6 normalized tables)
- `scripts/etl_pyspark.py` - Main PySpark ETL script
- `scripts/run_etl.py` - Automated runner (handles JDBC driver download)
- `requirements.txt` - Python dependencies (PySpark, MySQL connector)
- `docs/README.md` - Complete documentation

## Requirements

- Docker (for MySQL)
- Python 3.8+
- Java 8 or 11 (for PySpark)

## Sample Query

```sql
-- Properties with high rehab costs
SELECT 
    p.address,
    p.city,
    p.state,
    r.underwriting_rehab,
    v.list_price
FROM property p
JOIN rehab r ON p.property_id = r.property_id
JOIN valuation v ON p.property_id = v.property_id
WHERE r.underwriting_rehab > 50000
ORDER BY r.underwriting_rehab DESC
LIMIT 10;
```

See `docs/README.md` for detailed documentation.

