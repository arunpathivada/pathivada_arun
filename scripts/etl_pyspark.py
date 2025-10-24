#!/usr/bin/env python3
"""
PySpark ETL Script for Property Data Normalization

This script reads raw property data from JSON, transforms it into a normalized
relational structure, and loads it into MySQL database.

Author: Pathivada Arun
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, explode, monotonically_increasing_id, row_number, lit, 
    regexp_replace, trim, when, upper
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, ArrayType, DecimalType
)
from pyspark.sql.window import Window
import sys
import logging
from typing import Optional, Dict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PropertyETL:
    """ETL Pipeline for Property Data"""
    
    def __init__(self, json_path: str, mysql_config: Dict):
        """
        Initialize ETL pipeline
        
        Args:
            json_path (str): Path to input JSON file
            mysql_config (dict): MySQL connection configuration
        """
        self.json_path = json_path
        self.mysql_config = mysql_config
        self.spark: Optional[SparkSession] = None
        self.raw_df: Optional[DataFrame] = None
        
    def create_spark_session(self):
        """Create and configure Spark session"""
        logger.info("Creating Spark session...")
        
        self.spark = (SparkSession.builder  # type: ignore
            .appName("PropertyDataETL")  # type: ignore
            .config("spark.jars", "mysql-connector-j-8.2.0.jar")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate())
        
        self.spark.sparkContext.setLogLevel("WARN")  # type: ignore[union-attr]
        logger.info("Spark session created successfully")
        
    def extract_data(self):
        """Extract data from JSON file"""
        logger.info(f"Extracting data from {self.json_path}...")
        
        try:
            df = self.spark.read.option("multiLine", "true").json(self.json_path)  # type: ignore[union-attr]
            
            # Add property_id to raw data for easier joins
            window_spec = Window.orderBy(monotonically_increasing_id())
            self.raw_df = df.withColumn("property_id", row_number().over(window_spec))
            
            count = self.raw_df.count()
            logger.info(f"Successfully extracted {count} records")
            return self.raw_df
        except Exception as e:
            logger.error(f"Error extracting data: {str(e)}")
            raise
    
    def transform_property(self):
        """Transform and create property table dataframe"""
        logger.info("Transforming property data...")
        
        property_df = self.raw_df.select(  # type: ignore[union-attr]
            col("property_id"),
            col("Property_Title").alias("property_title"),
            col("Address").alias("address"),
            col("Street_Address").alias("street_address"),
            col("City").alias("city"),
            col("State").alias("state"),
            col("Zip").alias("zip"),
            col("Market").alias("market"),
            col("Property_Type").alias("property_type"),
            col("Latitude").cast(DecimalType(10, 8)).alias("latitude"),
            col("Longitude").cast(DecimalType(11, 8)).alias("longitude"),
            col("Subdivision").alias("subdivision"),
            
            # Property characteristics
            col("Year_Built").cast(IntegerType()).alias("year_built"),
            col("Bed").cast(IntegerType()).alias("bed"),
            col("Bath").cast(IntegerType()).alias("bath"),
            col("SQFT_Total").alias("sqft_total"),
            col("SQFT_MU").cast(IntegerType()).alias("sqft_mu"),
            col("SQFT_Basement").cast(IntegerType()).alias("sqft_basement"),
            col("BasementYesNo").alias("basement_yes_no"),
            col("Layout").alias("layout"),
            col("Parking").alias("parking"),
            
            # Property features
            col("Flood").alias("flood"),
            col("Highway").alias("highway"),
            col("Train").alias("train"),
            col("HTW").alias("htw"),
            col("Pool").alias("pool"),
            col("Commercial").alias("commercial"),
            col("Water").alias("water"),
            col("Sewage").alias("sewage"),
            
            # Financial and location metrics
            col("Tax_Rate").cast(DecimalType(10, 4)).alias("tax_rate"),
            col("Rent_Restricted").alias("rent_restricted"),
            col("Neighborhood_Rating").cast(IntegerType()).alias("neighborhood_rating"),
            col("School_Average").cast(DecimalType(5, 2)).alias("school_average")
        )
        
        logger.info(f"Property table created with {property_df.count()} records")
        return property_df
    
    def transform_leads(self, property_df):
        """Transform and create leads table dataframe"""
        logger.info("Transforming leads data...")
        
        leads_df = self.raw_df.select(  # type: ignore[union-attr]
            col("property_id"),
            col("Reviewed_Status").alias("reviewed_status"),
            col("Most_Recent_Status").alias("most_recent_status"),
            col("Source").alias("source"),
            col("Occupancy").alias("occupancy"),
            col("Net_Yield").cast(DecimalType(10, 4)).alias("net_yield"),
            col("IRR").cast(DecimalType(10, 4)).alias("irr"),
            col("Selling_Reason").alias("selling_reason"),
            col("Seller_Retained_Broker").alias("seller_retained_broker"),
            col("Final_Reviewer").alias("final_reviewer")
        )
        
        logger.info(f"Leads table created with {leads_df.count()} records")
        return leads_df
    
    def transform_taxes(self, property_df):
        """Transform and create taxes table dataframe"""
        logger.info("Transforming taxes data...")
        
        taxes_df = self.raw_df.select(  # type: ignore[union-attr]
            col("property_id"),
            col("Taxes").cast(DecimalType(12, 2)).alias("taxes")
        )
        
        logger.info(f"Taxes table created with {taxes_df.count()} records")
        return taxes_df
    
    def transform_valuation(self, property_df):
        """Transform and create valuation table dataframe"""
        logger.info("Transforming valuation data...")
        
        # Explode the Valuation array
        valuation_df = self.raw_df.select(  # type: ignore[union-attr]
            col("property_id"),
            explode(col("Valuation")).alias("valuation_record")
        ).select(
            col("property_id"),
            col("valuation_record.List_Price").cast(DecimalType(12, 2)).alias("list_price"),
            col("valuation_record.Zestimate").cast(DecimalType(12, 2)).alias("zestimate"),
            col("valuation_record.ARV").cast(DecimalType(12, 2)).alias("arv"),
            col("valuation_record.Redfin_Value").cast(DecimalType(12, 2)).alias("redfin_value"),
            col("valuation_record.Previous_Rent").cast(DecimalType(10, 2)).alias("previous_rent"),
            col("valuation_record.Expected_Rent").cast(DecimalType(10, 2)).alias("expected_rent"),
            col("valuation_record.Rent_Zestimate").cast(DecimalType(10, 2)).alias("rent_zestimate"),
            col("valuation_record.Low_FMR").cast(DecimalType(10, 2)).alias("low_fmr"),
            col("valuation_record.High_FMR").cast(DecimalType(10, 2)).alias("high_fmr")
        )
        
        logger.info(f"Valuation table created with {valuation_df.count()} records")
        return valuation_df
    
    def transform_hoa(self, property_df):
        """Transform and create HOA table dataframe"""
        logger.info("Transforming HOA data...")
        
        # Explode the HOA array
        hoa_df = self.raw_df.select(  # type: ignore[union-attr]
            col("property_id"),
            explode(col("HOA")).alias("hoa_record")
        ).select(
            col("property_id"),
            col("hoa_record.HOA").cast(DecimalType(10, 2)).alias("hoa"),
            col("hoa_record.HOA_Flag").alias("hoa_flag")
        )
        
        logger.info(f"HOA table created with {hoa_df.count()} records")
        return hoa_df
    
    def transform_rehab(self, property_df):
        """Transform and create rehab table dataframe"""
        logger.info("Transforming rehab data...")
        
        # Explode the Rehab array
        rehab_df = self.raw_df.select(  # type: ignore[union-attr]
            col("property_id"),
            explode(col("Rehab")).alias("rehab_record")
        ).select(
            col("property_id"),
            col("rehab_record.Underwriting_Rehab").cast(DecimalType(12, 2)).alias("underwriting_rehab"),
            col("rehab_record.Rehab_Calculation").cast(DecimalType(12, 2)).alias("rehab_calculation"),
            col("rehab_record.Paint").alias("paint"),
            col("rehab_record.Flooring_Flag").alias("flooring_flag"),
            col("rehab_record.Foundation_Flag").alias("foundation_flag"),
            col("rehab_record.Roof_Flag").alias("roof_flag"),
            col("rehab_record.HVAC_Flag").alias("hvac_flag"),
            col("rehab_record.Kitchen_Flag").alias("kitchen_flag"),
            col("rehab_record.Bathroom_Flag").alias("bathroom_flag"),
            col("rehab_record.Appliances_Flag").alias("appliances_flag"),
            col("rehab_record.Windows_Flag").alias("windows_flag"),
            col("rehab_record.Landscaping_Flag").alias("landscaping_flag"),
            col("rehab_record.Trashout_Flag").alias("trashout_flag")
        )
        
        logger.info(f"Rehab table created with {rehab_df.count()} records")
        return rehab_df
    
    def load_to_mysql(self, df, table_name, mode="append"):
        """
        Load dataframe to MySQL table
        
        Args:
            df: Spark DataFrame to load
            table_name (str): Target table name
            mode (str): Write mode ('append', 'overwrite', etc.)
        """
        logger.info(f"Loading data to table: {table_name}...")
        
        jdbc_url = f"jdbc:mysql://{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['database']}"
        
        try:
            df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", self.mysql_config['user']) \
                .option("password", self.mysql_config['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .mode(mode) \
                .save()
            
            logger.info(f"Successfully loaded {df.count()} records to {table_name}")
        except Exception as e:
            logger.error(f"Error loading data to {table_name}: {str(e)}")
            raise
    
    def run_etl(self):
        """Execute the complete ETL pipeline"""
        try:
            logger.info("=" * 70)
            logger.info("STARTING ETL PIPELINE")
            logger.info("=" * 70)
            
            # Create Spark session
            self.create_spark_session()
            
            # Extract
            self.extract_data()
            
            # Transform
            property_df = self.transform_property()
            leads_df = self.transform_leads(property_df)
            taxes_df = self.transform_taxes(property_df)
            valuation_df = self.transform_valuation(property_df)
            hoa_df = self.transform_hoa(property_df)
            rehab_df = self.transform_rehab(property_df)
            
            # Load (in correct order due to foreign key constraints)
            logger.info("\n" + "=" * 70)
            logger.info("LOADING DATA TO MYSQL")
            logger.info("=" * 70)
            
            self.load_to_mysql(property_df, "property", mode="append")
            self.load_to_mysql(leads_df, "leads", mode="append")
            self.load_to_mysql(taxes_df, "taxes", mode="append")
            self.load_to_mysql(valuation_df, "valuation", mode="append")
            self.load_to_mysql(hoa_df, "hoa", mode="append")
            self.load_to_mysql(rehab_df, "rehab", mode="append")
            
            logger.info("\n" + "=" * 70)
            logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 70)
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {str(e)}")
            raise
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")


def main():
    """Main execution function"""
    
    # Configuration
    JSON_PATH = "data/fake_property_data_new.json"
    
    MYSQL_CONFIG = {
        'host': 'localhost',
        'port': 3306,
        'database': 'home_db',
        'user': 'db_user',
        'password': '6equj5_db_user'
    }
    
    # Create and run ETL pipeline
    etl = PropertyETL(JSON_PATH, MYSQL_CONFIG)
    etl.run_etl()


if __name__ == "__main__":
    main()

