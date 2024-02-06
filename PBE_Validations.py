#!/usr/bin/env python
# coding: utf-8

# # **Validations**

# ### **Generic Validations**

# In[1]:


import pyspark.sql.functions as F
from pyspark.sql import Window
import pandas as pd
import warnings
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from pandas import read_csv
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, BooleanType, StringType
warnings.filterwarnings("ignore")

class DataValidationHandler:
    def __init__(self, spark, missing_threshold=2880):
        """
        Initialize the DataValidationHandler.

        Parameters:
        - spark: SparkSession instance
        - missing_threshold: Threshold for missing values (default is 60)
        """
        self.spark = spark
        self.missing_threshold = missing_threshold

    def perform_generic_validations(self, df):
        """
        Perform generic validations on inverter data.

        Parameters:
        - df: PySpark DataFrame

        Returns:
        - df_validated_generic: DataFrame after generic validations
        - df_validated_failed_generic: DataFrame containing failed generic validations
        """
        df_validated_generic = self.validate_inverter_data(df)
        df_validated_generic = self.flag_duplicate_timestamps(df_validated_generic)
        df_validated_generic, df_validated_failed_generic = self.separate_failed_and_valid_records(df_validated_generic)
        return df_validated_generic, df_validated_failed_generic

    def validate_inverter_data(self, df):
        """
        Validate inverter data based on row count and rejection threshold.

        Parameters:
        - df: PySpark DataFrame

        Returns:
        - df_out: Updated DataFrame with 'Is_Rejected' column indicating rejected inverters
        """
        # Calculate the count of rows for each inverter
        row_count_for_each_inverter = df.groupBy("inverter_serial_no").count()

        # Identify inverters with a count below the threshold
        less_num_of_inverters = row_count_for_each_inverter[row_count_for_each_inverter['count'] < 17521 - self.missing_threshold]

        # Join data to mark the rejected inverters and create 'Is_Rejected' column
        updated_df = df.join(less_num_of_inverters, on="inverter_serial_no", how="left").withColumn("Is_Rejected", F.coalesce(less_num_of_inverters["count"], F.lit(0)))

        # Replace values greater than 1 with 1 in the 'Is_Rejected' column
        df_out = updated_df.withColumn("Is_Rejected", F.when(F.col("Is_Rejected") > 0, 1).otherwise(F.col("Is_Rejected")))

        return df_out

    def flag_duplicate_timestamps(self, df):
        """
        Flag duplicate timestamps in the DataFrame based on 'inverter_serial_no' and 'date_time'.

        Parameters:
        - df: PySpark DataFrame

        Returns:
        - df_out: DataFrame with an 'Is_Duplicate' column indicating duplicate timestamps
        """
        # Group data to find duplicate timestamps based on 'inverter_serial_no' and 'date_time'
        duplicate_counts = df.groupBy("inverter_serial_no", "date_time").count().filter(F.col("count") > 1)

        # Join data to mark the rows with duplicate timestamps and create 'Is_Duplicate' column
        updated_df = df.join(duplicate_counts, on=["inverter_serial_no", "date_time"], how="left").withColumn("Is_Duplicate", F.coalesce(duplicate_counts["count"], F.lit(0)))

        # Replace values greater than 1 with 1 in the 'Is_Duplicate' column
        df_out = updated_df.withColumn("Is_Duplicate", F.when(F.col("Is_Duplicate") > 0, 1).otherwise(F.col("Is_Duplicate")))

        return df_out

    def separate_failed_and_valid_records(self, result_df):
        """
        Separate failed and valid records based on 'Is_Rejected' and 'Is_Duplicate' flags.

        Parameters:
        - result_df: PySpark DataFrame containing flags 'Is_Rejected' and 'Is_Duplicate'

        Returns:
        - valid_df: DataFrame containing valid records (without 'Is_Rejected' and 'Is_Duplicate' columns)
        - failed_rows: DataFrame containing failed records based on the flags
        """
        # Get failed rows based on 'Is_Rejected' or 'Is_Duplicate' flags
        failed_rows = result_df.filter((result_df['Is_Rejected'] == 1) | (result_df['Is_Duplicate'] == 1))

        # Filter result_df for non-failed rows by excluding 'Is_Rejected' and 'Is_Duplicate' flags
        valid_df = result_df.filter((result_df['Is_Rejected'] == 0) & (result_df['Is_Duplicate'] == 0))

        # Drop columns 'Is_Rejected', 'Is_Duplicate', 'count' from valid_df
        valid_df = valid_df.drop('Is_Rejected', 'Is_Duplicate', 'count')

        return valid_df, failed_rows

    def level_2_validations(self, df):
        """
        Apply level 2 validations to the DataFrame.

        Parameters:
        - df: PySpark DataFrame

        Returns:
        - df_with_errors: DataFrame with added columns for validation results and error messages
        """
        df_with_errors = self.apply_validation_logic(df, self.validate_data)
        return df_with_errors

    
    @staticmethod
    def validate_data(row):
        errors = []
        if row['Instantaneous_on_grid_total_yield_kwh'] < 0:
            errors.append("On Grid Total Cannot be Negative")
        if row['Instantaneous_total_feed_in_energy_kwh'] < 0:
            errors.append("Feed in energy Cannot be Negative")
        if row['battery_capacity_percentage'] < 10 and row['Instantaneous_total_feed_in_energy_kwh'] > 0:
            errors.append("Feed in cannot happen if battery % is less than 10")
        if row['Instantaneous_total_feed_in_energy_kwh'] > 0 and row['total_consume_energy_from_grid_kwh'] > 0:
            errors.append("Feed in and consumption can not happen same time")
        if row['total_consume_energy_from_grid_kwh'] < 0:
            errors.append("Consumption from grid cannot be negative")
        if row['battery_capacity_percentage'] < 0 or row['battery_capacity_percentage'] > 100:
            errors.append("Battery capacity should be within the range")
        if row['Instantaneous_total_battery_charge_kwh'] > 0 and row['Instantaneous_total_battery_discharge_kwh'] > 0:
            errors.append("Battery charge and discharge cannot happen at same time")
        if row['Instantaneous_total_battery_charge_kwh'] < 0:
            errors.append("Battery charge be negative")
        if row['Instantaneous_total_battery_discharge_kwh'] < 0:
            errors.append("Battery discharge cannot be negative")
        if row['Total_power_used_in_house'] == 0 and row['Instantaneous_total_feed_in_energy_kwh'] == 0 and row['Instantaneous_total_battery_discharge_kwh'] > 0:
            errors.append("Battery discharge can not have any value if there is no consumption and feed in to grid")
        if row["Instantaneous_pv_total_power_generation_kwh"] < 0:
            errors.append("PV Power generation can not be negative")
        if row["TimeofDay"] == 'Night' and row["Instantaneous_pv_total_power_generation_kwh"] != 0:
            errors.append("PV value can not be at night")
        if row['Instantaneous_total_consume_energy_from_grid_kwh'] > 0 and row['Total_power_used_in_house'] == 0:
            errors.append("Cannot consume from grid, if no usage")

        if errors:
            return False, ', '.join(errors)
        else:
            return True, "Valid"

    
    @staticmethod
    def apply_validation_logic(df, validate_udf):
        """
        Apply validation logic to each row and add new columns with validation results and error messages.

        Args:
        - df: PySpark DataFrame containing the data
        - validate_udf: User-defined function (UDF) for validation logic

        Returns:
        - df_with_errors: DataFrame with added columns for validation results and error messages
        """
        df_with_errors = df.withColumn("validation_result", validate_udf(F.struct(*df.columns)))
        schema = StructType([
            StructField("is_valid", BooleanType(), True),
            StructField("error_message", StringType(), True)
        ])
        df_with_errors = df_with_errors.withColumn("validation_result", F.col("validation_result").cast(schema))
        df_with_errors = df_with_errors.withColumn("is_valid", F.col("validation_result.is_valid"))
        df_with_errors = df_with_errors.withColumn("error_message", F.col("validation_result.error_message"))
        return df_with_errors

    def validate_data_udf(self,df):
        validate_udf = udf(self.validate_data, StructType([StructField("is_valid", BooleanType()), StructField("error_message", StringType())]))
        df_validated = self.apply_validation_logic(df,validate_udf)
        return df_validated




# In[ ]:




