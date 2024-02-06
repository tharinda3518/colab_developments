#!/usr/bin/env python
# coding: utf-8

# # **Transformation**

# ### **Utility Functions**

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

class DataProcessor:
    def __init__(self,spark):
        self.spark = spark
        
        
    def get_unique_inverter_serial_numbers(self,df):
        """ 
        Extract unique inverter_serial_no values from a PySpark DataFrame and convert them to a Python list.

        Parameters:
        - df: PySpark DataFrame containing 'inverter_serial_no' column

        Returns:
        - unique_inverters_list: List containing unique 'inverter_serial_no' values
        """

        # Fetch unique 'inverter_serial_no' values from DataFrame
        unique_inverters_df = df.select('inverter_serial_no').distinct()

        # Convert DataFrame column to a list
        unique_inverters_list = [row['inverter_serial_no'] for row in unique_inverters_df.collect()]

        return unique_inverters_list

    def reset_dataframe_to_empty(self,df):
        """
        Reset a pandas DataFrame to an empty DataFrame.

        Parameters:
        - df: Input pandas DataFrame

        Returns:
        - df_empty: Empty pandas DataFrame with the same columns as the input DataFrame
        """

        # Create a copy of the input DataFrame
        df_out_fnl = df.copy()

        # Reset the copied DataFrame to an empty DataFrame
        df_out_fnl = df_out_fnl.iloc[0:0]

        return df_out_fnl


    def Get_Regular_Half_Hours(self,df):
    # Function: Get_Regular_Half_Hours
    # Description: Converts the 'date_time' column in a DataFrame to regular half-hour intervals.
    # Parameters:
    #  df (pd.DataFrame): The DataFrame containing the 'date_time' column to be processed.
    # Returns:
    #  pd.DataFrame: A DataFrame with 'date_time' values rounded to the nearest half-hour interval.

        # Round 'date_time' values to the nearest correct half-hour interval
        half_hour_interval = timedelta(minutes=30)
        df['date_time'] = df['date_time'].apply(lambda dt: dt - timedelta(minutes=dt.minute % 30) - timedelta(seconds=dt.second))
        df = df.drop_duplicates(subset='date_time')
        df=df.sort_values(by='date_time')
        return df


    def Merge_Regular_HH_DF(self,df):
    # Function: Merge_Regular_HH_DF
    # Description: Merges a DataFrame with a full one-year time range with half-hour intervals.
    # Parameters:
    #  df (pd.DataFrame): The DataFrame to be merged with the full time range.
    # Returns:
    #  pd.DataFrame: A DataFrame containing the merged data with the full time range.

        # Convert 'date_time' column to datetime
        df['date_time'] = pd.to_datetime(df['date_time'])

        # Calculate the end date for one year
        start_date = datetime(2022, 7, 1, 0, 0, 0)
        end_date = start_date + timedelta(days=365)

        # Generate a full one-year time range with half-hour intervals
        time_interval = timedelta(minutes=30)
        date_range = pd.date_range(start=start_date, end=end_date, freq=time_interval)

        # Create a new DataFrame with the full time range
        full_year_df = pd.DataFrame({'date_time': date_range})

        # Merge the full time range DataFrame with existing data based on 'date_time' column
        merged_df = pd.merge(full_year_df, df, on='date_time', how='left')

        return merged_df

    def update_categorical_values(self,df):
        """
        Function to update missing categorical values using backward fill and reset the DataFrame index.

        Parameters:
        - df: Pandas DataFrame containing categorical columns that need to be updated.

        Returns:
        - df: Updated Pandas DataFrame with missing categorical values filled using backward fill and reset index.
        """

        # Update missing categorical values using backward fill
        df['inverter_serial_no'].fillna(method='bfill', inplace=True)
        df['site_name'].fillna(method='bfill', inplace=True)
        df['registration_no'].fillna(method='bfill', inplace=True)

        # Reset the DataFrame index
        df = df.reset_index(drop=True)

        return df

    def check_first_record_null(self,df):
        """
        Function to check if the first record (after sorting by 'date_time') has null values
        in specified columns except 'date_time', 'site_name', 'inverter_serial_no', and 'registration_no'.

        Parameters:
        - df: Pandas DataFrame containing relevant columns for checking null values.

        Returns:
        - int: Returns 0 if the first record has non-null values in the specified columns, otherwise None.
        """

        # Convert 'date_time' to datetime type
        df['date_time'] = pd.to_datetime(df['date_time'])

        # Sort DataFrame by 'date_time' column
        df = df.sort_values(by='date_time')

        # Check if the first record (after sorting) has null values in specified columns
        first_record_except_datetime = df.drop(columns=['date_time', 'site_name', 'inverter_serial_no', 'registration_no']).iloc[0]
        is_first_rows_nan = first_record_except_datetime.isnull().all()

        if is_first_rows_nan:
            return None  # Return None if the first record has all null values in specified columns
        else:
            return 0  # Return 0 if the first record has non-null values in specified columns

    def generate_supporting_columns(self,df):
        """
        Generate supporting columns based on 'date_time' and update missing categorical values.

        Args:
        - df: Pandas DataFrame

        Returns:
        - df: DataFrame with new supporting columns and updated missing categorical values
        """
        # Generate new columns based on date_time
        df['dayOfWeek'] = df['date_time'].dt.dayofweek
        df['hour'] = df['date_time'].dt.hour
        df['minute'] = df['date_time'].dt.minute
        df['weekOfYear'] = df['date_time'].dt.weekofyear
        df['yearMonth'] = df['date_time'].dt.to_period('M')

        return df

    def replace_missing_values(self,df):
        """
        Replace missing values in specified columns using backward fill (bfill) based on grouped criteria.

        Args:
        - df: Pandas DataFrame

        Returns:
        - df: DataFrame with missing values replaced
        """
        # Columns to fill missing values
        columns_to_fill = [
            'grid_power_incl_house_load_w',
            'on_grid_daily_solar_generation_yield_kwh',
            'on_grid_total_yield_kwh',
            'total_feed_in_energy_kwh',
            'total_consume_energy_from_grid_kwh',
            'battery_voltage_v',
            'battery_current_a',
            'battery_power_w',
            'bms_internal_temperature_c',
            'battery_capacity_percentage',
            'pv_total_power_generation_kwh',
            'total_battery_charge_kwh',
            'total_battery_discharge_kwh',
            'inverter_temperature',
            'file_id'
        ]

        # Group criteria for filling missing values
        group_criteria = ['inverter_serial_no', 'dayOfWeek', 'hour', 'minute', 'yearMonth']

        # Fill missing values using backward fill (bfill) based on grouped criteria for each column
        for column in columns_to_fill:
            df[column] = df.groupby(group_criteria)[column].fillna(method='bfill')

        return df

    def drop_supporting_columns(self,df):
        """
        Drop supporting columns generated to fill missing data from a pandas DataFrame.

        Args:
        - df: pandas DataFrame

        Returns:
        - df_without_columns: DataFrame without the specified columns
        """

        # List of columns to drop
        columns_to_drop = ['dayOfWeek', 'hour', 'minute', 'weekOfYear', 'yearMonth']

        # Drop the specified columns
        df_without_columns = df.drop(columns=columns_to_drop, axis=1)

        return df_without_columns

    """
        Identify the month with complete date_time values but NaN in other columns.

        Parameters:
        - df: DataFrame containing the data
        - datetime_column: Name of the datetime column in the DataFrame

        Returns:
        - Period object representing the complete month if found, else 0
        """

    def find_complete_month_with_nan(self,df, datetime_column='date_time'):
        # Extract month and year information
        df['year_month'] = df[datetime_column].dt.to_period('M')

        # Check if there are rows for each month before groupby
        month_presence = df.groupby('year_month').size() > 0

        # Group by month and check if all values are NaN for each column except 'year_month' and 'date_time'
        all_nan_mask = df.groupby('year_month').apply(lambda x: x.drop(columns=[datetime_column, 'inverter_serial_no', 'site_name', 'registration_no', 'year_month']).isnull().all(axis=0)).all(axis=1)

        # Identify the month with all NaN values in other columns, if any
        complete_months = all_nan_mask[all_nan_mask & month_presence]

        if not complete_months.empty:
            complete_month = complete_months.index[0]
            return complete_month
        else:
            return 0


    """
        Fill missing data for a month with complete date_time but NaN values in other columns.

        Parameters:
        - df (pd.DataFrame): The input DataFrame containing the data.
        - month_to_fill (pd.Period): The target month for filling missing data.
        - datetime_column (str, optional): The name of the column containing datetime information. Default is 'date_time'.

        Returns:
        pd.DataFrame: The DataFrame with missing data filled for the specified month. First two weeks will be filled from last two
                      week of previous month and rest days by next months days
        """

    def Fill_Complete_Missing_Month(self,df,month_to_fill,datetime_column='date_time'):

        # Extract year and month information
        year, month = month_to_fill.year, month_to_fill.month

        # Extract the start and end dates of the identified month
        month_start = pd.Timestamp(f'{year:04d}-{month:02d}-01 00:00:00')
        month_end = pd.Timestamp(f'{year:04d}-{month:02d}-{month_to_fill.days_in_month:02d} 23:59:59')



        # Extract the start dates of the last two weeks of the previous month and the next month
        prev_month_start = month_start - pd.Timedelta(days=14)  # 14 days from 01 of Missing Month will be same day as well
        next_month_start = month_end + pd.Timedelta(days=1)

        next_month_start_date = month_start + pd.DateOffset(days=21 + 14) # 21 days plus 14 days filled by previous month will give same day of next month

        # Identify the rows in month_to_fill for the first 14 days
        target_rows = (df['year_month'] == month_to_fill) & (df[datetime_column].dt.day <= 14)

        # Get the first 14 rows of month_to_fill
        target_rows_first_14 = df.loc[target_rows].head(14 * 48).index

        # Copy the values from first_14_days_data to month_to_fill

        # Copy data for the first 14 days from the previous month
        first_14_days_data = df[(df[datetime_column] >= prev_month_start) & (df[datetime_column] < prev_month_start + pd.DateOffset(days=14))].copy()


        # First_14_days_data is a DataFrame with data for each half-hour of the first 14 days of the previous month
        # and month_to_fill is a period for the target month.



        # Get the first 14 rows of month_to_fill
        target_rows_first_14 = df.loc[target_rows].head(14 * 48).index

        # Copy the values from first_14_days_data to month_to_fill
        df.loc[target_rows_first_14, df.columns.difference([datetime_column, 'year_month'])] = first_14_days_data[df.columns.difference([datetime_column, 'year_month'])].values


        # Get total remainig days of missing month to fill
        total_days_in_month = month_to_fill.days_in_month
        days_rem_to_fill=total_days_in_month-14
        # Copy data for the Next remaining days from the next month
        last_rem_days_data = df[(df[datetime_column] >= next_month_start_date) & (df[datetime_column] < next_month_start_date + pd.DateOffset(days=days_rem_to_fill))].copy()


        # Identify the rows in month_to_fill for the first last days
        target_rows_last = (df['year_month'] == month_to_fill) & (df[datetime_column].dt.day > 14)


        # Get the first 14 rows of month_to_fill
        target_rows_last_14 = df.loc[target_rows_last].head(days_rem_to_fill * 48).index

        # Copy the values from nexy month _days_data to month_to_fill remaining days
        df.loc[target_rows_last_14, df.columns.difference([datetime_column, 'year_month'])] = last_rem_days_data[df.columns.difference([datetime_column, 'year_month'])].values

        # Assuming 'year_month' is the column to be dropped
        #df.drop('year_month', axis=1, inplace=True)

        return df

    # Function: Fill_New_Date_Rows
    # Description: Fills missing values in specific columns of a DataFrame based on the chosen method.
    # Parameters:
    #     merged_df (pd.DataFrame): The DataFrame containing data to be filled.
    #     fill_flag (int): An indicator for the filling method (1 for forward fill, backward fill, and rolling mean, 0 for linear interpolation).
    # Returns:
    #     pd.DataFrame: A DataFrame with missing values filled according to the chosen method.


    def Fill_New_Date_Rows(self,merged_df):
        columns_to_fill = merged_df.columns.difference(['date_time'])
        columns_to_fill=columns_to_fill.drop(['inverter_serial_no','registration_no','file_id','site_name'])

        merged_df.sort_values(by='date_time', inplace=True)
        for col in columns_to_fill:
            #merged_df[col].interpolate(method='linear', inplace=True)
            merged_df[col] = merged_df[col].fillna(method='bfill')

        return merged_df


    def preprocess_data(self,df):
        """
        Preprocesses the DataFrame by converting 'date_time' column to timestamp
        and selecting required columns.

        Args:
        - result_df_frm: PySpark DataFrame to be preprocessed

        Returns:
        - df_proc: Preprocessed DataFrame containing selected columns
        """

        # Convert 'date_time' column to timestamp
        #df = df.withColumn("date_time", F.to_timestamp("date_time", "yyyy-mm-dd HH:mm"))

        # Select required columns for further processing
        df_processed = df.select(
            "inverter_serial_no", "date_time", "on_grid_total_yield_kwh",
            "total_feed_in_energy_kwh", "total_consume_energy_from_grid_kwh",
            "pv_total_power_generation_kwh", "total_battery_charge_kwh",
            "total_battery_discharge_kwh", "battery_capacity_percentage"
        )

        # Sort dataframe
        df_ordered = df_processed.orderBy(F.col("inverter_serial_no"), F.col("date_time"))


        return df_ordered

    def createInstantaneousColumns(self,df, columns_list):
        """
        Generate instantaneous columns based on the provided list of column names.

        Args:
        - df: PySpark DataFrame containing data
        - columns_list: List of column names for which instantaneous columns are generated

        Returns:
        - df_with_inst_cols: DataFrame with added instantaneous columns
        """

        window_spec = Window.partitionBy('inverter_serial_no').orderBy("date_time")

        # Create a new DataFrame to store the modifications
        df_with_inst_cols = df

        # Iterate through each column in the given list
        for name in columns_list:
            # Use the lag function to calculate the difference between consecutive rows
            df_with_inst_cols = df_with_inst_cols.withColumn("Instantaneous_" + name, F.col(name) - F.lag(name).over(window_spec))

        return df_with_inst_cols

    def createBatteryBehaviorColumn(self,df):
        """
        Create a 'BatteryBehaviour' column based on different charging and discharging scenarios.

        Args:
        - df: PySpark DataFrame containing relevant columns for behavior analysis

        Returns:
        - df_btry_behav: DataFrame with added 'BatteryBehaviour' column
        """

        # Define conditions to determine battery behavior based on instantaneous values
        df_btry_behav = df.withColumn("BatteryBehaviour",
                                      F.when((F.col("Instantaneous_pv_total_power_generation_kwh") > 0) &
                                           (F.col("Instantaneous_total_battery_charge_kwh") > 0), "Solar Charging")
                                      .when((F.col("Instantaneous_total_consume_energy_from_grid_kwh") > 0) &
                                            (F.col("Instantaneous_total_battery_charge_kwh") > 0), "Grid Charging")
                                      .when((F.col("Instantaneous_total_battery_discharge_kwh") > 0), "Discharging")
                                      .otherwise('Other'))  # Assign 'Other' if none of the conditions match

        return df_btry_behav

    def createTotalPowerConsumptionInHouse(self,df):
        """
        Generate a column indicating the total power used in the house.

        Args:
        - df: PySpark DataFrame containing relevant columns for power consumption calculation

        Returns:
        - df_power_cons: DataFrame with added 'Total_power_used_in_house' column
        """

        # Calculate the total power used in the house by summing different instantaneous power columns
        df_power_cons = df.withColumn("Total_power_used_in_house",
                                      (F.col("Instantaneous_on_grid_total_yield_kwh") +
                                       F.col("Instantaneous_total_consume_energy_from_grid_kwh") +
                                       F.col("Instantaneous_total_feed_in_energy_kwh")))

        return df_power_cons

    def add_season_column(self,df_season):
        """
        Add a 'Season' column to the DataFrame based on the month information.

        Args:
        - df_season: PySpark DataFrame containing a 'month' column

        Returns:
        - df_season_out: DataFrame with added 'Season' column
        """

        global df_season_out  # Declaring as global to use it outside the function scope

        # Define conditions for each season based on months
        conditions = [
            (F.col('month').between(3, 5), 'Spring'),
            (F.col('month').between(6, 8), 'Summer'),
            (F.col('month').between(9, 11), 'Autumn'),
            ((F.col('month') == 12) | (F.col('month').between(1, 2)), 'Winter')
        ]

        # Use the `when` function to apply the conditions and create the 'Season' column
        df_season_out = df_season.withColumn('Season', F.when(conditions[0][0], conditions[0][1])
                                                      .when(conditions[1][0], conditions[1][1])
                                                      .when(conditions[2][0], conditions[2][1])
                                                      .when(conditions[3][0], conditions[3][1])
                                                      .otherwise('Unknown'))

        return df_season_out

    def add_time_of_day_column(self,df_time_Day):
        """
        Add a 'TimeofDay' column to the DataFrame based on the 'Season' and 'date_time' columns.

        Args:
        - df_time_Day: PySpark DataFrame containing 'Season' and 'date_time' columns

        Returns:
        - df_time_Day_out: DataFrame with added 'TimeofDay' column
        """

        global df_time_Day_out  # Declaring as global to use it outside the function scope

        # Define conditions for time of day based on 'Season' and 'timestamp' columns
        conditions = [
            (F.col('Season') == 'Spring', F.when(F.hour(F.col('date_time')).between(7, 19), 'Day').otherwise('Night')),
            (F.col('Season') == 'Summer', F.when(F.hour(F.col('date_time')).between(5, 21), 'Day').otherwise('Night')),
            (F.col('Season') == 'Autumn', F.when(F.hour(F.col('date_time')).between(7, 19), 'Day').otherwise('Night')),
            (F.col('Season') == 'Winter', F.when(F.hour(F.col('date_time')).between(8, 17), 'Day').otherwise('Night')),
            (F.col('Season') == 'Unknown', 'Unknown')
        ]

        # Use the `when` function to apply the conditions and create the 'TimeofDay' column
        df_time_Day_out = df_time_Day.withColumn('TimeofDay', F.coalesce(
            F.when(conditions[0][0], conditions[0][1]),
            F.when(conditions[1][0], conditions[1][1]),
            F.when(conditions[2][0], conditions[2][1]),
            F.when(conditions[3][0], conditions[3][1]),
            F.when(conditions[4][0], conditions[4][1])
        ))

        return df_time_Day_out
    

    
    def replace_negative_values(self,df):
        """
        Replace negative values in certain columns with either previous values or 0 in the DataFrame.

        Args:
        - df: PySpark DataFrame containing data with potential negative values

        Returns:
        - df_cleansed: DataFrame with negative values replaced
        """

        window_spec = Window.partitionBy("inverter_serial_no").orderBy("date_time")

        # Create a temporary DataFrame to hold intermediate results
        df_cleansed_tmp = df

        # Replace negative values with the value from the previous row for specific columns
        columns_to_replace_prev_value = [
            "on_grid_total_yield_kwh",
            "total_feed_in_energy_kwh",
            "total_consume_energy_from_grid_kwh",
            "pv_total_power_generation_kwh",
            "total_battery_charge_kwh",
            "total_battery_discharge_kwh"
        ]

        for column in columns_to_replace_prev_value:
            df_cleansed_tmp = df_cleansed_tmp.withColumn(
                column,
                F.when(F.col(f"Instantaneous_{column}") < 0, F.lag(column).over(window_spec)).otherwise(F.col(column))
            )

        # Replace negative values with 0 for specific instantaneous columns
        columns_to_replace_with_zero = [
            "Instantaneous_on_grid_total_yield_kwh",
            "Instantaneous_total_feed_in_energy_kwh",
            "Instantaneous_total_consume_energy_from_grid_kwh",
            "Instantaneous_total_battery_charge_kwh",
            "Instantaneous_total_battery_discharge_kwh",
            "Instantaneous_pv_total_power_generation_kwh"
        ]

        df_cleansed = df_cleansed_tmp  # Assign df_cleansed_tmp to df_cleansed for further modifications

        for column in columns_to_replace_with_zero:
            df_cleansed = df_cleansed.withColumn(
                column,
                F.when(F.col(column) < 0, 0).otherwise(F.col(column))
            )

        return df_cleansed


    def cleanse_pv_error_at_night(self,df_with_errors):
        """
        Cleanse records with 'PV value cannot be at night' error by updating values in the DataFrame.

        Args:
        - df_with_errors: PySpark DataFrame containing error messages

        Returns:
        - df_cleansed: DataFrame with corrected values for the specified error
        """

        # Define a window specification for ordering by the "date_time" column within each "inverter_serial_no" group
        window_spec = Window.partitionBy("inverter_serial_no").orderBy("date_time")

        # Define the substring to search for in the error messages
        substring_to_find = "PV value can not be at night"

        # Update values based on the presence of the substring in the error messages
        df_cleansed = df_with_errors.withColumn(
            "pv_total_power_generation_kwh",
            F.when(F.col("error_message").like(f"%{substring_to_find}%"), F.lag("pv_total_power_generation_kwh").over(window_spec)).otherwise(F.col("pv_total_power_generation_kwh"))
        )

        df_cleansed = df_cleansed.withColumn(
            "Instantaneous_pv_total_power_generation_kwh",
            F.when(F.col("error_message").like(f"%{substring_to_find}%"), 0).otherwise(F.col("Instantaneous_pv_total_power_generation_kwh"))
        )

        return df_cleansed

    
    def cleanse_feed_in_consumption_same_time_error(self,df_with_errors):
        """
        Cleanse records where 'Feed in and consumption can not happen same time' error is detected by updating values.

        Args:
        - df_with_errors: PySpark DataFrame containing error messages

        Returns:
        - df_cleansed: DataFrame with corrected values for the specified error
        """

        # Define a window specification for ordering by the "date_time" column within each "inverter_serial_no" group
        window_spec = Window.partitionBy("inverter_serial_no").orderBy("date_time")

        # Define the substring to search for in the error messages
        substring_to_find = "Feed in and consumption can not happen same time"

        # Update values based on the presence of the substring in the error messages
        df_cleansed = df_with_errors.withColumn(
            "total_feed_in_energy_kwh",
            F.when(F.col("error_message").like(f"%{substring_to_find}%"), F.lag("total_feed_in_energy_kwh").over(window_spec)).otherwise(F.col("total_feed_in_energy_kwh"))
        )

        df_cleansed = df_cleansed.withColumn(
            "Instantaneous_total_feed_in_energy_kwh",
            F.when(F.col("error_message").like(f"%{substring_to_find}%"), 0).otherwise(F.col("Instantaneous_total_feed_in_energy_kwh"))
        )

        return df_cleansed

    
    def cleanse_battery_capacity_error(self,df_with_errors):
        """
        Cleanse records where 'Battery capacity should be within the range' error is detected by updating values.

        Args:
        - df_with_errors: PySpark DataFrame containing error messages

        Returns:
        - df_cleansed: DataFrame with corrected values for the specified error
        """

        # Define a window specification for ordering by the "date_time" column within each "inverter_serial_no" group
        window_spec = Window.partitionBy("inverter_serial_no").orderBy("date_time")

        # Define the substring to search for in the error messages
        substring_to_find = "Battery capacity should be within the range"

        # Update values based on the presence of the substring in the error messages
        df_cleansed = df_with_errors.withColumn(
            "battery_capacity_percentage",
            F.when(F.col("error_message").like(f"%{substring_to_find}%"), F.lag("battery_capacity_percentage").over(window_spec)).otherwise(F.col("battery_capacity_percentage"))
        )

        return df_cleansed

    
    def cleanse_consume_from_grid_if_no_usage_error(self,df_with_errors):
        """
        Cleanses consume energy data from the grid if there's a specific error message.

        Args:
        - df_with_errors (DataFrame): Input DataFrame containing error messages and consumption data.

        Returns:
        - df_cleansed (DataFrame): DataFrame with updated consume energy data based on error conditions.
        """

        # Define a window specification for ordering by the "date_time" column within each "inverter_serial_no" group
        window_spec = Window.partitionBy("inverter_serial_no").orderBy("date_time")

        # Define the substring to search for in the error messages
        substring_to_find = "Cannot consume from grid, if no usage"

        # Update values based on the presence of the substring in the error messages
        df_cleansed = df_with_errors.withColumn(
            "total_consume_energy_from_grid_kwh",
            F.when(F.col("error_message").like(f"%{substring_to_find}%"), F.lag("total_consume_energy_from_grid_kwh").over(window_spec)).otherwise(F.col("total_consume_energy_from_grid_kwh"))
        )

        # Set consume energy to 0 if error message is found
        df_cleansed = df_cleansed.withColumn(
            "Instantaneous_total_consume_energy_from_grid_kwh",
            F.when(F.col("error_message").like(f"%{substring_to_find}%"), 0).otherwise(F.col("Instantaneous_total_consume_energy_from_grid_kwh"))
        )

        return df_cleansed

    
    def cleanse_battery_charge_discharge_error(self,df_with_errors):
        """
        Cleanse records where 'Battery charge and discharge cannot happen at same time' error is detected by updating values.

        Args:
        - df_with_errors: PySpark DataFrame containing error messages

        Returns:
        - df_cleansed: DataFrame with corrected values for the specified error
        """

        # Define a window specification for ordering by the "date_time" column within each "inverter_serial_no" group
        window_spec = Window.partitionBy("inverter_serial_no").orderBy("date_time")

        # Define the substring to search for in the error messages
        substring_to_find = "Battery charge and discharge cannot happen at same time"

        # Update values based on the presence of the substring in the error messages and 'TimeofDay'
        df_cleansed = df_with_errors.withColumn(
            "total_battery_charge_kwh",
            F.when((F.col("error_message").like(f"%{substring_to_find}%")) & (F.col("TimeofDay") == 'Night'), F.lag("total_battery_charge_kwh").over(window_spec)).otherwise(F.col("total_battery_charge_kwh"))
        ).withColumn(
            "Instantaneous_total_battery_charge_kwh",
            F.when((F.col("error_message").like(f"%{substring_to_find}%")) & (F.col("TimeofDay") == 'Night'), 0).otherwise(F.col("Instantaneous_total_battery_charge_kwh"))
        ).withColumn(
            "total_battery_discharge_kwh",
            F.when((F.col("error_message").like(f"%{substring_to_find}%")) & (F.col("TimeofDay") == 'Day'), F.lag("total_battery_discharge_kwh").over(window_spec)).otherwise(F.col("total_battery_discharge_kwh"))
        ).withColumn(
            "Instantaneous_total_battery_discharge_kwh",
            F.when((F.col("error_message").like(f"%{substring_to_find}%")) & (F.col("TimeofDay") == 'Day'), 0).otherwise(F.col("Instantaneous_total_battery_discharge_kwh"))
        )

        return df_cleansed

    def cleanse_battery_discharge_error(self,df_with_errors):
        """
        Cleanse records where 'Battery discharge can not have any value if there is no consumption and feed in to grid' error is detected by updating values.

        Args:
        - df_with_errors: PySpark DataFrame containing error messages

        Returns:
        - df_cleansed: DataFrame with corrected values for the specified error
        """

        # Define a window specification for ordering by the "date_time" column within each "inverter_serial_no" group
        window_spec = Window.partitionBy("inverter_serial_no").orderBy("date_time")

        # Define the substring to search for in the error messages
        substring_to_find = "Battery discharge can not have any value if there is no consumption and feed in to grid"

        # Update values based on the presence of the substring in the error messages
        df_cleansed = df_with_errors.withColumn(
            "total_battery_discharge_kwh",
            F.when(F.col("error_message").like(f"%{substring_to_find}%"), F.lag("total_battery_discharge_kwh").over(window_spec)).otherwise(F.col("total_battery_discharge_kwh"))
        ).withColumn(
            "Instantaneous_total_battery_discharge_kwh",
            F.when(F.col("error_message").like(f"%{substring_to_find}%"), 0).otherwise(F.col("Instantaneous_total_battery_discharge_kwh"))
        )

        return df_cleansed

    def replace_outliers_with_previous(self,df, column_names, multiplier=1.5):
        """
        Replace outliers in PySpark DataFrame columns with the previous row's values if outliers are found
        in specified columns. Outliers are determined based on the IQR.

        Args:
        - df: PySpark DataFrame
        - column_names: List of column names to check for outliers
        - multiplier: Multiplier for determining the outlier threshold (default is 1.5)

        Returns:
        - PySpark DataFrame with rows replaced by the previous row's values if outliers are found
        """
        # Ensure column_names is a list of strings
        if not all(isinstance(col, str) for col in column_names):
            raise ValueError("All column names should be strings.")

        # Create a window specification based on the order of rows
        window_spec = Window().orderBy("date_time")

        # Iterate over each column and handle outliers
        for col in column_names:
            # Use lag function to get the previous row's value
            previous_value = F.lag(df[col]).over(window_spec)

            # Calculate the IQR (Interquartile Range) for each column
            quantiles = df.approxQuantile(col, [0.25, 0.75], 0.01)
            q1, q3 = quantiles[0], quantiles[1]
            iqr = q3 - q1

            # Calculate the lower and upper bounds for identifying outliers
            lower_bound = q1 - multiplier * iqr
            upper_bound = q3 + multiplier * iqr

            # Replace entire row with previous row's values if outliers are found
            df = df.withColumn(col, F.when((df[col] < lower_bound) | (df[col] > upper_bound), previous_value).otherwise(df[col]))

        return df

    def process_fillna(self,df):
        """
        Converts a PySpark DataFrame to a Pandas DataFrame, sorts it by 'inverter_serial_number' and 'date_time',
        and fills null values in each column with the next available value within each 'inverter_serial_number' group.

        Parameters:
        - processed_df (pyspark.sql.DataFrame): Input PySpark DataFrame.

        Returns:
        - pd.DataFrame: Processed Pandas DataFrame.
        """
        # Sort the DataFrame by 'inverter_serial_number' and 'date_time'
        processed_df_pd = df.sort_values(by=['inverter_serial_no', 'date_time'])

        # Group by 'inverter_serial_number' and fill null values in each column with the next available value within each group
        processed_df_pd = processed_df_pd.fillna(method='bfill')

        return processed_df_pd
    
    def make_full_data(self,df):
        """
         Make data of valid inverters into 365 days worth of data. Missing records will be filled.

        Parameters:
        - df (pyspark.sql.DataFrame): Input PySpark DataFrame.

        Returns:
        - pyspark.DataFrame: Spark DataFrame.
        """
        
        unique_inv_list = self.get_unique_inverter_serial_numbers(df)
        
        # Convert to pandas as 365 day creation part will be handled by pandas
        # This is Bottleneck in the code. Will need to change these functions to Pyspark as well.
        df_gen_val_succeded_pd = df.toPandas()
        
        # Merge 365 Days below
        
        df_merge_ready = self.reset_dataframe_to_empty(df_gen_val_succeded_pd)
        
       # Invoke Merge_Regular_HH_DF to create missing records
        for inv in unique_inv_list:
            print(inv)
            temp_df = df_gen_val_succeded_pd['inverter_serial_no'] == inv
            result_df = df_gen_val_succeded_pd[temp_df]
            # Merge Start
            reg_hh_df = self.Get_Regular_Half_Hours(result_df)
            df_merged = self.Merge_Regular_HH_DF(reg_hh_df)
            # Filling Start
            df_updated_catog = self.update_categorical_values(df_merged)
            is_first_rec_nan = self.check_first_record_null(df_updated_catog)
            # Filling records where first rows are missing
            if(is_first_rec_nan!=0):
                # Fill missing records where first rows are missing 
                df_updated_catog = self.generate_supporting_columns(df_updated_catog)
                df_updated_catog = self.replace_missing_values(df_updated_catog)
                df_updated_catog = self.drop_supporting_columns(df_updated_catog)

            # Fill missing records where complete month is missing 
            month_to_fill = self.find_complete_month_with_nan(df_updated_catog)
            if(month_to_fill!=0):
                df_updated_catog =  self.Fill_Complete_Missing_Month(df_updated_catog,month_to_fill)

            # Fill missing records in between 
            df_updated_catog=self.Fill_New_Date_Rows(df_updated_catog)
            # Appending to one final dataframe 
            df_merge_ready = df_merge_ready.append(df_updated_catog)

        print('Checkpoint 1')
        # Drop supporting Columns
        df_merge_ready = df_merge_ready.drop('year_month', axis=1)

        # Convert Pandas DataFrame to PySpark DataFrame after 365 records creation and filling
        df_merge_ready_py = self.spark.createDataFrame(df_merge_ready)
        
        return df_merge_ready_py
    
    
    def generate_derived_inst_col(self,df):
        """
         Generate derived and Inst columns .

        Parameters:
        - df (pyspark.sql.DataFrame): Input PySpark DataFrame.

        Returns:
        - pyspark.DataFrame: Spark DataFrame.
        """
        
        # Generate Derived Columns 
        # pre-process activity for datafame
        df_merged_365 = self.preprocess_data(df)
        # List of column names to create instantaneous columns
        # Call the function to get Instantaneous columns appended
        columns_list = ["on_grid_total_yield_kwh", "total_feed_in_energy_kwh", "total_consume_energy_from_grid_kwh", "pv_total_power_generation_kwh", "total_battery_charge_kwh", "total_battery_discharge_kwh"]
        df_with_inst = self.createInstantaneousColumns(df_merged_365, columns_list)
        print('Checkpoint 2')
        # List of instantaneous columns
        # Replace null values with 0 in the specified columns
        instantaneous_columns = [
          "Instantaneous_on_grid_total_yield_kwh",
          "Instantaneous_total_feed_in_energy_kwh",
          "Instantaneous_total_consume_energy_from_grid_kwh",
          "Instantaneous_pv_total_power_generation_kwh",
          "Instantaneous_total_battery_charge_kwh",
          "Instantaneous_total_battery_discharge_kwh"
                                 ]
        df_with_inst = df_with_inst.na.fill(value=0, subset=instantaneous_columns)
        # Invoke function to create battery behavior column
        df_with_inst = self.createBatteryBehaviorColumn(df_with_inst)
        # Invoke function to create Total Consumption
        df_with_inst = self.createTotalPowerConsumptionInHouse(df_with_inst)
        # Call the function to add the 'Season' column
        df_with_inst = df_with_inst.withColumn("month", F.month(df_with_inst["date_time"]))
        df_with_inst = self.add_season_column(df_with_inst)
        # Call the function to add the 'Time of Day column
        df_with_inst= self.add_time_of_day_column(df_with_inst)
        # Create validate ready dataframe
        df_validate_ready = df_with_inst
        print('Checkpoint 3')
        
        return df_validate_ready


    def cleaning_validated_data(self,df):
        """
         Cleaning the validated columns data

        Parameters:
        - df (pyspark.sql.DataFrame): Input PySpark DataFrame.

        Returns:
        - pyspark.DataFrame: Spark DataFrame.
        """
        df_cleansed = self.replace_negative_values(df)
        df_cleansed = self.cleanse_pv_error_at_night(df_cleansed)
        print('Checkpoint 4.1')
        df_cleansed = self.cleanse_feed_in_consumption_same_time_error(df_cleansed)
        df_cleansed = self.cleanse_battery_capacity_error(df_cleansed)
        print('Checkpoint 4.2')
        df_cleansed = self.cleanse_battery_charge_discharge_error(df_cleansed)
        df_cleansed = self.cleanse_battery_discharge_error(df_cleansed)
        print('Checkpoint 4.3')
        df_cleansed = self.cleanse_consume_from_grid_if_no_usage_error(df_cleansed)
        print('Checkpoint 4.4')
        column_list = ['total_battery_charge_kwh', 'total_battery_discharge_kwh']
        df_tot_cleansed = self.replace_outliers_with_previous(df_cleansed,column_list)
        print('Checkpoint 5')
        
        return df_tot_cleansed
        


# In[ ]:




