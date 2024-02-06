#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *

class SQLServerDataHandler:
    def __init__(self, spark):
        self.spark = spark

    def read_data_from_sqlserver(self, url, db_table, properties):
        """
        Read data from SQL Server table into a DataFrame
        """
        df = self.spark.read.jdbc(url=url, table=db_table, properties=properties)
        return df

    def write_data_to_sqlserver(self, df, url, db_table, properties, mode="overwrite"):
        """
        Write data from DataFrame to SQL Server table
        """
        df.write.jdbc(url=url, table=db_table, mode=mode, properties=properties)

    @staticmethod
    def get_db_connection_properties():
        """
        Function to get the database connection properties for SQL Server.

        Returns:
            tuple: A tuple containing the URL for the database connection and a dictionary of properties.
        """

        # Database connection URL
        url = "jdbc:sqlserver://c1ylwjuviw.database.windows.net:1433;databaseName=DbGeniusProjectBetterEnergy"

        # Database connection properties
        properties = {
            "user": "ArslanFarooq",
            "password": "HNG:8K78CvRHrw:@y",
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }

        return url, properties

    def load_inverter_data(self, file_path):
        """
        Load data from a CSV file into a PySpark DataFrame and perform necessary transformations.

        Parameters:
        - file_path (str): Path to the CSV file.

        Returns:
        - df (pyspark.sql.DataFrame): Transformed PySpark DataFrame.
        """

        # Read data from a CSV file into a PySpark DataFrame
        df = self.spark.read.csv(file_path, header=True, inferSchema=True)

        # Convert the 'date_time' column to a timestamp format
        # The format "dd/MM/yyyy HH:mm" specifies the input format of the 'date_time' column
        df = df.withColumn("date_time", to_timestamp("date_time", "dd/MM/yyyy HH:mm"))

        # Drop the 'date_time_simulated' column from the DataFrame
        df = df.drop('date_time_simulated')

        return df



# In[ ]:




