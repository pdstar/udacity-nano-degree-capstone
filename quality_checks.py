import configparser
import pandas as pd
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import asc
from pyspark.sql.functions import sum as Fsum
import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from pyspark.sql import types as T
from pyspark.sql.window import Window
import os
from pyspark.sql import functions as F


from pyspark.sql.functions import monotonically_increasing_id,row_number


def empty_table(tables, tablename):
    """
    Checks if the schema tables, dim and fact, are empty 
    Parameters: 
        tables: list of table objects
        tablename: list of tale names
    Output:
        fail: flag (1 any table is empty)
        fail_table: list of empty tables 
    """
    fail = 0
    fail_table = []
    i = 0
    for table in tables:
        count = table.count()
        if count < 1:
            fail = 1
            fail_table.append(table)
            print(f"Empty Table quality check failed for {tablename[i]}: 0 records")
        print(f"Empty Table quality check passed for {tablename[i]}: {count} records")
        i = i+1
    return fail, fail_table

# Perform quality checks: null primary key table
def null_key(tables, tablename, primary_key):
    """
    Checks if the any primary key in schema tables, dim and fact, has null value 
    Parameters: 
        tables: list of table objects
        tablename: list of tale names
        primary_key: primary keys of the respective tables
    Output:
        fail: 1 any table is empty
        fail_table: list of empty tables 
    """
    fail = 0
    fail_table = []
    i = 0
    for table in tables:
        count = table.where(F.col(primary_key[i]).isNull()).count()
        if count > 1:
            fail = 1
            fail_table.append(table)
            print(f"Null Key check failed for {tablename[i]}: {count} records")
        print(f"Null Key check passed for {tablename[i]}: {count} records")
        i = i+1
    return fail, fail_table