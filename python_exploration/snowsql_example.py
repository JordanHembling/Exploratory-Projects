from snowflake.snowpark import Session
from utilities.configurations import *
import pandas as pd


session = getSFConn()

session.sql("USE ROLE SYSADMIN;").collect()

rows = session.sql("SELECT * FROM customerInfo").collect()

my_df = pd.DataFrame(rows)

sum = 0
for index, row in my_df.iterrows():
  sum += row['AMOUNT']

print(sum)

print(my_df['AMOUNT'].agg(['sum']))

query = "update customerinfo set LOCATION = 'US' where COURSENAME = 'Jmeter'"

session.sql(query).collect()

session.close()
