from pyspark.sql import SparkSession
import pandas
import sys

user_declared_state = sys.argv[1]
print(user_declared_state,'state:::::::::::::')

spark=SparkSession.builder.appName("jsontocsv").getOrCreate()

df_mapping = pandas.read_csv("/home/amith/Desktop/Ek_step/removing_dup_dist/Internal UP Text Book Data  - State_district.csv")
df_mapping = df_mapping.loc[df_mapping['State'] == user_declared_state]
city_list = list(df_mapping['District'])



df = spark.read.csv("/home/amith/Desktop/Ek_step/removing_dup_dist/Internal UP Text Book Data  - Output.csv",header=True)\
    #.select("state","city","device_id","first_access")
#df2=df.filter(df['user_declared_state'].isNull)
df1 = df.filter(df['derived_loc_district'].isin(city_list )).select("")
#df2 = df.filter((df.user_declared_district != city_list))
#df_merage= df2.unionByName(df1)
df1.repartition(1).write.mode("append").format("csv").option("header",True).save("/home/amith/Desktop/Ek_step/removing_dup")