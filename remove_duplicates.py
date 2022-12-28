from pyspark.sql import SparkSession
import pandas
import sys

user_declared_state = sys.argv[1]
print(user_declared_state,'state:::::::::::::')

df_mapping = pandas.read_csv('/home/raja/Downloads/Recent UP TextBook 23rd Aug2022 - state_district.csv')
df_mapping = df_mapping.loc[df_mapping['state'] == user_declared_state]
city_list = list(df_mapping['district'])

spark = SparkSession.builder.appName('getnishta').getOrCreate()


df1= spark.read.csv("/home/raja/Downloads/UP TextBook Data - QR Code Data 27th Dec 2022.csv",header=True)
df3=df1.filter(df1["derived_loc_district"].isin(city_list)).select("derived_loc_district","state_slug","SUM(total_count)")
# df4=df1.filter(df1["User District"].isin(city_list)).select('Date','User State','User District','User Block','User Cluster','User School Name','Total Plays','Total Unique Devices')

df3.repartition(1).write.mode('overwrite').format('csv').option('header',True).save("/home/raja/Desktop/Ekstep/up/QR_output")
