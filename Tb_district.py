from pyspark.sql import SparkSession
import pandas as pd

spark=SparkSession.builder.appName("filtering").getOrCreate()

df=spark.read.csv("/home/raja/Downloads/13th Sept 2022 UP Textbook - District Level_13th Sept 2022.csv",header=True)
df1=pd.read_csv("/home/raja/Downloads/13th Sept 2022 UP Textbook - Copy of District Level_13th Sept 2022.csv")
l=df1["User District"].to_list()

#df_f=df.filter(df["User District"] != df1["User District"]).select("User District")
df_2=df.filter(~ df['User District'].isin(l) ).select("User District")
df_2.repartition(1).write.mode("overwrite").format('csv').option('header',True).save("/home/raja/Desktop/Ekstep/up/text_book")
