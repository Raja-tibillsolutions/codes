from pyspark.sql import SparkSession
import pandas as pd

spark=SparkSession.builder.appName("finding device_id").getOrCreate()

df_pandas=pd.read_csv("/home/raja/Downloads/wrong - Pivot Table 6.csv")
l=df_pandas["context_did"].to_list()

df=spark.read.csv("/home/raja/Downloads/wrong_mapped_device_id's - wrong_mapped_device_id's.csv",header=True).select("device_id")
df_f=df.filter(df["device_id"].isin(l))
df_f.repartition(1).write.mode("overwrite").format('csv').option('header',True).save('/home/raja/Desktop/Ekstep/postgresql/up_first_access_and_wrong_mapping')

