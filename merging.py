from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("merging").getOrCreate()

df=spark.read.csv("/home/raja/Downloads/UP TextBook Data - TB Plays 27th Dec 2022.csv",header=True)
df1=spark.read.csv("/home/raja/Downloads/UP TextBook Data - TB Devices 27th Dec 2022.csv",header=True)

df_join =df1.join(df,['collection_gradelevel','collection_name','collection_channel_slug','collection_board','collection_subject','object_rollup_l1','collection_medium'],how="inner")
df_join.repartition(1).write.mode("overwrite").format("csv").option('header',True).save("/home/raja/Desktop/Ekstep/up/TB_OUTPUT")