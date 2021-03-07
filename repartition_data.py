from pyspark.sql import SparkSession

def repartition_df():
    spark = SparkSession.builder.appName("repartition_data").getOrCreate()

    cities_data = "s3n://udacity-emr-data/parking_violation.csv"
    df = spark.read.csv(cities_data,header=True)

    # investigate what columns you have
    col_list = df.columns
    print(col_list)

    year_df = df.groupby("year").count().collect()
    print(year_df)
    month_df = df.groupby("month").count().collect()
    print(month_df)

    df.write.mode("overwrite").partitionBy('year').csv("s3n://udacity-emr-data/parking_violation_year_parition")
    workers=3
    df.repartition(workers).write.mode("overwrite").csv("s3n://udacity-emr-data/parking_violation_year_parition_base_partition")

if __name__ == "__main__":
	repartition_df()