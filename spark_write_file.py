  
from pyspark.sql import SparkSession

def write_file():
	
	spark = SparkSession.builder.appName("write file to S3").getOrCreate()

	cities_data = "s3n://udacity-emr-data/cities.csv"
	df = spark.read.csv(cities_data,header=True)

	# investigate what columns you have
	col_list = df.columns
	print(col_list)
	agg_df = df.groupby("country_state").count()

	output_path = "s3n://udacity-emr-data/re-written-cities"
	agg_df.write.mode("overwrite").parquet(output_path)

if __name__ == "__main__":
	write_file()