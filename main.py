from pyspark.sql import SparkSession


S3_DATA_SOURCE_PATH = "S3 URI"

def main():

    print("Creating spark session builder...")
    spark = SparkSession.builder.appName("HelloWordCount").getOrCreate()
    print("Done creating spark session builder! \n")

    print("Reading text file...")
    data_df = spark.read.text(S3_DATA_SOURCE_PATH)
    print("Processing text file...")
    hello_count = data_df.filter(data_df.value.rlike("(?i)\\bHello\\b")).count()
    print("Occurrences of 'Hello':", hello_count)

if __name__ == "__main__":
    main()
