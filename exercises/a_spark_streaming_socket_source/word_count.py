import findspark

findspark.init()

from pyspark.sql import SparkSession


def read(spark):
    lines_df = (
        spark.readStream.format("socket")
        .option("host", "localhost")
        .option("port", "9999")
        .load()
    )
    return lines_df


# TODO: Complete this method for the exercise.
#  Count how many times each word appears in real-time as you'll be reading from a socket.
#  Don't forget to start the socket connection before running this program by typing in a new terminal ncat -lk 9999
def transform(input_df):
    return input_df


def main():
    """
    Example of a streaming word count reading from a TCP/IP port 9999 and using spark structured streaming
    """
    # Create Spark Session
    spark = (
        SparkSession.builder.appName("Streaming Word Count")
        .master("local[3]")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.shuffle.partitions", 3)
        .getOrCreate()
    )

    # 1. [READ] Input dataframe -> read from port 9999 what we type in our terminal
    lines_df = read(spark)

    # 1.1. Check the schema of my input dataframe
    lines_df.printSchema()
    lines_df.show()

    # 2. TODO: [TRANSFORM]
    counts_df = transform(lines_df)

    # 3. [SINK] Output dataframe - Streaming Sink
    # checkpointLocation is needed to store progress information about the streaming job
    word_count_query = (
        counts_df.writeStream.format("console")
        .outputMode("complete")
        .option("checkpointLocation", "chk-point-dir")
        .start()
    )  # starts background job

    # wait until the background job finishes
    word_count_query.awaitTermination()


if __name__ == "__main__":
    main()

# NOTE: Delete the chk-point-dir folder that is created automatically everytime you want to execute this program again.
# Check the Spark UI at localhost:4040/jobs when running this sample
