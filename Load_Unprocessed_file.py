from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("Load Unprocessed Files into Hive") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport() \
    .getOrCreate()

from py4j.java_gateway import java_import

# Import Hadoop FileSystem classes
java_import(spark._jvm, "org.apache.hadoop.fs.FileSystem")
java_import(spark._jvm, "org.apache.hadoop.fs.Path")

# HDFS Configuration
hadoop_conf = spark._jsc.hadoopConfiguration()
fs = spark._jvm.FileSystem.get(hadoop_conf)

# List all files in the source directory
source_path = "/source/"
file_statuses = fs.listStatus(spark._jvm.Path(source_path))
all_files = [file.getPath().getName() for file in file_statuses]

print("Files in HDFS source path:", all_files)

# Read processed file names from the Hive control table
control_table_name = "default.hive_control_table"
processed_files_df = spark.sql(f"SELECT file_name FROM {control_table_name}")
processed_files = [row.file_name for row in processed_files_df.collect()]

# Identify unprocessed files
unprocessed_files = [file for file in all_files if file not in processed_files]
print("Unprocessed files:", unprocessed_files)

# Load and write each unprocessed file
for file_name in unprocessed_files:
    file_path = f"hdfs://10.140.0.2:8020/source/{file_name}"
    target_path = f"hdfs://10.140.0.2:8020/target/{file_name}"
    
    # Read the CSV file
    df = spark.read.format("csv").option("header", "true").load(file_path)
    
    # Cast columns to proper types
    df = df.withColumn("EmployeeID", col("EmployeeID").cast("int"))
    df = df.withColumn("Salary", col("Salary").cast("int"))
    
    # Write the DataFrame to the target path
    df.write.format("csv").option("header", "true").option("delimiter", ",").mode("overwrite").save(target_path)

# Create a DataFrame with the processed file names
if unprocessed_files:
    processed_files_df = spark.createDataFrame([(file,) for file in unprocessed_files], ["file_name"])
    
    # Write the processed file names to the Hive control table
    processed_files_df.write.insertInto("default.hive_control_table", overwrite=False)
    
    print("Unprocessed files loaded into Hive external table.")
else:
    print("No unprocessed files to load.")

# Stop the Spark session
spark.stop()

