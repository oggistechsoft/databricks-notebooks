# Import necessary libraries
from pyspark.sql import SparkSession

# Set up Spark session
spark = SparkSession.builder.appName("DatalakeExample").getOrCreate()

# Set up configurations for your Data Lake Storage
# Replace the placeholders with your own credentials and storage details

# For Azure Data Lake Storage Gen2
account_name = "your_account_name"
container_name = "your_container_name"
storage_key = "your_storage_key"
adls_path = "abfss://%s@%s.dfs.core.windows.net/%s" % (container_name, account_name, "your_path")

# For AWS S3
# access_key = "your_access_key"
# secret_key = "your_secret_key"
# s3_path = "s3a://%s:%s@your-bucket-name/your-path" % (access_key, secret_key)

# Configure Spark to use the Data Lake Storage
# For Azure Data Lake Storage Gen2
spark.conf.set("fs.azure.account.key.%s.dfs.core.windows.net" % account_name, storage_key)

# For AWS S3
# spark.conf.set("spark.hadoop.fs.s3a.access.key", access_key)
# spark.conf.set("spark.hadoop.fs.s3a.secret.key", secret_key)

# Load data from your Data Lake Storage
# Replace "your_file.csv" with the actual file path or pattern in your Data Lake Storage
data = spark.read.csv(adls_path + "/your_file.csv", header=True, inferSchema=True)

# Show the loaded data
data.show()

# You can perform further operations on the loaded data as needed

# Stop the Spark session
spark.stop()
