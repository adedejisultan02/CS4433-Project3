from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sc = spark.sparkContext

customers_schema = """
    ID INT,
    Name STRING,
    Age INT,
    CountryCode INT,
    Salary FLOAT
"""

purchases_schema = """
    TransID INT,
    CustID INT,
    TransTotal FLOAT,
    TransNumItems INT,
    TransDesc STRING
"""

purchpath = "data/purchases.txt"
custpath = "data/customers.txt"
purchdf = spark.read.csv(purchpath, header=False, schema=purchases_schema)
custdf = spark.read.csv(custpath, header=False, schema=customers_schema)

T1_df = purchdf.filter(purchdf['TransTotal'] > 600)

T1_df.createOrReplaceTempView("T1")

T1_df.show(n=100)

spark.stop()