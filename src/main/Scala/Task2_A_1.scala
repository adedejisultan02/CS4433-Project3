import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Task2_A_1 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Task2_A_2").master("local").getOrCreate()
    val customerSchema = new StructType()
      .add("id", IntegerType)
      .add("name", StringType)
      .add("age", IntegerType)
      .add("countryCode", IntegerType)
      .add("salary", FloatType)

    val purchaseSchema = new StructType()
      .add("transID", IntegerType)
      .add("custID", IntegerType)
      .add("transTotal", FloatType)
      .add("transNumItems", IntegerType)
      .add("transDesc", StringType)

    val customersDF = spark.read
      .schema(customerSchema)
      .csv("data/customers.txt")
    val purchasesDF = spark.read
      .schema(purchaseSchema)
      .csv("data/purchases.txt")

    customersDF.createOrReplaceTempView("customers")
    purchasesDF.createOrReplaceTempView("purchases")

    val t1 = spark.sql("SELECT * FROM purchasesT WHERE TransTotal > 600")

    t1.coalesce(1).write.option("header",true).csv("data/T1.csv")

    t1.show()

    spark.stop()
  }
}