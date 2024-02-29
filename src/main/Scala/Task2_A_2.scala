import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Task2_A_2 {

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

    // createOrReplaceTempView instead of registerTempTable
    customersDF.createOrReplaceTempView("customers")
    purchasesDF.createOrReplaceTempView("purchases")

    val t1 = spark.sql("SELECT * FROM purchases WHERE transTotal > 600")
    //t1.show()

    t1.createOrReplaceTempView("t1")
    val t2 = spark.sql("SELECT transNumItems, percentile_approx(transTotal, 0.5), Min(transTotal), Max(transTotal) FROM t1 GROUP BY transNumItems");
    t2.show()
  }
}