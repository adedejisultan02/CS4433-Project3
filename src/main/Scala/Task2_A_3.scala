import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Task2_A_3 {

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
    //val t2 = spark.sql("SELECT transNumItems, percentile_approx(transTotal, 0.5), Min(transTotal), Max(transTotal) FROM t1 GROUP BY transNumItems");

    val t3 = spark.sql("SELECT * FROM customers WHERE age BETWEEN 18 AND 25")
    t3.createOrReplaceTempView("t3");

    val t4 = spark.sql("SELECT t3.id, t3.age, SUM(t1.transNumItems), SUM(t1.transtotal) FROM t3 " +
      "JOIN t1 ON t3.id = t1.custID " +
      "GROUP BY id,age")

    t4.show
  }
}