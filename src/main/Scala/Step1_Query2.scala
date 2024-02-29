import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Step1_Query2 {
    def main(args: Array[String]): Unit = {
      val sparConf = new SparkConf().setMaster("local").setAppName("Query2")
      val sc = new SparkContext(sparConf)
      val peopleRDD = sc.textFile("data/PEOPLE-large")
      val infectedRDD = sc.textFile("data/INFECTED-small")

      val people_large_RDD = peopleRDD.map(line => {
        val parts = line.split(",")
        val pid = parts(0)
        val x = parts(1).toDouble
        val y = parts(2).toDouble
        (pid, (x, y))
      })

      val infected_small_RDD = infectedRDD.map(line => {
        val parts = line.split(",")
        val pid = parts(0)
        val x = parts(1).toDouble
        val y = parts(2).toDouble
        (pid, (x, y))
      })

      // pair each record from list of infected and list of people
      val cartesian_join_RDD = infected_small_RDD.cartesian(people_large_RDD)

      val close_contacts_RDD = cartesian_join_RDD.filter {
        case ((_, (x1, y1)), (_, (x2, y2))) =>
          val distance = math.sqrt(math.pow(x1 - x2, 2) + math.pow(y1 - y2, 2))
          distance <= 6.0
      }

      val unique_close_contacts_RDD = close_contacts_RDD.map {
        case (infected, closeContact) => closeContact._1
      }.distinct()

      val result = unique_close_contacts_RDD.collect()

      result.foreach(println)

      sc.stop()

    }
}
