import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
object Step1_Query1 {

  def main(args: Array[String]): Unit = {


    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")

    val sc = new SparkContext(sparConf)

    val largeFile = sc.textFile("data/PEOPLE-large");
    val smallFile = sc.textFile("data/INFECTED-small")

    val allPeople = largeFile.map(x => {
      val line = x.split(",");
      val xCord = line(1);
      val yCord = line(2);
      (line(0), (xCord.toDouble,yCord.toDouble))
    })

    val infectedPeople = smallFile.map(x => {
      val line = x.split(",");
      val xCord = line(1);
      val yCord = line(2);
      (line(0), (xCord.toDouble,yCord.toDouble))
    })


    val combinedFiles = allPeople.cartesian(infectedPeople)
    val filterFiles = combinedFiles.filter(x => {
      var x1 = x._1._2._1;
      var x2 = x._2._2._1;
      var y1 = x._1._2._2;
      var y2 = x._2._2._2;
      var distance = math.hypot((x2-x1),(y2-y1))
      distance <= 6;
    } )

    val answer = filterFiles.map(x => {
      var pidClose = x._1._1;
      var infectee = x._2._1;
      (pidClose,infectee)
    })

    answer.foreach(println);


    sc.stop()
  }
}
