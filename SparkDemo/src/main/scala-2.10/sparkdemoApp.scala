/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {

    // Should be some file on your system
    val someTextFilePath = "C:/Temp/SomeTextFile.txt"

    val conf = new SparkConf().setAppName("Simple Application")

    //use local Spark (non clustered in this example). Note this relies
    // on all the SBT dependencies being downloaded to
    // C:\Users\XXXXX\.ivy2 cache folder
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    //creaate an RDD using external data (ie the text file)
    val textFileRDD = sc.textFile(someTextFilePath, 2).cache()

    //FILTER TRANSFORMATION example, note that count() is actually an action
    val numAs = textFileRDD.filter(line => line.contains("a")).count()
    val numBs = textFileRDD.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))

    //MAP TRANSFORMATION example, note that first() is actually an action
    val mapHoHo = textFileRDD.map(line => line + "HO HO")
    println("HoHoHo line : %s".format(mapHoHo.first().toString()))



    //COLLECT ACTION example, note that filter() is actually an transformation
    val numAsArray = textFileRDD.filter(line => line.contains("a")).collect()
    println("Lines with a: %s".format(numAsArray.length))
    numAsArray.foreach(println)
    println("Lines with a as Array: %s".format(numAsArray.getClass().getTypeName()))


    //FIRST ACTION example
    val firstLine = textFileRDD.first()
    println("First Line: %s".format(firstLine))

    readLine()
  }
}