import org.apache.spark.SparkContext

object RddOperations {

  def doSomeRDDOperations(sc : SparkContext) : Unit = {

    // Should be some file on your system
    val someTextFilePath = "C:/Users/sacha/Desktop/SparkDemoApp/SparkDemo/SparkDemo/src/main/resources/SomeTextFile.txt"

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
    println("Lines with a as Array: %s".format(numAsArray.getClass().getName()))


    //FIRST ACTION example
    val firstLine = textFileRDD.first()
    println("First Line: %s".format(firstLine))
  }

}
