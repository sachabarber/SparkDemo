/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {



    val conf = new SparkConf().setAppName("Simple Application")

    //use local Spark (non clustered in this example). Note this relies
    // on all the SBT dependencies being downloaded to
    // C:\Users\XXXXX\.ivy2 cache folder
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    //do some regular Spark RDD operations
    RddOperations.doSomeRDDOperations(sc)

    //do some regular Spark SQL operations om some JSON
    JsonSQLOperations.doSomeJSONOperations(sc)

    readLine()
  }
}