import org.apache.spark.SparkContext


object JsonSQLOperations {

  def doSomeJSONOperations(sc : SparkContext) : Unit = {

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // A JSON dataset is pointed to by path.
    // The path can be either a single text file or a directory storing text files.
    val someJsonFilePath = "C:/Users/sacha/Desktop/SparkDemoApp/SparkDemo/SparkDemo/src/main/resources/People.json"


    // Create a SchemaRDD from the file(s) pointed to by path
    val people = sqlContext.read.json(someJsonFilePath)

    // The inferred schema can be visualized using the printSchema() method.
    people.printSchema()
    // root
    //  |-- age: IntegerType
    //  |-- name: StringType

    // Register this SchemaRDD as a table.
    people.registerTempTable("people")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")
    teenagers.foreach(x=>
      println(s"Teenager found :  $x")
    )

  }
}
