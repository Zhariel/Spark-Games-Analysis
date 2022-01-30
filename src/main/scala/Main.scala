import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Main extends App {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
}