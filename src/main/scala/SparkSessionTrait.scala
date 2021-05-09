import org.apache.spark.sql.SparkSession

trait SparkSessionTrait {
 val sparkSession = SparkSession.builder()
   .appName("Spark Generic Rules")
   .master("local[2]")
   .getOrCreate()
}
