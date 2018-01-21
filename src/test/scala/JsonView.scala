
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql._
// import sqlContext.implicits._

object JsonView {
  	def main(args: Array[String]) {
  	  val sc = new SparkContext(new SparkConf().setAppName("JSON View"))
  	  val log = Logger.getLogger(getClass.getName)
  	  log.setLevel(Level.ERROR)
  	  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  	  val jl = sqlContext.read.json(args(0))
  	  val appid = jl.filter(jl("`App ID`").isNotNull).limit(1).select("`App ID`").first().get(0)
   	  // jl.printSchema()
    	println("#########################################")
  	  println("Log for " + appid)
  	  // jl.write.json(proj + "/out/jl.json")
  	  jl.registerTempTable("jltab")
  	  
  	  println("Types of Event")
  	  val eventTypes = sqlContext.sql("SELECT DISTINCT Event FROM jltab")
  	  eventTypes.show(false)

  	  val executor_adds = sqlContext.sql("SELECT `Executor Info`.Host, Timestamp FROM jltab " +
  	                                     "WHERE Event = 'SparkListenerExecutorAdded'")
  	  // executor_adds.printSchema()
  	  // executor_adds.foreach(println) ...prints output with square brackets, use below instead
  	  executor_adds.show(false) // false means do not truncate column width
  	  executor_adds.foreach(x => println(x.get(0) + ", @" + x.get(1)))

  	  // jl.rdd.saveAsTextFile(proj + "/out/A" )
  	  /**
  	   * requires "--packages com.databricks:spark-csv_2.10:1.2.0" on the spark-submit line
  	   */
  	  // jl.write.format("com.databricks.spark.csv").save(proj + "/out/ACSV")
  	  println("#########################################")
  	}
}