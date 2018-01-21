import metricUtils.ClusterMetricSetup._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object Test00 {
    def main(args: Array[String]) {
    val conf = clusterMetricsConfig().setAppName("John's Evil Experiments")

    val sc = new SparkContext(conf)
        
    println("### CSV metrics directory is " + conf.get("spark.metrics.conf.*.sink.csv.directory") +
            " on driver and any cluster node")
    val eventLog = System.getProperty("spark.eventLog.dir") + "/" + sc.applicationId
    
    /**
     * do something trivial
    */
    val qmulHome = scala.io.Source.fromURL("http://qmul.ac.uk")
    val qmulRDD = sc.parallelize(qmulHome.toList)
    println("### " + qmulRDD.count() + " lines in QMUL web site")
   
    /**
     * Grab some spark environment settings and then close the context
     */
    
    val spark_app_id = sc.applicationId // should be same as System.getProperty("spark.app.id")
    val spark_user_id = System.getProperty("sparkUserFromOS")
    val spark_eventlog_dir = System.getProperty("spark.eventLog.dir")
    sc.stop()
    
    println("### Stopped previous Spark Context")
    /**
     * val conf2 = new SparkConf().setAppName("John's Evil Experiments")
       val sc2 = new SparkContext(conf2)
       println("### Started new Spark Context")
       * 
       */

    /**
     * Advise on where to get JSON event logs
     */
    println("### Event log JSON is " + spark_eventlog_dir + "/" + spark_app_id)
    println("### To fetch: hadoop fs -copyToLocal " + eventLog + " " + spark_app_id + ".json")
    
    clusterMetricPostProcessing(sc)
  }
}