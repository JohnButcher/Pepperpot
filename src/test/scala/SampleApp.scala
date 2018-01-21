package ppt

/* SampleApp.scala:
   This application simply counts the number of lines that contain "Processing" from README.md
 */
import metricUtils.ClusterMetricSetup._
// import metricUtils.ExtraSparkListener._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Logger

object SampleApp {
  
  def main(args: Array[String]) {
    val moon = "hdfs://moonshot-ha-nameservice"
    val conf = clusterMetricsConfig().setAppName("John's Evil Experiments")
              .set("spark.extraListeners","org.apache.spark.scheduler.StatsReportListener")
              // .set("spark.extraListeners","CustomSparkListener")
    val sc = new SparkContext(conf)

    // get access to the logfile
    val log = Logger.getLogger(getClass.getName)
    // create extra listener methods to gather more stuff
    val myListeners = new metricUtils.ExtraSparkListener(sc.applicationId, sc.sparkUser, log)
    sc.addSparkListener(myListeners)
    val sparkUserFromOS = scala.util.Properties.envOrElse("SPARK_USER",System.getProperty("user.name"))
    
    println("CSV directory is " + conf.get("spark.metrics.conf.*.sink.csv.directory"))
    val eventLog = if (!sc.isLocal) System.getProperty("spark.eventLog.dir") + "/" + sc.applicationId
    else "(no event log in local mode"
    val txtFile = if (sc.isLocal) System.getProperty("user.home") + "/pp/README.md"
    else moon + "/user/" + sparkUserFromOS + "/pp/README.md"
    val txtFileLines = sc.textFile(txtFile , 2).cache() // for no particular reason
    val searchstring = "Processing"
    val numAs = txtFileLines .filter(line => line.contains(searchstring)).count()
    println("#################### Lines with " + searchstring + ": %s ##################".format(numAs))
    if (!sc.isLocal) {
        println("#################### Event log is " + System.getProperty("spark.eventLog.dir") + "/" + sc.applicationId)
        println("####################")
	      println("#################### To fetch: hadoop fs -copyToLocal " + eventLog + " " + sc.applicationId + ".json")
        println("####################")
        println("#################### (scrunched JSON format)")
   }
   clusterMetricPostProcessing(sc)
  }
}
