package metricUtils

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import scala.util.Properties
import sys.process._
import java.io.File
import java.io.PrintWriter
import java.net.InetAddress.getLocalHost
import scala.io.Source

/**
 * Setup cluster metrics to report to CSV files on the driver and cluster nodes.
 * Sadly we can't write direct to HDFS but a bash script "sparkcsvd" on cluster manager
 * can be run to move later.
 * 
 * To use this Scala object, use code like this in the calling block:
 * 
 *     val conf = clusterMetricsConfig().setAppName("John's Evil Experiments")
 *     val sc = new SparkContext(conf) 
*/

object ClusterMetricSetup {
  
  def clusterMetricsConfig () = {
    
      val conf = new SparkConf()
      val log = Logger.getLogger(getClass.getName)
      /**
       * derive the Spark user the hard way as we don't have a Spark context yet
       */
      val OSUser = System.getProperty("user.name")
      log.info("### I am OSuser " + OSUser + " on " + getLocalHost.getHostName)
      val sparkUserFromOS = if (OSUser.contentEquals("yarn") || OSUser.contentEquals("nobody")) {
          scala.util.Properties.envOrElse("SPARK_USER",System.getProperty("user.name"))
      } else {
          OSUser
      }
      log.info("### I am Spark user " + sparkUserFromOS)
      /**
       * Define the directory to be the CSV file metrics sink
       *  
       *  We have to assume that this directory already exists on EVERY cluster node as at the moment, 
       *  it is only the driver program context which may be running on the client or a single cluster node.
       *  Ensure that the directory is owned by "nobody:nobody" (if that is the executor's effective userid).
       *  Create the metrics directory at least on the driver node but make sure permissions are wide open.
      */
      val metricsRoot = "/tmp/spark-metrics/"
      val metricsDir = metricsRoot + sparkUserFromOS
      log.info("### CSV metrics will be written to " + metricsDir + "(if precreated on cluster 777)")
      try {
        val metricsRootFile = new File(metricsRoot)
        if (!metricsRootFile.exists) {
           metricsRootFile.mkdirs()  
           "chmod -R 777 " + metricsRoot !!
        }
        
        val metricsDirFile = new File(metricsDir)
        if (!metricsDirFile.exists) metricsDirFile.mkdirs()
        val toeTest = File.createTempFile(sparkUserFromOS, ".tmp", new File(metricsDir)).deleteOnExit()
      }
      catch {
        case _: Throwable => {
            log.warn("### Driver cannot write CSV metrics to " + metricsDir)
        }
      }
      /**
       * setup the conf for the SparkContext
       */
      conf.set("spark.metrics.conf.*.sink.csv.class", "org.apache.spark.metrics.sink.CsvSink")
          .set("spark.metrics.conf.*.sink.csv.period","1")
          .set("spark.metrics.conf.*.sink.csv.unit","seconds")
          .set("spark.metrics.conf.*.sink.csv.directory", metricsDir)
          .set("spark.metrics.conf.worker.sink.csv.period","1")
          .set("spark.metrics.conf.worker.sink.csv.unit","seconds")
          .set("spark.metrics.conf.master.source.jvm.class","org.apache.spark.metrics.source.JvmSource")
          .set("spark.metrics.conf.worker.source.jvm.class","org.apache.spark.metrics.source.JvmSource")
          .set("spark.metrics.conf.driver.source.jvm.class","org.apache.spark.metrics.source.JvmSource")
          .set("spark.metrics.conf.executor.source.jvm.class","org.apache.spark.metrics.source.JvmSource")
          .set("spark.app.id", sparkUserFromOS + "_" + System.currentTimeMillis / 1000)
          .set("sparkUserFromOS",sparkUserFromOS)
          /**
           * this doesn't seem to work - nothing extra showing on Spark History UI
           * 
          */                   
          .set("spark.metrics.conf.*.sink.servlet.class", "org.apache.spark.metrics.sink.MetricsServlet")
          .set("spark.metrics.conf.*.sink.servlet.path", "/metrics/json")
          /**
           *
           * write metrics to console log
           * 
           .set("spark.metrics.conf.*.sink.console.class","org.apache.spark.metrics.sink.ConsoleSink")
           .set("spark.metrics.conf.*.sink.console.period","10")
           .set("spark.metrics.conf.*.sink.console.unit","seconds")
           * 
           */
     conf
  }   

  def csvFiles(dir: File) : List[File] = { 
     // ref: http://alvinalexander.com/scala/how-to-list-files-in-directory-filter-names-scala
     dir.listFiles.filter(_.isFile).toList.filter { _.getName.endsWith(".csv") }
  }
  
  def clusterMetricPostProcessing (sc :SparkContext) {
     /**
      * Post processing (if necessary) to load client side driver metrics to HDFS
      */
    val log = Logger.getLogger(getClass.getName)
    val sparkMaster = sc.master
    val sparkUser = sc.sparkUser
    val appId = sc.applicationId
    val metricsDir =  sc.getConf.get("spark.metrics.conf.*.sink.csv.directory")
    log.info("master was " + sparkMaster + ", spark user was " + sparkUser)
    if (sparkMaster.contentEquals("yarn-client") && (!sparkUser.contentEquals("nobody")) 
                                                 && (!sparkUser.contentEquals("yarn"))
                                                 && (metricsDir.length() > 0)) 
    {
       sc.stop() // has to be done when sparkContext is closed as CSV files may still be writing
       /**
        * Collate all CSV files into one main file for the application such that each line identifies
        * the node, executor, metric, user, timestamp, value
        */
       val host = getLocalHost.getHostName.split('.')(0)
       val metricsDirFile = new File(metricsDir)
       val masterTMP = metricsDir + "/" + appId + ".tmp"
       val masterCSV = metricsDir + "/" + appId + ".csv"
       val masterTMPFile = new PrintWriter(masterTMP)
       csvFiles(metricsDirFile).foreach(currentFile => {
         val bits = currentFile.toString().split('/').last.split("\\.")
         if (bits(0) == appId && !bits.mkString.contains("DAGScheduler")) {
            val executor = bits(1)
            val metric = bits.slice(2,bits.length-1).mkString("_")
            val csvfile = Source.fromFile(currentFile)
            val lineIter = csvfile.getLines
            val header = lineIter.next()
            lineIter.foreach(m => masterTMPFile.println(host+","+executor+","+metric+","+sparkUser+","+m))
            csvfile.close()
            new File(currentFile.toString()).delete()
         }
       })
       masterTMPFile.close()
       new File(masterTMP).renameTo(new File(masterCSV))
       log.info("Collated CSV metrics into " + masterCSV)
       /**
        * Use a temporary shell script to run the Hadoop commands to load into HDFS
        */
       val hdfsMetricsDir = "/user/" + sparkUser + "/spark-metrics"
       val moveLog = metricsDir + "/move-to-HDFS.log"
       val baseCSV = masterCSV.split('/').last
       log.info("attempting to move client side CSV metrics into hdfs://" + hdfsMetricsDir + "/" + baseCSV)
       log.info("check " + moveLog + " for any errors")
       val tempScript = File.createTempFile(sparkUser, ".sh", metricsDirFile)
       val pw = new PrintWriter(tempScript)
       tempScript.deleteOnExit()
       val hadoopMkdirCmd  = "hadoop fs -mkdir -p " + hdfsMetricsDir
       val hadoopAppendCmd = "hadoop fs -appendToFile " + masterCSV + " " + 
                              hdfsMetricsDir + "/" + baseCSV
       val hadoopLsCmd = "hadoop fs -ls " + hdfsMetricsDir + "/" + baseCSV
       pw.println("exec >" + moveLog + " 2>&1")
       pw.println("ls -l " + masterCSV)
       pw.println("echo " + hadoopMkdirCmd)
       pw.println(hadoopMkdirCmd)
       pw.println("echo " + hadoopAppendCmd)
       pw.println(hadoopAppendCmd)
       pw.println("[ $? -eq 0 ] && /bin/rm " + masterCSV)
       pw.println("echo " + hadoopLsCmd)
       pw.println(hadoopLsCmd)
       pw.println("chmod 777 " + moveLog)
       pw.println("rm -f " + metricsDir + "/*DAGScheduler*")
       pw.close()
       val runMove = Process("bash " + tempScript)
       runMove !
    }
  }
}