package metricUtils
/**
 * Ref: https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/exercises/spark-exercise-custom-scheduler-listener.html
 * https://spark.apache.org/docs/1.5.0/api/java/org/apache/spark/scheduler/SparkListener.html
 * 
 * Ref: https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.scheduler.SparkListener
 */
// import org.apache.spark.scheduler.{SparkListenerStageCompleted, SparkListener, SparkListenerJobStart}

import org.apache.spark.scheduler._
import org.apache.log4j.Logger
import java.io.File
import java.io.PrintWriter
import java.net.InetAddress.getLocalHost
import sys.process._
import scala.io.Source

class ExtraSparkListener(applicationId :String, sparkUser :String, log :Logger) extends SparkListener {
 
  /**
   * Constructor section immediately follow the class header before its methods are defined
   */
  val host = getLocalHost.getHostName.split('.')(0)
  
  /**
   *  Creates a temp CSV file on the driver node to record events and metrics.
   *  Although this could have been done in a cleaner way with an RDD/Dataframe and saved to HDFS later,
   *  this has less impact on the running application in terms of multi-node operations
   *  and can be monitored from the OS.
   *  
   *  Note this is a "dodgy" CSV file for which I can only apologise. 
   *  - first column is either "event" or "metric"
   *  - second column is Epoch timestamp to 1 second
   *  - third column is the listener event such as "TASK-END"
   *  - subsequent columns are in key/value pairs e.g. "executor", 1
   *  
   *  This means that there is a variable number of columns per line.
   */
    
  val metricsDir = "/tmp/spark-metrics/" + sparkUser
  val tmpDirFile = new File(metricsDir)
  tmpDirFile.mkdirs()
  val tempListenerFile = File.createTempFile(applicationId, ".lsnr", tmpDirFile)
  val lsnrCSV = new PrintWriter(tempListenerFile)  
  tempListenerFile.deleteOnExit()
   
  log.info("~ Opened bespoke listener output " + tempListenerFile.getPath + " on " + host)
  
  /**
   * end of constructor
   */
  
  override def onApplicationStart(applicationStart: SparkListenerApplicationStart) {
    log.info("~ Application started for Spark user " + applicationStart.sparkUser + " " + applicationStart.time)
    lsnrCSV.println("event," + applicationStart.time/1000 + ",APPLICATION-START")
  }
  
  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded) {
    lsnrCSV.println("event," + System.currentTimeMillis/1000 + ",BLOCK-MANAGER-ADDED," +
                    "host," + blockManagerAdded.blockManagerId.host + "," +
                    "executor," + blockManagerAdded.blockManagerId.executorId
                   )
  }
  
  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved) {
    // unexciting
  }
     
  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated) {
    val stamp = System.currentTimeMillis/1000
    if (!blockUpdated.blockUpdatedInfo.blockId.isBroadcast && (blockUpdated.blockUpdatedInfo.blockId.name.startsWith("broadcast_"))) {
        lsnrCSV.println("event," + stamp + ",BLOCK-UDPATED," +
                        "node," + blockUpdated.blockUpdatedInfo.blockManagerId.host + "," +
                        "name," + blockUpdated.blockUpdatedInfo.blockId.name)
                        
        lsnrCSV.println("metric," + stamp + ",BLOCK-UDPATED," +
                        "blockId," + blockUpdated.blockUpdatedInfo.blockId.name + "," +
                        "isRDD," + blockUpdated.blockUpdatedInfo.blockId.isRDD + "," + 
                        "isShuffle," + blockUpdated.blockUpdatedInfo.blockId.isShuffle + "," +
                        "host," + blockUpdated.blockUpdatedInfo.blockManagerId.host + "," +
                        "diskSize," +  blockUpdated.blockUpdatedInfo.diskSize + "," + 
                        "externalBlockStoreSize," + blockUpdated.blockUpdatedInfo.externalBlockStoreSize + "," + 
                        "memSize," + blockUpdated.blockUpdatedInfo.memSize + "," +
                        "storageLevel," + blockUpdated.blockUpdatedInfo.storageLevel.description
                       )
    }
  }
  
  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate) {
    // unexciting
  }
  
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded) {
     val stamp = System.currentTimeMillis/1000
     lsnrCSV.println("event," + stamp + ",EXECUTOR-ADDED," +
                     "node," + executorAdded.executorInfo.executorHost + "," +
                     "executorId," + executorAdded.executorId
                    )
  }
  
  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate) {
     // this looked exciting but the Eclipse IDE doesn't seem to present any access to actual metrics :(
     // taskEnd is more fruitful
  }
  
  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved) {
    // unexciting
  }
  
  override def onJobStart(jobStart: SparkListenerJobStart) {
    val stamp = jobStart.time/1000
    lsnrCSV.println("event," + stamp + ",JOB-START," + 
                    "jobId," + jobStart.jobId + "," +
                    "stageIds," + jobStart.stageIds.mkString(":")
                    )
  }
  
  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
     val stamp = System.currentTimeMillis/1000
     lsnrCSV.println("event," + stamp + ",JOB-END," +
                     "jobId," + jobEnd.jobId + "," +
                     "jobResult," + jobEnd.jobResult
                    )
  }
   
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) {
        val stamp = stageSubmitted.stageInfo.submissionTime.getOrElse(System.currentTimeMillis) / 1000
        lsnrCSV.println("event," + stamp + ",STAGE-SUBMITTED," +
                        "stageId," + stageSubmitted.stageInfo.stageId + "," +
                        "name,\"" + stageSubmitted.stageInfo.name + "\"," +
                        "parentStageIds," + stageSubmitted.stageInfo.parentIds.mkString(":") + "," +
                        "numTasks," + stageSubmitted.stageInfo.numTasks
                       )
  }
  
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    val stamp = stageCompleted.stageInfo.completionTime.getOrElse(System.currentTimeMillis) / 1000
    lsnrCSV.println("event," + stamp + ",STAGE-COMPLETED," + 
                    "stageId," + stageCompleted.stageInfo.stageId + "," +
                    "reason," + stageCompleted.stageInfo.failureReason.getOrElse("succeeded")
                   )
  }
  
  override def onTaskStart(taskStart: SparkListenerTaskStart) {
    val stamp = taskStart.taskInfo.launchTime /1000
    lsnrCSV.println("event," + stamp + ",TASK-START," + 
                    "node," + taskStart.taskInfo.host.split('.')(0) + "," +
                    "taskId," + taskStart.taskInfo.taskId + "," +
                    "stageId," + taskStart.stageId + "," +
                    "executor," + taskStart.taskInfo.executorId + "," +
                    "taskLocality," + taskStart.taskInfo.taskLocality
                   )
  }
  
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
     val stamp = taskEnd.taskInfo.finishTime/1000
     lsnrCSV.println("event," + stamp + ",TASK-END," + 
                     "node," + taskEnd.taskInfo.host.split('.')(0) + "," +
                     "taskId," + taskEnd.taskInfo.taskId + "," +
                     "taskType," + taskEnd.taskType + "," +
                     "stageId," + taskEnd.stageId + "," +
                     "executor," + taskEnd.taskInfo.executorId + "," +
                     {if (taskEnd.taskInfo.failed) "failed" else "succeeded"} + "," + 
                     "reason," + taskEnd.reason
                    )
       
     /**
      * Rubbish code. Note to self, learn proper Scala...this is all better done working with "options" I expect
      */
     val bytesWritten = { try { taskEnd.taskMetrics.outputMetrics.get.bytesWritten } catch { case _: Throwable => 0 }}
     val recordsWritten = { try { taskEnd.taskMetrics.outputMetrics.get.recordsWritten } catch { case _: Throwable => 0 }}
     val shuffleBytesWritten = { try { taskEnd.taskMetrics.shuffleWriteMetrics.get.shuffleBytesWritten } catch { case _: Throwable => 0 }}
     val shuffleRecordsWritten = { try { taskEnd.taskMetrics.shuffleWriteMetrics.get.shuffleRecordsWritten } catch { case _: Throwable => 0 }}
     val shuffleWriteTime = { try { taskEnd.taskMetrics.shuffleWriteMetrics.get.shuffleWriteTime } catch { case _: Throwable => 0 }}
     val shuffleReadLocalBlocksFetched = { try { taskEnd.taskMetrics.shuffleReadMetrics.get.localBlocksFetched } catch { case _: Throwable => 0 }}
     val shuffleReadLocalBytesRead = { try { taskEnd.taskMetrics.shuffleReadMetrics.get.localBytesRead } catch { case _: Throwable => 0 }}
     val shuffleReadRecordsRead = { try { taskEnd.taskMetrics.shuffleReadMetrics.get.recordsRead } catch { case _: Throwable => 0 }}
     val shuffleReadRemoteBlocksFetched = { try { taskEnd.taskMetrics.shuffleReadMetrics.get.remoteBlocksFetched } catch { case _: Throwable => 0 }}
     val shuffleReadRemoteBytesRead = { try { taskEnd.taskMetrics.shuffleReadMetrics.get.remoteBytesRead } catch { case _: Throwable => 0 }}
     val shuffleReadTotalBlocksFetched = { try { taskEnd.taskMetrics.shuffleReadMetrics.get.totalBlocksFetched } catch { case _: Throwable => 0 }}
     val shuffleReadTotalBytesRead = { try { taskEnd.taskMetrics.shuffleReadMetrics.get.totalBytesRead } catch { case _: Throwable => 0 }}
     val memoryBytesSpilled = { try { taskEnd.taskMetrics.memoryBytesSpilled } catch { case _: Throwable => 0 }}

     /**
      * loads more metrics available than I have time to analyse :(
      * ...sticking to bytes, not blocks or records as a unit of currency
      */
     lsnrCSV.println("metric," + stamp + ",TASK-END," +
                     "node," + taskEnd.taskInfo.host.split('.')(0) + "," +
                     "taskId," + taskEnd.taskInfo.taskId + "," +
                     "bytesWritten," + bytesWritten + "," +
                     "memoryBytesSpilled," + memoryBytesSpilled + "," +
                     "shuffleBytesWritten," + shuffleBytesWritten + "," +
                     "shuffleWriteTime," + shuffleWriteTime + "," +
                     "shuffleReadLocalBytesRead," + shuffleReadLocalBytesRead + "," +
                     "shuffleReadRemoteBytesRead," + shuffleReadRemoteBytesRead + "," +
                     "shuffleReadTotalBytesRead," + shuffleReadTotalBytesRead
                    )
  }
  
  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult) {
    // unexciting
  }
    
  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD) {
    // unexciting
  }
  
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    
    /**
     * Close the temp CSV file and upload to HDFS user's sub-directory "spark-extras"
     * Note this needs to pre-exist with "chmod 777" permissions as we upload via a push
     * to shell which will be done with effective user id of the driver program, not necessarily the spark user
     * if it is run in cluster mode.
     * 
     * Further note - the "sparkcsvd" cluster daemon should eventually make this sub directory for any user code that
     * used the ClusterMetricSetup.clusterMetricsConfig method at the start
     */
    lsnrCSV.println("event," + applicationEnd.time/1000 + ",APPLICATION-END")
    lsnrCSV.close()
    log.info(" ~ Closed bespoke event logger " + tempListenerFile.getPath + " on " + host)
    val hdfsMetricsDir = "/user/" + sparkUser + "/spark-extras"
    val moveLog = metricsDir + "/move-extras-to-HDFS.log"
    val baseCSV = applicationId + ".csv"
    log.info(" ~ attempting to move extra metrics and events into hdfs://" + hdfsMetricsDir + "/" + baseCSV)
    log.info(" ~ check " + moveLog + " for any errors")
 
    val hadoopMkdirCmd  = "hadoop fs -mkdir -p " + hdfsMetricsDir //should exist anyway after a while
    val hadoopCopyCmd   = "hadoop fs -copyFromLocal " + tempListenerFile.getPath + " " + 
                           hdfsMetricsDir + "/" + baseCSV
    val hadoopLsCmd = "hadoop fs -ls " + hdfsMetricsDir + "/" + baseCSV
    
    val tempScript = File.createTempFile(sparkUser, ".sh", tmpDirFile)
    val pw = new PrintWriter(tempScript)
    tempScript.deleteOnExit()
    pw.println("exec >" + moveLog + " 2>&1")
    pw.println("user=$(id -u -n)")
    pw.println("[[ \"$user\" = \"" + sparkUser + "\" ]] && " + hadoopMkdirCmd) // if effective OS user is same as sparkUser
    pw.println("echo effective OS user is $user")
    pw.println("echo " + hadoopCopyCmd)
    pw.println(hadoopCopyCmd)
    pw.println("echo " + hadoopLsCmd)
    pw.println(hadoopLsCmd)
    pw.println("chmod 777 " + moveLog)
    pw.close()
    val runMove = Process("bash " + tempScript)
    runMove !
        
    Source.fromFile(moveLog).getLines.foreach(l => if (!l.startsWith("SLF4J") && !l.startsWith("log4j")) log.info(" ~ "+ l))

  }  
      
}