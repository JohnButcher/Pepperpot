package bench.msd

import Track._
import metricUtils.ClusterMetricSetup._
// import metricUtils.CustomSparkListener._
import org.apache.spark._
import org.apache.spark.SparkContext._
import scala.math._
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD.doubleRDDToDoubleRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions


//import org.apache.spark.rdd.RDD._

/**
 * Benchmarking from Million Song dataset
 */
object BasicCorrelations {

  val moon = "hdfs://moonshot-ha-nameservice"
 
  def main(args: Array[String]): Unit = {
    
    // Setup configuration for automated executor metrics and create a Spark context from it
    
    val conf = clusterMetricsConfig().setAppName("MillionSong Benchmark with metrics")
    //  .set("spark.extraListeners","org.apache.spark.scheduler.StatsReportListener")
    
    // create the Spark Context from the metrics conf
    val sc = new SparkContext(conf)
    

    // get access to the logfile
    val log = Logger.getLogger(getClass.getName)
    
    // create extra listener methods to gather more stuff
    val myListeners = new metricUtils.ExtraSparkListener(sc.applicationId, sc.sparkUser, log)
    sc.addSparkListener(myListeners)

    val appStart = System.currentTimeMillis

    // log.setLevel(Level.TRACE)
    // Logger.getRootLogger.setLevel(Level.TRACE)
    
 
    // ingest the HDFS set and repartition to the supplied second parameter
    
    val numparts = args(1).toInt
    val csvlines = if (numparts > 26) sc.textFile(moon + args(0)).repartition(numparts).setName("RDDcsvlines")
                   else sc.textFile(moon + args(0)).coalesce(numparts).setName("RDDcsvlines")

    println ("#DancingDads : csvlines RDD = " + csvlines.name)
    
    // map to track features
    
    val tracks  = csvlines.map(track => Track.createTrack(track))
    tracks.setName("RDDtracks")
    println ("#DancingDads : tracks RDD = " + tracks.name)
    val time1 = System.currentTimeMillis
    val total_tracks = tracks.count()
    println ("#DancingDads : mapped features of " + total_tracks + " tracks from " + moon + args(0))
    
    // filter non-zero song hotness and tempos

    val id_hot_pairs = tracks.filter(to => to != None).map(t => (t.get.track7Id,t.get.songHotness)).filter(hp => hp._2 != 0).persist().setName("RDDhotpairs")
    println ("#DancingDads : hotness pairs RDD = " + id_hot_pairs.id)
    println ("#DancingDads : " + id_hot_pairs.partitions.size + " partitions for hotness key/value pairs")
    // val id_hot_pairs = tracks.filter(to => to != None).map(t => (t.get.track7Id,t.get.songHotness)).filter(hp => hp._2 != 0).
                       // partitionBy(new HashPartitioner(8)).persist()
    println ("#DancingDads : " + id_hot_pairs.count() + " songs have a non-zero hotness")
    val id_tempo_pairs = tracks.filter(to => to != None).map(t => (t.get.track7Id,t.get.tempo)).filter(tp => tp._2 != 0).persist().setName("RDDtempopairs")
    println ("#DancingDads : tempo pairs RDD = " + id_tempo_pairs.id)
    println ("#DancingDads : " + id_tempo_pairs.partitions.size + " partitions for tempo key/value pairs")
    //val id_tempo_pairs = tracks.filter(to => to != None).map(t => (t.get.track7Id,t.get.tempo)).filter(tp => tp._2 != 0).
                       // partitionBy(new HashPartitioner(8)).persist()

    println ("#DancingDads : " + id_tempo_pairs.count() + " songs have a non-zero tempo")
    
    // join by track7ID key to get tuples of all non-zero hotness and tempo (id,(hotness,tempo))
    
    val valid_tracks = id_hot_pairs.join(id_tempo_pairs).setName("RDDvalid_tracks")
    println ("#DancingDads : valid tracks RDD = " + valid_tracks.id)
    val n = valid_tracks.groupByKey().count()
    println ("#DancingDads : resulting join on track7Id key gives " + n + " to play with")
    
    /**
    Clunky Spearman correlation
    - let's call song hotness "x" and tempo "y"
    */
    
    // get tuples of (X,Y,X*Y,X^2,Y^2)
    //                                     X        Y         X*Y                   X^2             Y^2      
    val xy = valid_tracks.map(vt => (vt._2._1,vt._2._2,vt._2._1*vt._2._2,pow(vt._2._1,2),pow(vt._2._2,2))).setName("RDDxy")
    println ("#DancingDads : xy RDD = " + xy.id)
    
    // keep in memory while computing several different values
    xy.persist()
    val sum_x = xy.map(s => s._1).sum()
    val sum_y = xy.map(s => s._2).sum()
    val sum_xy = xy.map(s => s._3).sum()
    val sum_xsqrd = xy.map(s => s._4).sum()
    val sum_ysqrd = xy.map(s => s._5).sum()
    xy.unpersist()
    
    val numerator = (n*sum_xy) - ((sum_x)*(sum_y))
    val denominator = sqrt((n*sum_xsqrd) - pow(sum_x,2)) *
                      sqrt((n*sum_ysqrd) - pow(sum_y,2))
    val corr = numerator / denominator
    val time_taken = System.currentTimeMillis - time1
    println ("#DancingDads : Spearman correlation of Song Hotness to Temp is " + corr + " in " + time_taken + " milliseconds")
    if (corr >= 0.5 ) println ("#DancingDads : ...which implies a reasonable, positive correlation")
    else if (corr <= -0.5) println ("#DancingDads : ...which implies a reasonable, negative correlation")
    else if ((abs(corr) < 0.5) & (abs(corr) >= 0.1)) println ("#DancingDads : ...which implies a weak correlation")
    else println ("#DancingDads : ...which implies almost no correlation [but that does not mean independence]")
    
    val appEnd = System.currentTimeMillis
    var execs:String = "?"
    var cores:String = "?"
    var mem:String = "?"
    try { execs = sc.getConf.get("spark.executor.instances") } catch {
      case _: Throwable => {
            log.warn("### Could not get number of executors from spark.executor.instances")
      }}
    try { cores = sc.getConf.get("spark.executor.cores") } catch {
      case _: Throwable => {
            log.warn("### Could not get number of executor cores from spark.executor.cores")
      }}
    try { mem = sc.getConf.get("spark.executor.memory") } catch {
      case _: Throwable => {
            log.warn("### Could not get quantity of RAM from spark.executor.memory")
      }}
       
    println ("##Application elapsed time = " + (appEnd - appStart)/1000 + " seconds, using " +
             numparts + " partitions," + execs + " executors (RAM " + mem + "), and " + cores + " cores # " + sc.applicationId)
    
    clusterMetricPostProcessing(sc, true)
  }
}
